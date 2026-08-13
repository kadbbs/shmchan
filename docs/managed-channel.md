# managed_byte_channel 架构与故障语义

`managed_byte_channel` 是 `shmchan` 的生产增强层。它仍是本机、非持久化 IPC，但补上了参与者登记、
generation 隔离、崩溃检测、整体重建、ACK/重投、协议校验、指标和零拷贝接口。

它解决的是“某个进程死在共享内存操作中，其他进程不能继续把已损坏的队列当成正常队列使用”的问题。
它不把共享内存变成磁盘消息队列，也不承诺 exactly-once。

## 两层共享内存

一个逻辑 Channel 使用一个稳定的控制面和一个按 generation 切换的数据面：

```text
进程 A ─┐
进程 B ─┼── mmap ── /orders.shmchan.control
监督者 ─┘              │
                       ├─ state / reason / current generation
                       ├─ protocol id + version
                       ├─ participant slots + heartbeat
                       ├─ counters + futex event epoch
                       │
                       └─ generation = 7
                                  │
                                  ▼
                         /orders.shmchan.g0000000000000007
                                  ├─ robust process-shared mutex
                                  ├─ message descriptors
                                  └─ fixed-size payload slots
```

- 控制面名称不变，负责发现当前 generation、进程存活和运行状态。
- 每次恢复都创建全新的数据面对象；旧映射仍可被持有它的进程安全释放，但不能再向新 generation 提交。
- 数据面使用进程共享的 robust mutex 保护元数据。进程持锁死亡时，下一位访问者得到 owner-death，立即把
  当前 generation 标记为 `broken`，不会尝试猜测半完成操作是否安全。
- 等待者睡在控制面的 futex epoch 上；发送、ACK、NACK、关闭、损坏和重建都会唤醒相关等待者。

## 参与者槽位与 fencing

每次 `create/open` 会占用一个参与者槽位。通常一个进程长期持有一个 Channel 句柄，因此可以把它理解为
“进程槽位”；同一进程打开多个独立句柄时会占用多个槽位。

`managed_byte_channel` 内含心跳线程，不应把已经打开的句柄跨 `fork()` 继承使用。应先 fork/exec，再在
各子进程中分别 `open()`；线程间可以共享同一个句柄。

槽位记录：

- PID 和随机 session ID；session ID 用于避免 PID 重用造成误认。
- 角色：producer、consumer、both 或 supervisor。
- 登记时间和最后心跳时间。
- 该参与者最后观察到的 generation。

角色用于诊断和拓扑观察，不是权限控制；库不会据此阻止 API 调用。访问控制依赖 POSIX 共享内存权限和
应用自己的进程身份管理。

后台心跳线程定期更新时间。超过 `participant_timeout` 后，监督者以 CAS 把槽位从 active 改为 stale，
并将 Channel 标记为 `broken`。重建时 stale 槽位会被回收。

被判定 stale 的旧进程即使后来恢复运行，也会因为 session 不再匹配而得到
`participant_expired`；旧预留或旧 delivery 则会得到 `generation_changed`。这就是 fencing：旧参与者无法
关闭、污染或向新 generation 提交数据。

## 消息状态与 ACK

每条消息按下面的状态机推进：

```text
FREE ── reserve ──> WRITING ── commit ──> READY ── receive ──> INFLIGHT
  ▲                                           ▲                    │
  │                                           └────── NACK ────────┤
  │                                                                │
  └──────────────────────── ACK（且零拷贝读者归零）────────────────┘
                                                │
                                                └─ ACK 超时后再次投递
```

- `receive` 只表示投递，不删除消息。
- `ack()` 才最终释放槽位。
- `nack()` 立即把消息放回 ready 状态。
- delivery 未调用 ACK/NACK 就析构时，消息保持 inflight，并在 ACK 租约到期后重投。
- 超过 `acknowledgment_timeout` 的 inflight 消息可以再次投递，`attempt()` 会递增。
- 同一条消息可能同时被旧消费者和重投消费者处理，所以语义是 **at-least-once**。
- `message_id` 在当前 generation 的未完成消息中做重复检测；ACK 后和跨 generation 的永久去重必须由应用
  或数据库完成。

消费者应先完成业务事务，再 ACK。常见做法是在同一数据库事务中写业务结果和已处理 `message_id`，以便
重复投递时幂等返回。

## 崩溃恢复与上游重放

恢复采用保守的“整代作废”协议：

1. 心跳超时、写预留超时、robust mutex owner-death 或显式调用把当前状态改为 `broken`。
2. 所有正常 send/receive 立即停止，等待者被唤醒并得到 `broken`。
3. 一个监督者调用 `rebuild()` 或 `rebuild_with_replay()`。
4. 控制对象上的 `flock` 保证同一时刻只有一个重建者。
5. 创建 `generation + 1` 的全新数据面，回收 stale 槽位，发布新 generation 和 `replaying` 状态，并
   unlink 旧数据面名称；已有旧映射只活到其本地引用释放。
6. `rebuild_with_replay` 回调从应用自己的 durable outbox/WAL 重发未确认消息；此时只允许重建者发送，
   消费者仍可接收和 ACK，以便有界 Channel 流式回放，其他生产者得到 `recovery_in_progress`。
7. 回放成功后发布 `healthy`。

共享内存中的旧消息不会自动复制到新 generation，因为进程可能死在修改元数据或负载的任意指令之间，
库无法可靠判断半写消息。需要恢复的消息必须先存在共享内存之外的可靠来源。replay 回调抛出异常时，
新 generation 会再次标记为 `broken`，并记录 `replay_failures`，不会假装恢复成功。回放期间消费者可能
已经处理部分消息，因此下一轮重放仍需依靠 `message_id` 幂等；这也是 at-least-once 语义的一部分。

监督者本身在创建阶段崩溃时，文件锁会由内核释放，下一位监督者会清理未完成的
`building_generation` 后继续。若回放进程死亡，其参与者心跳会超时并令 `replaying` generation
重新变为 `broken`。

## 协议版本

库布局版本用于校验共享结构、偏移、大小、`pthread_mutex_t` ABI 和 generation 数据面。应用还必须给每种
wire format 指定稳定的 `protocol_descriptor`：

```cpp
constexpr shmchan::protocol_descriptor document_protocol{
    shmchan::protocol_id("com.example.document"),
    3,
};
```

创建端和打开端的 protocol ID/version 必须完全相同。否则 `open` 抛出
`managed_channel_error`，其 `code()` 为 `protocol_mismatch`。升级不兼容格式时应提升版本，并安排停止、
清理或迁移旧 Channel，不能让不同版本同时解释同一负载。

## 零拷贝和性能边界

发送端可用 `reserve()` 直接写共享内存，再用 `commit(size)` 发布；接收端可用
`receive_loaned()` 直接读取映射，再 ACK/NACK。对应 span 只在 reservation/delivery 完成前有效。
payload 槽位复用时不会为了清零而额外遍历；调用方只能 `commit()` 已经完整初始化的前 N 个字节，不能把
未写入区域计入长度。

```cpp
auto reservation = channel.reserve(message_id);
encode_into(reservation->buffer());
reservation->commit(encoded_size);

auto delivery = channel.receive_loaned();
consume(delivery->bytes());
delivery->ack();
```

为保证 owner-death 可检测，managed 数据面的元数据操作通过一个很短的 robust mutex 串行化；负载编码和
零拷贝读取发生在锁外。它优先保证故障边界清晰，不等同于基础 `byte_channel` 的 lock-free 快路径。

每个槽位为最大消息预留空间，数据面大致占用：

```text
header + message_capacity × (64 字节描述符 + max_message_size 按 64 字节对齐)
```

如果最重要的是极致吞吐、消息大小跨度大，并且能接受任一参与进程崩溃后由外部整体删除队列，可继续使用
更紧凑的 `byte_channel`。如果必须识别崩溃、阻止旧进程写入并执行可审计恢复，应使用
`managed_byte_channel`。

## 可观测指标

`stats()` 返回当前状态和累计计数，包括：

- state、break reason、generation、replay owner session、失败 participant session。
- free/writing/ready/inflight 数量和最老消息年龄。
- active/stale participants、等待中的 sender/receiver。
- sent/delivered/acknowledged/redelivered/NACK 消息与字节数。
- reservation 取消、send/receive 超时、broken/rebuilt generation、replay failure。

`participants()` 返回各槽位的 PID、session、角色、心跳年龄和 observed generation，可用于状态页或日志。
指标是共享原子的运行快照，不是跨字段事务快照。

建议至少告警：`state != healthy`、`stale_participants > 0`、`oldest_message_age` 持续增长、重投率升高、
超时增加和 generation 频繁重建。

## 九项增强对应关系

1. generation：控制面指向独立的版本化数据面。
2. 参与者登记：PID + session + heartbeat + observed generation。
3. 监督检测：后台监控和显式 `supervise_once()`。
4. 异常重建：broken 后由文件锁选出唯一重建者，整代切换。
5. 上游重放：`rebuild_with_replay()` 对接 durable outbox/WAL。
6. 故障注入：测试实际覆盖 reservation 中 `SIGKILL`、回放中 `SIGKILL`、持 robust mutex 时
   `SIGKILL`、持锁 `SIGSTOP` 超时，以及旧参与者恢复后的 fencing。
7. 协议与指标：应用协议 ID/version、布局 ABI 校验、stats/participants。
8. ACK 恢复：ACK/NACK、租约超时重投和 attempt 计数。
9. 零拷贝与性能：发送 reservation、loaned receive、批量便利接口和锁外负载处理。

## 生产使用前仍需完成的应用工作

- 让独立监督进程负责告警和重建；不要把唯一监督者和业务进程放在同一故障域。
- 使用数据库 outbox、WAL 或其他持久存储保存可重放消息。
- 消费端按 `message_id` 做幂等或去重，并明确 ACK 与业务事务的顺序。
- 根据最长 GC/调度暂停设置心跳超时，避免过小阈值误判。
- 限制共享内存权限，核算 `/dev/shm` 容量，并监控 inode/空间。
- 在目标内核、libc、编译器和真实负载上做延迟、容量、断电/重启和故障演练。
- 约定谁可以 `close`、`rebuild`、`unlink`；`unlink` 是管理操作，不应由普通 worker 随意调用。

共享内存位于内核管理的 tmpfs 中，发送进程退出后，已 commit 的消息仍在数据面对象里；它会一直存在到
被 ACK 后复用、显式 unlink，或系统重启。它不是磁盘持久化，机器故障和重启后的恢复仍依赖上游存储。
