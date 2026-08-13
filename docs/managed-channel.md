# `managed_byte_channel` 架构与崩溃语义

`managed_byte_channel` 是本机、非持久化的不定长 MPMC Channel。它的恢复目标不是把消息变成磁盘数据，
而是保证一个进程死在 `send()` 或 `receive()` 中间时：

- 未写完整的消息不会被消费者看到。
- 已经完整发布的消息不会因为其他进程死亡而丢失。
- 消费者没有完整复制消息时，共享内存槽位不会删除。
- 其他生产者和消费者无需重建 Channel，可以继续通信。

## 单共享内存对象

每个逻辑 Channel 对应一个 POSIX 共享内存对象：

```text
生产者 P1 ─┐
生产者 P2 ─┤
消费者 C1 ─┼── shm_open + mmap ── /orders.shmchan.managed
消费者 C2 ─┘                           │
                                      ├─ 布局和协议版本
                                      ├─ Channel 状态和指标
                                      ├─ process-shared robust mutex
                                      ├─ 消息描述符数组
                                      └─ payload 槽位数组
```

所有进程的虚拟地址可以不同，但 `MAP_SHARED` 映射指向同一批内核页面。发送进程正常退出后，已经发布的
消息仍在共享内存中，直到被消费者取走、显式 `unlink()` 或机器重启。

与旧的两层 generation 设计不同，普通进程崩溃不会创建新共享内存对象，也不会清空整个 Channel。

## 存储布局

创建参数：

```cpp
shmchan::managed_channel_options options;
options.message_capacity = 1024;
options.max_message_size = 64 * 1024;
```

映射布局：

```text
┌──────────────────────────────────────┐
│ managed_shared_header                │
│  magic / layout version / protocol   │
│  state / reason / futex event epoch  │
│  robust pthread mutex                │
│  sequence / slot counts / metrics    │
├──────────────────────────────────────┤
│ descriptor[capacity]                 │
│  state / payload_size / sequence     │
├──────────────────────────────────────┤
│ payload[capacity][payload_stride]     │
└──────────────────────────────────────┘
```

消息的实际长度由 `payload_size` 保存；每个槽位仍然预留 `max_message_size` 字节。这样避免了共享内存中的
动态分配器、跨进程指针和内存碎片。

## 槽位状态机

```text
FREE ────────────────> WRITING ────────────────> READY
  ▲                      │                         │
  │                      │ owner death             │ receive 完整复制
  │                      ▼                         │
  └──────────────── 自动恢复                       ┘
```

- `FREE`：可供生产者使用。
- `WRITING`：生产者正在复制，消费者不可见。
- `READY`：描述符和 payload 已完整，可以接收。

没有长期存在的 `INFLIGHT` 状态，也没有 ACK/NACK。`receive()` 成功即表示完整消息已经进入消费者的
进程私有内存，并从共享 Channel 中取走。

## 发送事务

一次 `send()` 的顺序是：

1. 获取 process-shared robust mutex。
2. 检查 Channel 状态和槽位计数。
3. 找到 `FREE` 槽位。
4. 将状态改为 `WRITING`。
5. 填充 sequence 和描述符。
6. 复制全部 payload。
7. 写入最终 `payload_size`。
8. 最后将状态改为 `READY`。
9. 释放 mutex，递增 futex event epoch 并唤醒接收者。

`READY` 是消息的发布点。在它之前崩溃，消息不可见；在它之后崩溃，消息已完整。

```text
进程死亡位置                  下一位访问者的处理
────────────────────────────────────────────────
获取槽位之前                  无需处理
WRITING / payload 复制中       WRITING → FREE
READY 发布之后                保留 READY 消息
释放锁之后                    无需处理
```

## 接收事务

一次 `receive()` 的顺序是：

1. 获取 robust mutex。
2. 找到 sequence 最小的 `READY` 槽位。
3. 在进程私有内存中创建 `std::vector<std::byte>`。
4. 将完整 payload 复制到该 vector。
5. 复制完成后才把共享槽位改为 `FREE`。
6. 释放 mutex，唤醒可能等待空槽位的生产者。
7. 将本地 `std::vector<std::byte>` 返回给调用方。

在第 3～4 步死亡时，共享槽位仍是 `READY`。下一位消费者在 robust mutex 恢复后会再次读取原消息。

在第 5 步之后死亡时，消息已经完整复制到死亡进程的私有内存，但还可能没有进入业务处理代码。此时消息
不会重投。这是普通 Channel 的“接收即取走”语义，而不是带业务确认的消息队列语义。

## robust mutex 恢复

数据操作使用：

```text
PTHREAD_PROCESS_SHARED
PTHREAD_MUTEX_ROBUST
```

进程在临界区内被 `SIGKILL` 后，Linux 释放其执行上下文。下一位调用者获取 mutex 时会收到
`EOWNERDEAD`，随后：

1. 调用 `pthread_mutex_consistent()` 接管 mutex。
2. 扫描所有槽位。
3. 将所有 `WRITING` 槽位恢复为 `FREE`。
4. 校验每个 `READY` 的长度和 sequence。
5. 重新计算 free/writing/ready 计数。
6. 增加 `owner_death_recoveries` 和 `discarded_incomplete_writes` 指标。
7. 释放 mutex，正常继续当前 send/receive。

因为 payload 复制始终在 robust mutex 内完成，所以恢复时看到的 `WRITING` 只可能属于已经死亡的 mutex
owner；不存在仍在锁外写该 payload 的正常生产者。

如果 mutex 返回 `ENOTRECOVERABLE`，或者扫描发现非法状态，Channel 才进入 `broken`。这是内部结构已经
不能安全解释的严重故障，普通 worker 退出不会触发它。

## SIGSTOP 与 SIGKILL

`SIGSTOP` 只是暂停进程，不代表进程死亡。暂停的进程仍然持有 mutex，因此其他调用：

- `try_send/try_receive` 返回 `would_block`。
- `send_for/receive_for` 到期后返回 `timed_out`。
- 无限等待版本继续等待。
- Channel 保持 `healthy`，不会擅自回收活进程正在使用的内存。

当该进程真正被杀后，robust mutex 的 owner-death 机制才会触发自动恢复。

这避免了把长时间调度暂停错误当成死亡，也避免了旧进程恢复后继续写已经复用的共享地址。

## MPMC 并发

多个进程和线程可以共享同一个 Channel：

```text
P1 ─┐
P2 ─┼── robust mutex ── 槽位状态与 payload 复制
C1 ─┤
C2 ─┘
```

锁内部分包括槽位查找和共享内存复制，锁外业务处理可以完全并行。这种实现选择了清晰的崩溃线性化点，
而不是追求所有复制操作完全并行。

首次接收按照发布 sequence 选择最早的 `READY` 消息。多个生产者的实际调度仍会影响它们获得 sequence
的先后。

## 等待和背压

Channel 没有空槽位时：

- `try_send()` 返回 `would_block`。
- `send()` 使用 futex 睡眠，等待消费者释放槽位。
- `send_for()` 可以返回 `timed_out`。

没有 READY 消息时，接收端采用相同机制。发送、接收、关闭、unlink 和 owner-death 恢复都会更新共享的
`event_epoch` 并唤醒等待者。阻塞等待还会以很低频率重新检查 robust mutex；这样即使 mutex owner 死在
更新 `event_epoch` 之前，已经睡眠的调用者也能主动发现 owner death 并完成恢复。

## 协议校验

不同进程必须以相同协议解释 payload：

```cpp
constexpr shmchan::protocol_descriptor order_protocol{
    shmchan::protocol_id("com.example.order"),
    3,
};
```

创建端和打开端的 ID/version 不一致时，`open()` 抛出 `managed_channel_error`，错误码为
`protocol_mismatch`。这只校验声明的协议版本，不替用户校验每条 payload 的业务内容。

## 指标

`stats()` 返回：

- state 和 break reason。
- free/writing/ready 槽位数。
- 等待中的 sender/receiver 数量。
- 发送和接收的消息数、字节数。
- robust owner-death 恢复次数。
- 被丢弃的未完成写入数量。
- send/receive 超时数。

正常运行时 `writing_messages` 通常为 0，因为统计快照也在同一个 mutex 下读取。建议监控：

- `state == broken`
- `owner_death_recoveries` 增长
- `discarded_incomplete_writes` 增长
- `free_slots == 0` 持续时间
- send/receive timeout 增长

## 非持久化边界

共享内存通常位于 `/dev/shm` 的 tmpfs：

- 单个发送进程退出后，READY 消息仍然存在。
- 所有业务进程退出后，只要没有 unlink，共享内存对象仍可再次打开。
- `shm_unlink()`、系统重启、机器掉电或 `/dev/shm` 被清理后，消息消失。

是否把消息另外写入数据库、文件、WAL 或其他系统，是业务自己的选择，不是使用本库的前置条件。

## 设计限制

- 全部功能仅限同一台 Linux 主机。
- 默认是接收即删除，不提供 ACK 重投。
- 不提供 exactly-once 或持久化。
- 每个槽位按最大消息长度预留空间。
- 单个 robust mutex 限制超高并发大消息复制的吞吐。
- `unlink()` 是全局管理操作，普通 worker 不应随意调用。
