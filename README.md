# shmchan

`shmchan` 是一个面向 Linux 的 C++20 共享内存有界 Channel。API 类似 Go channel，底层使用
`shm_open`、`mmap`、原子操作与 futex，适用于同机多进程/多线程之间传递定长和不定长消息。

项目提供两种故障模型：基础 Channel 追求紧凑和低延迟；`managed_byte_channel` 增加控制面与 robust
恢复协议，用于需要识别进程崩溃、ACK 重投和 generation fencing 的场景。

主要能力：

- 固定容量 MPMC（多生产者、多消费者）队列
- 按实际消息长度计费的 MPMC 字节环形缓冲区
- 阻塞 `send` / `receive`
- 非阻塞 `try_send` / `try_receive`
- 带超时的 `send_for` / `receive_for`
- Go 风格关闭语义：关闭后拒绝新发送，接收端排空缓冲区后得到 `closed`
- 生产增强的不定长 Channel：参与者心跳、generation、broken/rebuild、ACK/NACK 和超时重投
- 应用协议 ID/version 校验、运行指标和参与者快照
- 发送 reservation 与 loaned receive 零拷贝接口
- 创建、打开和显式 `shm_unlink` 生命周期管理
- header-only，无第三方依赖

| API | 数据布局 | 进程异常后的行为 | 适合场景 |
|---|---|---|---|
| `channel<T>` | 固定类型槽位 | 由外部删除并重建 | POD、极简低延迟 |
| `byte_channel` / `serialized_channel` | 紧凑字节环 | 由外部删除并重建 | 大小差异明显的不定长对象 |
| `managed_byte_channel` | 固定最大负载槽位 + 控制面 | 自动检测 broken，监督者切换 generation | 需要 ACK、重投和可审计恢复的本机 IPC |

## 快速开始

```cpp
#include <shmchan/channel.hpp>

#include <cstdint>

struct event {
    std::uint64_t id;
    double value;
};

// 进程 A：创建并发送
auto tx = shmchan::channel<event>::create("events", 1024);
tx.send(event{.id = 1, .value = 3.14});
tx.close();

// 进程 B：打开并接收
auto rx = shmchan::channel<event>::open("events");
for (;;) {
    auto result = rx.receive();
    if (result.code == shmchan::channel_status::closed) {
        break;
    }
    use(*result);
}
rx.unlink();
```

名称可以写成 `events` 或 `/events`；库会规范化为 POSIX 共享内存名称 `/events`。容量在创建时固定。

## 不定长对象

不要把 `std::string` 或 `std::vector` 对象的内存表示直接放进共享内存，因为其中包含的堆指针只在
当前进程有效。`byte_channel` 传输对象序列化后的字节，接收端得到当前进程拥有的
`std::vector<std::byte>`：

```cpp
#include <shmchan/byte_channel.hpp>

// 进程 A：参数是整个消息环形区的字节容量，不是消息条数。
auto tx = shmchan::byte_channel::create("blobs", 1024 * 1024);
tx.send(std::string_view{"a variable-length message"});
tx.close();

// 进程 B
auto rx = shmchan::byte_channel::open("blobs");
auto result = rx.receive();
if (result) {
    consume_bytes(*result);
}
```

字符串可以直接使用 `string_channel`，API 仍然是 `send/receive`：

```cpp
#include <shmchan/serialized_channel.hpp>

auto tx = shmchan::string_channel::create("strings", 1024 * 1024);
tx.send("short");
tx.send(std::string(100'000, 'x'));
tx.close();

auto rx = shmchan::string_channel::open("strings");
while (auto message = rx.receive()) {
    use(*message); // std::string
}
```

自定义对象通过 codec 接入。编码结果必须是元素宽度为 1 的连续字节范围，解码结果必须拥有自己的数据：

```cpp
struct document {
    std::uint64_t id;
    std::string body;
};

struct document_codec {
    std::vector<std::byte> encode(const document&) const;
    document decode(std::span<const std::byte>) const;
};

using document_channel =
    shmchan::serialized_channel<document, document_codec>;

auto channel = document_channel::create("documents", 4 * 1024 * 1024);
channel.send(document{.id = 7, .body = "variable"});
```

库校验 `byte_channel` 的共享内存布局，但不会理解应用层 wire format；打开同一名称的所有进程必须使用
兼容的 codec 和协议版本。解码失败时该条消息已经从 Channel 消费，应由应用决定重试、死信或重建策略。

`capacity_bytes()` 返回按 32 字节向上对齐后的实际环形容量；单条消息最大为
`max_message_size()`。记录只占用“32 字节元数据 + 实际负载”向上对齐后的空间，负载可以跨环尾存储，
因此不需要为每个槽预留最大消息长度。

## 带崩溃恢复的不定长 Channel

`managed_byte_channel` 传输不定长字节消息，并要求消费者显式 ACK：

```cpp
#include <shmchan/managed_channel.hpp>

// 初始化一次；句柄退出后，共享内存对象和已提交消息仍然存在。
shmchan::managed_channel_options options;
options.message_capacity = 1024;
options.max_message_size = 64 * 1024;
auto tx = shmchan::managed_byte_channel::create("orders", options);
tx.send("order-42", 42); // 第二个参数是稳定的应用 message_id

// 另一个进程
auto rx = shmchan::managed_byte_channel::open("orders");
auto delivery = rx.receive();
if (delivery) {
    handle(delivery->bytes());
    delivery->ack();
}
```

未 ACK 消息在租约超时后会再次投递，因此消费者必须幂等。进程死亡或半完成预留会把当前 generation 标记为
`broken`；监督者创建全新的 generation，并从应用自己的 durable outbox/WAL 重放消息：

```cpp
if (channel.state() == shmchan::managed_channel_state::broken) {
    channel.rebuild_with_replay([&](auto& rebuilt, auto, auto) {
        for (const auto& message : durable_outbox.pending()) {
            if (rebuilt.send(message.bytes, message.id) != shmchan::channel_status::success) {
                throw std::runtime_error("replay failed");
            }
        }
    });
}
```

回调执行期间状态为 `replaying`：只允许该回调使用的重建句柄发送，消费者可以持续接收并 ACK，其他生产者
得到 `recovery_in_progress`。因此大于 Channel 容量的重放需要有消费者同时排空；回调必须检查每次发送
结果，失败时抛出异常。

完整的槽位、generation、ACK、重建、零拷贝和生产边界见
[managed_byte_channel 架构与故障语义](docs/managed-channel.md)。

## 构建与测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

演示程序可以分两次运行（共享内存中的消息会保留到消费端打开）：

```bash
./build/shmchan_demo produce my-demo
./build/shmchan_demo consume my-demo

./build/shmchan_variable_demo produce my-variable-demo
./build/shmchan_variable_demo consume my-variable-demo
```

生产增强示例使用多个命令模拟不同进程：

```bash
./build/shmchan_managed_demo init my-managed-demo
./build/shmchan_managed_demo send my-managed-demo 'hello shm' 1001
./build/shmchan_managed_demo status my-managed-demo
./build/shmchan_managed_demo recv my-managed-demo
```

故障恢复演示：

```bash
./build/shmchan_managed_demo break my-managed-demo
./build/shmchan_managed_demo rebuild my-managed-demo
./build/shmchan_managed_demo close my-managed-demo
./build/shmchan_managed_demo cleanup my-managed-demo
```

`rebuild` 会创建空的新 generation；示例程序没有 durable outbox，所以不会恢复旧 generation 的负载。

安装后使用：

```cmake
find_package(shmchan CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE shmchan::shmchan)
```

## API 状态

所有操作使用 `shmchan::channel_status`：

| 状态 | 含义 |
|---|---|
| `success` | 操作成功 |
| `closed` | Channel 已关闭，且接收端已无可读数据 |
| `would_block` | `try_*` 当前无法立即完成 |
| `timed_out` | `*_for` 在时限内未完成 |
| `message_too_large` | 不定长消息超过该 Channel 的单条消息上限 |
| `broken` | managed Channel 当前 generation 已判定不可继续使用 |
| `generation_changed` | 操作属于已经被替换的旧 generation |
| `protocol_mismatch` | 应用协议 ID/version 不一致 |
| `participant_limit` | 参与者槽位已满 |
| `participant_expired` | 当前句柄的槽位已被 fencing |
| `recovery_in_progress` | 正在重放，仅恢复所有者可发送；消费者仍可接收 |
| `stale_delivery` | reservation/delivery 已完成、失效或不再对应当前槽位 |
| `duplicate_message` | 当前 generation 已存在同 ID 的未完成消息 |

`receive` 系列返回 `receive_result<T>`，成功时其中的 `value` 有值，也可以用 `if (result)`、
`*result` 和 `result->field`。

Channel 句柄可移动、不可复制。销毁句柄只会解除当前进程的映射，不会自动关闭 Channel，也不会删除
共享内存名称：

- `close()` 是进程间可见、幂等的逻辑关闭；第一次调用返回 `true`。
- 基础 Channel 的 `unlink()` 删除名称后，已打开映射仍可工作到句柄释放；managed Channel 会先进入
  `destroying` 并 fencing 所有存量句柄，再删除控制面和已知 generation。
- `channel<T>::unlink(name)` 可用于显式清理遗留对象。

## 消息类型约束

对于 `channel<T>`，`T` 必须可平凡复制（`std::is_trivially_copyable_v<T>`）且可复制构造。它适合
整数、浮点数和只包含定长字段的 POD 风格结构体。不定长对象应使用 `byte_channel` 或
`serialized_channel<T, Codec>`。

不要通过 `channel<T>` 直接传递以下内容：

- `std::string`、`std::vector` 等拥有堆内存的对象
- 虚函数对象
- 原始指针或引用（地址在另一个进程中通常无意义）
- 需要构造/析构资源的类型

打开端会校验元素大小、对齐、类型指纹和库布局。所有进程应使用相同的消息定义、架构、编译工具链和
`shmchan` 版本。

## 实现说明与边界

容量大于 1 时，队列使用逐槽 sequence 的有界 MPMC 环形算法；容量为 1 时使用专门的原子槽状态机。
生产/消费快路径不进入内核，仅在队列满或空时通过两个 32 位 epoch futex 休眠和唤醒。热原子与槽按
64 字节缓存行对齐，以减少伪共享。

`byte_channel` 使用三个单调位置分别完成记录预留、消费者领取和按序回收。生产者发布可以并行，消费者
复制也可以并行；只有已经领取的前序记录完成后，其空间才会重新交给生产者，避免慢消费者的数据被覆盖。
按序回收扫描由一个极短的进程共享门保护，无竞争时只执行原子操作，竞争时通过 futex 等待。

这里使用的是 Linux 主流架构上的进程间 lock-free 原子实践；C++ 标准本身没有完整规定把
`std::atomic` 放进 POSIX 共享映射后的跨进程语义，因此库在编译期要求 32/64 位原子始终无锁。

基础 `channel<T>` 和 `byte_channel` 与多数无锁共享内存环形队列一样，不提供槽位级进程崩溃恢复：如果
某进程在占用一个槽之后、发布或释放该槽之前被强制终止，应由外部整体删除并重建。

`managed_byte_channel` 实现了监督、generation 切换和 ACK 重投，但仍然是非持久化的本机 IPC。它采用
保守策略：任何无法证明安全的半完成操作都会令整代 broken，而不是原地修补；旧消息依靠应用的持久
outbox/WAL 重放。它提供 at-least-once，不提供 exactly-once，也不能在机器重启后替代磁盘消息系统。

## 许可证

本项目基于 [MIT License](LICENSE) 开源。
