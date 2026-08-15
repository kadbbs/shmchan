# shmchan

`shmchan` 是一个面向 Linux 的现代 C++20 共享内存 Channel 库。它使用
`shm_open + mmap + atomic + futex + process-shared robust mutex`，为同一台机器上的进程提供
类似 Go channel 的 `send/receive` 接口。

它是本机、非持久化 IPC：消息是否另外写入数据库、文件或 WAL，完全由业务决定。

## 主要能力

- 多生产者、多消费者（MPMC）。
- 支持不同长度的字节消息，每条消息不超过创建时指定的最大长度。
- 阻塞、非阻塞和带超时的 `send/receive`。
- 生产者完整写入后消息才变成可见状态。
- 消费者完整复制到进程私有内存后，消息槽位才会释放。
- 进程死在共享内存操作中时，由下一位访问者自动进行槽位级恢复。
- 协议 ID/version 校验、运行指标和 `open_or_create()`。
- 仅依赖 Linux/POSIX 与 C++20 标准库。

## Channel 类型

| 类型 | 消息 | 并发实现 | 进程中途崩溃恢复 |
|---|---|---|---|
| `channel<T>` | 固定大小、trivially-copyable 类型 | 有界 MPMC ring | 不保证 |
| `byte_channel` | 紧凑的不定长字节记录 | 有界字节 ring | 不保证 |
| `managed_byte_channel` | 每槽位固定上限、实际长度可变 | robust mutex + futex | 自动槽位级恢复 |
| `serialized_channel<T, Codec>` | 编解码后的对象 | 基于 `byte_channel` | 不保证 |

如果参与进程可能被 `SIGKILL`，并且其他进程必须继续使用同一个 Channel，应选择
`managed_byte_channel`。

## 快速开始

### 创建或打开

所有进程都可以调用 `open_or_create()`，不需要单独编写初始化进程：

```cpp
#include <shmchan/shmchan.hpp>

shmchan::managed_channel_options options;
options.message_capacity = 1024;
options.max_message_size = 64 * 1024;
options.protocol = {
    shmchan::protocol_id("example.orders"),
    1,
};

auto channel =
    shmchan::managed_byte_channel::open_or_create("orders", options);
```

如果需要严格区分初始化者和使用者，也可以分别使用：

```cpp
auto owner = shmchan::managed_byte_channel::create("orders", options);

shmchan::managed_open_options open_options;
open_options.protocol = options.protocol;
auto peer = shmchan::managed_byte_channel::open("orders", open_options);
```

### 生产者

```cpp
using namespace std::chrono_literals;

const auto status = channel.send_for(
    R"({"order_id":42})",
    1s);

if (status != shmchan::channel_status::success) {
    // 处理 timed_out、closed、message_too_large 等状态。
}
```

### 消费者

```cpp
auto result = channel.receive_for(1s);

if (result) {
    std::span<const std::byte> bytes{result->data(), result->size()};
    handle(bytes);
}
```

`receive()` 返回成功时，完整消息已经复制到当前进程的 `std::vector<std::byte>` 中，共享内存槽位已经
释放。

### 关闭和删除

```cpp
channel.close();
```

`close()` 是整个 Channel 的全局关闭：拒绝新消息，但消费者仍可排空已经发布的消息。对象析构只会解除
当前进程的映射，不会自动关闭或删除 Channel。

完全删除共享内存对象：

```cpp
shmchan::managed_byte_channel::unlink("orders");
```

## 崩溃恢复语义

消息槽位只有三个状态：

```text
FREE ── send 开始 ──> WRITING ── 完整复制 ──> READY
 ▲                                                │
 └──────── receive 完整复制到本地内存 ────────────┘
```

### 生产者被杀

`send()` 在持有进程共享 robust mutex 时完成整条消息复制，并在最后发布 `READY`：

- 死在发布前：槽位停在 `WRITING`，消费者看不到；下一位访问者自动将它恢复为 `FREE`。
- 死在发布后：槽位已经是完整的 `READY`，消息继续保留并可正常接收。
- 死在 Channel 操作之外：对 Channel 没有影响。

### 消费者被杀

`receive()` 在复制期间让槽位保持 `READY`：

- 死在复制中：消息没有删除，下一位消费者仍能完整接收。
- 完整复制后：槽位才切换为 `FREE`。

这是一种与普通 Go channel 接近的“接收即取走”语义。如果消费者已经完整取走消息，随后在业务处理前
崩溃，该消息不会重新投递。要求业务处理确认、重投或持久化时，应由业务另外实现，或者选择专门的消息队列。

### 不需要全局重建

普通进程崩溃不会让整个 Channel `broken`，也不会清空其他 `READY` 消息。下一位获取 robust mutex 的
生产者或消费者会自动执行恢复，然后继续通信。

只有检测到无法解释的共享内存布局、非法槽位状态，或者 robust mutex 已经不可恢复时，Channel 才进入
`broken`。这种情况表示内存结构本身损坏，应由运维停止相关进程、`unlink()` 后重新创建，而不是在运行中
猜测和修补数据。

## “不定长”如何存储

创建时指定：

```cpp
options.message_capacity = 1024;
options.max_message_size = 64 * 1024;
```

每个槽位都预留 `max_message_size` 字节，但通过 `payload_size` 记录实际长度。因此消息可以是 0～64 KiB
中的任意长度。共享内存占用大致为：

```text
header + capacity × (64 字节描述符 + max_message_size 按 64 字节对齐)
```

这种布局没有跨进程堆分配和碎片问题，但当最大消息很大、平均消息很小时会浪费 `/dev/shm` 空间。

不能直接发送包含进程私有指针的对象，例如 `std::string` 或 `std::vector` 的内存布局；应先将对象编码成
字节，例如 Protobuf、FlatBuffers、JSON 或自定义格式。

## 并发和性能边界

`managed_byte_channel` 使用一个 process-shared robust mutex 串行化槽位选择和消息复制。这带来清晰的
崩溃边界：进程死在操作中时，内核会让下一位访问者得到 owner-death 通知并自动修复。

代价是多个生产者和消费者的共享内存复制不会完全并行。业务处理发生在 `receive()` 返回之后，不持锁，
因此通常较慢的业务计算仍可以并发执行。

适合：

- 本机多个 worker 之间的低延迟任务或数据分发。
- 消息大小和 Channel 容量有明确上限。
- 要求进程被杀后保留已发布消息并继续通信。
- 接受“接收成功即从 Channel 删除”的 IPC 语义。

不适合：

- 跨机器通信。
- 机器重启后恢复消息。
- exactly-once、业务处理失败后自动重投或永久去重。
- 单条消息非常大、复制必须高度并行的场景。

## 状态码

| 状态 | 含义 |
|---|---|
| `success` | 操作成功 |
| `closed` | Channel 已关闭；消费者排空后结束 |
| `would_block` | 非阻塞操作当前无法完成 |
| `timed_out` | 带超时操作到期 |
| `message_too_large` | 消息超过 `max_message_size` |
| `protocol_mismatch` | 打开端协议 ID/version 不一致 |
| `broken` | 共享内存结构或 robust mutex 已不可安全使用 |

## 构建和测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

运行示例：

```bash
./build/shmchan_managed_demo init my-channel
./build/shmchan_managed_demo send my-channel hello
./build/shmchan_managed_demo recv my-channel
./build/shmchan_managed_demo status my-channel
./build/shmchan_managed_demo cleanup my-channel
```

## 平台要求

- Linux
- C++20 编译器
- 支持 process-shared robust pthread mutex
- `/dev/shm` 有足够容量

## 许可证

[MIT License](LICENSE)
