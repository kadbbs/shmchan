# shmchan

`shmchan` 是一个面向 Linux 的 C++20 共享内存有界 Channel。API 类似 Go channel，底层仅使用
`shm_open`、`mmap`、无锁原子操作与 futex，适用于同机多进程/多线程之间传递定长和不定长消息。

当前版本是可直接使用和继续演进的 MVP：

- 固定容量 MPMC（多生产者、多消费者）队列
- 按实际消息长度计费的 MPMC 字节环形缓冲区
- 阻塞 `send` / `receive`
- 非阻塞 `try_send` / `try_receive`
- 带超时的 `send_for` / `receive_for`
- Go 风格关闭语义：关闭后拒绝新发送，接收端排空缓冲区后得到 `closed`
- 创建、打开和显式 `shm_unlink` 生命周期管理
- header-only，无第三方依赖

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

`receive` 系列返回 `receive_result<T>`，成功时其中的 `value` 有值，也可以用 `if (result)`、
`*result` 和 `result->field`。

Channel 句柄可移动、不可复制。销毁句柄只会解除当前进程的映射，不会自动关闭 Channel，也不会删除
共享内存名称：

- `close()` 是进程间可见、幂等的逻辑关闭；第一次调用返回 `true`。
- `unlink()` 删除名称，已经打开的映射仍可继续工作，直至所有句柄释放。
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

和多数无锁共享内存环形队列一样，当前版本不提供进程崩溃恢复：如果某进程在占用一个槽之后、发布或
释放该槽之前被强制终止，队列可能无法继续推进。需要处理不可信进程或强制终止场景时，应在上层加入
监督、重建和版本化名称切换机制。
