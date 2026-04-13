# MiniRedis

一个基于C++实现的轻量级的远程的KV数据库服务，目标是以较小代码体量实现高性能事件驱动网络模型，并支持 **单线程处理路径** 与 **多线程处理路径** 两种架构模式。

---

## 1. 项目简介

MiniRedis 当前实现了 RESP 协议解析、基础 KV 命令执行、epoll 事件循环、TCP/Unix Socket 双监听、连接空闲回收、批量写回（`writev`）等核心能力。

- 使用 `epoll` 构建高并发事件循环；
- 使用内置 `ThreadPool` + `LockFreeQueue` 预留多线程网络 IO/任务分发能力；
- 使用 RESP（Redis Serialization Protocol）与 `redis-benchmark` 对接进行压测；
- 对比官方 Redis（8.0）在同测试维度下的吞吐表现。

---

## 2. 核心特性

- **网络模型**
  - `epoll` 统一事件池（监听 socket、客户端 socket、timerfd）。
  - 支持 TCP (`0.0.0.0:9736`) 与 Unix Domain Socket（默认 `/tmp/miniredis.sock`）监听。

- **协议与命令**
  - 支持 RESP 请求解析与标准返回封装。
  - 已实现命令：`GET`、`SET`、`DEL`、`EXISTS`、`PING`。

- **连接管理**
  - 非阻塞 socket + `EPOLLIN/EPOLLOUT` 动态注册。
  - 连接活跃时间统计，基于 `timerfd` 的空闲连接清理。
  - 输出缓冲区上限保护（`MaxPendingWriteBytes`）。

- **可切换执行模型（单线程 / 多线程）**
  - 单线程路径：事件线程直接读写与执行（当前主路径）。
  - 多线程路径：通过 `ThreadPool::submitBatch()` 并发处理读写任务（代码中已保留调用入口，可切换）。

- **性能测试**
  - 提供 `benchmark/compare.sh` 脚本，使用 `redis-benchmark` 对 MiniRedis 与官方 Redis 做并发对比。

---

## 3. 架构分析

### 3.1 模块分层

- `server/src/server.cpp`
  - 服务主入口、事件循环、连接生命周期、读写处理、命令执行调度。

- `include/common/EventPool.hpp`
  - `epoll` 封装（`add/mod/del/wait`）。

- `include/common/ThreadPool.hpp`
  - 线程池与批量任务提交能力。

- `include/common/LockFreeQueue.hpp`
  - MPSC 无锁队列，用于跨线程待处理/待关闭连接传递。

- `server/include/KvCommandEngine.hpp`
  - 命令分发表与内存 KV 存储（`std::unordered_map<std::string, std::string>`）。

- `include/common/Resp.hpp`
  - RESP 协议解析器与响应包装器。

- `include/configure/Configure.hpp`
  - 运行时参数（监听端口、线程数、超时时间、写队列上限等）。

### 3.2 事件循环主流程

1. `epoll_wait` 获取就绪事件；
2. 区分事件类型：
   - 监听 fd：执行 `accept4`；
   - 定时器 fd：执行 idle 检查；
   - 客户端 fd：按 `EPOLLIN/EPOLLOUT` 分发读写。
3. 读路径：`recv` -> 追加 query buffer -> RESP 解析 -> 入命令队列；
4. 命令执行：`KvCommandEngine::execute()` -> 生成 RESP 回复；
5. 写路径：`writev` 批量发送回复队列；
6. 异常/断开/超限：进入待关闭队列并统一回收。

### 3.3 单线程与多线程架构说明

代码中同时存在两组处理函数：

- 单线程：
  - `singletonThreadReadQueryBuffer(...)`
  - `singletonThreadWriteReply(...)`

- 多线程：
  - `threadPoolBatchReadQueryBufferAndWait(...)`
  - `threadPoolBatchWriteReplyAndWait(...)`

在 `eventLoop()` 中目前默认启用的是单线程函数，多线程函数调用位置已预留（可通过替换调用实现切换）。

> 设计意义：
> - 单线程路径逻辑更简单、锁竞争少；
> - 多线程路径可在高连接高并发场景下提高网络读写吞吐弹性；
> - 结合无锁队列可减少线程间同步开销。

---

## 4. 已实现命令与语义

- `PING` -> `+PONG`
- `SET key value` -> `+OK`
- `GET key` -> `$<len>...` 或 `$-1`
- `DEL key [key ...]` -> `:<removed_count>`
- `EXISTS key [key ...]` -> `:<exists_count>`

命令匹配由 `KvCommandEngine` 内部简单哈希分发完成。

---

## 5. 编译与运行

## 5.1 环境要求

- Linux（依赖 `epoll`/`timerfd`/Unix Socket）
- CMake >= 3.22.1
- C++17 编译器（如 GCC 12+/Clang 12+）

## 5.2 构建

```bash
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j
```

## 5.3 运行服务端

```bash
./server/miniredis-server
```

默认监听：
- TCP: `0.0.0.0:9736`
- Unix Socket: `/tmp/miniredis.sock`

## 5.4 运行客户端（若需）

```bash
./client/miniredis-cli
```

---

## 6. Benchmark 说明（基于 `benchmark/data.txt`）

### 6.0 测试目标机器参数

- CPU：Intel(R) Xeon(R) Platinum × 1
- 核心：1 个物理 CPU，4 个物理核心，8 个逻辑核心
- 内存：16GB

测试命令：`SET/GET`，默认请求总数：`100000`，并发从 `50` 到 `10000`。

### 6.1 关键观察

- 在低到中等并发（50~1000）范围，MiniRedis 与 Redis 8.0 吞吐基本同量级，且多组数据下 MiniRedis 略高。
- 在高并发（3000~10000）下，双方都出现回落后再趋稳，整体仍较接近。
- 结果说明 MiniRedis 当前网络与命令执行链路具备较高效率，尤其在 SET/GET 简单 KV 场景。

### 6.2 示例对比（摘录）

| 并发 | MiniRedis SET | Redis8 SET | MiniRedis GET | Redis8 GET |
|---:|---:|---:|---:|---:|
| 50 | 85034.02 | 81168.83 | 84961.77 | 82372.32 |
| 100 | 83542.19 | 84033.61 | 84245.99 | 81168.83 |
| 1000 | 65359.48 | 66137.57 | 63251.11 | 61957.87 |
| 5000 | 56274.62 | 57405.28 | 57208.24 | 56915.20 |
| 10000 | 54229.93 | 57836.90 | 55463.12 | 56433.41 |

### 6.3 关于 `CONFIG` 报错提示

`benchmark/data.txt` 中出现：

- `ERR unknown command 'CONFIG'`
- `failed to fetch CONFIG`

这是 `redis-benchmark` 在某些流程中会尝试获取服务端配置，而 MiniRedis 未实现 `CONFIG` 命令导致。

该提示 **不会阻断 SET/GET 测试本身**，从日志中可见吞吐结果仍已正常产出。

---

## 7. 配置项（`Configure::ServerRuntime`）

主要参数包括：

- `TcpListenPort`：默认 `9736`
- `TcpListenAddress`：默认 `0.0.0.0`
- `UnixSocketPath`：默认 `/tmp/miniredis.sock`
- `WorkerNetIoThreads`：线程池线程数
- `ClientIdleTimeout`：连接空闲超时
- `IdleCheckInterval`：idle 检查间隔
- `MaxPendingWriteBytes`：单连接待发送队列字节上限

---

## 8. 后续优化建议

- 补齐更多 Redis 命令（如 `MGET/MSET/INCR/EXPIRE/TTL/CONFIG`）。
- 将多线程路径做成可配置运行模式（启动参数或配置文件切换）。
- 优化命令执行层：
  - 支持分片哈希表；
  - 并发安全策略（读写锁、分段锁或无锁结构）。
- 增加 AOF/RDB 持久化能力。
- 增加主从功能，以及cluster
- 引入更系统化 benchmark 报告（P50/P99 延迟、CPU/内存、不同 payload）。

---

## 9. 免责声明

MiniRedis 当前定位为学习/实验性质的高性能服务端实现示例，并非完整替代官方 Redis。用于生产环境前，建议补齐持久化、主从复制、事务、脚本、安全鉴权等能力。