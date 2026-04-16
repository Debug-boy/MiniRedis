#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <csignal>
#include <filesystem>

#include <iostream>

#include "ServerConfigure.hpp"

#include "common/EventPool.hpp"
#include "common/ThreadPool.hpp"
#include "common/LockFreeQueue.hpp"

#include "KvCommandEngine.hpp"
#include "Connection.hpp"
#include "KeyValueDumper.hpp"

#include <spdlog/spdlog.h>

namespace MiniRedis {

    class Server : public Singleton<Server>{
        friend class Singleton<Server>;
    private:
        struct PendingCloseStruct {
            std::shared_ptr<Connection> connection;
            std::string reason;
        };

        EventPool eventPool;
        std::unique_ptr<ThreadPool> threadPool;
        KvCommandEngine engine;

        int tcpSocketListenFd = -1;
        int unixSocketListenFd = -1;
        int signalFd = -1;
        int timerFd = -1;

        std::unique_ptr<Channel> tcpListenChannel;
        std::unique_ptr<Channel> unixSocketChannel;
        std::unique_ptr<Channel> timerChannel;
        std::unique_ptr<Channel> signalChannel;

        std::unordered_map<int, std::shared_ptr<Connection> > connectionsMap;
        LockFreeQueue<std::shared_ptr<Connection> > pendingExecCommandedConnectionQueue;
        LockFreeQueue<PendingCloseStruct> pendingCloseQueue;

    public:
        Server() {
            commonCheckKernelParameter();
            setupTcpSocketListen();
            setupUnixSocketListen();
            setupIdleTimer();
            registerTcpListenEvent();
            registerUnixListenEvent();
            registerTimerClickEvent();
            registerSignalEvent();
        }

        ~Server() {
            for (auto &kv: connectionsMap) {
                if (kv.second) {
                    kv.second->closeSocketOnly();
                }
            }
            if (tcpSocketListenFd != -1) {
                ::close(tcpSocketListenFd);
            }
            if (unixSocketListenFd != -1) {
                ::close(unixSocketListenFd);
            }
            if (timerFd != -1) {
                ::close(timerFd);
            }
            if (signalFd != -1) {
                ::close(signalFd);
            }
        }

        void eventLoop() {
            spdlog::info("The server has successfully initiated the event loop.");

            /*
             *The thread pool can only be created after registering with the signal masking set;
             *otherwise, the child threads or processes will not inherit the signal masking strategy
             */
            //threadPool = std::make_unique<ThreadPool>(ServerConfigure::WorkerNetIoThreads);

            std::vector<epoll_event> ready(SOMAXCONN);

            for (;;) {
                const int readyEventNumberOf = eventPool.wait(ready.data(), static_cast<int>(ready.size()), -1);

                if (readyEventNumberOf == -1 && errno == EINTR) {
                    spdlog::warn("{}",getSysLastError("epoll_wait"));
                    continue;
                }

                std::vector<std::shared_ptr<Connection> > needReadConnections;
                needReadConnections.reserve(readyEventNumberOf);

                std::vector<std::shared_ptr<Connection> > needWriteConnections;
                needWriteConnections.reserve(readyEventNumberOf);

                for (int i = 0; i < readyEventNumberOf; ++i) {
                    const auto *channel = static_cast<Channel *>(ready[i].data.ptr);
                    if (!channel) {
                        continue;
                    }

                    const uint32_t ev = ready[i].events;

                    if (channel->type == Channel::Type::Client) {
                        auto node = connectionsMap.find(channel->fd);
                        if (node == connectionsMap.end()) {
                            spdlog::error("client({}) not found", channel->fd);
                            continue;
                        }
                        auto connection = node->second;
                        if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                            closeConnection(connection, "peer closed / error");
                            continue;
                        }
                        if (ev & EPOLLIN) {
                            needReadConnections.push_back(connection);
                        }
                        if (ev & EPOLLOUT) {
                            needWriteConnections.push_back(connection);
                        }
                    } else if (channel->type == Channel::Type::TcpListen) {
                        if (ev & (EPOLLERR | EPOLLHUP)) {
                            throw std::runtime_error("listen tcp socket fd error ev=0x{:x}");
                        }
                        if (ev & EPOLLIN) {
                            handleAccept(tcpSocketListenFd);
                        }
                    } else if (channel->type == Channel::Type::UnixListen) {
                        if (ev & (EPOLLERR | EPOLLHUP)) {
                            throw std::runtime_error("listen unix socket fd error ev=0x{:x}");
                        }
                        if (ev & EPOLLIN) {
                            handleAccept(unixSocketListenFd);
                        }
                    } else if (channel->type == Channel::Type::Timer) {
                        if (ev & (EPOLLERR | EPOLLHUP)) {
                            throw std::runtime_error("timerfd event error: ev=0x{:x}");
                        }
                        if (ev & EPOLLIN) {
                            handleTimerTick();
                        }
                    } else if (channel->type == Channel::Type::Signal) {
                        if (ev & (EPOLLERR | EPOLLHUP)) {
                            throw std::runtime_error("signal event error: ev=0x{:x}");
                        }
                        if (ev & EPOLLIN) {
                            handleSignal();
                        }
                    }

                } //end for()

                if (!needReadConnections.empty()) {
                    //threadPoolBatchReadQueryBufferAndWait(needReadConnections);
                    singletonThreadReadQueryBuffer(needReadConnections);
                    execPendingCloseConnection();
                    execClientRespCommands();
                }

                if (!needWriteConnections.empty()) {
                    //threadPoolBatchWriteReplyAndWait(needWriteConnections);
                    singletonThreadWriteReply(needWriteConnections);
                    execPendingCloseConnection();
                }

                const auto dirtyFrequency = engine.accumulateDurationDirtyCount();
                for (const auto& triggerPolicy : ServerConfigure::DUMP_TRIGGER_POLICIES) {
                    if (
                        dirtyFrequency >= triggerPolicy.dirtyThreshold &&
                        std::chrono::steady_clock::now() - KeyValueDumper::getInstance().lastDumperTriggerTimePoint >= triggerPolicy.millisecond) {
                        spdlog::info("dump trigger condition is ({} millisecond,{} dirtyThreshold)",triggerPolicy.millisecond.count(),triggerPolicy.dirtyThreshold);
                        KeyValueDumper::getInstance().dump(
                            ServerConfigure::DUMP_SAVED_BINARY_FILE_PATH,
                            ServerConfigure::VERSION,
                            engine.store,
                            true);
                        break;
                    }
                }

            }

            spdlog::warn("event loop is over!");
        }

        void recoverDatabasesFormBinaryFile(const std::optional<std::string_view> &filePath) {
            if (!filePath.has_value()) {
                return;
            }
            KeyValueDumper::getInstance().loader(filePath.value(),engine.store);
        }

    private:
        static void commonCheckKernelParameter() {
            struct rlimit rlim{};
            if (getrlimit(RLIMIT_NOFILE, &rlim) == -1) {
                spdlog::error(getSysLastError("getrlimit failed"));
                return;
            }
            spdlog::info("software file descript restrict max {}", rlim.rlim_cur);
            spdlog::info("hardware file descript restrict max {}", rlim.rlim_cur);
        }


        void setupTcpSocketListen() {
            tcpSocketListenFd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (tcpSocketListenFd == -1) {
                throw std::runtime_error(getSysLastError("socket"));
            }

            spdlog::info("tcp socket({}) create success.", tcpSocketListenFd);

            constexpr int option_value = 1;
            if (::setsockopt(tcpSocketListenFd, SOL_SOCKET, SO_REUSEADDR, &option_value, sizeof(option_value)) == -1) {
                throw std::runtime_error(getSysLastError("setsockopt(SO_REUSEADDR)"));
            }
            spdlog::info("setsockopt(SO_REUSEADDR) success.");

            int old_flag = ::fcntl(tcpSocketListenFd, F_GETFL, 0);
            if (old_flag == -1 || ::fcntl(tcpSocketListenFd, F_SETFL, old_flag | O_NONBLOCK) == -1) {
                spdlog::error("Failed to change the TCP socket to non-blocking mode.");
                throw std::runtime_error(getSysLastError("fcntl"));
            }

            spdlog::info("set tcp socket is no block success.");

            sockaddr_in bindAddress{};
            bindAddress.sin_family = AF_INET;
            bindAddress.sin_addr.s_addr = htonl(INADDR_ANY);
            bindAddress.sin_port = htons(ServerConfigure::TCP_LISTEN_PORT);

            if (::bind(tcpSocketListenFd, reinterpret_cast<sockaddr *>(&bindAddress), sizeof(bindAddress)) == -1) {
                throw std::runtime_error(getSysLastError("bind tcp socket error"));
            }

            spdlog::info("bind tcp socket success.", tcpSocketListenFd);

            if (::listen(tcpSocketListenFd, SOMAXCONN) == -1) {
                throw std::runtime_error(getSysLastError("listen tcp socket error"));
            }

            spdlog::info("listen tcp socket on the {}:{} success.", ServerConfigure::TCP_LISTEN_ADDRESS,
                         ServerConfigure::TCP_LISTEN_PORT);
        }

        void setupUnixSocketListen() {
            unixSocketListenFd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (unixSocketListenFd == -1) {
                throw std::runtime_error(getSysLastError("socket"));
            }
            spdlog::info("unix socket({}) create success.", unixSocketListenFd);

            int old_flag = ::fcntl(unixSocketListenFd, F_GETFL, 0);
            if (old_flag == -1 || ::fcntl(unixSocketListenFd, F_SETFL, old_flag | O_NONBLOCK) == -1) {
                throw std::runtime_error(getSysLastError("fcntl"));
            }

            unlink(ServerConfigure::UNIX_SOCKET_FILE_PATH.c_str());

            sockaddr_un bindAddress{};
            bindAddress.sun_family = AF_UNIX;
            strncpy(bindAddress.sun_path, ServerConfigure::UNIX_SOCKET_FILE_PATH.c_str(), sizeof(bindAddress.sun_path));
            if (::bind(unixSocketListenFd, reinterpret_cast<sockaddr *>(&bindAddress), sizeof(bindAddress)) != 0) {
                throw std::runtime_error(getSysLastError("bind unix socket error"));
            }

            spdlog::info("bind unix socket success.", unixSocketListenFd);

            if (::listen(unixSocketListenFd, SOMAXCONN) == -1) {
                throw std::runtime_error(getSysLastError("listen unix socket error"));
            }

            spdlog::info("listen unix socket on the {} success.", ServerConfigure::UNIX_SOCKET_FILE_PATH.c_str());
        }

        void setupIdleTimer() {
            timerFd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
            if (timerFd == -1) {
                throw std::runtime_error(getSysLastError("timerfd_create"));
            }

            itimerspec spec{};
            spec.it_interval.tv_sec = ServerConfigure::IDLE_CHECK_INTERVAL.count();
            spec.it_value.tv_sec = ServerConfigure::IDLE_CHECK_INTERVAL.count();
            if (::timerfd_settime(timerFd, 0, &spec, nullptr) == -1) {
                throw std::runtime_error(getSysLastError("timerfd_settime"));
            }
        }

        void registerTcpListenEvent() {
            tcpListenChannel = std::make_unique<Channel>(Channel::Type::TcpListen, tcpSocketListenFd);
            eventPool.add(tcpSocketListenFd, EPOLLIN | EPOLLERR | EPOLLHUP, tcpListenChannel.get());
        }

        void registerUnixListenEvent() {
            unixSocketChannel = std::make_unique<Channel>(Channel::Type::UnixListen, tcpSocketListenFd);
            eventPool.add(unixSocketListenFd, EPOLLIN | EPOLLERR | EPOLLHUP, unixSocketChannel.get());
        }

        void registerTimerClickEvent() {
            timerChannel = std::make_unique<Channel>(Channel::Type::Timer, timerFd);
            eventPool.add(timerFd, EPOLLIN | EPOLLERR | EPOLLHUP, timerChannel.get());
        }

        void registerSignalEvent() {
            sigset_t mask;
            sigemptyset(&mask);
            sigaddset(&mask, SIGCHLD);
            //sigaddset(&mask, SIGINT);
            sigaddset(&mask, SIGPIPE);
            sigaddset(&mask, SIGTERM);
            sigprocmask(SIG_BLOCK, &mask, nullptr);
            signalFd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
            signalChannel = std::make_unique<Channel>(Channel::Type::Signal, signalFd);
            eventPool.add(signalFd, EPOLLIN | EPOLLERR | EPOLLHUP, signalChannel.get());
        }

        void addConnection(int clientFd, const bool isUnixSocket) {
            auto connection = std::make_shared<Connection>();
            connection->socketFd = clientFd;
            connection->isUnixSocket = isUnixSocket;
            connection->fdContext.fd = clientFd;

            if (!isUnixSocket) {
                constexpr int option_value = 1;
                if (::setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &option_value, sizeof(option_value)) == -1) {
                    spdlog::warn("setsockopt(TCP_NODELAY) failed for fd={}, err={}", clientFd, std::strerror(errno));
                }
            }

            connection->registerEvents = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;

            eventPool.add(clientFd, connection->registerEvents, &connection->fdContext);
            connectionsMap.emplace(clientFd, connection);
        }

        void singletonThreadReadQueryBuffer(
            const std::vector<std::shared_ptr<Connection> > &pendingHandleReadConnections) {
            for (auto &conn: pendingHandleReadConnections) {
                handleRead(conn);
            }
        }

        void threadPoolBatchReadQueryBufferAndWait(
            std::vector<std::shared_ptr<Connection> > &pendingHandleReadConnections) {
            std::vector<ThreadPool::Task> tasks;
            tasks.reserve(pendingHandleReadConnections.size());
            for (auto &conn: pendingHandleReadConnections) {
                tasks.emplace_back([this, conn] {
                    handleRead(conn);
                });
            }
            threadPool->submitBatch(tasks);
            threadPool->wait();
        }

        void singletonThreadWriteReply(std::vector<std::shared_ptr<Connection> > &pendingHandleWriteConnections) {
            for (auto &conn: pendingHandleWriteConnections) {
                handleWrite(conn);
            }
        }

        void threadPoolBatchWriteReplyAndWait(
            std::vector<std::shared_ptr<Connection> > &pendingHandleWriteConnections) {
            std::vector<ThreadPool::Task> tasks;
            tasks.reserve(pendingHandleWriteConnections.size());
            for (auto &conn: pendingHandleWriteConnections) {
                tasks.emplace_back([this, conn] {
                    handleWrite(conn);
                });
            }
            threadPool->submitBatch(tasks);
            threadPool->wait();
        }

        void handleAccept(const int listenedFd) {
            for (;;) {
                sockaddr_in tcpSocketAddr{};
                sockaddr_un unixSocketAddr{};
                int clientFd = -1;
                socklen_t socketTypeSize = -1;
                sockaddr *xSockaddr = nullptr;
                if (listenedFd == this->tcpSocketListenFd) {
                    socketTypeSize = sizeof(tcpSocketAddr);
                    xSockaddr = reinterpret_cast<sockaddr *>(&tcpSocketAddr);
                } else {
                    socketTypeSize = sizeof(unixSocketAddr);
                    xSockaddr = reinterpret_cast<sockaddr *>(&unixSocketAddr);
                }

                clientFd = ::accept4(listenedFd, xSockaddr, &socketTypeSize,SOCK_NONBLOCK);

                if (clientFd > 0) {
                    if (xSockaddr->sa_family == AF_INET) {
                        char ipv4[INET_ADDRSTRLEN]{};
                        inet_ntop(AF_INET, &tcpSocketAddr.sin_addr, ipv4, sizeof(ipv4));
                        spdlog::info("new tcp connection : {}:{} (fd={})", ipv4,ntohs(tcpSocketAddr.sin_port),
                                     clientFd);
                    } else {
                        spdlog::info("new unix connection : (fd={})", &unixSocketAddr.sun_path[1], clientFd);
                    }

                    addConnection(clientFd, listenedFd == this->unixSocketListenFd);
                    continue;
                }

                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                spdlog::error("accept error: {}", std::strerror(errno));
                break;
            }
        }


        //only run singleton thread
        void handleTimerTick() {
            uint64_t expirations = 0;
            for (;;) {
                const ssize_t n = ::read(timerFd, &expirations, sizeof(expirations));
                if (n == static_cast<ssize_t>(sizeof(expirations))) break;
                if (n == -1 && errno == EINTR) continue;
                if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
                break;
            }

            for (auto &node: connectionsMap) {
                auto &connection = node.second;
                if (connection->isExceedActiveDuration(ServerConfigure::CLIENT_IDLE_TIMEOUT)) {
                    closeConnection(connection, "idle timeout");
                }
            }
        }

        void handleSignal() const {
            struct signalfd_siginfo signalDetailInformation;
            for (;;) {
                ssize_t n = read(signalFd, &signalDetailInformation, sizeof(signalDetailInformation));
                if (n > 0) {
                    if (signalDetailInformation.ssi_signo == SIGCHLD) {
                        spdlog::warn("received SIGCHLD");
                        int status;
                        pid_t pid;
                        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
                            auto bgSaveDuration =
                                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - KeyValueDumper::getInstance().lastDumperTriggerTimePoint);
                            if (WIFEXITED(status)) {
                                const int exitCode = WEXITSTATUS(status);
                                if (exitCode == 1) {
                                    spdlog::info("The child({}) process is normal exit({}),accumulated {} milliseconds",pid,exitCode,bgSaveDuration.count());
                                }else {
                                    spdlog::info("The child({}) process is exception exit({}),accumulated {} milliseconds",pid,exitCode,bgSaveDuration.count());
                                }
                                KeyValueDumper::getInstance().lastForkedChildProcessPid = -1;
                            } else if (WIFSIGNALED(status)) {
                                const int signalCode = WTERMSIG(status);
                                spdlog::error("The child({}) process was killed by a signal({}).",pid,signalCode);
                            }
                        }
                    }else if (signalDetailInformation.ssi_signo == SIGTERM) {
                        spdlog::warn("received SIGTERM");
                    }else if (signalDetailInformation.ssi_signo == SIGINT) {
                        spdlog::warn("received SIGINT");
                    }else if (signalDetailInformation.ssi_signo == SIGPIPE) {
                        spdlog::warn("received SIGPIPE");
                    }
                }else if (errno != EINTR) {
                    break;
                }
            }
        }

        void handleRead(const std::shared_ptr<Connection> &connection) {
            if (!connection) {
                return;
            }

            char buf[4096];
            for (;;) {
                const ssize_t n = ::recv(connection->socketFd, buf, sizeof(buf), 0);
                if (n > 0) {
                    connection->lastActive = std::chrono::steady_clock::now();
                    connection->queryBuffer.append(buf, static_cast<size_t>(n));
                    continue;
                }
                if (n == -1 && errno == EINTR) {
                    continue;
                }
                if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    break;
                }
                pushConnectionToCloseQueue(connection, "recv failed");
                return;
            }

            processRespCommands(connection);
        }

        void handleWrite(const std::shared_ptr<Connection> &connection) {
            if (!connection)
                return;

            while (true) {
                if (connection->replyQueue.empty()) {
                    disableClientWrite(connection);
                    return;
                }

                constexpr uint32_t MAX_IOVEC_SIZE = 16;
                struct iovec buffers[MAX_IOVEC_SIZE]{};
                int paddingBufferCount = 0;

                buffers[0].iov_base = static_cast<void *>(
                    connection->replyQueue.front().data() + connection->sendOffset);
                buffers[0].iov_len = connection->replyQueue.front().size() - connection->sendOffset;
                ++paddingBufferCount;

                for (int i = 1; (i < connection->replyQueue.empty()) && (paddingBufferCount < MAX_IOVEC_SIZE);
                     ++i, ++paddingBufferCount) {
                    auto &replyChunk = connection->replyQueue[i];
                    buffers[paddingBufferCount].iov_base = replyChunk.data();
                    buffers[paddingBufferCount].iov_len = replyChunk.size();
                }

                const ssize_t sentByteNumberOf = writev(connection->socketFd, buffers, paddingBufferCount);

                if (sentByteNumberOf > 0) {
                    connection->lastActive = std::chrono::steady_clock::now();
                    size_t remaining = sentByteNumberOf;

                    for (int i = 0; i < paddingBufferCount && remaining > 0; i++) {
                        const size_t sentBytes = std::min(remaining, buffers[i].iov_len);
                        remaining -= sentBytes;
                        if (i == 0) {
                            connection->sendOffset += sentBytes;
                            if (connection->sendOffset == connection->replyQueue.front().size()) {
                                connection->replyBytes -= connection->replyQueue.front().size();
                                connection->replyQueue.pop_front();
                                connection->sendOffset = 0;
                            }
                        } else {
                            connection->replyBytes -= buffers[i].iov_len;
                            if (sentBytes == buffers[i].iov_len) {
                                connection->replyQueue.pop_front();
                            } else {
                                connection->sendOffset = sentBytes;
                            }
                        }
                    }
                } else {
                    if (sentByteNumberOf == -1 && errno == EINTR) {
                        continue;
                    }

                    if (sentByteNumberOf == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        return;
                    }

                    pushConnectionToCloseQueue(connection, "writev failed");
                    return;
                }
            }
        }

        void processRespCommands(const std::shared_ptr<Connection> &connection) {
            std::vector<std::string> argv;
            while (true) {
                size_t consumed = 0;
                try {
                    if (!Resp::Parser::parseOnce(connection->queryBuffer, consumed, argv)) {
                        break;
                    }
                    connection->argvPipeline.emplace_back(std::move(argv));
                } catch (const std::exception &ex) {
                    if (!enqueueReply(connection, Resp::Wrapper::error(ex.what()))) {
                        pushConnectionToCloseQueue(connection, "output reply queue overflow");
                        break;
                    }
                    break;
                }
                connection->queryBuffer.erase(0, consumed);
            }
            pendingExecCommandedConnectionQueue.push(connection);
        }

        //only run singleton thread
        void execClientRespCommands() {
            pendingExecCommandedConnectionQueue.pop_all([this](const std::shared_ptr<Connection> &connection) {
                while (!connection->argvPipeline.empty()) {
                    const auto& argv = connection->argvPipeline.front();
                    auto execResult = engine.execute(argv);
                    if (!enqueueReply(connection, std::move(execResult))) {
                        pushConnectionToCloseQueue(connection, "output reply queue overflow");
                    }
                    connection->argvPipeline.pop_front();
                }
            });
        }

        bool enqueueReply(const std::shared_ptr<Connection> &connection, std::string &&reply) const {
            if (!connection) {
                return false;
            }

            if (connection->replyBytes + reply.size() > ServerConfigure::MAX_PENDING_WRITE_BYTES) {
                spdlog::warn("fd={} output queue overflow: current={}, incoming={}, limit={}",
                             connection->socketFd,
                             connection->replyBytes,
                             reply.size(),
                             ServerConfigure::MAX_PENDING_WRITE_BYTES);
                return false;
            }
            connection->replyBytes += reply.size();
            connection->replyQueue.emplace_back(std::move(reply));

            enableClientWrite(connection);
            return true;
        }

        void enableClientWrite(const std::shared_ptr<Connection> &connection) const {
            if (!connection->hasRegisterWriteEvent()) {
                connection->registerEvents = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP | EPOLLOUT;
                eventPool.mod(connection->socketFd, connection->registerEvents, &connection->fdContext);
            }
        }

        void disableClientWrite(const std::shared_ptr<Connection> &connection) const {
            if (connection->hasRegisterWriteEvent()) {
                connection->registerEvents = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
                eventPool.mod(connection->socketFd, connection->registerEvents, &connection->fdContext);
            }
        }

        //only run singleton thread
        void closeConnection(const std::shared_ptr<Connection> &connection, const std::string &reason) {
            if (!connection) {
                return;
            }
            if (!connectionsMap.erase(connection->socketFd)) {
                spdlog::error("No closed connections were found");
                return;
            }
            spdlog::warn("close fd={}, reason={}", connection->socketFd, reason);
            eventPool.del(connection->socketFd);
            connection->closeSocketOnly();
        }

        void pushConnectionToCloseQueue(const std::shared_ptr<Connection> &connection, const char *reason) {
            if (!connection) {
                return;
            }
            pendingCloseQueue.push(PendingCloseStruct{connection, reason});
        }

        //only singleton thread
        void execPendingCloseConnection() {
            //spdlog::info("exec pending close connection");
            pendingCloseQueue.pop_all([this](const PendingCloseStruct &item) {
                closeConnection(item.connection, item.reason);
            });
        }
    };
} // namespace MiniRedis

struct ParameterOptions {
    std::optional<std::string_view> configPath;
    std::optional<std::string_view> database;
};

void Usage(const char* prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]\n\n"
              << "Options:\n"
              << "  -v, --version            Show version\n"
              << "  -c, --config <file>      Config file path (optional)\n"
              << "  -d, --database <file>    Database file path (optional)\n"
              << "  -h, --help               Show this help\n";
}

int main(const int argc, const char *argv[]) {

    constexpr option long_opts[] = {
        {"version", no_argument, nullptr, 'v'},
        {"config",  required_argument, nullptr, 'c'},
        {"database", required_argument, nullptr, 'd'},
        {nullptr,   0,                 nullptr,  0 }
    };

    int opt;
    ParameterOptions parameterOptions;
    while ((opt = getopt_long(argc, const_cast<char* const*>(argv),"vc:d:", long_opts, nullptr)) != -1) {
        switch (opt) {
            case 'v':
                std::cout << "Version " << MiniRedis::ServerConfigure::VERSION << std::endl;
                return 0;
            case 'c':
                parameterOptions.configPath = optarg;
                break;
            case 'm':
                parameterOptions.database = optarg;
                break;
            default:
                std::cout << "Unknown option: " << opt << std::endl;
                return -1;
        }
    }

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [tid %t] %v");
    spdlog::set_level(spdlog::level::info);

    MiniRedis::ServerConfigure::loader(parameterOptions.configPath.value_or(MiniRedis::ServerConfigure::DEFAULT_CONFIG_FILE_PATH));
    MiniRedis::KeyValueDumper::init();
    MiniRedis::Server::init();
    MiniRedis::Server::getInstance().recoverDatabasesFormBinaryFile(parameterOptions.database.value_or(MiniRedis::ServerConfigure::DEFAULT_DUMP_BINARY_FILE_PATH));

    try {
        MiniRedis::Server::getInstance().eventLoop();
    } catch (const std::exception &ex) {
        spdlog::critical("fatal: {}", ex.what());
        return 1;
    }

    return 0;
}
