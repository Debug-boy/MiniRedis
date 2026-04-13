#include <arpa/inet.h>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <netinet/tcp.h>

namespace {

std::string makeError(const std::string &op) {
    return op + ": " + std::strerror(errno);
}

std::string buildRespArray(const std::vector<std::string> &args) {
    std::string out;
    out.reserve(64);
    out += "*" + std::to_string(args.size()) + "\r\n";
    for (const auto &arg : args) {
        out += "$" + std::to_string(arg.size()) + "\r\n";
        out += arg;
        out += "\r\n";
    }
    return out;
}

std::vector<std::string> splitByWhitespace(const std::string &line) {
    std::vector<std::string> parts;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        parts.push_back(token);
    }
    return parts;
}

void sendAll(int fd, const std::string &data) {
    size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) continue;
        throw std::runtime_error(makeError("send"));
    }
}

std::string readLineCrlf(int fd) {
    std::string line;
    char ch = 0;
    while (true) {
        const ssize_t n = ::recv(fd, &ch, 1, 0);
        if (n == 0) throw std::runtime_error("connection closed by peer");
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(makeError("recv"));
        }
        line.push_back(ch);
        if (line.size() >= 2 && line[line.size() - 2] == '\r' && line[line.size() - 1] == '\n') {
            line.resize(line.size() - 2);
            return line;
        }
    }
}

std::string readNBytes(int fd, size_t n) {
    std::string out;
    out.resize(n);
    size_t got = 0;
    while (got < n) {
        const ssize_t r = ::recv(fd, out.data() + got, n - got, 0);
        if (r == 0) throw std::runtime_error("connection closed by peer");
        if (r < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(makeError("recv"));
        }
        got += static_cast<size_t>(r);
    }
    return out;
}

std::string parseRespReply(int fd) {
    std::string firstLine = readLineCrlf(fd);
    if (firstLine.empty()) {
        throw std::runtime_error("protocol error: empty reply");
    }

    const char prefix = firstLine[0];
    std::string_view body(firstLine.c_str() + 1, firstLine.size() - 1);

    switch (prefix) {
        case '+':
            return "(simple string) " + std::string(body);
        case '-':
            return "(error) " + std::string(body);
        case ':':
            return "(integer) " + std::string(body);
        case '$': {
            long long len = std::stoll(std::string(body));
            if (len == -1) return "(nil)";
            if (len < -1) throw std::runtime_error("protocol error: invalid bulk length");
            std::string bulk = readNBytes(fd, static_cast<size_t>(len));
            const std::string crlf = readNBytes(fd, 2);
            if (crlf != "\r\n") throw std::runtime_error("protocol error: bulk string missing CRLF");
            return "(bulk string) " + bulk;
        }
        default:
            throw std::runtime_error("protocol error: unsupported reply type");
    }
}

int connectServer(const std::string &ip, uint16_t port) {

    const int fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd == -1) {
        throw std::runtime_error(makeError("socket"));
    }

    constexpr int option_value = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &option_value, sizeof(option_value)) == -1) {
        throw std::runtime_error("setsockopt(TCP_NODELAY) failed!");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        throw std::runtime_error("invalid ip: " + ip);
    }

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == -1) {
        const std::string err = makeError("connect");
        ::close(fd);
        throw std::runtime_error(err);
    }

    return fd;
}

} // namespace

int main(int argc, char *argv[]) {
    const std::string ip = argc >= 2 ? argv[1] : "127.0.0.1";
    const uint16_t port = argc >= 3 ? static_cast<uint16_t>(std::stoi(argv[2])) : 9736;

    try {
        int fd = connectServer(ip, port);
        std::cout << "Connected to " << ip << ":" << port << "\n";
        std::cout << "Supported commands: PING [msg], SET key value, GET key, EXISTS key [key...], DEL key [key...], QUIT\n";

        std::string line;
        while (true) {
            std::cout << "> " << std::flush;
            if (!std::getline(std::cin, line)) break;
            if (line.empty()) continue;

            auto args = splitByWhitespace(line);
            if (args.empty()) continue;

            std::string cmdUpper = args[0];
            for (char &c : cmdUpper) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

            if (cmdUpper == "QUIT" || cmdUpper == "EXIT") break;

            sendAll(fd, buildRespArray(args));
            std::cout << parseRespReply(fd) << "\n";
        }

        ::close(fd);
        std::cout << "Bye.\n";
    } catch (const std::exception &ex) {
        std::cerr << "Client error: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
