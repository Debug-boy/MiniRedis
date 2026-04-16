#ifndef RESP_HPP
#define RESP_HPP

#include <stdexcept>
#include <string>
#include <vector>

namespace MiniRedis {
    namespace Resp {
        class Wrapper {
        public:
            static std::string simpleString(const std::string &s) { return "+" + s + "\r\n"; }
            static std::string pong(const std::string &s) { return "+PONG\r\n"; }
            static std::string ok() { return "+OK\r\n"; }
            static std::string error(const std::string &msg) { return "-ERR " + msg + "\r\n"; }

            static std::string bulkString(const std::string &s) {
                return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
            }

            static std::string nil() { return "$-1\r\n"; }
            static std::string integer(const long long value) { return ":" + std::to_string(value) + "\r\n"; }
        };

        class Parser {
        public:
            static bool parseOnce(const std::string_view &buffer, size_t &consumed, std::vector<std::string> &out) {
                consumed = 0;
                out.clear();
                if (buffer.empty()) return false;
                if (buffer[0] != '*') throw std::runtime_error("protocol error: expected '*'");

                size_t pos = 1;
                int argc = 0;
                if (!readIntegerLine(buffer, pos, argc)) return false;
                if (argc <= 0) throw std::runtime_error("protocol error: invalid array length");

                out.reserve(static_cast<size_t>(argc));
                for (int i = 0; i < argc; ++i) {
                    if (pos >= buffer.size()) return false;
                    if (buffer[pos] != '$') throw std::runtime_error("protocol error: expected '$'");
                    ++pos;

                    int len = 0;
                    if (!readIntegerLine(buffer, pos, len)) return false;
                    if (len < 0) throw std::runtime_error("protocol error: invalid bulk length");
                    if (buffer.size() < pos + static_cast<size_t>(len) + 2) return false;

                    out.emplace_back(buffer.substr(pos, static_cast<size_t>(len)));
                    pos += static_cast<size_t>(len);
                    if (buffer[pos] != '\r' || buffer[pos + 1] != '\n')
                        throw std::runtime_error(
                            "protocol error: expected CRLF after bulk string");
                    pos += 2;
                }

                consumed = pos;
                return true;
            }

        private:
            static bool readIntegerLine(const std::string_view &buffer, size_t &pos, int &value) {
                const size_t crlf = buffer.find("\r\n", pos);
                if (crlf == std::string::npos) return false;
                if (crlf == pos) throw std::runtime_error("protocol error: empty integer");

                bool negative = false;
                size_t start = pos;
                if (buffer[start] == '-') {
                    negative = true;
                    ++start;
                }
                if (start >= crlf) throw std::runtime_error("protocol error: invalid integer");

                long long result = 0;
                for (size_t i = start; i < crlf; ++i) {
                    const auto ch = static_cast<unsigned char>(buffer[i]);
                    if (!std::isdigit(ch)) throw std::runtime_error("protocol error: invalid integer char");
                    result = result * 10 + (ch - '0');
                    if (result > 2147483647LL) throw std::runtime_error("protocol error: integer overflow");
                }

                value = static_cast<int>(negative ? -result : result);
                pos = crlf + 2;
                return true;
            }
        };
    }
}


#endif //RESP_HPP
