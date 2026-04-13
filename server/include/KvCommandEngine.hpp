#ifndef KVCOMMANDENGINE_HPP
#define KVCOMMANDENGINE_HPP

#include <unordered_map>

#include "common/Resp.hpp"

namespace MiniRedis {
    class KvCommandEngine {
        using Handler = std::string (KvCommandEngine::*)(const std::vector<std::string> &);

        struct CommandEntry {
            std::string_view name;
            Handler handler;
        };

        std::unordered_map<std::string, std::string> store;

    public:
        std::string execute(const std::vector<std::string> &argv) {
            if (argv.empty()) {
                return Resp::Wrapper::error("empty command");
            }
            const auto commandIndex = perfectHash(argv[0]);
            if (commandIndex == -1) {
                return Resp::Wrapper::error("unknown command '" + argv[0] + "'");
            }
            return (this->*commandTable[commandIndex].handler)(argv);
        }

    private:
        std::string handleGet(const std::vector<std::string> &argv) {
            if (argv.size() != 2)
                return Resp::Wrapper::error("wrong number of arguments for 'get'");

            auto it = store.find(argv[1]);
            if (it == store.end())
                return Resp::Wrapper::nil();

            return Resp::Wrapper::bulkString(it->second);
        }

        std::string handleSet(const std::vector<std::string> &argv) {
            if (argv.size() != 3)
                return Resp::Wrapper::error("wrong number of arguments for 'set'");

            store[argv[1]] = argv[2];
            return Resp::Wrapper::ok();
        }

        std::string handleDel(const std::vector<std::string> &argv) {
            if (argv.size() < 2)
                return Resp::Wrapper::error("wrong number of arguments for 'del'");

            long long removed = 0;
            for (size_t i = 1; i < argv.size(); i++)
                removed += store.erase(argv[i]);

            return Resp::Wrapper::integer(removed);
        }

        std::string handleExists(const std::vector<std::string> &argv) {
            if (argv.size() < 2)
                return Resp::Wrapper::error("wrong number of arguments for 'exists'");

            long long count = 0;
            for (size_t i = 1; i < argv.size(); i++)
                if (store.contains(argv[i]))
                    count++;

            return Resp::Wrapper::integer(count);
        }

        std::string handlePing(const std::vector<std::string> &) {
            return Resp::Wrapper::simpleString("PONG");
        }

        static constexpr int perfectHash(const std::string_view s) {
            switch (s[0]) {
                case 'G': return 0;
                case 'S': return 1;
                case 'D': return 2;
                case 'E': return 3;
                case 'P': return 4;
                default: return -1;
            }
        }

        static constexpr CommandEntry commandTable[] = {
            {"GET", &KvCommandEngine::handleGet},
            {"SET", &KvCommandEngine::handleSet},
            {"DEL", &KvCommandEngine::handleDel},
            {"EXISTS", &KvCommandEngine::handleExists},
            {"PING", &KvCommandEngine::handlePing},
        };
    };
}


#endif //KVCOMMANDENGINE_HPP
