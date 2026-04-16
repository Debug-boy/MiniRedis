#ifndef KVCOMMANDENGINE_HPP
#define KVCOMMANDENGINE_HPP

#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "KeyValueDumper.hpp"
#include "common/Resp.hpp"

namespace MiniRedis {

    class KvCommandEngine {
    public:
        using Handler = std::tuple<std::string,bool> (KvCommandEngine::*)(const std::vector<std::string> &);

        enum class CommandEnum {
            GET = 1 << 0,
            SET = 1 << 2,
            DEL = 1 << 3,
            EXISTS = 1 << 4,
            PING = 1 << 5,
            BGSAVE = 1 << 6,
        };

        struct CommandEntry {
            std::string_view name;
            CommandEnum commandEnum;
            Handler handler;
        };

        std::unordered_map<std::string, std::string> store;

        std::atomic<uint64_t> totalSetCount{0};
        std::atomic<uint64_t> totalDelCount{0};
        std::atomic<uint64_t> durationSetCount{0};
        std::atomic<uint64_t> durationDelCount{0};

        std::string execute(const std::vector<std::string> &argv) {
            if (argv.empty()) {
                return Resp::Wrapper::error("empty command");
            }
            const auto commandIndex = perfectHash(argv[0]);
            if (commandIndex == -1) {
                return Resp::Wrapper::error("unknown command '" + argv[0] + "'");
            }
            const CommandEntry& commandEntry = commandTable[commandIndex];
            auto [executedResult,ok] = (this->*commandTable[commandIndex].handler)(argv);
            if (ok) {
                if (commandEntry.commandEnum == CommandEnum::SET) {
                    ++totalSetCount;
                    ++durationSetCount;
                }else if (commandEntry.commandEnum == CommandEnum::DEL) {
                    ++totalDelCount;
                    ++durationDelCount;
                }
            }
            return executedResult;
        }

        void resetDurationDirtyCount() {
            durationSetCount = 0;
            durationDelCount = 0;
        }

        auto accumulateDurationDirtyCount() const {
            return durationSetCount + durationDelCount;
        }

    private:
        std::tuple<std::string,bool> handleGet(const std::vector<std::string> &argv) {
            if (argv.size() != 2)
                return std::make_tuple(Resp::Wrapper::error("wrong number of arguments for 'get'"),false);

            const auto it = store.find(argv[1]);
            if (it == store.end())
                return std::make_tuple(Resp::Wrapper::nil(),false);

            return std::make_tuple(Resp::Wrapper::bulkString(it->second),true);
        }

        std::tuple<std::string,bool> handleSet(const std::vector<std::string> &argv) {
            if (argv.size() != 3)
                return std::make_tuple(Resp::Wrapper::error("wrong number of arguments for 'set'"),false);

            store[argv[1]] = argv[2];
            return std::make_tuple(Resp::Wrapper::ok(),false);
        }

        std::tuple<std::string,bool> handleDel(const std::vector<std::string> &argv) {
            if (argv.size() < 2)
                return std::make_tuple(Resp::Wrapper::error("wrong number of arguments for 'del'"),false);

            long long removed = 0;
            for (size_t i = 1; i < argv.size(); i++)
                removed += store.erase(argv[i]);

            return std::make_tuple(Resp::Wrapper::integer(removed),true);
        }

        std::tuple<std::string,bool> handleExists(const std::vector<std::string> &argv) {
            if (argv.size() < 2)
                return std::make_tuple(Resp::Wrapper::error("wrong number of arguments for 'exists'"),false);

            long long count = 0;
            for (size_t i = 1; i < argv.size(); i++)
                if (store.contains(argv[i]))
                    count++;

            return std::make_tuple(Resp::Wrapper::integer(count),true);
        }

        std::tuple<std::string,bool> handlePing(const std::vector<std::string> &) {
            return std::make_tuple(Resp::Wrapper::simpleString("PONG"),true);
        }

        std::tuple<std::string,bool> handleBackgroundSave(const std::vector<std::string> &)  {
            if (!KeyValueDumper::getInstance().dump(
                ServerConfigure::DUMP_SAVED_BINARY_FILE_PATH,
                ServerConfigure::VERSION,store,
                true
                )) {
                return std::make_tuple(Resp::Wrapper::error("ERROR"),false);
            }
            return std::make_tuple(Resp::Wrapper::ok(),true);
        }


        static constexpr int perfectHash(const std::string_view s) {
            switch (s[0]) {
                case 'G': return 0;
                case 'S': return 1;
                case 'D': return 2;
                case 'E': return 3;
                case 'P': return 4;
                case 'B': return 5;
                default: return -1;
            }
        }

        static constexpr CommandEntry commandTable[] = {
            {"GET",CommandEnum::GET, &KvCommandEngine::handleGet},
            {"SET",CommandEnum::SET, &KvCommandEngine::handleSet},
            {"DEL",CommandEnum::DEL, &KvCommandEngine::handleDel},
            {"EXISTS",CommandEnum::EXISTS, &KvCommandEngine::handleExists},
            {"PING",CommandEnum::PING, &KvCommandEngine::handlePing},
            {"BGSAVE",CommandEnum::BGSAVE,&KvCommandEngine::handleBackgroundSave},
        };
    };
}


#endif //KVCOMMANDENGINE_HPP
