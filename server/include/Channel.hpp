#ifndef MINIREDIS_CHANNEL_HPP
#define MINIREDIS_CHANNEL_HPP

namespace MiniRedis {

    struct Channel {

        enum class Type : unsigned int {
            TcpListen,
            UnixListen,
            Timer,
            Client
        };

        Type type;
        int fd;

        Channel(const Type t, const int f) : type(t), fd(f) {

        }

    };

}


#endif //MINIREDIS_CHANNEL_HPP