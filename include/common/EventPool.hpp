#ifndef EVENTPOOL_HPP
#define EVENTPOOL_HPP

#include <cstring>
#include <stdexcept>
#include <sys/epoll.h>
#include <unistd.h>

#include "common/SysError.hpp"

namespace MiniRedis {

    class EventPool {
    private:
        int epoll_fd = -1;

    public:
        EventPool() : epoll_fd(::epoll_create1(0)) {
            if (epoll_fd == -1) {
                throw std::runtime_error(getSysLastError("epoll_create1"));
            }
        }

        ~EventPool() {
            if (epoll_fd != -1) {
                ::close(epoll_fd);
                epoll_fd = -1;
            }
        }

        void ctl(const int op, const int fd, epoll_event *ev) const {
            if (::epoll_ctl(epoll_fd, op, fd, ev) == -1) {
                throw std::runtime_error(getSysLastError("epoll_ctl"));
            }
        }

        void add(const int fd, const uint32_t events, void *ptr) const {
            epoll_event ev{};
            ev.events = events;
            ev.data.ptr = ptr;
            ctl(EPOLL_CTL_ADD, fd, &ev);
        }

        void mod(const int fd, const uint32_t events, void *ptr) const {
            epoll_event ev{};
            ev.events = events;
            ev.data.ptr = ptr;
            ctl(EPOLL_CTL_MOD, fd, &ev);
        }

        void del(const int fd) const {
            epoll_event ev{};
            ctl(EPOLL_CTL_DEL, fd, &ev);
        }

        int wait(epoll_event *events, const int maxEvents, const int timeoutMilliSecond = -1) const {
            return ::epoll_wait(epoll_fd, events, maxEvents, timeoutMilliSecond);
        }
    };
}

#endif //EVENTPOOL_HPP
