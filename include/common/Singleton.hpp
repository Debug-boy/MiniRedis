#ifndef MINIREDIS_SINGLETON_HPP
#define MINIREDIS_SINGLETON_HPP

#include <memory>
#include <mutex>

namespace MiniRedis {

    //The class template is not multi thread safe!!!
    template <typename T>
    class Singleton {
    public:
        template <typename... Args>
        static void init(Args&&... args) {
            if (!instance) {
                instance.reset(new T(std::forward<Args>(args)...));
            }
        }

        static T& getInstance() {
            if (!instance) {
                throw std::runtime_error("Singleton not initialized");
            }
            return *instance;
        }

        static void destroy() {
            instance.reset();
        }

        Singleton(const Singleton&) = delete;
        Singleton& operator=(const Singleton&) = delete;

    protected:
        Singleton() = default;
        ~Singleton() = default;

    private:
        static inline std::unique_ptr<T> instance;
    };;

}

#endif //MINIREDIS_SINGLETON_HPP
