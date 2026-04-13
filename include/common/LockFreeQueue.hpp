#pragma once


#include <atomic>
#include <cstddef>

namespace MiniRedis {

    //MPSC Safe!!!
    template<typename T>
    class LockFreeQueue
    {
    private:
        struct Node
        {
            T data;
            std::atomic<Node*> next;

            Node() : next(nullptr) {}
            Node(const T& v) : data(v), next(nullptr) {}
        };

        alignas(64) std::atomic<Node*> tail;
        alignas(64) Node* head;

    public:

        LockFreeQueue()
        {
            Node* stub = new Node();
            head = stub;
            tail.store(stub, std::memory_order_relaxed);
        }

        ~LockFreeQueue()
        {
            T tmp;
            while (pop(tmp));
            delete head;
        }

        void push(const T& value)
        {
            Node* node = new Node(value);

            Node* prev = tail.exchange(node, std::memory_order_acq_rel);

            prev->next.store(node, std::memory_order_release);
        }

        bool pop(T& result)
        {
            Node* next = head->next.load(std::memory_order_acquire);

            if (!next)
                return false;

            result = next->data;

            const Node* old = head;
            head = next;

            delete old;

            return true;
        }

        template<typename Func>
        size_t pop_all(Func&& func)
        {
            size_t count = 0;

            Node* next = head->next.load(std::memory_order_acquire);

            while (next)
            {
                func(next->data);

                Node* old = head;
                head = next;

                next = next->next.load(std::memory_order_acquire);

                delete old;

                count++;
            }

            return count;
        }

        bool empty()
        {
            return head->next.load(std::memory_order_acquire) == nullptr;
        }

    };

} // namespace net
