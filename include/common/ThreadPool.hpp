#ifndef THREADPOOL_HPP
#define THREADPOOL_HPP

#include <thread>
#include <vector>
#include <future>
#include <functional>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>

namespace MiniRedis {

class ThreadPool
{
public:
    using Task = std::function<void()>;

    explicit ThreadPool(size_t n = std::thread::hardware_concurrency() * 2) : stop(false), tasks_in_flight(0)
    {
        if (n == 0) n = 1;
        workers.reserve(n);

        for (size_t i = 0; i < n; ++i)
        {
            workers.emplace_back([this] { worker_loop(); });
        }
    }

    ~ThreadPool()
    {
        {
            std::lock_guard<std::mutex> lock(queue_mtx);
            stop = true;
        }
        cv.notify_all();

        for (auto& t : workers)
        {
            if (t.joinable())
                t.join();
        }
    }

    template<class F, class... Args>
    auto submit(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type>
    {
        using Ret = typename std::invoke_result<F, Args...>::type;

        auto task = std::make_shared<std::packaged_task<Ret()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        std::future<Ret> res = task->get_future();

        {
            std::lock_guard<std::mutex> lock(queue_mtx);
            queue.push([task]() { (*task)(); });
        }

        cv.notify_one();
        return res;
    }

    void submitBatch(const std::vector<Task>& tasks)
    {
        if (tasks.empty()) return;

        {
            std::lock_guard<std::mutex> lock(queue_mtx);
            for (const auto& t : tasks)
            {
                queue.push(t);
            }
            tasks_in_flight.fetch_add(tasks.size(), std::memory_order_relaxed);
        }

        cv.notify_all();
    }

    void wait()
    {
        std::unique_lock<std::mutex> lock(wait_mtx);
        wait_cv.wait(lock, [this] {
            return tasks_in_flight.load(std::memory_order_acquire) == 0;
        });
    }

private:
    void worker_loop()
    {
        while (true)
        {
            Task task;

            {
                std::unique_lock<std::mutex> lock(queue_mtx);

                cv.wait(lock, [this] {
                    return stop || !queue.empty();
                });

                if (stop && queue.empty())
                    return;

                task = std::move(queue.front());
                queue.pop();
            }

            if (task)
            {
                task();
                finish_task();
            }
        }
    }

    void finish_task()
    {
        if (tasks_in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            std::lock_guard<std::mutex> lock(wait_mtx);
            wait_cv.notify_all();
        }
    }

private:
    std::vector<std::thread> workers;

    std::queue<Task> queue;
    std::mutex queue_mtx;
    std::condition_variable cv;

    std::atomic<bool> stop{false};
    std::atomic<size_t> tasks_in_flight{0};

    std::mutex wait_mtx;
    std::condition_variable wait_cv;
};

} // namespace net

#endif