#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool
{
public:
  ThreadPool(size_t num_threads); // Constructor declaration
  ~ThreadPool();                  // Destructor declaration

  // Enqueue a new task to the thread pool
  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args)
      -> std::future<typename std::result_of<F(Args...)>::type>;

private:
  // Workers (threads)
  std::vector<std::thread> workers_;

  // Task queue
  std::queue<std::function<void()>> tasks_;

  // Synchronization
  std::mutex queue_mutex_;
  std::condition_variable condition_;
  std::atomic<bool> stop_;
};

// Include template definitions
#include "thread_pool.tpp"
