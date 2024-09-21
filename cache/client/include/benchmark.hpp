#pragma once

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>

#include <vector>
#include <random>
#include <chrono>
#include <thread>

#include "policy.hpp"
#include "client.hpp"
#include "zipf.hpp"
#include "tqdm.hpp"
#include "workload.hpp"

const int C_I = 10;
const int C_U = 46;
const int C_M = C_I + C_U;

#define ASSERT(condition, message)             \
    do                                         \
    {                                          \
        if (!(condition))                      \
        {                                      \
            std::cerr << message << std::endl; \
            assert(condition);                 \
        }                                      \
    } while (false)

const std::string CACHE_ADDR = "10.128.0.34:50051";
const std::string DB_ADDR = "10.128.0.33:50051";

// If we do async.
const int NUM_CPUS = 4;

void _warm_thread(Client &client, int start, int end, int ttl, float ew, Workload *workload)
{
    int idx = 0;
    // for (const auto &[key, value_size] : tq::tqdm(workload->keys_to_val_size))
    for (const auto &[key, value_size] : workload->keys_to_val_size)
    {
        if (idx < start || idx >= end)
        {
            idx += 1;
            continue;
        }
        idx += 1;
        client.SetWarm(key, workload->get_value_from_size(value_size), ttl); // Disable invalidate
        client.SetCache(key, workload->get_value_from_size(value_size), ttl);
    }
}

void _warm(Client &client, int ttl, float ew, Workload *workload)
{
    int num_threads = NUM_CPUS;
    int operations_per_thread = workload->keys_to_val_size.size() / num_threads;
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t)
    {
        int start = t * operations_per_thread;
        int end = (t == num_threads - 1) ? workload->keys_to_val_size.size() : (t + 1) * operations_per_thread;
        threads.emplace_back(_warm_thread, std::ref(client), start, end, ttl, ew, workload);
    }
    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }
    threads.clear();
}

void benchmark_thread_async(Client &client, int start_op, int end_op, int ttl, float ew, Workload *workload)
{
    // for (int a : tq::trange(end_op - start_op))
    for (int a = 0; a < end_op - start_op; a++)
    {
        int i = a + start_op;
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);

        if (workload->get_is_write(i))
        {
            client.SetAsync(key, value, ttl, ew);
        }
        else
        {
            client.GetAsync(key);
        }
        std::this_thread::sleep_for(workload->get_interval(i));
    }
}

void benchmark_ew_tracker(Tracker *tracker, Workload *workload)
{
    // Only to get the overhead of the tracker.
    workload->init_ew();
    Tracker *gold_tracker = new ExactRWTracker();
    tracker->update(workload->get_num_keys());
    int correct = 0;
    int correct_pred = 0;
    int wrong = 0;
    int wrong_pred = 0;
    for (int i : tq::trange(workload->num_operations()))
    // for (int i = 0; i < workload->num_operations(); i++)
    {
        if (workload->get_is_write(i))
        {
            std::string key = workload->get_key(i);
            tracker->write(key);
            gold_tracker->write(key);
            if (tracker->get_ew(key) == gold_tracker->get_ew(key))
            {
                correct += 1;
            }
            else
            {
                wrong += 1;
            }
            if ((C_U * tracker->get_ew(key) > C_I + C_M || tracker->get_ew(key) == -1) == (C_U * gold_tracker->get_ew(key) > C_I + C_M || tracker->get_ew(key) == -1))
                correct_pred += 1;
            else
            {
                wrong_pred += 1;
                // std::cout << key << ": tracker ew: " << tracker->get_ew(key) << ", gold ew: " << gold_tracker->get_ew(key) << ", in TopK? " << tracker->is_in_topK(key) << std::endl;
            }
        }
        else
        {
            tracker->read(workload->get_key(i));
            gold_tracker->read(workload->get_key(i));
        }
    }
    std::cout << "Correct rate: " << (float)correct / (correct + wrong) << std::endl;
    std::cout << "Correct pred rate: " << (float)correct_pred / (correct_pred + wrong_pred) << std::endl;
    std::cout << "Storage serving: " << (float)gold_tracker->get_storage_overhead() / tracker->get_storage_overhead() << std::endl;
    tracker->report_latencies();
    std::cout << "gold_tracker: " << std::endl;
    gold_tracker->report_latencies();
    std::cout << "gold_tracker storage: " << gold_tracker->get_storage_overhead() << std::endl;
}

void benchmark(Client &client, int ttl, float ew, std::string workload_str = "Poisson", int num_threads = 1, bool skip_exp = false)
{
    Workload *workload;

    if (workload_str == "Poisson")
    {
        workload = new PoissonWorkload();
    }
    else if (workload_str == "Meta")
    {
        workload = new MetaWorkload();
    }
    else if (workload_str == "PoissonMix")
    {
        workload = new PoissonMixWorkload();
    }
    else if (workload_str == "PoissonWrite")
    {
        workload = new PoissonWriteWorkload();
    }
    else if (workload_str == "Twitter")
    {
        workload = new TwitterWorkload();
    }
    else if (workload_str == "Tencent")
    {
        workload = new TencentWorkload();
    }
    else if (workload_str == "IBM")
    {
        workload = new IBMWorkload();
    }
    else if (workload_str == "Alibaba")
    {
        workload = new AlibabaWorkload();
    }
    else if (workload_str == "WikiCDN")
    {
        ASSERT(false, "WikiCDN trace has No write");
        workload = new WikiCDNWorkload();
    }
    else
    {
        std::cerr << "Unrecognized workload: " << workload_str << std::endl;
        return;
    }
    if (skip_exp && client.get_tracker())
    {
        benchmark_ew_tracker(client.get_tracker(), workload);
    }

    if (skip_exp)
        return;

    workload->init();

    client.SetTTL(ttl);
    std::cout << "Begin Warming: " << std::endl;
    _warm(client, ttl, ew, workload);

    std::cout << "Warming done. Sleep for 10 seconds: " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10)); // Sleep for 10 seconds

    client.StartRecord();
    std::cout << "\nBegin Benchmarking: " << std::endl;
    std::vector<std::thread> threads;
    int operations_per_thread = workload->num_operations() / num_threads;

    for (int t = 0; t < num_threads; ++t)
    {
        int start_op = t * operations_per_thread;
        int end_op = (t == num_threads - 1) ? workload->num_operations() : start_op + operations_per_thread;
        threads.emplace_back(benchmark_thread_async, std::ref(client), start_op, end_op, ttl, ew, workload);
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    float mr = client.GetMR();
    std::tuple<int, int> stats = client.GetFreshnessStats();
    int invalidates = std::get<0>(stats);
    int updates = std::get<1>(stats);
    int load = client.GetLoad();

    std::cout << "\nResults: " << std::endl;
    std::cout << "Miss Ratio (MR): " << mr << std::endl;

    std::cout << "Invalidates: " << invalidates << std::endl;
    std::cout << "Updates: " << updates << std::endl;

    std::cout << "Load: " << load << std::endl;
}
