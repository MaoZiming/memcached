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
const int NUM_CPUS = 8;
int iter_loop = 1;

void _warm_thread(Client &client, int start, int end, int ttl, float ew, std::unique_ptr<Workload> &workload)
{
    int idx = 0;

    for (const auto &[key, value_size] : workload->keys_to_val_size)
    {
        if (idx < start || idx > end)
        {
            idx += 1;
            continue;
        }
        idx += 1;
        // std::string key = workload->get_key(i);
        // std::string value = workload->get_value(i);

#ifdef DEBUG
        std::cout << workload->get_key(i).size() << ", " << workload->get_value(i).size() << ", " << workload->get_is_write(i) << "," << workload->get_interval(i).count() << std::endl;
        std::cout << "Warm: " << key << ", " << idx - start << "/" << end - start << std::endl;
#endif
        client.SetWarm(key, workload->get_value_from_size(value_size), ttl); // Disable invalidate
        client.SetCache(key, workload->get_value_from_size(value_size), ttl);
    }
}

void _read_warm_thread(Client &client, int start, int end, int ttl, float ew, std::unique_ptr<Workload> &workload)
{
    int idx = 0;

    for (const auto &[key, value_size] : workload->keys_to_val_size)
    {
        if (idx < start || idx > end)
        {
            idx += 1;
            continue;
        }
        idx += 1;
        // std::string key = workload->get_key(i);
        // std::string value = workload->get_value(i);

#ifdef DEBUG
        std::cout
            << workload->get_key(i).size() << ", " << workload->get_value(i).size() << ", " << workload->get_is_write(i) << "," << workload->get_interval(i).count() << std::endl;
        std::cout << "Read Warm: " << key << ", " << idx - start << "/" << end - start << std::endl;
        client.GetWarmDB(key); // Disable invalidate
#endif
    }
}

void _warm(Client &client, int ttl, float ew, std::unique_ptr<Workload> &workload)
{
#ifdef DEBUG
    std::cout << "Start warming: " << workload->num_operations() << std::endl;
#endif

    int num_threads = NUM_CPUS;
    int operations_per_thread = workload->keys_to_val_size.size() / num_threads;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t)
    {
        int start = t * operations_per_thread;
        int end = (t == num_threads - 1) ? workload->keys_to_val_size.size() : start + operations_per_thread;

        threads.emplace_back(_warm_thread, std::ref(client), start, end, ttl, ew, std::ref(workload));
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    threads.clear();

    /*
    for (int t = 0; t < num_threads; ++t)
    {
        int start = t * operations_per_thread;
        int end = (t == num_threads - 1) ? workload->keys_to_val.size() : start + operations_per_thread;

        threads.emplace_back(_read_warm_thread, std::ref(client), start, end, ttl, ew, std::ref(workload));
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }
    */
}

void benchmark_thread(Client &client, int start_op, int end_op, int ttl, float ew, Workload *workload)
{
#ifdef DEBUG
    std::cout << "Start_op: " << start_op << ", end_op: " << end_op << std::endl;
#endif
    for (int itr = 0; itr < iter_loop; ++itr)
    {
        for (int i = start_op; i < end_op; ++i)
        {
            // std::cout << "start_op - i: " << i - start_op << std::endl;
            std::string key = workload->get_key(i);
            std::string value = workload->get_value(i);

            if (workload->get_is_write(i))
            {
#ifdef DEBUG
                std::cout << "Write: " << key << std::endl;
#endif
                client.Set(key, value, ttl, ew);
            }
            else
            {
                // std::cout << "Read: " << key << std::endl;
                std::string result = client.Get(key);
            }

#ifdef DEBUG
            std::cout << "interval: " << workload->get_interval(i).count() << std::endl;
#endif
            std::this_thread::sleep_for(workload->get_interval(i));
        }
    }
}

void benchmark(Client &client, int ttl, float ew, std::string workload_str = "Poisson", int num_threads = 1, bool skip_exp = false)
{

    std::unique_ptr<Workload> workload;

    if (workload_str == "Poisson")
    {
        workload = std::make_unique<PoissonWorkload>();
    }
    else if (workload_str == "Meta")
    {
        workload = std::make_unique<MetaWorkload>();
    }
    else if (workload_str == "PoissonMix")
    {
        iter_loop = 1;
        workload = std::make_unique<PoissonMixWorkload>();
    }
    else if (workload_str == "PoissonWrite")
    {
        workload = std::make_unique<PoissonWriteWorkload>();
    }
    else if (workload_str == "Twitter")
    {
        workload = std::make_unique<TwitterWorkload>();
    }
    else if (workload_str == "Tencent")
    {
        workload = std::make_unique<TencentWorkload>();
    }
    else if (workload_str == "IBM")
    {
        workload = std::make_unique<IBMWorkload>();
    }
    else if (workload_str == "Alibaba")
    {
        workload = std::make_unique<AlibabaWorkload>();
    }
    else if (workload_str == "WikiCDN")
    {
        ASSERT(false, "No write");
        workload = std::make_unique<WikiCDNWorkload>();
    }
    else
    {
        std::cerr << "Unrecognized workload: " << workload_str << std::endl;
        return;
    }

    workload->init();

    if (skip_exp)
        return;

    client.SetTTL(ttl);

    _warm(client, ttl, ew, workload);

    std::vector<std::thread> threads;
    int operations_per_thread = workload->num_operations() / num_threads;

    for (int t = 0; t < num_threads; ++t)
    {
        int start_op = t * operations_per_thread;
        int end_op = (t == num_threads - 1) ? workload->num_operations() : start_op + operations_per_thread;
        threads.emplace_back(benchmark_thread, std::ref(client), start_op, end_op, ttl, ew, workload.get());
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    float mr = client.GetMR();
    float load = client.GetLoad();

    std::cout << "Benchmark Results:" << std::endl;
    std::cout << "Miss Ratio (MR): " << mr << std::endl;
    std::cout << "Load: " << load << " us" << std::endl;
}
