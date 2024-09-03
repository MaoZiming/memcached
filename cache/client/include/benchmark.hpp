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

const std::string CACHE_ADDR = "10.128.0.34:50051";
const std::string DB_ADDR = "10.128.0.33:50051";
const int NUM_CPUS = 8;
int num_keys = 10000;              // Number of keys
double lambda = 50;                // Poisson distribution parameter (average request rate)
int num_operations = lambda * 100; // Number of operations to perform in the benchmark
double alpha = 1.3;

void _warm_thread(Client &client, int start, int end, int ttl, float ew, std::unique_ptr<Workload> &workload)
{
    for (int i = start; i < end; ++i)
    {
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);

#ifdef DEBUG
        std::cout << workload->get_key(i).size() << ", " << workload->get_value(i).size() << ", " << workload->get_is_write(i) << "," << workload->get_interval(i).count() << std::endl;
#endif
        client.Set(key, value, ttl, ew);
        client.SetCache(key, value, ttl);
    }
}

void _warm(Client &client, int num_keys, int ttl, int num_operations, float ew, std::unique_ptr<Workload> &workload)
{
    std::cout << "Start warming: " << num_operations << std::endl;

    int num_threads = NUM_CPUS;
    int operations_per_thread = num_operations / num_threads;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t)
    {
        int start = t * operations_per_thread;
        int end = (t == num_threads - 1) ? num_operations : start + operations_per_thread;

        threads.emplace_back(_warm_thread, std::ref(client), start, end, ttl, ew, std::ref(workload));
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }
}

void benchmark_thread(Client &client, int start_op, int end_op, int num_keys, int ttl, int num_operations, float ew, Workload *workload)
{
    std::cout << "Start_op: " << start_op << ", end_op: " << end_op << std::endl;
    for (int i = start_op; i < end_op; ++i)
    {
        // std::cout << "start_op - i: " << i - start_op << std::endl;
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);

        if (workload->get_is_write(i))
        {
            std::cout << "Write: " << key << std::endl;
            client.Set(key, value, ttl, ew);
        }
        else
        {
            std::cout << "Read: " << key << std::endl;
            std::string result = client.Get(key);
        }

#ifdef DEBUG
        std::cout << "interval: " << workload->get_interval(i).count() << std::endl;
#endif
        std::this_thread::sleep_for(workload->get_interval(i));
    }
}

void benchmark(Client &client, int num_keys, double lambda, int ttl, int num_operations, double alpha, float ew, std::string workload_str = "Poisson", int num_threads = 1)
{
    client.SetTTL(ttl);

    std::unique_ptr<Workload> workload;

    if (workload_str == "Poisson")
    {
        workload = std::make_unique<PoissonWorkload>(num_operations, alpha, lambda, num_keys);
    }
    else if (workload_str == "Meta")
    {
        workload = std::make_unique<MetaWorkload>(num_operations);
    }
    else
    {
        std::cerr << "Unrecognized workload: " << workload_str << std::endl;
        return;
    }

    _warm(client, num_keys, ttl, num_operations, ew, workload);

    std::vector<std::thread> threads;
    int operations_per_thread = num_operations / num_threads;

    for (int t = 0; t < num_threads; ++t)
    {
        int start_op = t * operations_per_thread;
        int end_op = (t == num_threads - 1) ? num_operations : start_op + operations_per_thread;
        threads.emplace_back(benchmark_thread, std::ref(client), start_op, end_op, num_keys, ttl, num_operations, ew, workload.get());
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
