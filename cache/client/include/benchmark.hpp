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

const std::string CACHE_ADDR = "10.128.0.34:50051";
const std::string DB_ADDR = "10.128.0.33:50051";

// Function to generate Poisson distributed request intervals
std::vector<std::chrono::milliseconds> generatePoissonIntervals(int num_requests, double lambda)
{
    std::vector<std::chrono::milliseconds> intervals(num_requests);
    std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
    std::exponential_distribution<double> distribution(lambda);

    for (int i = 0; i < num_requests; ++i)
    {
        intervals[i] = std::chrono::milliseconds(static_cast<int>(distribution(generator) * 1000));
    }

    return intervals;
}

void _warm(Client &client, int num_keys, int ttl, int num_operations, float ew)
{
    for (int i = 0; i < num_keys; ++i)
    {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        client.Set(key, value, ttl, ew);
    }
}

void benchmark(Client &client, int num_keys, double lambda, int ttl, int num_operations, double alpha, float ew)
{
    client.SetTTL(ttl);
    auto intervals = generatePoissonIntervals(num_operations, lambda);

    _warm(client, num_keys, ttl, num_operations, ew);
    FastZipf zipf_gen(alpha, num_keys - 1); // 0-indexed: -1
    std::vector<int> zipf_values = zipf_gen.generate_zipf(num_operations);
    std::vector<std::string> keys(num_keys);
    for (int i = 0; i < num_keys; ++i)
    {
        keys[i] = "key" + std::to_string(i);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i : tq::trange(num_operations))
    {
        // int key_index = std::rand() % num_keys;
        int key_index = zipf_values[i];
        std::string key = keys[key_index];
        std::string value = "value" + std::to_string(key_index);

        bool do_set = false;
        if (key_index % 2 == 0)
        {
            // Even key_index: 90% chance to read
            do_set = (std::rand() % 100) >= 90; // 10% chance to set
        }
        else
        {
            // Odd key_index: 10% chance to read
            do_set = (std::rand() % 100) < 90; // 90% chance to set
        }
        if (do_set)
        {
            client.Set(key, value, ttl, ew);
        }
        else
        {
            std::string result = client.Get(key);
        }
#ifdef DEBUG
        std::cout << "interval: " << intervals[i].count() << std::endl;
#endif
        std::this_thread::sleep_for(intervals[i]);
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    float mr = client.GetMR();
    float load = client.GetLoad();

    std::cout << "Benchmark Results:" << std::endl;
    std::cout << "Miss Ratio (MR): " << mr << std::endl;
    std::cout << "Load: " << load << " us" << std::endl;
}
