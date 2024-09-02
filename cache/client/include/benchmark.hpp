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

void _warm(Client &client, int num_keys, int ttl, int num_operations, float ew, std::unique_ptr<Workload> &workload)
{
    for (int i = 0; i < num_operations; ++i)
    {
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);
        // std::cout << "Warm" << key << std::endl;
        client.Set(key, value, ttl, ew);
    }
}

void benchmark(Client &client, int num_keys, double lambda, int ttl, int num_operations, double alpha, float ew, std::string workload_str = "Poisson")
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

    _warm(client, num_keys, ttl, num_operations, ew, workload);

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i : tq::trange(num_operations))
    {
        // int key_index = std::rand() % num_keys;
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);

#ifdef DEBUG
        std::cout << workload->get_key(i).size() << ", " << workload->get_value(i).size() << ", " << workload->get_is_write(i) << "," << workload->get_interval(i).count() << std::endl;
#endif

        if (workload->get_is_write(i))
        {
            client.Set(key, value, ttl, ew);
        }
        else
        {
            std::string result = client.Get(key);
        }
#ifdef DEBUG
        std::cout << "interval: " << workload->get_interval(i).count() << std::endl;
#endif
        std::this_thread::sleep_for(workload->get_interval(i));
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    float mr = client.GetMR();
    float load = client.GetLoad();

    std::cout << "Benchmark Results:" << std::endl;
    std::cout << "Miss Ratio (MR): " << mr << std::endl;
    std::cout << "Load: " << load << " us" << std::endl;
}
