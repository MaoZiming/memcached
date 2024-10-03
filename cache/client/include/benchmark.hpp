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
#include "parser.hpp"
#include "load_tracker.hpp"

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

const std::string CACHE_ADDR = "10.128.0.39:50051";

const std::vector<std::string> CACHE_ADDRESSES = {
    "10.128.0.39:50051",
    "10.128.0.40:50051"};

const std::string DB_ADDR = "10.128.0.33:50051";
int warmup_factor = 5;

// If we do async.
const int NUM_CPUS = 4;

void _warm_thread_db(Client &client, int start, int end, int ttl, float ew, Workload *workload)
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
    }
}

void _warm_thread_cache(Client &client, int start, int end, int ttl, float ew, Workload *workload)
{
    // for (const auto &[key, value_size] : tq::tqdm(workload->keys_to_val_size))
    for (int i = start; i < end; i++)
    {
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);
        client.SetCacheWarm(key, value, ttl); // Need to set for both reads and writes.
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
        threads.emplace_back(_warm_thread_db, std::ref(client), start, end, ttl, ew, workload);
    }
    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }
    threads.clear();

    /* Provide a short warming of cache. */

    int num_warmup_operations = workload->num_operations() / warmup_factor;
    operations_per_thread = num_warmup_operations / num_threads;
    for (int t = 0; t < num_threads; ++t)
    {
        int start = t * operations_per_thread;
        int end = (t == num_threads - 1) ? num_warmup_operations : (t + 1) * operations_per_thread;
        threads.emplace_back(_warm_thread_cache, std::ref(client), start, end, ttl, ew, workload);
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
    std::vector<std::future<bool>> set_futures;
    std::vector<std::future<std::string>> get_futures;

    for (int a = 0; a < end_op - start_op; a++)
    {
        int i = a + start_op;
        std::string key = workload->get_key(i);
        std::string value = workload->get_value(i);

        if (workload->get_is_write(i))
        {
            if (client.get_tracker() && client.get_tracker()->is_oracle)
            {
                ew = workload->getWriteCountAfterIndex(i);
                assert(ew > 0);
            }
            else if (client.get_tracker() && ew == ADAPTIVE_EW)
            {
                ew = client.get_tracker()->get_ew(key, a);
            }

            set_futures.push_back(client.SetAsync(key, value, ttl, ew));
        }
        else
        {
            get_futures.push_back(client.GetAsync(key));
        }
        std::this_thread::sleep_for(workload->get_interval(i));
    }

    // Wait for all SetAsync futures to complete
    for (auto &future : set_futures)
    {
        try
        {
            bool result = future.get();
            // Optionally process the result
            // std::cout << "SetAsync result: " << result << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cerr << "SetAsync RPC failed: " << e.what() << std::endl;
        }
    }

    // Wait for all GetAsync futures to complete
    for (auto &future : get_futures)
    {
        try
        {
            std::string value = future.get();
            // Optionally process the value
            // std::cout << "GetAsync value: " << value << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetAsync RPC failed: " << e.what() << std::endl;
        }
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
            if (tracker->get_ew(key, i) == gold_tracker->get_ew(key, i))
            {
                correct += 1;
            }
            else
            {
                wrong += 1;
            }
            if ((C_U * tracker->get_ew(key, i) > C_I + C_M || tracker->get_ew(key, i) == -1) == (C_U * gold_tracker->get_ew(key, i) > C_I + C_M || tracker->get_ew(key, i) == -1))
                correct_pred += 1;
            else
            {
                wrong_pred += 1;
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

void benchmark(Client &client, int ttl, float ew, Parser &parser, int num_threads = 1, bool skip_exp = false)
{
    Workload *workload = parser.workload;

    if (skip_exp && client.get_tracker())
    {
        benchmark_ew_tracker(client.get_tracker(), workload);
    }

    workload->init(parser.scale_factor);

    if (skip_exp)
        return;

    client.SetTTL(ttl);
    std::cout << "Begin Warming: " << std::endl;
    _warm(client, ttl, ew, workload);

    std::cout << "Warming done. Sleep for 10 seconds: " << std::endl;
    // std::this_thread::sleep_for(std::chrono::seconds(10)); // Sleep for 10 seconds

    client.StartRecord();
    std::cout << "\nBegin Benchmarking: " << std::endl;
    std::vector<std::thread> threads;
    int num_warmup_operations = workload->num_operations() / warmup_factor;
    int num_operations = workload->num_operations() - num_warmup_operations;

    int operations_per_thread = num_operations / num_threads;

    auto start_time = std::chrono::steady_clock::now();

    // std::cout << "SStart time (since epoch): "
    //           << std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch()).count()
    //           << " ms\n";

    START_COLLECTION(std::string(parser.log_path), client.get_db_client(), client.get_cache_client());

    for (int t = 0; t < num_threads; ++t)
    {
        int start_op = t * operations_per_thread + num_warmup_operations;
        int end_op = (t == num_threads - 1) ? num_operations : start_op + operations_per_thread;
        threads.emplace_back(benchmark_thread_async, std::ref(client), start_op, end_op, ttl, ew, workload);
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    // End time measurement
    auto end_time = std::chrono::steady_clock::now();

    // std::cout << "EEnd time (since epoch): "
    //           << std::chrono::duration_cast<std::chrono::milliseconds>(end_time.time_since_epoch()).count()
    //           << " ms\n";

    // Calculate the e2e latency
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

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
    std::cout << "End-to-End Latency: " << duration << " ms" << std::endl;

    std::cout << "Average cache latency: " << client.GetCacheAverageLatency() << " us" << std::endl;
    std::cout << "Average DB latency: " << client.GetDBAverageLatency() << " us" << std::endl;

    std::string latency_message = "Average cache latency: " + std::to_string(client.GetCacheAverageLatency()) + " us";
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    latency_message = "Average DB latency: " + std::to_string(client.GetDBAverageLatency()) + " us";
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    latency_message = "Median cache latency: " + std::to_string(client.GetCacheMedianLatency()) + " us";
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    latency_message = "Median DB latency: " + std::to_string(client.GetDBMedianLatency()) + " us";
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    latency_message = "End-to-End Latency: " + std::to_string(duration) + " ms";
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    latency_message = "Num operations: " + std::to_string(num_operations);
    WRITE_TO_LOG(std::string(parser.log_path), "stats", latency_message);

    END_COLLECTION();
}
