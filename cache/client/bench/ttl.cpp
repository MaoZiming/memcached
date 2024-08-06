#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>

#include <vector>
#include <random>
#include <chrono>
#include <thread>

#include "policy.hpp"
#include "client.hpp"

// Function to generate Poisson distributed request intervals
std::vector<std::chrono::milliseconds> generatePoissonIntervals(int num_requests, double lambda)
{
    std::vector<std::chrono::milliseconds> intervals(num_requests);
    std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
    std::poisson_distribution<int> distribution(lambda);

    for (int i = 0; i < num_requests; ++i)
    {
        intervals[i] = std::chrono::milliseconds(distribution(generator));
    }

    return intervals;
}

void _warm(Client &client, int num_keys, int ttl, int num_operations)
{
    for (int i = 0; i < num_keys; ++i)
    {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        client.Set(key, value, ttl);
    }
}

void benchmark(Client &client, int num_keys, double lambda, int ttl, int num_operations)
{
    client.SetTTL(ttl);
    auto intervals = generatePoissonIntervals(num_operations, lambda);

    _warm(client, num_keys, ttl, num_operations);

    std::vector<std::string> keys(num_keys);
    for (int i = 0; i < num_keys; ++i)
    {
        keys[i] = "key" + std::to_string(i);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_operations; ++i)
    {
        // Randomly choose to perform Get or Set
        bool do_set = (std::rand() % 2) == 0;
        int key_index = std::rand() % num_keys;
        std::string key = keys[key_index];
        std::string value = "value" + std::to_string(key_index);

        if (do_set)
        {
            client.Set(key, value, ttl);
        }
        else
        {
            std::string result = client.Get(key);
        }
        std::this_thread::sleep_for(intervals[i]);
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    float mr = client.GetMR();
    float load = client.GetLoad();

    std::cout << "Benchmark Results:" << std::endl;
    std::cout << "Miss Ratio (MR): " << mr << std::endl;
    std::cout << "Load: " << load << " seconds" << std::endl;
}

int main(int argc, char *argv[])
{
    Tracker tracker;

    // Create a channel to connect to the server
    Client client(grpc::CreateChannel("10.128.0.34:50051",
                                      grpc::InsecureChannelCredentials()),
                  grpc::CreateChannel("10.128.0.33:50051",
                                      grpc::InsecureChannelCredentials()),
                  &tracker);

    int num_keys = 1000;        // Number of keys
    double lambda = 5.0;        // Poisson distribution parameter (average request rate)
    int ttl = 10;               // TTL in seconds
    int num_operations = 10000; // Number of operations to perform in the benchmark

    benchmark(client, num_keys, lambda, ttl, num_operations);

    return 0;
}