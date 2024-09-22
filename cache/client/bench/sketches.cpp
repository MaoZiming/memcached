#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <vector>
#include <random>
#include <chrono> // For timing
#include <thread>
#include "policy.hpp"
#include "client.hpp"
#include "zipf.hpp"
#include "tqdm.hpp"
#include "benchmark.hpp"

int main(int argc, char *argv[])
{
    Parser parser(argc, argv);

    // Create a channel to connect to the server
    Client client(grpc::CreateChannel(CACHE_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  grpc::CreateChannel(DB_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  parser.tracker);

    float ew = ADAPTIVE_EW;
    int ttl = LONG_TTL;
    // alpha = 1.0;

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    // Pass the workload string to the benchmark function
    benchmark(client, ttl, ew, parser, NUM_CPUS, true);

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    // Print the time taken by the benchmark
    std::cout << "Benchmark time: " << elapsed_time.count() << " seconds" << std::endl;

    std::cout << "Tracker overhead: " << parser.tracker->get_storage_overhead() << " bytes" << std::endl;

    return 0;
}
