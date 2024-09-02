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

#include "benchmark.hpp"

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <workload>" << std::endl;
        return 1;
    }

    std::string workload = argv[1]; // Capture workload string from the command line

    // Create a channel to connect to the server
    Client client(grpc::CreateChannel(CACHE_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  grpc::CreateChannel(DB_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  nullptr);

    int num_keys = 20;          // Number of keys
    double lambda = 5.0;        // Poisson distribution parameter (average request rate)
    int ttl = LONG_TTL;         // TTL in seconds
    int num_operations = 10000; // Number of operations to perform in the benchmark
    double alpha = 1.3;
    float ew = INVALIDATE_EW;

    // Pass the workload string to the benchmark function
    benchmark(client, num_keys, lambda, ttl, num_operations, alpha, ew, workload);

    return 0;
}
