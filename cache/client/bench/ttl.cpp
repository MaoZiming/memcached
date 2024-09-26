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
    Parser parser(argc, argv);

    // Create a channel to connect to the server
    // Client client(grpc::CreateChannel(CACHE_ADDR,
    //                                   grpc::InsecureChannelCredentials()),
    //               grpc::CreateChannel(DB_ADDR,
    //                                   grpc::InsecureChannelCredentials()),
    //               nullptr);

    Client client(CACHE_ADDR,
                  DB_ADDR,
                  5, parser.tracker);

    float ew = TTL_EW;
    int ttl = 1;
    // alpha = 1.0;

    // Pass the workload string to the benchmark function
    benchmark(client, ttl, ew, parser, NUM_CPUS);

    return 0;
}
