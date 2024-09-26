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
#include "parser.hpp"

int main(int argc, char *argv[])
{
    Parser parser(argc, argv);

    // Create a channel to connect to the server
    // Client client(grpc::CreateChannel(CACHE_ADDR,
    //                                   grpc::InsecureChannelCredentials()),
    //               grpc::CreateChannel(DB_ADDR,
    //                                   grpc::InsecureChannelCredentials()),
    //               parser.tracker);

    Client client(CACHE_ADDR,
                  DB_ADDR,
                  5, parser.tracker);

    float ew = ADAPTIVE_EW;
    int ttl = LONG_TTL;

    benchmark(client, ttl, ew, parser, NUM_CPUS);

    std::cout << "Tracker overhead: " << parser.tracker->get_storage_overhead() << std::endl;

    return 0;
}
