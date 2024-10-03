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

    Client client(CACHE_ADDRESSES,
                  DB_ADDR,
                  nullptr);

    float ew = UPDATE_EW;
    int ttl = LONG_TTL;
    // alpha = 1.0;

    // Pass the workload string to the benchmark function
    benchmark(client, ttl, ew, parser, NUM_CPUS);

    return 0;
}
