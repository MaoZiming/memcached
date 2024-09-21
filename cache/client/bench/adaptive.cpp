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

    std::string workload = argv[1];                // Capture workload string from the command line
    std::string tracker_str = "TopKSketchTracker"; // Set TopKSketchTracker to be the default.

    if (argc == 3)
        tracker_str = argv[2];

    Tracker *tracker;

    if (tracker_str == "EveryKeyTracker")
    {
        tracker = new EveryKeyTracker();
    }
    else if (tracker_str == "TopKSketchTracker")
    {
        tracker = new TopKSketchTracker();
    }
    else if (tracker_str == "MinSketchTracker")
    {
        tracker = new MinSketchTracker();
    }
    else if (tracker_str == "ExactRWTracker")
    {
        tracker = new ExactRWTracker();
    }
    else if (tracker_str == "MinSketchConsTracker")
        tracker = new MinSketchConsTracker();
    else if (tracker_str == "TopKSketchSampleTracker")
        tracker = new TopKSketchSampleTracker();
    else
    {
        std::cerr << "Tracker unrecognized: " << tracker_str << std::endl;
    }

    // Create a channel to connect to the server
    Client client(grpc::CreateChannel(CACHE_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  grpc::CreateChannel(DB_ADDR,
                                      grpc::InsecureChannelCredentials()),
                  tracker);

    float ew = ADAPTIVE_EW;
    int ttl = LONG_TTL;
    // alpha = 1.0;

    // Pass the workload string to the benchmark function
    benchmark(client, ttl, ew, workload, NUM_CPUS);

    std::cout << "Tracker overhead: " << tracker->get_storage_overhead() << std::endl;

    return 0;
}
