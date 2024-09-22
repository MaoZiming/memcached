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

class Parser
{
public:
    // Publicly accessible members
    Tracker *tracker;
    Workload *workload;
    int scale_factor;

    // Constructor that takes argc and argv
    Parser(int argc, char *argv[])
    {
        if (argc < 2)
        {
            std::cerr << "Usage: " << argv[0] << " <workload> [<scale_factor>] [<tracker>]" << std::endl;
            return;
        }

        std::string workload_str = argv[1];
        scale_factor = (argc >= 3) ? std::stoi(argv[2]) : -1;                  // Default scale factor
        std::string tracker_str = (argc >= 4) ? argv[3] : "TopKSketchTracker"; // Default tracker

        // Initialize tracker based on input
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
        {
            tracker = new MinSketchConsTracker();
        }
        else if (tracker_str == "TopKSketchSampleTracker")
        {
            tracker = new TopKSketchSampleTracker();
        }
        else
        {
            std::cerr << "Tracker unrecognized: " << tracker_str << std::endl;
        }

        // Initialize workload based on input
        if (workload_str == "Poisson")
        {
            workload = new PoissonWorkload();
        }
        else if (workload_str == "Meta")
        {
            workload = new MetaWorkload();
        }
        else if (workload_str == "PoissonMix")
        {
            workload = new PoissonMixWorkload();
        }
        else if (workload_str == "PoissonWrite")
        {
            workload = new PoissonWriteWorkload();
        }
        else if (workload_str == "Twitter")
        {
            workload = new TwitterWorkload();
        }
        else if (workload_str == "Tencent")
        {
            workload = new TencentWorkload();
        }
        else if (workload_str == "IBM")
        {
            workload = new IBMWorkload();
        }
        else if (workload_str == "Alibaba")
        {
            workload = new AlibabaWorkload();
        }
        else if (workload_str == "WikiCDN")
        {
            std::cerr << "WikiCDN trace has No write" << std::endl;
            workload = new WikiCDNWorkload();
        }
        else
        {
            std::cerr << "Unrecognized workload: " << workload_str << std::endl;
        }
    }
};