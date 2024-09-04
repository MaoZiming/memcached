#pragma once

#include <iostream>
#include <memory>

#include <vector>
#include <random>
#include <chrono>
#include <fstream>
#include "zipf.hpp"
#include "policy.hpp"
#include "client.hpp"
#include "zipf.hpp"
#include "tqdm.hpp"

const int KB = 1000;
const int MB = 1000 * KB;

struct request
{
    std::chrono::milliseconds interval;
    bool is_write;
    std::string key;
    std::string value;
};

class Workload
{
public:
    virtual ~Workload() = default;

    std::string get_key(int i) const
    {
        return intervals_[i].key;
    }

    std::string get_value(int i) const
    {
        return intervals_[i].value;
    }

    bool get_is_write(int i) const
    {
        return intervals_[i].is_write;
    }

    std::chrono::milliseconds get_interval(int i) const
    {
        return intervals_[i].interval;
    }

    std::unordered_map<std::string, std::string> keys_to_val;

protected:
    std::vector<request> intervals_;
};

class PoissonWorkload : public Workload
{
public:
    PoissonWorkload(int num_requests, double alpha, double lambda, int num_keys)
    {
        generateRequests(num_requests, alpha, lambda, num_keys);
    }

private:
    void generateRequests(int num_requests, double alpha, double lambda, int num_keys)
    {
        std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
        std::exponential_distribution<double> distribution(lambda);
        std::vector<std::string> keys(num_keys);
        std::vector<int> distribution_values;
        if (alpha > 1.0)
        {
            FastZipf zipf_gen(alpha, num_keys - 1); // 0-indexed: -1
            distribution_values = zipf_gen.generate_zipf(num_requests);
        }
        else
        {
            std::uniform_int_distribution<int> uniform_dist(0, num_keys - 1); // 0-indexed: -1
            distribution_values.reserve(num_requests);
            for (int i = 0; i < num_requests; ++i)
            {
                distribution_values[i] = uniform_dist(generator);
            }
        }

        for (int i = 0; i < num_keys; ++i)
        {
            keys[i] = "key" + std::to_string(i);
        }

        for (int i = 0; i < num_requests; ++i)
        {
            int key_index = distribution_values[i];
            std::string key = keys[key_index];
            std::string value = "value" + std::to_string(key_index) + std::string(1 * MB, 'a');

            request r;
            int interval_in_ms = static_cast<int>(distribution(generator) * 1000);
            // std::cout << "lambda: " << lambda << ", Intervals in ms: " << interval_in_ms << std::endl;
            r.interval = std::chrono::milliseconds(interval_in_ms);

            /*
            if (key_index % 2 == 0)
            {
                // Even key_index: 90% chance to read
                r.is_write = (std::rand() % 100) >= 90; // 10% chance to set
            }
            else
            {
                // Odd key_index: 10% chance to read
                r.is_write = (std::rand() % 100) < 90; // 90% chance to set
            }
            */
            r.is_write = (std::rand() % 100) >= 99;
            r.key = key;
            r.value = value;

            intervals_.push_back(r);
            keys_to_val[key] = value;
        }
    }
};

class MetaWorkload : public Workload
{
public:
    MetaWorkload(int num_requests)
    {
        generateRequests(file_name_, num_requests);
    }

private:
    void generateRequests(const std::string &filename, int num_requests)
    {
        // std::cout << "GenerateRequests: " << filename << std::endl;
        std::ifstream file(filename);
        std::string line;
        std::getline(file, line); // Skip header line
        // std::cout << line << std::endl;
        std::chrono::seconds last_op_time(0);

        while (std::getline(file, line) && num_requests--)
        {
            // std::cout << line << std::endl;
            std::istringstream ss(line);
            std::string token;

            // Parse fields
            std::getline(ss, token, ','); // op_time
            std::chrono::seconds op_time(std::stoll(token));

            std::getline(ss, token, ','); // key (not used)
            std::string key = token;

            std::getline(ss, token, ','); // key_size (not used)
            int key_size = std::stoi(token);

            std::getline(ss, token, ','); // op
            bool is_write = (token.find("SET") != std::string::npos || token.find("SET_LEASE") != std::string::npos);

            std::getline(ss, token, ','); // op_count (not used)
            std::getline(ss, token, ','); // size
            int val_size = std::stoi(token);

            val_size = std::max(1, val_size);
            // Calculate interval between operations
            std::chrono::seconds interval;
            if (last_op_time.count() == 0)
                interval = std::chrono::seconds(0);
            else
                interval = op_time - last_op_time;
            last_op_time = op_time;

            // Create the value by repeating '1' size times
            // std::string key(key_size, '1');
            std::string value(val_size, 'a');

            keys_to_val[key] = value;
            // Add request to the list
            intervals_.push_back({interval, is_write, key, value});
        }
    }
    std::string file_name_ = "/home/maoziming/meta/kvcache_traces_1.csv";
};