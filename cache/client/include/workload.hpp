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
    int value_size;
};

class Workload
{
public:
    Workload() = default;

    void init()
    {
        generateRequests(); // Calls the derived class's generateRequests method
        report_stats();
    }

    virtual void generateRequests(void) = 0;

    virtual ~Workload() = default;

    std::string get_key(int i) const
    {
#ifdef DEBUG
        assert(i < intervals_.size());
#endif
        return intervals_[i].key;
    }

    std::string get_value_from_size(int value_size)
    {
#ifdef DEBUG
        assert(value_size > 0);
#endif
        return std::string(value_size, 'a');
    }

    std::string get_value(int i) const
    {
#ifdef DEBUG
        assert(i < intervals_.size());
        assert(intervals_[i].value_size > 0);
#endif
        return std::string(intervals_[i].value_size, 'a');
    }

    bool get_is_write(int i) const
    {
#ifdef DEBUG
        assert(i < intervals_.size());
#endif
        return intervals_[i].is_write;
    }

    std::chrono::milliseconds get_interval(int i) const
    {
#ifdef DEBUG
        assert(i < intervals_.size());
#endif
        return intervals_[i].interval;
    }

    int num_operations()
    {
        return intervals_.size();
    }

    double get_read_write_ratio() const
    {
        int write_count = 0;
        for (const auto &r : intervals_)
        {
            if (r.is_write)
            {
                write_count++;
            }
        }
        int read_count = intervals_.size() - write_count;
        return static_cast<double>(read_count) / intervals_.size();
    }

    double get_average_interval() const
    {
        if (intervals_.empty())
            return 0.0;

        size_t total_size = 0;
        for (const auto &r : intervals_)
        {
            total_size += r.interval.count();
        }
        return static_cast<double>(total_size) / intervals_.size();
    }

    int get_min_interval() const
    {
        if (intervals_.empty())
            return 0;

        int min_time = std::numeric_limits<int>::max();
        for (const auto &r : intervals_)
        {
            int interval_time = r.interval.count();
            if (interval_time < min_time)
                min_time = interval_time;
        }
        return min_time;
    }

    int get_max_interval() const
    {
        if (intervals_.empty())
            return 0;

        int max_time = 0;
        for (const auto &r : intervals_)
        {
            int interval_time = r.interval.count();
            if (interval_time > max_time)
                max_time = interval_time;
        }
        return max_time;
    }

    double get_average_value_size() const
    {
        if (intervals_.empty())
            return 0.0;

        int64_t total_size = 0;
        for (const auto &r : intervals_)
        {
            total_size += r.value_size;
        }
        return static_cast<double>(total_size) / intervals_.size();
    }

    size_t get_min_value_size() const
    {
        if (intervals_.empty())
            return 0;

        size_t min_size = std::numeric_limits<size_t>::max();
        for (const auto &r : intervals_)
        {
            size_t value_size = r.value_size;
            if (value_size < min_size)
                min_size = value_size;
        }
        return min_size;
    }

    size_t get_max_value_size() const
    {
        if (intervals_.empty())
            return 0;

        size_t max_size = 0;
        for (const auto &r : intervals_)
        {
            size_t value_size = r.value_size;
            if (value_size > max_size)
                max_size = value_size;
        }
        return max_size;
    }

    void report_stats()
    {
        std::cout << "Total requests: " << intervals_.size() << std::endl;
        std::cout << "Read Ratio: " << get_read_write_ratio() << std::endl;

        // Report value size stats
        std::cout << "Average Value Size: " << get_average_value_size() << " bytes" << std::endl;
        std::cout << "Min Value Size: " << get_min_value_size() << " bytes" << std::endl;
        std::cout << "Max Value Size: " << get_max_value_size() << " bytes" << std::endl;

        // Report interval stats
        std::cout << "Average Interval: " << get_average_interval() << " ms" << std::endl;
        std::cout << "Min Interval: " << get_min_interval() << " ms" << std::endl;
        std::cout << "Max Interval: " << get_max_interval() << " ms" << std::endl;
    }

    std::unordered_map<std::string, int> keys_to_val_size;

    std::chrono::milliseconds get_interval(std::chrono::milliseconds op_time)
    {
        std::chrono::milliseconds interval = (last_op_time.count() == 0)
                                                 ? std::chrono::milliseconds(0)
                                                 : std::max(std::chrono::milliseconds(0),
                                                            std::min(std::chrono::milliseconds((op_time - last_op_time).count() / scale_factor_),
                                                                     std::chrono::milliseconds(max_interval_)));

        last_op_time = op_time;
        return interval;
    }

    int get_value_size(int value_size)
    {
        return std::max(0, std::min(1000000, value_size));
    }

protected:
    std::vector<request> intervals_;
    std::chrono::milliseconds last_op_time{0};
    std::chrono::milliseconds max_interval_{1000};
    int scale_factor_ = 1;

    std::vector<std::string>
    getSortedFiles(const std::string &directory_path)
    {
        std::vector<std::string> files;

        for (const auto &entry : std::filesystem::directory_iterator(directory_path))
        {
            if (std::filesystem::is_regular_file(entry.path()))
            {
                files.push_back(entry.path().string());
            }
        }
        // Sort files alphabetically by their path
        std::sort(files.begin(), files.end());
        return files;
    }
};

class PoissonWorkload : public Workload
{
public:
    int num_operations_ = 200000;
    int num_keys = 10000; // Number of keys
    double lambda = 50;   // Poisson distribution parameter (average request rate)
    double alpha = 1.1;
    int key_size_in_KB = 100;

private:
    void generateRequests(void) override
    {
        // Same seed.
        std::default_random_engine generator(0);
        std::exponential_distribution<double> distribution(lambda);
        std::vector<std::string> keys(num_keys);
        std::vector<int> distribution_values;
        if (alpha > 1.0)
        {
            FastZipf zipf_gen(alpha, num_keys - 1); // 0-indexed: -1
            distribution_values = zipf_gen.generate_zipf(num_operations_);
        }
        else
        {
            std::uniform_int_distribution<int> uniform_dist(0, num_keys - 1); // 0-indexed: -1
            distribution_values.reserve(num_operations_);
            for (int i = 0; i < num_operations_; ++i)
            {
                distribution_values[i] = uniform_dist(generator);
            }
        }

        for (int i = 0; i < num_keys; ++i)
        {
            keys[i] = "key" + std::to_string(i);
        }

        for (int i = 0; i < num_operations_; ++i)
        {
            int key_index = distribution_values[i];
            std::string key = keys[key_index];
            std::string value = "value" + std::to_string(key_index) + std::string(key_size_in_KB * KB, 'a');

            request r;
            int interval_in_ms = static_cast<int>(distribution(generator) * 1000);
            r.interval = std::chrono::milliseconds(interval_in_ms);
            r.is_write = (std::rand() % 100) >= 90;
            r.key = key;
            r.value_size = get_value_size(key_size_in_KB * KB);

            intervals_.push_back(r);
            keys_to_val_size[key] = r.value_size;
        }
    }
};

class PoissonMixWorkload : public PoissonWorkload
{
    void generateRequests(void) override
    {
        // Same seed.
        std::default_random_engine generator(0);
        std::exponential_distribution<double> distribution(lambda);
        std::vector<std::string> keys(num_keys);
        std::vector<int> distribution_values;
        if (alpha > 1.0)
        {
            FastZipf zipf_gen(alpha, num_keys - 1); // 0-indexed: -1
            distribution_values = zipf_gen.generate_zipf(num_operations_);
        }
        else
        {
            std::uniform_int_distribution<int> uniform_dist(0, num_keys - 1); // 0-indexed: -1
            distribution_values.reserve(num_operations_);
            for (int i = 0; i < num_operations_; ++i)
            {
                distribution_values[i] = uniform_dist(generator);
            }
        }

        for (int i = 0; i < num_keys; ++i)
        {
            keys[i] = "key" + std::to_string(i);
        }

        for (int i = 0; i < num_operations_; ++i)
        {
            int key_index = distribution_values[i];
            std::string key = keys[key_index];
            std::string value = "value" + std::to_string(key_index) + std::string(key_size_in_KB * KB, 'a');

            request r;
            int interval_in_ms = static_cast<int>(distribution(generator) * 1000);
            r.interval = std::chrono::milliseconds(interval_in_ms);

            if (key_index % 2 == 0)
            {
                r.is_write = (std::rand() % 100) >= 90; // 10% chance to set
            }
            else
            {
                r.is_write = (std::rand() % 100) < 90; // 90% chance to set
            }

            r.key = key;
            r.value_size = get_value_size(key_size_in_KB * KB);

            intervals_.push_back(r);
            keys_to_val_size[key] = r.value_size;
        }
    }
};

class PoissonWriteWorkload : public PoissonWorkload
{
private:
    void generateRequests(void) override
    {
        // Same seed.
        std::default_random_engine generator(0);
        std::exponential_distribution<double> distribution(lambda);
        std::vector<std::string> keys(num_keys);
        std::vector<int> distribution_values;
        if (alpha > 1.0)
        {
            FastZipf zipf_gen(alpha, num_keys - 1); // 0-indexed: -1
            distribution_values = zipf_gen.generate_zipf(num_operations_);
        }
        else
        {
            std::uniform_int_distribution<int> uniform_dist(0, num_keys - 1); // 0-indexed: -1
            distribution_values.reserve(num_operations_);
            for (int i = 0; i < num_operations_; ++i)
            {
                distribution_values[i] = uniform_dist(generator);
            }
        }

        for (int i = 0; i < num_keys; ++i)
        {
            keys[i] = "key" + std::to_string(i);
        }

        for (int i = 0; i < num_operations_; ++i)
        {
            int key_index = distribution_values[i];
            std::string key = keys[key_index];
            std::string value = "value" + std::to_string(key_index) + std::string(key_size_in_KB * KB, 'a');

            request r;
            int interval_in_ms = static_cast<int>(distribution(generator) * 1000);
            r.interval = std::chrono::milliseconds(interval_in_ms);

            r.is_write = (std::rand() % 100) < 90; // 90% chance to set
            r.key = key;
            r.value_size = get_value_size(key_size_in_KB * KB);

            intervals_.push_back(r);
            keys_to_val_size[key] = r.value_size;
        }
    }
};

class MetaWorkload : public Workload
{
private:
    void generateRequests(void) override
    {
        std::string directory_path = file_name_;
        int i = 0;

        for (const auto &file_path : getSortedFiles(directory_path))
        {
            std::ifstream file(file_path);
            std::string line;
            std::getline(file, line); // Skip header line

            while (std::getline(file, line))
            {
                std::istringstream ss(line);
                std::string token;

                // Parse fields
                std::getline(ss, token, ','); // op_time
                std::chrono::milliseconds op_time(std::stoll(token) * 1000);

                std::getline(ss, token, ','); // key (not used)
                std::string key = token;

                std::getline(ss, token, ','); // key_size (not used)
                int key_size = std::stoi(token);

                std::getline(ss, token, ','); // op
                bool is_write = (token.find("SET") != std::string::npos || token.find("SET_LEASE") != std::string::npos);

                std::getline(ss, token, ','); // op_count (not used)
                std::getline(ss, token, ','); // size
                int val_size = std::stoi(token);
                if (i >= num_operations_)
                {
                    break;
                }
                request r;
                r.interval = get_interval(op_time);
                r.is_write = is_write;
                r.key = key;
                r.value_size = get_value_size(val_size + key_size);

                intervals_.push_back(r);
                keys_to_val_size[key] = r.value_size;
                i += 1;
            }
        }
    }

private:
    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/Meta/data";
    int num_operations_ = 500000;
};

class TwitterWorkload : public Workload
{

private:
    void generateRequests(void) override
    {
        std::string directory_path = file_name_;
        int i = 0;

        for (const auto &entry : getSortedFiles(directory_path))
        {
            std::string file_path = entry;
            if (std::filesystem::is_regular_file(file_path))
            {
                std::ifstream file(file_path);
                std::string line;

                while (std::getline(file, line))
                {
                    std::istringstream ss(line);
                    std::string token;
                    std::string key, op;
                    std::chrono::milliseconds op_time(0);
                    int ttl = 0;

                    // Parse fields from CSV
                    std::getline(ss, token, ','); // op_time
                    op_time = std::chrono::milliseconds(std::stoll(token) * 1000);
                    int key_size, value_size = 0;

                    std::getline(ss, key, ',');   // key
                    std::getline(ss, token, ','); // key_size
                    key_size = std::stoi(token);
                    std::getline(ss, token, ','); // Value_size
                    value_size = std::stoi(token);
                    std::getline(ss, token, ','); // misc4 (ignored)
                    std::getline(ss, op, ',');    // op (get/set)

                    std::getline(ss, token, ','); // ttl
                    ttl = std::stoi(token);

                    if (i >= num_operations_)
                    {
                        break;
                    }

                    request r;
                    r.interval = get_interval(op_time);
                    r.is_write = (op != "get" && op != "gets");
                    r.key = key;
                    std::string value = std::string(key_size + value_size, 'a');
                    r.value_size = get_value_size(key_size + value_size);
                    i += 1;
                    intervals_.push_back(r);
                    keys_to_val_size[key] = r.value_size;
                }
            }
        }
    }

private:
    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/Twitter/2020Mar";
    int num_operations_ = 3000000;
};

class IBMWorkload : public Workload
{
public:
    IBMWorkload()
    {
        max_interval_ = std::chrono::milliseconds{200};
    }

private:
    // Problem: ObjectStoreTrace object size is quite big. 100s MB.
    void generateRequests(void) override
    {
        std::string directory_path = file_name_;

        int i = 0;

        for (const auto &entry : getSortedFiles(directory_path))
        {
            std::string file_path = entry;
            if (std::filesystem::is_regular_file(file_path))
            {
                std::ifstream file(file_path);
                std::string line;

                while (std::getline(file, line))
                {
                    std::istringstream ss(line);
                    std::string token;
                    std::string request_type, object_id;
                    std::chrono::milliseconds op_time(0);
                    int64_t size = 0, offset_begin = 0, offset_end = 0;

                    // Parse fields from trace line
                    std::getline(ss, token, ' '); // op_time (in milliseconds)
                    op_time = std::chrono::milliseconds(std::stoll(token));

                    std::getline(ss, request_type, ' '); // request_type
                    std::getline(ss, object_id, ' ');    // object_id

                    if (request_type == "REST.PUT.OBJECT" || request_type == "REST.GET.OBJECT")
                    {
                        std::getline(ss, token, ' '); // size
                        size = std::stoll(token);
                    }
                    else
                    {
                        continue;
                    }

                    if (i >= num_operations_)
                    {
                        break;
                    }

                    request r;
                    r.interval = get_interval(op_time);
                    ;
                    r.is_write = (request_type == "REST.PUT.OBJECT");
                    r.key = object_id;
                    r.value_size = get_value_size(size);
                    i += 1;
                    intervals_.push_back(r);
                    keys_to_val_size[object_id] = r.value_size;
                }
            }
        }
    }

private:
    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/IBM/data";
    int num_operations_ = 60000;
    int scale_factor_ = 5;
};

class TencentWorkload : public Workload
{
    // Reference: https://github.com/1a1a11a/libCacheSim/blob/develop/libCacheSim/bin/dep/cpp/tencent.h
private:
    void generateRequests(void) override
    {
        std::string directory_path = file_name_;
        int i = 0;

        for (const auto &entry : getSortedFiles(directory_path))
        {
            std::string file_path = entry;
            if (std::filesystem::is_regular_file(file_path))
            {
                std::ifstream file(file_path);
                std::string line;

                while (std::getline(file, line))
                {
                    // std::cout << line << std::endl;
                    std::istringstream ss(line);
                    std::string token;
                    std::chrono::milliseconds op_time(0);
                    int size = 0, is_write = 0;
                    std::string lba, namespace_id;
                    // Parse fields from trace line
                    std::getline(ss, token, ',');                                  // timestamp (in seconds)
                    op_time = std::chrono::milliseconds(std::stoll(token) * 1000); // Convert to milliseconds

                    std::getline(ss, token, ','); // lba
                    lba = token;

                    std::getline(ss, token, ','); // size (multiplied by 512)
                    size = std::stoi(token) * 512;

                    std::getline(ss, token, ','); // is_write (0 for read, 1 for write)
                    is_write = std::stoi(token);

                    std::getline(ss, token, ','); // namespace_id
                    namespace_id = token;

                    if (i >= num_operations_)
                    {
                        break;
                    }

                    request r;
                    r.interval = get_interval(op_time);
                    r.is_write = (is_write == 1);
                    r.key = lba + "_" + namespace_id; // lba + namespace as key
                    r.value_size = get_value_size(size);

                    i += 1;
                    intervals_.push_back(r);
                    keys_to_val_size[r.key] = size;
                }
            }
        }
    }
    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/Tencent/cbs_trace1/atc_2020_trace/trace_ori";
    int num_operations_ = 100000; // Limit number of operations to process
};

class AlibabaWorkload : public Workload
{
private:
    void generateRequests(void) override
    {
        int i = 0;

        // Open the trace file
        std::ifstream file(file_name_);
        if (!file.is_open())
        {
            std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
            return;
        }

        std::string line;
        while (std::getline(file, line) && i < num_operations_)
        {
            std::istringstream ss(line);
            std::string token;
            request r;
            std::chrono::milliseconds op_time(0);
            uint32_t device_id = 0;
            char opcode = 'R';
            uint64_t offset = 0;
            uint32_t length = 0;
            uint64_t timestamp = 0;

            // Parse the CSV fields
            std::getline(ss, token, ','); // device_id
            device_id = std::stoul(token);

            std::getline(ss, token, ','); // opcode
            opcode = token[0];

            std::getline(ss, token, ','); // offset
            offset = std::stoull(token);

            std::getline(ss, token, ','); // length
            length = std::stoul(token);

            std::getline(ss, token, ','); // timestamp
            timestamp = std::stoull(token);
            // Timestamp of this operation received by server, in microseconds
            op_time = std::chrono::milliseconds(timestamp / 1000); // Convert to milliseconds

            // Set the request details
            r.key = std::to_string(device_id) + "_" + std::to_string(offset);
            r.value_size = get_value_size(length); // Dummy value
            r.is_write = (opcode == 'W');
            r.interval = get_interval(op_time);

            // Add the request to the intervals
            intervals_.push_back(r);
            keys_to_val_size[r.key] = r.value_size;

            i += 1;
        }
        file.close();
    }

    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/Alibaba/alibaba_block_traces_2020/io_traces.csv";
    int num_operations_ = 300000;
};

class WikiCDNWorkload : public Workload
{
    // Reference: /home/maoziming/memcached/cache/dataset/WikiCDN
private:
    void generateRequests(void) override
    {
        std::string directory_path = file_name_;
        int i = 0;

        for (const auto &entry : getSortedFiles(directory_path))
        {
            std::string file_path = entry;
            if (std::filesystem::is_regular_file(file_path))
            {
                std::ifstream file(file_path);
                std::string line;
                std::getline(file, line); // header

                while (std::getline(file, line))
                {
                    std::istringstream ss(line);
                    // std::cout << line << std::endl;
                    std::string token;
                    std::chrono::milliseconds op_time(0);
                    int64_t response_size = 0;
                    double time_firstbyte = 0.0;
                    int64_t hashed_host_path_query = 0;
                    int relative_unix = 0;

                    // Parse fields from trace line
                    std::getline(ss, token, '\t');                                // relative_unix (in seconds)
                    op_time = std::chrono::milliseconds(std::stoi(token) * 1000); // Convert to milliseconds

                    std::getline(ss, token, '\t'); // hashed_host_path_query
                    hashed_host_path_query = std::stoll(token);

                    std::getline(ss, token, '\t'); // response_size
                    response_size = std::stoll(token);

                    std::getline(ss, token, '\t'); // time_firstbyte
                    time_firstbyte = std::stod(token);

                    if (i >= num_operations_)
                    {
                        break;
                    }

                    request r;
                    r.interval = get_interval(op_time);
                    r.key = std::to_string(hashed_host_path_query); // Use hashed host path query as key
                    r.value_size = get_value_size(response_size);

                    i += 1;
                    intervals_.push_back(r);
                    keys_to_val_size[r.key] = response_size;
                }
            }
        }
    }
    std::string file_name_ = "/home/maoziming/memcached/cache/dataset/WikiCDN";
    int num_operations_ = 1000; // Limit number of operations to process
};