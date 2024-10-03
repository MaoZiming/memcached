#ifndef TRACKER_HPP
#define TRACKER_HPP
#pragma once

#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <shared_mutex>
#include <queue>
#include <cmath> // for NAN
#include <functional>
#include <string>
#include <vector>
#include <functional>
#include <random>
#include <cstdint>
#include <limits>
#include <ostream>
#include <iostream>
#include <cassert>
// #include <grpcpp/grpcpp.h>
// #include "../include/workload.hpp"

class Tracker
{
public:
    virtual void write(const std::string &key) = 0;
    virtual void read(const std::string &key) = 0;
    virtual double get_ew(const std::string &key, int idx) = 0;
    virtual size_t get_storage_overhead(void) const = 0;
    virtual void update(int num_keys) = 0;
    // New method to report average latencies
    void report_latencies() const
    {
        std::cout << "Average write latency: "
                  << (write_count_ > 0 ? write_latency_ / write_count_ : 0.0)
                  << " ms" << std::endl;
        std::cout << "Average read latency: "
                  << (read_count_ > 0 ? read_latency_ / read_count_ : 0.0)
                  << " ms" << std::endl;
        std::cout << "Average get_ew latency: "
                  << (get_ew_count_ > 0 ? get_ew_latency_ / get_ew_count_ : 0.0)
                  << " ms" << std::endl;
    }

    virtual std::string is_in_topK(std::string key)
    {
        return "N.A.";
    }

    bool is_oracle = false;

protected:
    mutable std::shared_mutex mutex_; // Mutex for protecting data
    // Tracking latency in milliseconds
    mutable double write_latency_ = 0;
    mutable double read_latency_ = 0;
    mutable double get_ew_latency_ = 0;

    // Tracking the number of calls to each method
    mutable size_t write_count_ = 0;
    mutable size_t read_count_ = 0;
    mutable size_t get_ew_count_ = 0;
};

class OracleTracker : public Tracker
{
public:
    OracleTracker()
    {
        is_oracle = true;
    }
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;
    void update(int num_keys) override {};
};

class EveryKeyTracker : public Tracker
{
public:
    EveryKeyTracker() {}
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;
    void update(int num_keys) override {};

private:
    struct KeyData
    {
        double expectedWrites = -1.0;
        int numWrites = 0;
        int numSamples = 0;
    };
    std::unordered_map<std::string, KeyData> data_;
};

// Hash function for count-min sketch
inline size_t
hashFunction(const std::string &key, size_t seed)
{
    std::hash<std::string> hasher;
    return hasher(key) + seed;
}

class CountMinSketch
{
public:
    bool conservative_ = false;

    CountMinSketch(size_t num_keys, bool conservative = false)
    {
        double epsilon = 1.0 / std::sqrt(num_keys);
        double delta = 1.0 / std::sqrt(num_keys);
        // double epsilon = 0.0001;
        // double delta = 0.001;

        // Calculate width and depth based on epsilon and delta
        width_ = std::ceil(std::exp(1) / epsilon); // w = ceil(e / epsilon)
        depth_ = std::ceil(std::log(1.0 / delta)); // d = ceil(ln(1 / delta))

        std::cout << "CountMinSketch initialized with width_ = " << width_ << " depth_ = " << depth_ << std::endl;
        // Initialize the sketch with the calculated width and depth
        sketch_ = std::vector<std::vector<int>>(depth_, std::vector<int>(width_, 0));

        conservative_ = conservative;
    }

    // Increment the count for a key
    void increment(const std::string &key, int count = 1)
    {
        int min_idx = 0;
        int min_count = 1e9;
        for (size_t i = 0; i < depth_; ++i)
        {
            size_t idx = hashFunction(key, i) % width_;
            if (sketch_[i][idx] < min_count)
            {
                min_idx = i;
                min_count = sketch_[i][idx];
            }
            if (!conservative_)
                sketch_[i][idx] += count;
        }

        if (conservative_)
            sketch_[min_idx][min_count] += count;
    }

    void decrement(const std::string &key, int count = 1)
    {
        assert(!conservative_);
        for (size_t i = 0; i < depth_; ++i)
        {
            size_t idx = hashFunction(key, i) % width_;
            sketch_[i][idx] -= count;
        }
    }

    // Estimate the count for a key
    int estimate(const std::string &key) const
    {
        int minCount = std::numeric_limits<int>::max();
        for (size_t i = 0; i < depth_; ++i)
        {
            size_t idx = hashFunction(key, i) % width_;
            minCount = std::min(minCount, sketch_[i][idx]);
        }
        return minCount;
    }

    size_t get_storage_overhead() const
    {
        return sizeof(sketch_) + (depth_ * width_ * sizeof(int));
    }

private:
    size_t width_;
    size_t depth_;
    std::vector<std::vector<int>> sketch_;
};

class TopKSketchOld
/* Too slow! */
{
public:
    TopKSketchOld(int k = 1000, int num_keys = 10000, bool negative = true)
        : k_(k), negative_(negative), countMinSketch_(num_keys) {}

    bool is_in_topK(const std::string &key)
    {
        return topKMap_.find(key) != topKMap_.end();
    }

    // Increment the count for a key
    void increment(const std::string &key)
    {
        // Hot key - only increment topK
        if (topKMap_.find(key) != topKMap_.end())
        {
            topKMap_[key] += 1;
        }
        else if (topKMap_.size() < k_)
        {
            int count = countMinSketch_.estimate(key);
            countMinSketch_.decrement(key, count);

            count += 1;
            topKMap_[key] = count;
        }
        else
        {
            int count = countMinSketch_.estimate(key);
            // Find the element with the minimum count in topKMap_
            auto minElemIt = std::min_element(topKMap_.begin(), topKMap_.end(),
                                              [](const auto &lhs, const auto &rhs)
                                              {
                                                  return lhs.second < rhs.second;
                                              });

            if (count >= minElemIt->second)
            {
                // Remove the smallest element from topKMap_
                std::string minKey = minElemIt->first;
                int minKeyCount = minElemIt->second;
                topKMap_.erase(minKey);
                countMinSketch_.increment(minKey, minKeyCount);

                // Insert the new key
                countMinSketch_.decrement(key, count);
                count += 1;
                topKMap_[key] = count;
            }
            else
            {
                // If the count is smaller, just update the countMinSketch
                countMinSketch_.increment(key);
            }
        }
    }

    // Get the count for a key (returns -1 if not found in the top K)
    int getCount(const std::string &key) const
    {
        auto it = topKMap_.find(key);
        if (it != topKMap_.end())
        {
            return it->second;
        }
        // Not in the top K
        // Get it from countMin.
        if (!negative_)
            return countMinSketch_.estimate(key);
        return 0;
    }

    size_t get_storage_overhead() const
    {
        // std::cout << "countMinSketch: " << countMinSketch_.get_storage_overhead();
        // std::cout << "topKMap_: " << topKMap_.size() * (sizeof(std::string) + sizeof(int)) << std::endl;

        // Calculate the overhead of the countMinSketch and topKMap
        size_t overhead = countMinSketch_.get_storage_overhead() + sizeof(topKMap_);
        overhead += topKMap_.size() * (sizeof(std::string) + sizeof(int)); // size of keys and counts

        // std::cout << "Total overhead: " << overhead << std::endl;

        return overhead;
    }

private:
    int k_;
    bool negative_;
    CountMinSketch countMinSketch_;

    // Map to store the counts of the top K keys
    std::unordered_map<std::string, int> topKMap_;
};

class TopKSketch
{
public:
    TopKSketch(int k = 1000, int num_keys = 10000, bool negative = true)
        : k_(k), negative_(negative), countMinSketch_(num_keys) {}

    bool is_in_topK(const std::string key)
    {
        return topKMap_.find(key) != topKMap_.end();
    }

    // Increment the count for a key
    void increment(const std::string &key)
    {
        // Hot key - only increment topK
        if (topKMap_.find(key) != topKMap_.end())
        {
            topKMap_[key] += 1;
        }
        else if (minHeap_.size() < k_)
        {
            int count = countMinSketch_.estimate(key);
            countMinSketch_.decrement(key, count);

            count += 1;
            topKMap_[key] = count;
            minHeap_.emplace(count, key);
        }
        else
        {
            int count = countMinSketch_.estimate(key);
            /* Count >= minHeap top */
            if (count >= minHeap_.top().first)
            {
                // Remove the smallest element from the heap
                std::string minKey = minHeap_.top().second;
                int minKeycount = topKMap_[minKey];
                topKMap_.erase(minKey);
                minHeap_.pop();
                countMinSketch_.increment(minKey, minKeycount);

                // Insert the new key
                countMinSketch_.decrement(key, count);
                count += 1;
                topKMap_[key] = count;
                minHeap_.emplace(count, key);
            }
            /* Count < minheap top. */
            else
            {
                countMinSketch_.increment(key);
            }
        }
    }

    // Get the count for a key (returns -1 if not found in the top K)
    int getCount(const std::string &key) const
    {
        auto it = topKMap_.find(key);
        if (it != topKMap_.end())
        {
            return it->second;
        }
        // Not in the top K
        // Get it from countMin.
        if (!negative_)
            return countMinSketch_.estimate(key);
        return 0;
    }

    size_t get_storage_overhead() const
    {
        // std::cout << "countMinSketch: " << countMinSketch_.get_storage_overhead();
        // std::cout << "topKMap_: " << topKMap_.size() * (sizeof(std::string) + sizeof(int)) << std::endl;
        // std::cout << "topKMap_.size()" << topKMap_.size() << std::endl;

        // Calculate the overhead of the countMinSketch and topKMap
        size_t overhead = countMinSketch_.get_storage_overhead() + sizeof(topKMap_);
        overhead += topKMap_.size() * (sizeof(std::string) + sizeof(int)); // size of keys and counts

        // Calculate the overhead of minHeap_
        size_t minHeapOverhead = sizeof(minHeap_) + minHeap_.size() * (sizeof(int) + sizeof(std::string));

        // std::cout << "minHeap_: " << minHeapOverhead << std::endl;
        // std::cout << "minHeap_.size(): " << minHeap_.size() << std::endl;

        // Add the minHeap overhead to the total overhead
        overhead += minHeapOverhead;

        return overhead;
    }

private:
    int k_;
    bool negative_;
    CountMinSketch countMinSketch_;

    // Min-heap to store the top K elements
    std::priority_queue<std::pair<int, std::string>,
                        std::vector<std::pair<int, std::string>>,
                        std::greater<std::pair<int, std::string>>>
        minHeap_;

    // Map to store the counts of the top K keys
    std::unordered_map<std::string, int> topKMap_;
};

class TopKSketchTracker : public Tracker
{
public:
    TopKSketchTracker(int k = 1000, int num_keys = 10000) : k_(k), read_sketch_(k, num_keys, true), write_sketch_(k, num_keys, true) {}

    void update(int num_keys)
    {
        k_ = std::sqrt(num_keys); // hottest keys.
        read_sketch_ = TopKSketch(k_, num_keys, true);
        write_sketch_ = TopKSketch(k_, num_keys, true);
    }
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;

    std::string is_in_topK(std::string key) override
    {
        return (read_sketch_.is_in_topK(key) || write_sketch_.is_in_topK(key)) ? "Yes" : "No";
    }

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    int k_;                           // Top-K value (limit to top-K keys)
    TopKSketch read_sketch_;          // Sketch for tracking read counts
    TopKSketch write_sketch_;         // Sketch for tracking write counts
};

class MinSketchTracker : public Tracker
{
public:
    MinSketchTracker(int num_keys = 10000)
        : read_sketch_(num_keys), write_sketch_(num_keys) {}

    void update(int num_keys)
    {
        read_sketch_ = CountMinSketch(num_keys);
        write_sketch_ = CountMinSketch(num_keys);
    }

    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    CountMinSketch read_sketch_;      // Sketch for tracking read counts
    CountMinSketch write_sketch_;     // Sketch for tracking write counts
};

class MinSketchConsTracker : public MinSketchTracker
{
public:
    MinSketchConsTracker(int num_keys = 10000)
        : read_sketch_(num_keys, true), write_sketch_(num_keys, true) {}

    void update(int num_keys)
    {
        read_sketch_ = CountMinSketch(num_keys, true);
        write_sketch_ = CountMinSketch(num_keys, true);
    }

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    CountMinSketch read_sketch_;      // Sketch for tracking read counts
    CountMinSketch write_sketch_;     // Sketch for tracking write counts
};

class ExactRWTracker : public Tracker
{
public:
    ExactRWTracker() {}
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;
    void update(int num_keys) override {};

private:
    struct KeyData
    {
        double expectedWrites = -1.0;
        int numReads = 0;
        int numWrites = 0;
    };
    std::unordered_map<std::string, KeyData> data_;
};

class TopKSketchSampleTracker : public Tracker
{
public:
    TopKSketchSampleTracker(int k = 100, int num_keys = 10000) : k_(k), read_sketch_(k, num_keys, false), write_sketch_(k, num_keys, false) {}

    void update(int num_keys)
    {
        k_ = num_keys / 20; // 1/20 hottest keys.
        read_sketch_ = TopKSketch(k_, num_keys, false);
        write_sketch_ = TopKSketch(k_, num_keys, false);
    }
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key, int idx) override;
    size_t get_storage_overhead(void) const override;

    std::string is_in_topK(std::string key) override
    {
        return (read_sketch_.is_in_topK(key) || write_sketch_.is_in_topK(key)) ? "Yes" : "No";
    }

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    int k_;                           // Top-K value (limit to top-K keys)
    TopKSketch read_sketch_;          // Sketch for tracking read counts
    TopKSketch write_sketch_;         // Sketch for tracking write counts
};

#endif // TRACKER_HPP
