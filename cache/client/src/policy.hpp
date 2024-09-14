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

class Tracker
{
public:
    virtual void write(const std::string &key) = 0;
    virtual void read(const std::string &key) = 0;
    virtual double get_ew(const std::string &key) = 0;
    virtual size_t get_storage_overhead(void) const = 0;

protected:
    mutable std::shared_mutex mutex_; // Mutex for protecting data
};

class EveryKeyTracker : public Tracker
{
public:
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key) override;
    size_t get_storage_overhead(void) const override;

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
    CountMinSketch(size_t width, size_t depth)
        : width_(width), depth_(depth), sketch_(depth, std::vector<int>(width, 0)) {}

    // Increment the count for a key
    void increment(const std::string &key)
    {
        for (size_t i = 0; i < depth_; ++i)
        {
            size_t idx = hashFunction(key, i) % width_;
            sketch_[i][idx]++;
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

class TopKSketch
{
public:
    TopKSketch(int k = 1000, size_t width = 2000, size_t depth = 3)
        : k_(k), countMinSketch_(width, depth) {}

    // Increment the count for a key
    void increment(const std::string &key)
    {
        countMinSketch_.increment(key);
        int count = countMinSketch_.estimate(key);

        // Update the top K elements
        if (topKMap_.find(key) != topKMap_.end())
        {
            // topKMap_[key] = count;
            topKMap_[key] += 1;
        }
        else if (topKMap_.size() < k_)
        {
            topKMap_[key] = count;
        }
        else
        {
            // Check if the new key should be in the top K
            auto minIt = std::min_element(topKMap_.begin(), topKMap_.end(),
                                          [](const auto &a, const auto &b)
                                          { return a.second < b.second; });

            if (count > minIt->second)
            {
                topKMap_.erase(minIt);
                topKMap_[key] = count;
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
        return -1; // Not in the top K
    }

    size_t get_storage_overhead() const
    {
        size_t overhead = countMinSketch_.get_storage_overhead() + sizeof(topKMap_);
        overhead += topKMap_.size() * (sizeof(std::string) + sizeof(int)); // size of keys and counts
        return overhead;
    }

private:
    int k_;
    CountMinSketch countMinSketch_;
    std::unordered_map<std::string, int> topKMap_;
};

class SketchesTracker : public Tracker
{
public:
    SketchesTracker(int k = 1000) : k_(k), read_sketch_(k), write_sketch_(k) {}
    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key) override;
    size_t get_storage_overhead(void) const override;

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    int k_;                           // Top-K value (limit to top-K keys)
    TopKSketch read_sketch_;          // Sketch for tracking read counts
    TopKSketch write_sketch_;         // Sketch for tracking write counts
};

class ApproximateSketch
{
public:
    ApproximateSketch(size_t width = 1000, size_t depth = 5)
        : w(width), d(depth), counts(depth, std::vector<uint32_t>(width, 0))
    {
        // Initialize hash seeds with random values
        std::mt19937 rng(123456); // Fixed seed for reproducibility
        std::uniform_int_distribution<uint32_t> dist(1, UINT32_MAX);
        for (size_t i = 0; i < d; ++i)
        {
            hash_seeds.push_back(dist(rng));
        }
    }

    // Increment the count for a key
    void increment(const std::string &key)
    {
        for (size_t i = 0; i < d; ++i)
        {
            size_t idx = hash(i, key) % w;
            counts[i][idx] += 1;
        }
    }

    // Estimate the count for a key
    int estimate(const std::string &key)
    {
        uint32_t min_count = std::numeric_limits<uint32_t>::max();
        for (size_t i = 0; i < d; ++i)
        {
            size_t idx = hash(i, key) % w;
            uint32_t count = counts[i][idx];
            if (count < min_count)
            {
                min_count = count;
            }
        }
        return static_cast<int>(min_count);
    }

    // Get the storage overhead in bytes
    size_t get_storage_overhead() const
    {
        return counts.size() * counts[0].size() * sizeof(uint32_t);
    }

private:
    size_t w;                                  // Width of the sketch
    size_t d;                                  // Depth (number of hash functions)
    std::vector<std::vector<uint32_t>> counts; // 2D array of counts
    std::vector<uint32_t> hash_seeds;          // Seeds for hash functions

    // Hash function combining std::hash and a seed
    size_t hash(size_t i, const std::string &key)
    {
        std::hash<std::string> hasher;
        uint64_t hash_value = hasher(key);

        // Combine the hash value with the seed to create different hash functions
        hash_value ^= hash_seeds[i];

        // Mix the bits (simple mixing)
        hash_value ^= (hash_value >> 17);
        hash_value *= 0xed5ad4bbU;
        hash_value ^= (hash_value >> 11);
        hash_value *= 0xac4c1b51U;
        hash_value ^= (hash_value >> 15);
        hash_value *= 0x31848babU;
        hash_value ^= (hash_value >> 14);

        return hash_value;
    }
};

class MinSketchTracker : public Tracker
{
public:
    MinSketchTracker(int k = 1000, size_t width = 2000, size_t depth = 3)
        : read_sketch_(width, depth), write_sketch_(width, depth) {}

    void write(const std::string &key) override;
    void read(const std::string &key) override;
    double get_ew(const std::string &key) override;
    size_t get_storage_overhead(void) const override;

private:
    mutable std::shared_mutex mutex_; // Mutex to protect read/write access
    CountMinSketch read_sketch_;      // Sketch for tracking read counts
    CountMinSketch write_sketch_;     // Sketch for tracking write counts
};

#endif // TRACKER_HPP
