#include "policy.hpp"
#include <iostream>
#include <shared_mutex> // C++17
#include <mutex>

void EveryKeyTracker::write(const std::string &key)
{
    std::unique_lock lock(mutex_);
    auto &entry = data_[key];
    entry.numWrites += 1;
#ifdef DEBUG
    std::cout << "write - " << "expectedWrites: " << entry.expectedWrites << ", numWrites: " << entry.numWrites << ", numSamples: " << entry.numSamples << std::endl;
#endif
}

double EveryKeyTracker::get_ew(const std::string &key)
{
    std::shared_lock lock(mutex_);
    auto &entry = data_[key];
    return data_[key].expectedWrites;
}

void EveryKeyTracker::read(const std::string &key)
{
    std::shared_lock lock(mutex_);

    auto &entry = data_[key];
    if (entry.numSamples == 0)
    {
        entry.expectedWrites = entry.numWrites;
        entry.numSamples += 1;
        entry.numWrites = 0; // Reset the write count
    }
    else if (entry.numWrites > 0)
    {
        // Update expected number of writes
        entry.expectedWrites = (entry.expectedWrites * entry.numSamples + entry.numWrites) / (entry.numSamples + 1);
        entry.numSamples += 1;
        entry.numWrites = 0; // Reset the write count
    }

#ifdef DEBUG
    std::cout << "read - " << "expectedWrites: " << entry.expectedWrites << ", numWrites: " << entry.numWrites << ", numSamples: " << entry.numSamples << std::endl;
#endif
}

size_t EveryKeyTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    size_t overhead = sizeof(data_) + (data_.size() * sizeof(KeyData));
    return overhead;
}

void SketchesTracker::write(const std::string &key)
{
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    write_sketch_.increment(key);
}

void SketchesTracker::read(const std::string &key)
{
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    read_sketch_.increment(key);
}

double SketchesTracker::get_ew(const std::string &key)
{
    std::shared_lock lock(mutex_); // Lock for thread-safe read access
    int write_count = write_sketch_.getCount(key);
    int read_count = read_sketch_.getCount(key);

    // If read count is 0, return NaN to avoid division by zero
    if (read_count == 0)
    {
        // Invalidate.
        return -1;
    }

    return static_cast<double>(write_count) / static_cast<double>(read_count);
}

size_t SketchesTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    return read_sketch_.get_storage_overhead() + write_sketch_.get_storage_overhead();
}

void MinSketchTracker::write(const std::string &key)
{
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    write_sketch_.increment(key);
}

void MinSketchTracker::read(const std::string &key)
{
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    read_sketch_.increment(key);
}

double MinSketchTracker::get_ew(const std::string &key)
{
    std::shared_lock lock(mutex_); // Lock for thread-safe read access
    int write_count = write_sketch_.estimate(key);
    int read_count = read_sketch_.estimate(key);

    // If read count is 0, return NaN to avoid division by zero
    if (read_count == 0)
    {
        // Invalidate.
        return -1;
    }

    return static_cast<double>(write_count) / static_cast<double>(read_count);
}

size_t MinSketchTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    return read_sketch_.get_storage_overhead() + write_sketch_.get_storage_overhead();
}