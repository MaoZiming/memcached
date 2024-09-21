#include "policy.hpp"
#include <iostream>
#include <shared_mutex> // C++17
#include <mutex>

void EveryKeyTracker::write(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock lock(mutex_);
    auto &entry = data_[key];
    entry.numWrites += 1;
#ifdef DEBUG
    std::cout << "write - " << "expectedWrites: " << entry.expectedWrites << ", numWrites: " << entry.numWrites << ", numSamples: " << entry.numSamples << std::endl;
#endif
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    write_latency_ += latency.count(); // Accumulate total write latency
    write_count_++;                    // Increment write count
}

double EveryKeyTracker::get_ew(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::shared_lock lock(mutex_);
    auto &entry = data_[key];

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    get_ew_latency_ += latency.count(); // Accumulate total get_ew latency
    get_ew_count_++;                    // Increment get_ew count

    // return entry.numSamples > 0 ? entry.expectedWrites : entry.numWrites;
    return entry.expectedWrites;
}

void EveryKeyTracker::read(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();
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

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    read_latency_ += latency.count(); // Accumulate total read latency
    read_count_++;                    // Increment read count
}

size_t EveryKeyTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    size_t overhead = sizeof(data_) + (data_.size() * sizeof(KeyData));
    return overhead;
}

void TopKSketchTracker::write(const std::string &key)
{
    // std::cout << "Tracker write: " << key << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    write_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    write_latency_ += latency.count(); // Accumulate total write latency
    write_count_++;                    // Increment write count
}

void TopKSketchTracker::read(const std::string &key)
{
    // std::cout << "Tracker read: " << key << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    std::unique_lock lock(mutex_); // Lock for thread-safe access
    read_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    read_latency_ += latency.count(); // Accumulate total read latency
    read_count_++;                    // Increment read count
}

double TopKSketchTracker::get_ew(const std::string &key)
{

    auto start = std::chrono::high_resolution_clock::now();

    std::shared_lock lock(mutex_); // Lock for thread-safe read access
    int write_count = write_sketch_.getCount(key);
    int read_count = read_sketch_.getCount(key);

    if (read_count == 0)
    {
        // Invalidate.
        return -1;
    }
    if (write_count == 0)
        return -1;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    get_ew_latency_ += latency.count(); // Accumulate total get_ew latency
    get_ew_count_++;                    // Increment get_ew count

    return static_cast<double>(write_count) / static_cast<double>(read_count);
}

size_t TopKSketchTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    return read_sketch_.get_storage_overhead() + write_sketch_.get_storage_overhead();
}

void MinSketchTracker::write(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::unique_lock lock(mutex_); // Lock for thread-safe access
    write_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    write_latency_ += latency.count(); // Accumulate total write latency
    write_count_++;                    // Increment write count
}

void MinSketchTracker::read(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::unique_lock lock(mutex_); // Lock for thread-safe access
    read_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    read_latency_ += latency.count(); // Accumulate total read latency
    read_count_++;                    // Increment read count
}

double MinSketchTracker::get_ew(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();

    std::shared_lock lock(mutex_); // Lock for thread-safe read access
    int write_count = write_sketch_.estimate(key);
    int read_count = read_sketch_.estimate(key);

    // If read count is 0, return NaN to avoid division by zero
    if (read_count == 0)
    {
        // Invalidate.
        return -1;
    }
    if (write_count == 0)
        return -1;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    get_ew_latency_ += latency.count(); // Accumulate total get_ew latency
    get_ew_count_++;                    // Increment get_ew count

    return static_cast<double>(write_count) / static_cast<double>(read_count);
}

size_t MinSketchTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    return read_sketch_.get_storage_overhead() + write_sketch_.get_storage_overhead();
}

void ExactRWTracker::write(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock lock(mutex_);
    auto &entry = data_[key];
    entry.numWrites += 1;
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    write_latency_ += latency.count(); // Accumulate total write latency
    write_count_++;                    // Increment write count
}

double ExactRWTracker::get_ew(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();
    std::shared_lock lock(mutex_);
    auto &entry = data_[key];

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    get_ew_latency_ += latency.count(); // Accumulate total get_ew latency
    get_ew_count_++;                    // Increment get_ew count

    if (entry.numReads == 0)
    {
        // Invalidate.
        return -1;
    }
    if (entry.numWrites == 0)
        return -1;

    return entry.numWrites / entry.numReads;
}

void ExactRWTracker::read(const std::string &key)
{
    auto start = std::chrono::high_resolution_clock::now();
    std::shared_lock lock(mutex_);

    auto &entry = data_[key];
    entry.numReads += 1;
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    read_latency_ += latency.count(); // Accumulate total read latency
    read_count_++;                    // Increment read count
}

size_t ExactRWTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    size_t overhead = sizeof(data_) + (data_.size() * sizeof(KeyData));
    return overhead;
}

void TopKSketchSampleTracker::write(const std::string &key)
{
    // std::cout << "Tracker write: " << key << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock lock(mutex_); // Lock for thread-safe access
    write_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    write_latency_ += latency.count(); // Accumulate total write latency
    write_count_++;                    // Increment write count
}

void TopKSketchSampleTracker::read(const std::string &key)
{
    // std::cout << "Tracker read: " << key << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::unique_lock lock(mutex_); // Lock for thread-safe access
    read_sketch_.increment(key);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    read_latency_ += latency.count(); // Accumulate total read latency
    read_count_++;                    // Increment read count
}

double TopKSketchSampleTracker::get_ew(const std::string &key)
{

    auto start = std::chrono::high_resolution_clock::now();

    std::shared_lock lock(mutex_); // Lock for thread-safe read access
    int write_count = write_sketch_.getCount(key);
    int read_count = read_sketch_.getCount(key);

    if (read_count == 0)
    {
        // Invalidate.
        return -1;
    }
    if (write_count == 0)
        return -1;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> latency = end - start;
    get_ew_latency_ += latency.count(); // Accumulate total get_ew latency
    get_ew_count_++;                    // Increment get_ew count

    return static_cast<double>(write_count) / static_cast<double>(read_count);
}

size_t TopKSketchSampleTracker::get_storage_overhead(void) const
{
    std::shared_lock lock(mutex_);
    return read_sketch_.get_storage_overhead() + write_sketch_.get_storage_overhead();
}