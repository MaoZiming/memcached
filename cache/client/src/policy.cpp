#include "policy.hpp"
#include <iostream>

void Tracker::write(const std::string &key)
{
    auto &entry = data[key];
    entry.numWrites += 1;
#ifdef DEBUG
    std::cout << "write - " << "expectedWrites: " << entry.expectedWrites << ", numWrites: " << entry.numWrites << ", numSamples: " << entry.numSamples << std::endl;
#endif
}

void Tracker::read(const std::string &key)
{
    auto &entry = data[key];
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

double Tracker::get(const std::string &key) const
{
    auto it = data.find(key);
    if (it != data.end())
    {
        return it->second.expectedWrites;
    }
    return -1.0; // Return -1 if key not found
}
