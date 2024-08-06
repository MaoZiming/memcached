#ifndef TRACKER_HPP
#define TRACKER_HPP

#include <unordered_map>
#include <string>

class Tracker
{
public:
    struct KeyData
    {
        double expectedWrites = -1.0;
        int numWrites = 0;
        int numSamples = 0;
    };

    void write(const std::string &key);

    void read(const std::string &key);

    double get(const std::string &key) const;

private:
    std::unordered_map<std::string, KeyData> data;
};

#endif // TRACKER_HPP
