#include "load_tracker.hpp"
#include <fstream>
#include <string>

// Global variables to control logging thread
std::atomic<bool> keepLogging(false);
std::thread loggingThread;
double cpuLoad = 0;

// Function to parse CPU stats from /proc/stat
CPUStats GetCPUStats()
{
    std::ifstream file("/proc/stat");
    std::string line;
    CPUStats stats = {0};

    if (file.is_open())
    {
        std::getline(file, line); // Read the first line for overall CPU stats
        sscanf(line.c_str(), "cpu %llu %llu %llu %llu %llu %llu %llu %llu",
               &stats.user, &stats.nice, &stats.system, &stats.idle, &stats.iowait,
               &stats.irq, &stats.softirq, &stats.steal);
    }

    return stats;
}

double get_cpu_load() { return cpuLoad; }

// Function to parse network stats from /proc/net/dev
NetStats GetNetStats()
{
    std::ifstream file("/proc/net/dev");
    std::string line;
    NetStats netStats = {0};

    if (file.is_open())
    {
        // Skip the first two lines (headers)
        std::getline(file, line);
        std::getline(file, line);

        while (std::getline(file, line))
        {
            std::istringstream ss(line);
            std::string iface;
            unsigned long long recvBytes, recvPackets, recvErrs, recvDrop, recvFifo,
                recvFrame, recvCompressed, recvMulticast;
            unsigned long long sendBytes, sendPackets, sendErrs, sendDrop, sendFifo,
                sendColls, sendCarrier, sendCompressed;

            // Parse the line, extracting the data for each interface
            ss >> iface >> recvBytes >> recvPackets >> recvErrs >> recvDrop >>
                recvFifo >> recvFrame >> recvCompressed >> recvMulticast >>
                sendBytes >> sendPackets >> sendErrs >> sendDrop >> sendFifo >>
                sendColls >> sendCarrier >> sendCompressed;

            if (iface == "ens4:" ||
                iface == "lo:")
            { // Adjust this based on your network interfaces of
              // interest
                netStats.recvBytes += recvBytes;
                netStats.sendBytes += sendBytes;
            }
        }
    }

    return netStats;
}

// Function to parse disk stats from /proc/diskstats
DiskStats GetDiskStats()
{
    std::ifstream file("/proc/diskstats");
    std::string line;
    DiskStats diskStats = {0};

    if (file.is_open())
    {
        while (std::getline(file, line))
        {
            std::istringstream ss(line);
            std::string device;
            unsigned long long major, minor, readsCompleted, readsMerged, sectorsRead,
                msReading, writesCompleted, writesMerged, sectorsWritten, msWriting;

            // Parse the line to extract disk stats
            ss >> major >> minor >> device >> readsCompleted >> readsMerged >>
                sectorsRead >> msReading >> writesCompleted >> writesMerged >>
                sectorsWritten >> msWriting;

            if (device == "sda")
            {                                             // Adjust this based on your disk of interest
                diskStats.readBytes += sectorsRead * 512; // Convert sectors to bytes
                diskStats.writeBytes += sectorsWritten * 512;
            }
        }
    }

    return diskStats;
}
// Function to get current timestamp as a string
std::string GetT()
{
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    char buffer[30];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S",
             std::localtime(&now_time));
    return std::string(buffer);
}

void LogCPULoad(const std::string &logFile, DBClient *db_client, CacheClient *cache_client)
{
    CPUStats previousStats = GetCPUStats();
    NetStats previousNetStats = GetNetStats();
    DiskStats previousDiskStats = GetDiskStats();

    while (keepLogging)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Wait for 1 second

        CPUStats currentStats = GetCPUStats();
        NetStats currentNetStats = GetNetStats();
        DiskStats currentDiskStats = GetDiskStats();

        unsigned long long totalDiff = currentStats.Total() - previousStats.Total();
        unsigned long long idleDiff = currentStats.Idle() - previousStats.Idle();

        double cpuUtilization =
            (totalDiff - idleDiff) / static_cast<double>(totalDiff);

        /* Moving average: */

        cpuLoad = cpuLoad * 0.5 + cpuUtilization * 0.5;

        double usrPercent = (double(currentStats.user - previousStats.user) /
                             (currentStats.Total() - previousStats.Total())) *
                            100;
        double sysPercent = (double(currentStats.system - previousStats.system) /
                             (currentStats.Total() - previousStats.Total())) *
                            100;
        double idlePercent = (double(currentStats.idle - previousStats.idle) /
                              (currentStats.Total() - previousStats.Total())) *
                             100;
        double iowaitPercent = (double(currentStats.iowait - previousStats.iowait) /
                                (currentStats.Total() - previousStats.Total())) *
                               100;
        double stealPercent = (double(currentStats.steal - previousStats.steal) /
                               (currentStats.Total() - previousStats.Total())) *
                              100;

        unsigned long long recvBytes =
            currentNetStats.recvBytes - previousNetStats.recvBytes;
        unsigned long long sendBytes =
            currentNetStats.sendBytes - previousNetStats.sendBytes;

        unsigned long long readBytes =
            currentDiskStats.readBytes - previousDiskStats.readBytes;
        unsigned long long writeBytes =
            currentDiskStats.writeBytes - previousDiskStats.writeBytes;

        // std::cout << "appending to " << logFile << std::endl;
        std::ofstream log(logFile, std::ios_base::app); // Append to log file
        if (log.is_open())
        {
            log << GetT() << " - CPU Utilization: " << cpuUtilization * 100 << "% | "
                << "usr: " << usrPercent << "%, sys: " << sysPercent << "%, "
                << "idle: " << idlePercent << "%, iowait: " << iowaitPercent << "%, "
                << "steal: " << stealPercent << "% | "
                << "Network recv: " << recvBytes << " bytes, send: " << sendBytes
                << " bytes | "
                << "Disk read: " << readBytes << " bytes, write: " << writeBytes
                << " bytes"
                // << ", DB current_rpcs: " << db_client->get_current_rpcs()
                << ", Cache current_rpcs: " << cache_client->get_current_rpcs()
                << std::endl;
        }

        previousStats = currentStats;
        previousNetStats = currentNetStats;
        previousDiskStats = currentDiskStats;
    }
}

void START_COLLECTION(const std::string &logFile,
                      DBClient *db_client, CacheClient *cache_client)
{
    keepLogging = true;
    loggingThread =
        std::thread(LogCPULoad, logFile, db_client, cache_client); // Start logging in a separate thread
}

void END_COLLECTION()
{
    keepLogging = false; // Signal the logging thread to stop
    if (loggingThread.joinable())
    {
        loggingThread.join(); // Wait for the logging thread to finish
    }
}

void WRITE_TO_LOG(const std::string &log_file, const std::string &suffix, const std::string &content)
{
    std::string full_file_name = log_file + "." + suffix;

    // Open the file in append mode
    std::ofstream log_stream(full_file_name, std::ios_base::app);

    // Check if the file is open
    if (log_stream.is_open())
    {
        log_stream << content << std::endl;
        log_stream.close(); // Close the file after writing
    }
    else
    {
        // Handle error in case the file cannot be opened
        std::cerr << "Error: Could not open log file " << full_file_name << std::endl;
    }
}
