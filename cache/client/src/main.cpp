#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include "policy.hpp"
#include "client.hpp"

int main(int argc, char *argv[])
{
    Tracker tracker;

    // Create a channel to connect to the server
    Client client(grpc::CreateChannel("10.128.0.34:50051",
                                      grpc::InsecureChannelCredentials()),
                  grpc::CreateChannel("10.128.0.33:50051",
                                      grpc::InsecureChannelCredentials()),
                  &tracker);

    // Example usage of Set and Get
    std::string key = "example_key";
    std::string value = "example_value";

    client.SetTTL(1);

    // Put example
    if (client.Set(key, value))
    {
        std::cout << "Put key-value pair successfully." << std::endl;
    }
    else
    {
        std::cout << "Failed to put key-value pair." << std::endl;
    }

    std::string result = client.Get(key);
    if (!result.empty())
    {
        std::cout << "Got value: " << result << std::endl;
    }
    else
    {
        std::cout << "Failed to get value or key not found." << std::endl;
    }

    result = client.Get(key);
    if (!result.empty())
    {
        std::cout << "Got value: " << result << std::endl;
    }
    else
    {
        std::cout << "Failed to get value or key not found." << std::endl;
    }

    sleep(2);

    result = client.Get(key);
    if (!result.empty())
    {
        std::cout << "Got value: " << result << std::endl;
    }
    else
    {
        std::cout << "Failed to get value or key not found." << std::endl;
    }

    return 0;
}
