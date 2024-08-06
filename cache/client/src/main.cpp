#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>

using freshCache::CacheService;
using freshCache::GetRequest;
using freshCache::GetResponse;
using freshCache::SetRequest;
using freshCache::SetResponse;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CacheClient
{
public:
    CacheClient(std::shared_ptr<Channel> channel)
        : stub_(CacheService::NewStub(channel)) {}

    std::string Get(const std::string &key)
    {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);

        if (status.ok())
        {
            if (response.success())
            {
                return response.value();
            }
            else
            {
                std::cerr << "Key not found." << std::endl;
            }
        }
        else
        {
            std::cerr << "RPC failed." << std::endl;
        }

        return "";
    }

    bool Set(const std::string &key, const std::string &value)
    {
        SetRequest request;
        request.set_key(key);
        request.set_value(value);

        SetResponse response;
        ClientContext context;

        Status status = stub_->Set(&context, request, &response);

        if (status.ok())
        {
            return response.success();
        }
        else
        {
            std::cerr << "RPC failed." << std::endl;
        }

        return false;
    }

private:
    std::unique_ptr<CacheService::Stub> stub_;
};

int main(int argc, char *argv[])
{
    // Create a channel to connect to the server
    CacheClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

    // Example usage of Set and Get
    std::string key = "example_key";
    std::string value = "example_value";

    if (client.Set(key, value))
    {
        std::cout << "Set key-value pair successfully." << std::endl;
    }
    else
    {
        std::cout << "Failed to set key-value pair." << std::endl;
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

    return 0;
}
