#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>
#include <myproto/db_service.grpc.pb.h>
#include <myproto/db_service.pb.h>

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>

using freshCache::CacheGetRequest;
using freshCache::CacheGetResponse;
using freshCache::CacheService;
using freshCache::CacheSetRequest;
using freshCache::CacheSetResponse;

using freshCache::DBDeleteRequest;
using freshCache::DBDeleteResponse;
using freshCache::DBGetRequest;
using freshCache::DBGetResponse;
using freshCache::DBPutRequest;
using freshCache::DBPutResponse;
using freshCache::DBService;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class DBClient
{
public:
    DBClient(std::shared_ptr<Channel> channel)
        : stub_(DBService::NewStub(channel)) {}

    bool Put(const std::string &key, const std::string &value)
    {
        DBPutRequest request;
        request.set_key(key);
        request.set_value(value);

        DBPutResponse response;
        ClientContext context;

        Status status = stub_->Put(&context, request, &response);

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

    bool Delete(const std::string &key)
    {
        DBDeleteRequest request;
        request.set_key(key);

        DBDeleteResponse response;
        ClientContext context;

        Status status = stub_->Delete(&context, request, &response);

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
    std::unique_ptr<DBService::Stub> stub_;
};

class CacheClient
{
public:
    CacheClient(std::shared_ptr<Channel> channel)
        : stub_(CacheService::NewStub(channel)) {}

    std::string Get(const std::string &key)
    {
        CacheGetRequest request;
        request.set_key(key);

        CacheGetResponse response;
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
        CacheSetRequest request;
        request.set_key(key);
        request.set_value(value);

        CacheSetResponse response;
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
    CacheClient cache_client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    DBClient db_client(grpc::CreateChannel("10.128.0.33:50051", grpc::InsecureChannelCredentials()));

    // Example usage of Set and Get
    std::string key = "example_key";
    std::string value = "example_value";

    // Put example
    if (db_client.Put(key, value))
    {
        std::cout << "Put key-value pair successfully." << std::endl;
    }
    else
    {
        std::cout << "Failed to put key-value pair." << std::endl;
    }

    std::string result = cache_client.Get(key);
    if (!result.empty())
    {
        std::cout << "Got value: " << result << std::endl;
    }
    else
    {
        std::cout << "Failed to get value or key not found." << std::endl;
    }

    // Delete example
    if (db_client.Delete(key))
    {
        std::cout << "Deleted key-value pair successfully." << std::endl;
    }
    else
    {
        std::cout << "Failed to delete key-value pair." << std::endl;
    }

    return 0;
}
