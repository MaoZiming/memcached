#include <libmemcached/memcached.h>
#include <grpcpp/grpcpp.h>
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
using freshCache::CacheSetTTLRequest;
using freshCache::CacheSetTTLResponse;

using freshCache::DBDeleteRequest;
using freshCache::DBDeleteResponse;
using freshCache::DBGetRequest;
using freshCache::DBGetResponse;
using freshCache::DBPutRequest;
using freshCache::DBPutResponse;
using freshCache::DBService;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class DBClient
{
public:
    DBClient(std::shared_ptr<Channel> channel)
        : stub_(DBService::NewStub(channel)) {}

    std::string Get(const std::string &key)
    {
        DBGetRequest request;
        request.set_key(key);

        DBGetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);

        if (status.ok())
        {
            if (response.found())
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

private:
    std::unique_ptr<DBService::Stub> stub_;
};

class CacheServiceImpl final : public CacheService::Service
{
public:
    CacheServiceImpl(std::shared_ptr<Channel> db_channel)
        : db_client_(db_channel)
    {
        memc = memcached_create(NULL);
        memcached_server_st *servers = memcached_server_list_append(NULL, "localhost", 11211, &rc);
        rc = memcached_server_push(memc, servers);
        memcached_server_list_free(servers);

        if (rc != MEMCACHED_SUCCESS)
        {
            throw std::runtime_error("Failed to connect to Memcached server.");
        }
    }

    ~CacheServiceImpl()
    {
        memcached_free(memc);
    }

    grpc::Status Get(grpc::ServerContext *context, const CacheGetRequest *request, CacheGetResponse *response) override
    {
        char *value = nullptr;
        size_t value_length;
        uint32_t flags;
        memcached_return_t result;

#ifdef DEBUG
        std::cout << "Get: " << request->key() << std::endl;
#endif

        value = memcached_get(memc, request->key().c_str(), request->key().size(), &value_length, &flags, &result);
        if (result == MEMCACHED_SUCCESS)
        {
            cache_hits++;

#ifdef DEBUG
            std::cout << "Cache Hit!" << std::endl;
#endif
            response->set_value(std::string(value, value_length));
            response->set_success(true);
        }
        else
        {
            cache_miss++;

#ifdef DEBUG
            std::cout << "Cache Miss!" << std::endl;
#endif
            // Cache miss: fetch from the database
            std::string db_value = db_client_.Get(request->key());
            if (!db_value.empty())
            {
                response->set_value(db_value);
                response->set_success(true);

                // Optionally, cache the value for future requests
#ifdef DEBUG
                std::cout << "Cache: " << ttl_ << std::endl;
#endif
                memcached_set(memc, request->key().c_str(), request->key().size(), db_value.c_str(), db_value.size(), (time_t)ttl_, (uint32_t)0);
            }
            else
            {
                response->set_success(false);
            }
        }

        if (value)
        {
            free(value);
        }
        return grpc::Status::OK;
    }

    grpc::Status Set(grpc::ServerContext *context, const CacheSetRequest *request, CacheSetResponse *response) override
    {
        memcached_return_t result;

#ifdef DEBUG
        std::cout << "Set: " << request->key() << ", " << request->value()
                  << ", TTL: " << request->ttl() << std::endl;
#endif

        // Convert ttl to time_t. Using (time_t)request->ttl() is sufficient as it should be already in seconds.
        time_t ttl = static_cast<time_t>(request->ttl());

        result = memcached_set(memc, request->key().c_str(), request->key().size(),
                               request->value().c_str(), request->value().size(),
                               ttl, (uint32_t)0);
        if (result == MEMCACHED_SUCCESS)
        {
            response->set_success(true);
        }
        else
        {
            response->set_success(false);
        }

        return grpc::Status::OK;
    }

    grpc::Status SetTTL(grpc::ServerContext *context, const CacheSetTTLRequest *request, CacheSetTTLResponse *response) override
    {
        memcached_return_t result;

#ifdef DEBUG
        std::cout << "SetTTL: " << request->ttl() << std::endl;
#endif

        ttl_ = request->ttl();
        response->set_success(true);

        return grpc::Status::OK;
    }

private:
    memcached_st *memc;
    memcached_return rc;
    DBClient db_client_;
    // We assume a uniform ttl for the entire cache.
    // Default to no ttl requirement.
    int32_t ttl_ = 0;
    int32_t cache_hits = 0;
    int32_t cache_miss = 0;
};

void RunServer()
{
    std::string server_address("10.128.0.34:50051");
    std::string db_address("10.128.0.33:50051");
    CacheServiceImpl service(grpc::CreateChannel(db_address, grpc::InsecureChannelCredentials()));

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    RunServer();
    return 0;
}
