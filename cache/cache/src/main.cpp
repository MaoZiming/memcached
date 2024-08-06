#include <libmemcached/memcached.h>
#include <grpcpp/grpcpp.h>
#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>

using freshCache::CacheService;
using freshCache::GetRequest;
using freshCache::GetResponse;
using freshCache::SetRequest;
using freshCache::SetResponse;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class CacheServiceImpl final : public CacheService::Service
{
public:
    CacheServiceImpl()
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

    grpc::Status Get(grpc::ServerContext *context, const GetRequest *request, GetResponse *response) override
    {
        char *value = nullptr;
        size_t value_length;
        uint32_t flags;
        memcached_return_t result;

        value = memcached_get(memc, request->key().c_str(), request->key().size(), &value_length, &flags, &result);
        if (result == MEMCACHED_SUCCESS)
        {
            response->set_value(std::string(value, value_length));
            response->set_success(true);
        }
        else
        {
            response->set_success(false);
        }

        if (value)
        {
            free(value);
        }
        return grpc::Status::OK;
    }

    grpc::Status Set(grpc::ServerContext *context, const SetRequest *request, SetResponse *response) override
    {
        memcached_return_t result;

        result = memcached_set(memc, request->key().c_str(), request->key().size(), request->value().c_str(), request->value().size(), (time_t)0, (uint32_t)0);
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

private:
    memcached_st *memc;
    memcached_return rc;
};

void RunServer()
{
    std::string server_address("0.0.0.0:50051");
    CacheServiceImpl service;

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
