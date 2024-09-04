#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <grpcpp/grpcpp.h>
#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>
#include <myproto/db_service.grpc.pb.h>
#include <myproto/db_service.pb.h>

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include "client.hpp"
#include <atomic>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// #define DEBUG

class CacheServiceImpl final : public CacheService::Service
{
public:
    CacheServiceImpl(std::shared_ptr<Channel> db_channel)
        : db_client_(db_channel)
    {
        // memc = create_mc();
        const char *config_string =
            "--SERVER=localhost:11211";

        pool = memcached_pool(config_string, strlen(config_string));
        assert(pool != nullptr);
    }

    ~CacheServiceImpl()
    {
        memcached_pool_destroy(pool);
        // if (memc)
        //     memcached_free(memc);
    }

    memcached_st *create_mc(void)
    {
        memcached_return_t rc;
        memcached_st *memc = memcached_pool_pop(pool, true, &rc);
        if (rc != MEMCACHED_SUCCESS)
        {
            std::cerr << rc << std::endl;
        }
        assert(rc == MEMCACHED_SUCCESS);
        assert(memc != nullptr);
        return memc;
    }

    void free_mc(memcached_st *memc)
    {
        memcached_pool_push(pool, memc);
    }

    memcached_st *_create_mc(void)
    {
        memcached_return rc;
        memcached_st *memc = memcached_create(NULL);
        memcached_server_st *servers = memcached_server_list_append(NULL, "localhost", 11211, &rc);
        servers = memcached_server_list_append(servers, "localhost", 11212, &rc);
        memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
        rc = memcached_server_push(memc, servers);
        memcached_server_list_free(servers);

        if (rc != MEMCACHED_SUCCESS)
        {
            throw std::runtime_error("Failed to connect to Memcached server.");
        }
        return memc;
    }

    void _free_mc(memcached_st *memc)
    {
        memcached_free(memc);
    }

    grpc::Status Get(grpc::ServerContext *context, const CacheGetRequest *request, CacheGetResponse *response) override
    {
        char *value = nullptr;
        size_t value_length = 0;
        uint32_t flags = 0;
        memcached_return_t result;
        memcached_st *memc = create_mc();

#ifdef DEBUG
        std::cout << "Get: " << request->key() << std::endl;
        std::cout << request->key() << std::endl;
#endif
        value = memcached_get(memc, request->key().c_str(), request->key().size(), &value_length, &flags, &result);
        if (result == MEMCACHED_SUCCESS)
        {
            cache_hits_++;
#ifdef DEBUG
            std::cout << "key: " << request->key() << ", Cache Hit!" << std::endl;
#endif
            response->set_value(std::string(value, value_length));
            response->set_success(true);
        }
        else
        {
            cache_miss_++;
#ifdef DEBUG
            std::cout << "key: " << request->key() << ", Cache Miss!" << std::endl;
#endif
            // Cache miss: fetch from the database

#ifndef CLIENT_DRIVEN_FILL
            std::cout << "Miss Key: " << request->key() << std::endl;
            std::string db_value = db_client_.Get(request->key());
            std::cout << "Miss key finished: " << request->key() << std::endl;
            if (!db_value.empty())
            {
                response->set_value(db_value);
                response->set_success(true);

                // Optionally, cache the value for future requests
                memcached_return_t set_result;
                set_result = memcached_set(memc, request->key().c_str(), request->key().size(), db_value.c_str(), db_value.size(), (time_t)ttl_, (uint32_t)0);

                /*
                  MEMCACHED_SUCCESS,
                  MEMCACHED_FAILURE,
                  MEMCACHED_HOST_LOOKUP_FAILURE, // getaddrinfo() and getnameinfo() only
                  MEMCACHED_CONNECTION_FAILURE,
                  MEMCACHED_CONNECTION_BIND_FAILURE, // DEPRECATED, see MEMCACHED_HOST_LOOKUP_FAILURE
                  MEMCACHED_WRITE_FAILURE,
                  MEMCACHED_READ_FAILURE,
                  MEMCACHED_UNKNOWN_READ_FAILURE,
                  MEMCACHED_PROTOCOL_ERROR,
                  MEMCACHED_CLIENT_ERROR,
                  MEMCACHED_SERVER_ERROR, // Server returns "SERVER_ERROR"
                  MEMCACHED_ERROR,        // Server returns "ERROR"
                  MEMCACHED_DATA_EXISTS,
                  MEMCACHED_DATA_DOES_NOT_EXIST,
                  MEMCACHED_NOTSTORED,
                  MEMCACHED_STORED,
                  MEMCACHED_NOTFOUND,
                  MEMCACHED_MEMORY_ALLOCATION_FAILURE,
                  MEMCACHED_PARTIAL_READ,
                  MEMCACHED_SOME_ERRORS,
                  MEMCACHED_NO_SERVERS,
                  MEMCACHED_END,
                  MEMCACHED_DELETED,
                  MEMCACHED_VALUE,
                  MEMCACHED_STAT,
                  MEMCACHED_ITEM,
                  MEMCACHED_ERRNO,
                  MEMCACHED_FAIL_UNIX_SOCKET,
                  MEMCACHED_NOT_SUPPORTED,
                  MEMCACHED_NO_KEY_PROVIDED,
                  MEMCACHED_FETCH_NOTFINISHED,
                  MEMCACHED_TIMEOUT,
                  MEMCACHED_BUFFERED,
                */

                if (set_result == MEMCACHED_BUFFERED || set_result == MEMCACHED_DELETED || set_result == MEMCACHED_END || set_result == MEMCACHED_ITEM || set_result == MEMCACHED_STAT || set_result == MEMCACHED_STORED || set_result == MEMCACHED_SUCCESS || set_result == MEMCACHED_VALUE)
                {
                    // std::cerr << "Success: " << memcached_strerror(memc, set_result) << ", TTL: " << ttl_ << ", key: " << request->key() << ", value length: " << db_value.length() << ", key length: " << request->key().length() << std::endl;
                    ;
                }
                else
                {
                    std::cerr << "Failure: " << memcached_strerror(memc, set_result) << ", TTL: " << ttl_ << ", key: " << request->key() << ", value: " << db_value.length() << ", key length: " << request->key().length() << std::endl;
                }
#ifdef DEBUG
                if (set_result != MEMCACHED_SUCCESS)
                {
                    std::cout << set_result << std::endl;
                    std::cout << "Cache: " << ttl_ << ", key: " << request->key() << ", value: " << db_value << ", key length: " << request->key().length() << std::endl;
                }

                assert(set_result == MEMCACHED_SUCCESS);

                // TODO: For some reason doesn't work
                memcached_return_t get_result;
                char *value = memcached_get(memc, request->key().c_str(), request->key().size(), &value_length, &flags, &get_result);

                if (value == NULL)
                {
                    std::cout << "Cache: " << ttl_ << ", key: " << request->key() << ", value: " << db_value << ", key length: " << request->key().length() << std::endl;
                    std::cout << set_result << std::endl;
                    std::cout << get_result << std::endl;
                }
                assert(value != NULL);
#endif
            }
            else
            {
                std::cerr << "DB Key not found." << std::endl;
                response->set_success(false);
            }
#else
            // Let client fetches the value from the DB.
            response->set_value("");
            response->set_success(false);
#endif
        }

        // if (value)
        // {
        //     free(value);
        // }
        free_mc(memc);
        return grpc::Status::OK;
    }

    grpc::Status Set(grpc::ServerContext *context, const CacheSetRequest *request, CacheSetResponse *response) override
    {
        memcached_return_t result;

        memcached_st *memc = create_mc();

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
        free_mc(memc);
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

    grpc::Status GetMR(grpc::ServerContext *context, const CacheGetMRRequest *request, CacheGetMRResponse *response) override
    {
        memcached_return_t result;

        response->set_success(true);
#ifdef DEBUG
        std::cout << "cache_hits_: " << cache_hits_.load() << ", " << "cache_miss_: " << cache_miss_.load() << std::endl;
#endif
        if (cache_hits_.load() + cache_miss_.load() > 0)
            response->set_mr(static_cast<float>(cache_miss_.load()) / (cache_hits_.load() + cache_miss_.load()));
        else
            response->set_mr(-1);

        return grpc::Status::OK;
    }

    grpc::Status Invalidate(grpc::ServerContext *context, const CacheInvalidateRequest *request, CacheInvalidateResponse *response) override
    {
        memcached_return_t result;
        // #ifdef DEBUG
        std::cout << "Invalidate: " << request->key() << std::endl;
        // #endif

        memcached_st *memc = create_mc();
        result = memcached_delete(memc, request->key().c_str(), request->key().size(),
                                  (time_t)0);
        if (result == MEMCACHED_SUCCESS)
        {
            response->set_success(true);
        }
        else
        {
            response->set_success(false);
        }
        free_mc(memc);
        return grpc::Status::OK;
    }

    grpc::Status Update(grpc::ServerContext *context, const CacheUpdateRequest *request, CacheUpdateResponse *response) override
    {
        memcached_return_t result;
        memcached_st *memc = create_mc();

#ifdef DEBUG
        std::cout << "Update: " << request->key() << ", " << request->value()
                  << std::endl;
#endif
        result = memcached_replace(memc, request->key().c_str(), request->key().size(),
                                   request->value().c_str(), request->value().size(),
                                   (time_t)0, (uint32_t)0);
        if (result == MEMCACHED_SUCCESS)
        {
            response->set_success(true);
        }
        else
        {
            response->set_success(false);
        }
        free_mc(memc);
        return grpc::Status::OK;
    }

private:
    // memcached_st *memc;
    DBClient db_client_;
    memcached_pool_st *pool;
    // We assume a uniform ttl for the entire cache.
    // Default to no ttl requirement.
    int32_t ttl_ = 0;
    std::atomic<int32_t> cache_hits_{0};
    std::atomic<int32_t> cache_miss_{0};
};

void RunServer()
{
    std::string server_address("10.128.0.34:50051");
    std::string db_address("10.128.0.33:50051");
    std::shared_ptr<Channel> channel = grpc::CreateChannel(db_address, grpc::InsecureChannelCredentials());
    if (!channel)
    {
        std::cerr << "Failed to create channel to DB server." << std::endl;
        return;
    }

    CacheServiceImpl service(channel);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server)
    {
        std::cerr << "Failed to start server." << std::endl;
        return;
    }

    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    RunServer();
    return 0;
}
