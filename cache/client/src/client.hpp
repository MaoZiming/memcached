#ifndef CLIENT_HPP
#define CLIENT_HPP

#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "policy.hpp"
#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>
#include <myproto/db_service.grpc.pb.h>
#include <myproto/db_service.pb.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using freshCache::CacheGetMRRequest;
using freshCache::CacheGetMRResponse;
using freshCache::CacheGetRequest;
using freshCache::CacheGetResponse;
using freshCache::CacheService;
using freshCache::CacheSetRequest;
using freshCache::CacheSetResponse;
using freshCache::CacheSetTTLRequest;
using freshCache::CacheSetTTLResponse;

using freshCache::DBDeleteRequest;
using freshCache::DBDeleteResponse;
using freshCache::DBGetLoadRequest;
using freshCache::DBGetLoadResponse;
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
    explicit DBClient(std::shared_ptr<Channel> channel);
    bool Put(const std::string &key, const std::string &value);
    bool Delete(const std::string &key);
    void SetTracker(Tracker *tracker);
    std::string Get(const std::string &key);
    float GetLoad(void);

private:
    std::unique_ptr<DBService::Stub> stub_;
    Tracker *tracker_ = nullptr;
};

class CacheClient
{
public:
    explicit CacheClient(std::shared_ptr<Channel> channel);
    std::string Get(const std::string &key);
    bool Set(const std::string &key, const std::string &value, int ttl = 0);
    bool SetTTL(const int32_t &ttl);
    void SetTracker(Tracker *tracker);
    float GetMR(void);

private:
    std::unique_ptr<CacheService::Stub> stub_;
    Tracker *tracker_ = nullptr;
};

class Client
{
public:
    explicit Client(std::shared_ptr<Channel> cache_channel, std::shared_ptr<Channel> db_channel, Tracker *tracker);
    std::string Get(const std::string &key);
    bool Set(const std::string &key, const std::string &value, int ttl = 0);
    void SetTTL(const int32_t &ttl);
    float GetMR(void);
    float GetLoad(void);

private:
    std::unique_ptr<DBClient> db_client_;
    std::unique_ptr<CacheClient> cache_client_;
};

#endif // CLIENT_HPP
