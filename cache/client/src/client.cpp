#include "client.hpp"
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>

memcached_st *Client::create_mc(void)
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

void Client::free_mc(memcached_st *memc)
{
    memcached_pool_push(pool, memc);
}

Client::Client(std::shared_ptr<Channel> cache_channel, std::shared_ptr<Channel> db_channel, Tracker *tracker)
    : cache_client_(std::make_unique<CacheClient>(cache_channel)),
      db_client_(std::make_unique<DBClient>(db_channel))
{
    if (tracker != nullptr)
    {
        cache_client_->SetTracker(tracker);
        db_client_->SetTracker(tracker);
    }
    // memc = create_mc();
    const char *config_string =
        "--SERVER=localhost:11211";

    pool = memcached_pool(config_string, strlen(config_string));
    assert(pool != nullptr);
}

std::string Client::Get(const std::string &key)
{
#ifdef GET_LOCAL_CACHE
    std::string value = GetLocalCache(key);
#else
    std::string value = cache_client_->Get(key);
#endif

#ifdef CLIENT_DRIVEN_FILL
    if (value == "")
    {
#ifdef DEBUG
        std::cout << "Miss: " << key << std::endl;
#endif
        value = GetWarmDB(key);
        assert(value != "");
        SetCache(key, value, ttl_);
    }
#endif
    return value;
}

std::string Client::GetWarmDB(const std::string &key)
{
    return db_client_->Get(key);
}

bool Client::Set(const std::string &key, const std::string &value, int ttl, float ew)
{
    // Call Put method on DBClient to store data
    bool db_result = db_client_->Put(key, value, ew);

    // Optionally handle the result if necessary
    if (!db_result)
    {
        return false; // Or handle the error as needed
    }

    return true;
}

bool Client::SetWarm(const std::string &key, const std::string &value, int ttl)
{
    // Call Put method on DBClient to store data
    bool db_result = db_client_->PutWarm(key, value);

    // Optionally handle the result if necessary
    if (!db_result)
    {
        return false; // Or handle the error as needed
    }
    return true;
}

void Client::SetTTL(const int32_t &ttl)
{
    ttl_ = ttl;

    cache_client_->SetTTL(ttl);
}

float Client::GetMR(void)
{
    return cache_client_->GetMR();
}

int Client::GetLoad(void)
{
    return db_client_->GetLoad();
}

bool Client::SetCache(const std::string &key, const std::string &value, int ttl)
{

#ifdef GET_LOCAL_CACHE
    // Get the value associated with the key from Memcached
    memcached_st *memc = create_mc();

    memcached_return_t set_result;
    set_result = memcached_set(memc, key.c_str(), key.size(), value.c_str(), value.size(), (time_t)ttl_, (uint32_t)0);

    free_mc(memc);
    if (set_result == MEMCACHED_SUCCESS)
    {
        return true;
        // free(value);
    }
    else
    {
        std::cerr << "Cache set failed: " << key << std::endl;
        return false;
    }

#else

    return cache_client_->Set(key, value, ttl);
#endif
}

std::string Client::GetLocalCache(const std::string &key)
{
    char *value;
    memcached_return_t rc;
    size_t value_length = 0;
    uint32_t flags = 0;

    // Get the value associated with the key from Memcached
    memcached_st *memc = create_mc();

    value = memcached_get(memc, key.c_str(), key.size(), &value_length, &flags, &rc);

    std::string result = "";
    if (rc == MEMCACHED_SUCCESS && value != NULL && value_length != 0)
    {
        result = std::string(value, value_length);
        // free(value);
    }
    else
    {
        ;
        // std::cout << "key: " << key << ", key.size(): " << key.size() << std::endl;
        // std::cerr << "Failed to retrieve value from Memcached: " << memcached_strerror(memc, rc) << std::endl;
    }
    free_mc(memc);

    return result;
}