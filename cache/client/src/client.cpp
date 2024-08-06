#include "client.hpp"

Client::Client(std::shared_ptr<Channel> cache_channel, std::shared_ptr<Channel> db_channel, Tracker *tracker)
    : cache_client_(std::make_unique<CacheClient>(cache_channel)),
      db_client_(std::make_unique<DBClient>(db_channel))
{
    cache_client_->SetTracker(tracker);
    db_client_->SetTracker(tracker);
}

std::string Client::Get(const std::string &key)
{
    return cache_client_->Get(key);
}

bool Client::Set(const std::string &key, const std::string &value, int ttl)
{
    // Call Put method on DBClient to store data
    bool db_result = db_client_->Put(key, value);

    // Optionally handle the result if necessary
    if (!db_result)
    {
        return false; // Or handle the error as needed
    }

    return true;
}

void Client::SetTTL(const int32_t &ttl)
{
    cache_client_->SetTTL(ttl);
}

float Client::GetMR(void)
{
    return cache_client_->GetMR();
}

float Client::GetLoad(void)
{
    return db_client_->GetLoad();
}