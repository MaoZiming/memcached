#include "client.hpp"

// Implementation of CacheClient methods

CacheClient::CacheClient(std::shared_ptr<Channel> channel)
    : stub_(CacheService::NewStub(channel)) {}

std::string CacheClient::Get(const std::string &key)
{
    CacheGetRequest request;
    request.set_key(key);

    CacheGetResponse response;
    ClientContext context;

    Status status = stub_->Get(&context, request, &response);

    if (tracker_)
    {
        tracker_->read(key);
    }

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

bool CacheClient::Set(const std::string &key, const std::string &value, int ttl)
{
    CacheSetRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_ttl(ttl);

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

bool CacheClient::SetTTL(const int32_t &ttl)
{
    CacheSetTTLRequest request;
    request.set_ttl(ttl);

    CacheSetTTLResponse response;
    ClientContext context;

    Status status = stub_->SetTTL(&context, request, &response);

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

void CacheClient::SetTracker(Tracker *tracker)
{
    tracker_ = tracker;
}

float CacheClient::GetMR(void)
{
    CacheGetMRRequest request;

    CacheGetMRResponse response;
    ClientContext context;

    Status status = stub_->GetMR(&context, request, &response);

    if (status.ok())
    {
        return response.mr();
    }
    else
    {
        std::cerr << "RPC failed." << std::endl;
    }

    return -1;
}
