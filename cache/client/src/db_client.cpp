#include "client.hpp"

// Implementation of DBClient methods

DBClient::DBClient(std::shared_ptr<Channel> channel)
    : stub_(DBService::NewStub(channel)) {}

std::string DBClient::Get(const std::string &key)
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
            std::cerr << "DB Key not found." << std::endl;
        }
    }
    else
    {
        std::cerr << "RPC failed." << std::endl;
    }

    return "";
}

bool DBClient::Put(const std::string &key, const std::string &value, float ew)
{
    DBPutRequest request;
    request.set_key(key);
    request.set_value(value);

    if (tracker_)
    {
        assert(ew == ADAPTIVE_EW);
        ew = tracker_->get(key);
        request.set_ew(ew);
    }
    else
    {
        assert(ew != ADAPTIVE_EW);
        request.set_ew(ew);
    }

    DBPutResponse response;
    ClientContext context;

    Status status = stub_->Put(&context, request, &response);
    if (tracker_)
    {
        tracker_->write(key);
    }

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

bool DBClient::PutWarm(const std::string &key, const std::string &value)
{
    DBPutRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_ew(TTL_EW);

    DBPutResponse response;
    ClientContext context;

    Status status = stub_->Put(&context, request, &response);
    if (tracker_)
    {
        tracker_->write(key);
    }

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

bool DBClient::Delete(const std::string &key)
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

void DBClient::SetTracker(Tracker *tracker)
{
    tracker_ = tracker;
}

int DBClient::GetLoad(void)
{
    DBGetLoadRequest request;
    DBGetLoadResponse response;
    ClientContext context;
    Status status = stub_->GetLoad(&context, request, &response);

    if (status.ok())
    {
        return response.load();
    }
    else
    {
        std::cerr << "RPC failed." << std::endl;
        return -1.0;
    }
}