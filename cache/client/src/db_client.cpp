#include "client.hpp"

// Implementation of DBClient methods

DBClient::DBClient(std::shared_ptr<Channel> channel)
    : stub_(DBService::NewStub(channel)) {}

bool DBClient::Put(const std::string &key, const std::string &value)
{
    DBPutRequest request;
    request.set_key(key);
    request.set_value(value);

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
