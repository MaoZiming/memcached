#ifndef CLIENT_HPP
#define CLIENT_HPP

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "policy.hpp"
#include <myproto/cache_service.pb.h>
#include <myproto/cache_service.grpc.pb.h>
#include <myproto/db_service.grpc.pb.h>
#include <myproto/db_service.pb.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <condition_variable>
#include <mutex>
#include <future>
#define ASSERT(condition, message)             \
    do                                         \
    {                                          \
        if (!(condition))                      \
        {                                      \
            std::cerr << message << std::endl; \
            assert(condition);                 \
        }                                      \
    } while (false)

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using freshCache::CacheGetFreshnessStatsRequest;
using freshCache::CacheGetFreshnessStatsResponse;
using freshCache::CacheGetMRRequest;
using freshCache::CacheGetMRResponse;
using freshCache::CacheGetRequest;
using freshCache::CacheGetResponse;
using freshCache::CacheInvalidateRequest;
using freshCache::CacheInvalidateResponse;
using freshCache::CacheService;
using freshCache::CacheSetRequest;
using freshCache::CacheSetResponse;
using freshCache::CacheSetTTLRequest;
using freshCache::CacheSetTTLResponse;
using freshCache::CacheUpdateRequest;
using freshCache::CacheUpdateResponse;

using freshCache::DBDeleteRequest;
using freshCache::DBDeleteResponse;
using freshCache::DBGetLoadRequest;
using freshCache::DBGetLoadResponse;
using freshCache::DBGetReadCountRequest;
using freshCache::DBGetReadCountResponse;
using freshCache::DBGetRequest;
using freshCache::DBGetResponse;
using freshCache::DBGetWriteCountRequest;
using freshCache::DBGetWriteCountResponse;
using freshCache::DBPutRequest;
using freshCache::DBPutResponse;
using freshCache::DBService;
using freshCache::DBStartRecordRequest;
using freshCache::DBStartRecordResponse;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

const int LONG_TTL = 0; /* Never expires */

const int ADAPTIVE_EW = 0;
const int TTL_EW = -2;
const int INVALIDATE_EW = -3;
const int UPDATE_EW = -4;
const int MAX_CONCURRENT_RPCS = 100;

class DBClient
{
public:
    DBClient(std::shared_ptr<Channel> channel)
        : stub_(DBService::NewStub(channel))
    {
        cq_thread_ = std::thread(&DBClient::AsyncCompleteRpc, this);
    }

    ~DBClient()
    {
        cq_.Shutdown();
        cq_thread_.join();
    }

    // Modified AsyncGet to return a std::future
    std::future<std::string> AsyncGet(const std::string &key)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBGetRequest request;
        request.set_key(key);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GET;
        call->key = key;
        call->get_promise = std::make_shared<std::promise<std::string>>();

        // Get the future from the promise
        std::future<std::string> result_future = call->get_promise->get_future();

        // Start the asynchronous RPC
        call->get_response_reader = stub_->AsyncGet(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->get_response_reader->Finish(&call->get_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous Get method that uses AsyncGet and waits for the result
    std::string Get(const std::string &key, std::chrono::milliseconds timeout_duration)
    {
        try
        {
            std::future<std::string> result_future = AsyncGet(key);

            // Wait for the result for a limited time
            if (result_future.wait_for(timeout_duration) == std::future_status::ready)
            {
                std::string result = result_future.get(); // Get the result if it's ready
                if (result.empty())
                {
                    std::cerr << "DB Key not found" << std::endl;
                }
                return result;
            }
            else
            {
                std::cerr << "Get operation timed out after waiting for " << timeout_duration.count() << "ms. " << std::endl;
                return ""; // Timeout occurred
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "Get failed: " << e.what() << std::endl;
            return "";
        }
    }

    std::string Get(const std::string &key)
    {
        try
        {
            std::future<std::string> result_future = AsyncGet(key);
            std::string result = result_future.get(); // Wait for the result
            if (result.empty())
            {
                std::cerr << "DB Key not found." << std::endl;
            }
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Get failed: " << e.what() << std::endl;
            return "";
        }
    }

    memcached_st *create_mc(memcached_pool_st *pool)
    {
        memcached_return_t rc;
        memcached_st *memc = memcached_pool_pop(pool, true, &rc);
        if (rc != MEMCACHED_SUCCESS)
        {
            printf("Error: %s\n", memcached_strerror(memc, rc));
        }
        assert(rc == MEMCACHED_SUCCESS);
        assert(memc != nullptr);
        return memc;
    }

    void free_mc(memcached_pool_st *pool, memcached_st *memc)
    {
        memcached_pool_push(pool, memc);
    }

    std::future<std::string> AsyncFill(const std::string &key, memcached_pool_st *pool, int ttl)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Create a promise and return a future associated with it
        auto promise = std::make_shared<std::promise<std::string>>();
        std::future<std::string> result_future = promise->get_future();

        // Launch an async task to query the DB and fill the cache when done
        std::thread([this, key, pool, ttl, promise]()
                    {
                        try
                        {
                            // Asynchronously get the value from the DB
                            std::string db_value = Get(key);
                            if (!db_value.empty())
                            {
                                // Store the value in Memcached
                                memcached_st *memc = create_mc(pool);
                                memcached_set(memc, key.c_str(), key.size(), db_value.c_str(), db_value.size(), (time_t)ttl, (uint32_t)0);
                                free_mc(pool, memc);
#ifdef DEBUG
                                std::cerr << "Miss finished: " << key << std::endl;
#endif
                                // Fulfill the promise with success
                                promise->set_value(db_value);
                            }
                            else
                            {
                                std::cerr << "Async: DB Key not found." << std::endl;
                            }
                        }
                        catch (const std::exception &e)
                        {
                            // If an exception occurs, set the promise exception
                            promise->set_exception(std::make_exception_ptr(e));
                        }

                        // Decrease the current RPC count and notify the condition variable
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            --current_rpcs;
                        }
                        cv_.notify_one(); })
            .detach(); // Detach the thread to avoid blocking

        // Return the future so the caller can wait on it
        return result_future;
    }

    std::future<bool> AsyncPut(const std::string &key, const std::string &value, float ew)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBPutRequest request;
        request.set_key(key);
        request.set_value(value);

        if (tracker_ && ew == ADAPTIVE_EW)
        {
            ew = tracker_->get_ew(key);
            request.set_ew(ew);
        }
        else
        {
            request.set_ew(ew);
        }

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::PUT;
        call->key = key;
        call->put_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->put_promise->get_future();

        // Start the asynchronous RPC
        call->put_response_reader = stub_->AsyncPut(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->put_response_reader->Finish(&call->put_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    bool Put(const std::string &key, const std::string &value, float ew)
    {
        try
        {
            std::future<bool> result_future = AsyncPut(key, value, ew);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Put failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Modified PutWarm method using AsyncPut
    bool PutWarm(const std::string &key, const std::string &value)
    {
        // Reuse AsyncPut with a fixed ew
        return Put(key, value, TTL_EW);
    }

    // Modified Delete method using Async pattern
    std::future<bool> AsyncDelete(const std::string &key)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBDeleteRequest request;
        request.set_key(key);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::DELETE;
        call->key = key;
        call->delete_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->delete_promise->get_future();

        // Start the asynchronous RPC
        call->delete_response_reader = stub_->AsyncDelete(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->delete_response_reader->Finish(&call->delete_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous Delete method that waits for the result
    bool Delete(const std::string &key)
    {
        try
        {
            std::future<bool> result_future = AsyncDelete(key);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Delete failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Asynchronous GetLoad method returning a future
    std::future<int> AsyncGetLoad()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBGetLoadRequest request;

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GETLOAD;
        call->load_promise = std::make_shared<std::promise<int>>();

        // Get the future from the promise
        std::future<int> result_future = call->load_promise->get_future();

        // Start the asynchronous RPC
        call->get_load_response_reader = stub_->AsyncGetLoad(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->get_load_response_reader->Finish(&call->get_load_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous GetLoad method
    int GetLoad()
    {
        try
        {
            std::future<int> result_future = AsyncGetLoad();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetLoad failed: " << e.what() << std::endl;
            return -1;
        }
    }

    // Asynchronous StartRecord method returning a future
    std::future<bool> AsyncStartRecord()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBStartRecordRequest request;
        // Set the log_path in the request if necessary (assuming log_path is part of the message)
        // request.set_log_path(log_path); // Uncomment if log_path is needed

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::STARTRECORD;
        call->start_record_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->start_record_promise->get_future();

        // Start the asynchronous RPC
        call->start_record_response_reader = stub_->AsyncStartRecord(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->start_record_response_reader->Finish(&call->start_record_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous StartRecord method
    bool StartRecord(void)
    {
        try
        {
            std::future<bool> result_future = AsyncStartRecord();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "StartRecord failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Asynchronous GetDBReadCount method returning a future
    std::future<int> AsyncGetDBReadCount()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBGetReadCountRequest request;

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GETREADCOUNT;
        call->read_count_promise = std::make_shared<std::promise<int>>();

        // Get the future from the promise
        std::future<int> result_future = call->read_count_promise->get_future();

        // Start the asynchronous RPC
        call->get_read_count_response_reader = stub_->AsyncGetReadCount(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->get_read_count_response_reader->Finish(&call->get_read_count_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous GetDBReadCount method
    int GetDBReadCount()
    {
        try
        {
            std::future<int> result_future = AsyncGetDBReadCount();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetDBReadCount failed: " << e.what() << std::endl;
            return -1;
        }
    }

    // Asynchronous GetDBWriteCount method returning a future
    std::future<int> AsyncGetDBWriteCount()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        DBGetWriteCountRequest request;

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GETWRITECOUNT;
        call->write_count_promise = std::make_shared<std::promise<int>>();

        // Get the future from the promise
        std::future<int> result_future = call->write_count_promise->get_future();

        // Start the asynchronous RPC
        call->get_write_count_response_reader = stub_->AsyncGetWriteCount(&call->context, request, &cq_);

        // Request that, upon completion of the RPC, "call" be updated
        call->get_write_count_response_reader->Finish(&call->get_write_count_reply, &call->status, (void *)call);

        return result_future; // Return the future immediately
    }

    // Synchronous GetDBWriteCount method
    int GetDBWriteCount()
    {
        try
        {
            std::future<int> result_future = AsyncGetDBWriteCount();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetDBWriteCount failed: " << e.what() << std::endl;
            return -1;
        }
    }

    void SetTracker(Tracker *tracker)
    {
        tracker_ = tracker;
    }

private:
    std::unique_ptr<DBService::Stub> stub_;
    Tracker *tracker_ = nullptr;
    std::mutex mutex_;
    std::condition_variable cv_;
    int current_rpcs = 0;

    grpc::CompletionQueue cq_;
    std::thread cq_thread_;

    struct AsyncClientCall
    {
        enum class CallType
        {
            GET,
            PUT,
            DELETE,
            GETLOAD,
            GETREADCOUNT,
            GETWRITECOUNT,
            STARTRECORD,
        };

        CallType call_type;
        std::string key;

        // For Get RPC
        DBGetResponse get_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBGetResponse>> get_response_reader;
        std::shared_ptr<std::promise<std::string>> get_promise;

        // For Put RPC
        DBPutResponse put_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBPutResponse>> put_response_reader;
        std::shared_ptr<std::promise<bool>> put_promise;

        // For Delete RPC
        DBDeleteResponse delete_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBDeleteResponse>> delete_response_reader;
        std::shared_ptr<std::promise<bool>> delete_promise;

        // For GetLoad RPC
        DBGetLoadResponse get_load_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBGetLoadResponse>> get_load_response_reader;
        std::shared_ptr<std::promise<int>> load_promise;

        DBStartRecordResponse start_record_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBStartRecordResponse>> start_record_response_reader;
        std::shared_ptr<std::promise<bool>> start_record_promise;

        // For GetDBReadCount RPC
        DBGetReadCountResponse get_read_count_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBGetReadCountResponse>> get_read_count_response_reader;
        std::shared_ptr<std::promise<int>> read_count_promise;

        // For GetDBWriteCount RPC
        DBGetWriteCountResponse get_write_count_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<DBGetWriteCountResponse>> get_write_count_response_reader;
        std::shared_ptr<std::promise<int>> write_count_promise;

        grpc::ClientContext context;
        grpc::Status status;
    };

    void AsyncCompleteRpc()
    {
        void *got_tag;
        bool ok = false;

        while (cq_.Next(&got_tag, &ok))
        {
            AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

            if (call->status.ok())
            {
                switch (call->call_type)
                {
                case AsyncClientCall::CallType::GET:
                    if (call->get_reply.found())
                    {
                        call->get_promise->set_value(call->get_reply.value());
                    }
                    else
                    {
                        call->get_promise->set_value("");
                    }
                    break;
                case AsyncClientCall::CallType::PUT:
                    call->put_promise->set_value(call->put_reply.success());
                    break;
                case AsyncClientCall::CallType::DELETE:
                    call->delete_promise->set_value(call->delete_reply.success());
                    break;
                case AsyncClientCall::CallType::GETLOAD:
                    call->load_promise->set_value(call->get_load_reply.load());
                    break;
                case AsyncClientCall::CallType::STARTRECORD:
                    call->start_record_promise->set_value(call->start_record_reply.success());
                    break;
                case AsyncClientCall::CallType::GETREADCOUNT:
                    call->read_count_promise->set_value(call->get_read_count_reply.read_count());
                    break;
                case AsyncClientCall::CallType::GETWRITECOUNT:
                    call->write_count_promise->set_value(call->get_write_count_reply.write_count());
                    break;
                }
            }
            else
            {
                // Set exception in promise in case of RPC failure
                if (call->call_type == AsyncClientCall::CallType::GET)
                {
                    call->get_promise->set_exception(std::make_exception_ptr(
                        std::runtime_error("GET RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::PUT)
                {
                    call->put_promise->set_exception(std::make_exception_ptr(
                        std::runtime_error("PUT RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::DELETE)
                {
                    call->delete_promise->set_exception(std::make_exception_ptr(
                        std::runtime_error("DELETE RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::GETLOAD)
                {
                    call->load_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error("GETLOAD RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::STARTRECORD)
                {
                    call->load_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error("STARTRECORD RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::GETREADCOUNT)
                {
                    call->read_count_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error("GETREADCOUNT RPC failed: " + call->status.error_message())));
                }
                else if (call->call_type == AsyncClientCall::CallType::GETWRITECOUNT)
                {
                    call->write_count_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error("GETWRITECOUNT RPC failed: " + call->status.error_message())));
                }
            }

            // Decrement current_rpcs and notify
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --current_rpcs;
            }
            cv_.notify_one();

            delete call;
        }
    }
};

class CacheClient
{
public:
    CacheClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(CacheService::NewStub(channel))
    {
        // Start the completion queue thread
        cq_thread_ = std::thread(&CacheClient::AsyncCompleteRpc, this);
    }

    ~CacheClient()
    {
        // Shutdown the completion queue and join the thread
        cq_.Shutdown();
        cq_thread_.join();
    }

    // Asynchronous Get method returning a future
    std::future<std::string> GetAsync(const std::string &key)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheGetRequest request;
        request.set_key(key);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GET;
        call->key = key;
        call->get_promise = std::make_shared<std::promise<std::string>>();

        // Get the future from the promise
        std::future<std::string> result_future = call->get_promise->get_future();

        // Start the asynchronous RPC
        call->get_response_reader = stub_->AsyncGet(&call->context, request, &cq_);
        call->get_response_reader->Finish(&call->get_reply, &call->status, (void *)call);

        return result_future;
    }

    // Asynchronous Set method returning a future
    std::future<bool> SetAsync(const std::string &key, const std::string &value, int ttl)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheSetRequest request;
        request.set_key(key);
        request.set_value(value);
        request.set_ttl(ttl);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::SET;
        call->key = key;
        call->set_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->set_promise->get_future();

        // Start the asynchronous RPC
        call->set_response_reader = stub_->AsyncSet(&call->context, request, &cq_);
        call->set_response_reader->Finish(&call->set_reply, &call->status, (void *)call);

        return result_future;
    }

    // Asynchronous Invalidate method returning a future
    std::future<bool> InvalidateAsync(const std::string &key)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheInvalidateRequest request;
        request.set_key(key);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::INVALIDATE;
        call->key = key;
        call->invalidate_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->invalidate_promise->get_future();

        // Start the asynchronous RPC
        call->invalidate_response_reader = stub_->AsyncInvalidate(&call->context, request, &cq_);
        call->invalidate_response_reader->Finish(&call->invalidate_reply, &call->status, (void *)call);

        return result_future;
    }

    // Asynchronous Update method returning a future
    std::future<bool> UpdateAsync(const std::string &key, const std::string &value, int ttl)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheUpdateRequest request;
        request.set_key(key);
        request.set_value(value);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::UPDATE;
        call->key = key;
        call->update_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->update_promise->get_future();

        // Start the asynchronous RPC
        call->update_response_reader = stub_->AsyncUpdate(&call->context, request, &cq_);
        call->update_response_reader->Finish(&call->update_reply, &call->status, (void *)call);

        return result_future;
    }

    // Asynchronous SetTTL method returning a future
    std::future<bool> SetTTLAsync(int32_t ttl)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheSetTTLRequest request;
        request.set_ttl(ttl);

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::SETTTL;
        call->set_ttl_promise = std::make_shared<std::promise<bool>>();

        // Get the future from the promise
        std::future<bool> result_future = call->set_ttl_promise->get_future();

        // Start the asynchronous RPC
        call->set_ttl_response_reader = stub_->AsyncSetTTL(&call->context, request, &cq_);
        call->set_ttl_response_reader->Finish(&call->set_ttl_reply, &call->status, (void *)call);

        return result_future;
    }

    // Asynchronous GetMR method returning a future
    std::future<float> GetMRAsync()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheGetMRRequest request;

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GETMR;
        call->get_mr_promise = std::make_shared<std::promise<float>>();

        // Get the future from the promise
        std::future<float> result_future = call->get_mr_promise->get_future();

        // Start the asynchronous RPC
        call->get_mr_response_reader = stub_->AsyncGetMR(&call->context, request, &cq_);
        call->get_mr_response_reader->Finish(&call->get_mr_reply, &call->status, (void *)call);

        return result_future;
    }

    std::future<std::tuple<int, int>> GetFreshnessStatsAsync()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]()
                     { return current_rpcs < MAX_CONCURRENT_RPCS; });
            ++current_rpcs;
        }

        // Build the request
        CacheGetFreshnessStatsRequest request;

        // Call object to store RPC data
        AsyncClientCall *call = new AsyncClientCall;
        call->call_type = AsyncClientCall::CallType::GETFRESHNESSSTATS;
        call->get_freshness_stats_promise = std::make_shared<std::promise<std::tuple<int, int>>>();

        // Get the future from the promise
        std::future<std::tuple<int, int>> result_future = call->get_freshness_stats_promise->get_future();

        // Start the asynchronous RPC
        call->get_freshness_stats_response_reader = stub_->AsyncGetFreshnessStats(&call->context, request, &cq_);
        call->get_freshness_stats_response_reader->Finish(&call->get_freshness_stats_reply, &call->status, (void *)call);

        return result_future;
    }

    // Synchronous Get method that waits for the result
    std::string Get(const std::string &key)
    {
        try
        {
            std::future<std::string> result_future = GetAsync(key);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Get failed: " << e.what() << std::endl;
            return "";
        }
    }

    // Synchronous Set method that waits for the result
    bool Set(const std::string &key, const std::string &value, int ttl)
    {
        try
        {
            std::future<bool> result_future = SetAsync(key, value, ttl);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Set failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Synchronous Invalidate method that waits for the result
    bool Invalidate(const std::string &key)
    {
        try
        {
            std::future<bool> result_future = InvalidateAsync(key);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Invalidate failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Synchronous Update method that waits for the result
    bool Update(const std::string &key, const std::string &value, int ttl)
    {
        try
        {
            std::future<bool> result_future = UpdateAsync(key, value, ttl);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "Update failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Synchronous SetTTL method that waits for the result
    bool SetTTL(int32_t ttl)
    {
        try
        {
            std::future<bool> result_future = SetTTLAsync(ttl);
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "SetTTL failed: " << e.what() << std::endl;
            return false;
        }
    }

    // Synchronous GetMR method that waits for the result
    float GetMR()
    {
        try
        {
            std::future<float> result_future = GetMRAsync();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetMR failed: " << e.what() << std::endl;
            return -1.0;
        }
    }

    std::tuple<int, int> GetFreshnessStats()
    {
        try
        {
            std::future<std::tuple<int, int>> result_future = GetFreshnessStatsAsync();
            return result_future.get(); // Wait for the result
        }
        catch (const std::exception &e)
        {
            std::cerr << "GetFreshnessStats failed: " << e.what() << std::endl;
            return std::make_tuple(-1, -1); // Return error values
        }
    }

    // Other methods remain unchanged
    void SetTracker(Tracker *tracker)
    {
        tracker_ = tracker;
    }

    Tracker *getTracker()
    {
        return tracker_;
    }

private:
    // Struct to keep state and data information for the asynchronous calls
    struct AsyncClientCall
    {
        enum class CallType
        {
            GET,
            SET,
            INVALIDATE,
            UPDATE,
            SETTTL,
            GETMR,
            GETFRESHNESSSTATS,
        };

        CallType call_type;
        std::string key;

        // For GetAsync
        CacheGetResponse get_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheGetResponse>> get_response_reader;
        std::shared_ptr<std::promise<std::string>> get_promise;

        // For SetAsync
        CacheSetResponse set_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheSetResponse>> set_response_reader;
        std::shared_ptr<std::promise<bool>> set_promise;

        // For InvalidateAsync
        CacheInvalidateResponse invalidate_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheInvalidateResponse>> invalidate_response_reader;
        std::shared_ptr<std::promise<bool>> invalidate_promise;

        // For UpdateAsync
        CacheUpdateResponse update_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheUpdateResponse>> update_response_reader;
        std::shared_ptr<std::promise<bool>> update_promise;

        // For SetTTLAsync
        CacheSetTTLResponse set_ttl_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheSetTTLResponse>> set_ttl_response_reader;
        std::shared_ptr<std::promise<bool>> set_ttl_promise;

        // For GetMRAsync
        CacheGetMRResponse get_mr_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheGetMRResponse>> get_mr_response_reader;
        std::shared_ptr<std::promise<float>> get_mr_promise;

        // For GetFreshnessStats
        CacheGetFreshnessStatsResponse get_freshness_stats_reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<CacheGetFreshnessStatsResponse>> get_freshness_stats_response_reader;
        std::shared_ptr<std::promise<std::tuple<int, int>>> get_freshness_stats_promise;

        grpc::ClientContext context;
        grpc::Status status;
    };

    int current_rpcs = 0;
    grpc::CompletionQueue cq_;
    std::thread cq_thread_;
    std::mutex mutex_;
    std::condition_variable cv_;

    std::unique_ptr<CacheService::Stub> stub_;
    Tracker *tracker_ = nullptr;

    // Async completion handler
    void AsyncCompleteRpc()
    {
        void *got_tag;
        bool ok = false;

        while (cq_.Next(&got_tag, &ok))
        {
            AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);
            if (call->status.ok())
            {
                switch (call->call_type)
                {
                case AsyncClientCall::CallType::GET:
                    call->get_promise->set_value(call->get_reply.value());
                    break;
                case AsyncClientCall::CallType::SET:
                    call->set_promise->set_value(call->set_reply.success());
                    break;
                case AsyncClientCall::CallType::INVALIDATE:
                    call->invalidate_promise->set_value(call->invalidate_reply.success());
                    break;
                case AsyncClientCall::CallType::UPDATE:
                    call->update_promise->set_value(call->update_reply.success());
                    break;
                case AsyncClientCall::CallType::SETTTL:
                    call->set_ttl_promise->set_value(call->set_ttl_reply.success());
                    break;
                case AsyncClientCall::CallType::GETMR:
                    call->get_mr_promise->set_value(call->get_mr_reply.mr());
                    break;
                case AsyncClientCall::CallType::GETFRESHNESSSTATS:
                    int invalidates = call->get_freshness_stats_reply.num_invalidates();
                    int updates = call->get_freshness_stats_reply.num_updates();
                    call->get_freshness_stats_promise->set_value(std::make_tuple(invalidates, updates));
                    break;
                }
            }
            else
            {
                // Handle the RPC failure by setting an exception in the promise
                std::string error_message = "RPC failed: " + call->status.error_message();
                switch (call->call_type)
                {
                case AsyncClientCall::CallType::GET:
                    error_message += "GET ";
                    call->get_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::SET:
                    error_message += "SET ";
                    call->set_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::INVALIDATE:
                    error_message += "INVALIDATE ";
                    call->invalidate_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::UPDATE:
                    error_message += "UPDATE ";
                    call->update_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::SETTTL:
                    error_message += "SETTTL ";
                    call->set_ttl_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::GETMR:
                    error_message += "GETMR ";
                    call->get_mr_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                case AsyncClientCall::CallType::GETFRESHNESSSTATS:
                    error_message += "GETFRESHNESSSTATS ";
                    call->get_freshness_stats_promise->set_exception(
                        std::make_exception_ptr(std::runtime_error(error_message)));
                    break;
                }

                // Optionally, log the error
                std::cerr << error_message << " for key: " << call->key << std::endl;
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                --current_rpcs;
            }
            cv_.notify_one();
            delete call;
        }
    }
};

class Client
{
public:
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

    Client(std::shared_ptr<Channel> cache_channel, std::shared_ptr<Channel> db_channel, Tracker *tracker)
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

    std::string Get(const std::string &key)
    {
        if (get_tracker())
            get_tracker()->read(key);
        std::string value = cache_client_->Get(key);
        return value;
    }

    void GetAsync(const std::string &key)
    {
        if (get_tracker())
            get_tracker()->read(key);
        cache_client_->GetAsync(key);
    }

    std::string GetWarmDB(const std::string &key)
    {
        return db_client_->Get(key);
    }

    void SetAsync(const std::string &key, const std::string &value, int ttl, float ew)
    {
        if (get_tracker())
            get_tracker()->write(key);
        db_client_->AsyncPut(key, value, ew);
    }

    bool Set(const std::string &key, const std::string &value, int ttl, float ew)
    {
        if (get_tracker())
            get_tracker()->write(key);

        // Call Put method on DBClient to store data
        bool db_result = db_client_->Put(key, value, ew);

        // Optionally handle the result if necessary
        if (!db_result)
        {
            return false; // Or handle the error as needed
        }

        return true;
    }

    bool SetWarm(const std::string &key, const std::string &value, int ttl)
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

    void SetTTL(const int32_t &ttl)
    {
        ttl_ = ttl;

        cache_client_->SetTTL(ttl);
    }

    float GetMR(void)
    {
        return cache_client_->GetMR();
    }

    std::tuple<int, int> GetFreshnessStats(void)
    {
        return cache_client_->GetFreshnessStats();
    }

    int GetLoad(void)
    {
        return db_client_->GetLoad();
    }

    bool StartRecord(void)
    {
        return db_client_->StartRecord();
    }

    bool SetCache(const std::string &key, const std::string &value, int ttl)
    {
        return cache_client_->Set(key, value, ttl);
    }

    Tracker *get_tracker(void)
    {
        return cache_client_->getTracker();
    }

private:
    std::unique_ptr<DBClient> db_client_;
    std::unique_ptr<CacheClient> cache_client_;
    int32_t ttl_;
    memcached_pool_st *pool;
};
#endif // CLIENT_HPP
