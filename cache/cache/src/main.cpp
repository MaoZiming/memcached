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
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <cassert>
#include "thread_pool.hpp"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// A simple object pool for reusing CallData instances
template <typename T>
class ObjectPool
{
public:
    T *acquire()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!pool_.empty())
        {
            T *instance = pool_.back();
            pool_.pop_back();
            return instance;
        }
        return new T();
    }

    void release(T *instance)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        pool_.push_back(instance);
    }

private:
    std::vector<T *> pool_;
    std::mutex mutex_;
};

class CacheServiceImpl final
{
public:
    CacheServiceImpl(std::shared_ptr<Channel> db_channel)
        : db_client_(db_channel)
    {
        const char *config_string = "--SERVER=localhost:11211";

        pool = memcached_pool(config_string, strlen(config_string));
        assert(pool != nullptr);
    }

    ~CacheServiceImpl()
    {
        server_->Shutdown();
        cq_->Shutdown();
        memcached_pool_destroy(pool);
    }

    memcached_st *create_mc(void)
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

    void free_mc(memcached_st *memc)
    {
        memcached_pool_push(pool, memc);
    }

    void Run()
    {
        std::string server_address("10.128.0.39:50051");

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&async_service_);

        cq_ = builder.AddCompletionQueue();

        server_ = builder.BuildAndStart();
        std::cout << "Async server listening on " << server_address << std::endl;

        CacheServiceImpl::HandleRpcs();
        // Start the thread that will process RPCs
        // server_thread_ = std::thread(&CacheServiceImpl::HandleRpcs, this);
    }

    void Shutdown()
    {
        server_->Shutdown();
        cq_->Shutdown();
        if (server_thread_.joinable())
            server_thread_.join();
    }

private:
    class CallDataBase
    {
    public:
        virtual void Proceed(bool ok) = 0;
        virtual ~CallDataBase() {}
    };

    // Implementations for each RPC method
    template <typename ServiceType, typename RequestType, typename ResponseType>
    class CallData : public CallDataBase
    {
    public:
        CallData(ServiceType *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), impl_(impl)
        {
            // Do not call Proceed() here
        }

        void Proceed(bool ok) override
        {
            if (status_ == CREATE)
            {
                status_ = PROCESS;
                // Request the next RPC
                RequestRPC();
            }
            else if (status_ == PROCESS)
            {
                // Spawn a new instance to serve new clients while we process the current one
                CreateNewInstance();
                ProcessRequest();
                status_ = FINISH;
                responder_.Finish(response_, Status::OK, this);
            }
            else
            {
                delete this;
            }
        }

    protected:
        virtual void RequestRPC() = 0;
        virtual void ProcessRequest() = 0;
        virtual void CreateNewInstance() = 0;

        typename std::remove_pointer<ServiceType>::type *service_;
        ServerCompletionQueue *cq_;
        ServerContext ctx_;
        RequestType request_;
        ResponseType response_;
        ServerAsyncResponseWriter<ResponseType> responder_;
        enum CallStatus
        {
            CREATE,
            PROCESS,
            FINISH
        };
        CallStatus status_;
        CacheServiceImpl *impl_;
    };

    // Specific CallData implementations for each RPC
    class GetCallData : public CallData<CacheService::AsyncService, CacheGetRequest, CacheGetResponse>
    {
    public:
        GetCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

        GetCallData()
            : CallData(nullptr, nullptr, nullptr)
        {
            // Constructor does minimal work; initialization is done in Initialize()
        }

        // Initialize method to set up service, completion queue, and implementation
        void Initialize(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
        {
            service_ = service;
            cq_ = cq;
            impl_ = impl;
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestGet(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = impl_->get_call_pool_.acquire();
            new_call->Initialize(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            char *value = nullptr;
            size_t value_length = 0;
            uint32_t flags = 0;
            memcached_return_t result;
            memcached_st *memc = impl_->create_mc();
            value = memcached_get(memc, request_.key().c_str(), request_.key().size(), &value_length, &flags, &result);
            if (result == MEMCACHED_SUCCESS)
            {
                impl_->cache_hits_++;
                response_.set_value(std::string(value, value_length));
                response_.set_success(true);
                free(value); // Free allocated memory
            }
            else if (true)
            {
                impl_->cache_miss_++;
                std::string key = request_.key();
                auto ttl = impl_->ttl_;
                auto impl = impl_;
                // Start a detached thread to perform AsyncFill
                std::thread([impl, key, ttl]()
                            {
                                std::future<std::string> fill_future = impl->db_client_.AsyncFill(key, ttl);
                                try
                                {
                                    memcached_st *memc_async = impl->create_mc();
                                    std::string value = fill_future.get();
                                    memcached_set(memc_async, key.c_str(), key.size(),
                                                value.c_str(), value.size(), static_cast<time_t>(ttl), 0);
                                    impl->free_mc(memc_async);
                                }
                                catch (const std::exception &e)
                                {
                                    std::cerr << "Exception occurred in AsyncFill thread: " << e.what() << std::endl;
                                    // Optional: Handle the error, e.g., log it or set a default value in memcached
                                } })
                    .detach();

                // Set the response to indicate the value will be available later
                response_.set_value("will be available later");
                response_.set_success(true);
            }
            else
            {
                impl_->cache_miss_++;
                std::future<std::string> fill_future = impl_->db_client_.AsyncFill(request_.key(), impl_->ttl_);
                try
                {
                    std::string value = fill_future.get();
                    memcached_set(memc, request_.key().c_str(), request_.key().size(), value.c_str(), value.size(), (time_t)impl_->ttl_, (uint32_t)0);
                    response_.set_value(value);
                    response_.set_success(true);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Exception occurred: " << e.what() << std::endl;
                    response_.set_value(std::string("Error during AsyncFill"));
                    response_.set_success(false);
                }
            }
            impl_->free_mc(memc);
        }
    };

    class SetCallData : public CallData<CacheService::AsyncService, CacheSetRequest, CacheSetResponse>
    {
    public:
        SetCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

        SetCallData()
            : CallData(nullptr, nullptr, nullptr)
        {
            // Constructor does minimal work; initialization is done in Initialize()
        }

        // Initialize method to set up service, completion queue, and implementation
        void Initialize(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
        {
            service_ = service;
            cq_ = cq;
            impl_ = impl;
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestSet(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = impl_->set_call_pool_.acquire();
            new_call->Initialize(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            memcached_return_t result;
            memcached_st *memc = impl_->create_mc();
            time_t ttl = static_cast<time_t>(request_.ttl());
            result = memcached_set(memc, request_.key().c_str(), request_.key().size(),
                                   request_.value().c_str(), request_.value().size(),
                                   ttl, (uint32_t)0);
            response_.set_success(result == MEMCACHED_SUCCESS);
            impl_->free_mc(memc);
        }
    };

    class SetTTLCallData : public CallData<CacheService::AsyncService, CacheSetTTLRequest, CacheSetTTLResponse>
    {
    public:
        SetTTLCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestSetTTL(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = new SetTTLCallData(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            impl_->ttl_ = request_.ttl();
            response_.set_success(true);
        }
    };

    class GetMRCallData : public CallData<CacheService::AsyncService, CacheGetMRRequest, CacheGetMRResponse>
    {
    public:
        GetMRCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestGetMR(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = new GetMRCallData(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            response_.set_success(true);
            int32_t hits = impl_->cache_hits_.load();
            int32_t misses = impl_->cache_miss_.load();
            if (hits + misses > 0)
                response_.set_mr(static_cast<float>(misses) / (hits + misses));
            else
                response_.set_mr(-1);
        }
    };

    class InvalidateCallData : public CallData<CacheService::AsyncService, CacheInvalidateRequest, CacheInvalidateResponse>
    {
    public:
        InvalidateCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestInvalidate(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = new InvalidateCallData(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            memcached_return_t result;
            memcached_st *memc = impl_->create_mc();
            result = memcached_delete(memc, request_.key().c_str(), request_.key().size(), (time_t)0);
            response_.set_success(result == MEMCACHED_SUCCESS);
            // std::cout << "Invalidate: " << request_.key() << std::endl;
            impl_->free_mc(memc);
            impl_->num_invalidates_++;
        }
    };

    class UpdateCallData : public CallData<CacheService::AsyncService, CacheUpdateRequest, CacheUpdateResponse>
    {
    public:
        UpdateCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

    protected:
        void RequestRPC() override
        {
            service_->RequestUpdate(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            auto *new_call = new UpdateCallData(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            memcached_return_t result;
            memcached_st *memc = impl_->create_mc();
            result = memcached_replace(memc, request_.key().c_str(), request_.key().size(),
                                       request_.value().c_str(), request_.value().size(),
                                       (time_t)0, (uint32_t)0);
            if (result == MEMCACHED_NOTFOUND)
            {
                // The key does not exist in the cache
                std::cerr << "Key: " << request_.key() << "is not in cache!" << std::endl;
            }
            response_.set_success(result == MEMCACHED_SUCCESS);
            // std::cout << "Update: " << request_.key() << std::endl;
            impl_->free_mc(memc);
            impl_->num_updates_++;
        }
    };

    class GetFreshnessStatsCallData : public CallData<CacheService::AsyncService, CacheGetFreshnessStatsRequest, CacheGetFreshnessStatsResponse>
    {
    public:
        GetFreshnessStatsCallData(CacheService::AsyncService *service, ServerCompletionQueue *cq, CacheServiceImpl *impl)
            : CallData(service, cq, impl)
        {
        }

    protected:
        void RequestRPC() override
        {
            // Request the GetFreshnessStats RPC from the gRPC service
            service_->RequestGetFreshnessStats(&ctx_, &request_, &responder_, cq_, cq_, this);
        }

        void CreateNewInstance() override
        {
            // Create a new instance to handle new RPCs
            auto *new_call = new GetFreshnessStatsCallData(service_, cq_, impl_);
            new_call->Proceed(true);
        }

        void ProcessRequest() override
        {
            // Populate the response with the freshness stats
            response_.set_num_invalidates(impl_->num_invalidates_.load());
            response_.set_num_updates(impl_->num_updates_.load());
            response_.set_success(true);
        }
    };

    void HandleRpcs()
    {
        // Spawn new CallData instances to serve new clients.
        auto *get_call = get_call_pool_.acquire();
        get_call->Initialize(&async_service_, cq_.get(), this);
        get_call->Proceed(true);

        auto *set_call = set_call_pool_.acquire();
        set_call->Initialize(&async_service_, cq_.get(), this);
        set_call->Proceed(true);

        auto *setttl_call = new SetTTLCallData(&async_service_, cq_.get(), this);
        setttl_call->Proceed(true);

        auto *getmr_call = new GetMRCallData(&async_service_, cq_.get(), this);
        getmr_call->Proceed(true);

        auto *invalidate_call = new InvalidateCallData(&async_service_, cq_.get(), this);
        invalidate_call->Proceed(true);

        auto *update_call = new UpdateCallData(&async_service_, cq_.get(), this);
        update_call->Proceed(true);

        auto *get_freshness_stats_call = new GetFreshnessStatsCallData(&async_service_, cq_.get(), this);
        get_freshness_stats_call->Proceed(true);

        size_t num_worker_threads = std::thread::hardware_concurrency();
        ThreadPool thread_pool(num_worker_threads);

        void *tag; // Uniquely identifies a request.
        bool ok;

        while (cq_->Next(&tag, &ok))
        {
            if (ok)
            {
                // Enqueue the processing task into the ThreadPool
                thread_pool.enqueue([tag]()
                                    { static_cast<CallDataBase *>(tag)->Proceed(true); });
            }
            else
            {
                // Enqueue the cleanup task into the ThreadPool
                thread_pool.enqueue([tag]()
                                    { delete static_cast<CallDataBase *>(tag); });
            }
        }
    }

    std::unique_ptr<ServerCompletionQueue> cq_;
    CacheService::AsyncService async_service_;
    std::unique_ptr<Server> server_;
    std::thread server_thread_;

    /* Call pool */
    ObjectPool<GetCallData> get_call_pool_;
    ObjectPool<SetCallData> set_call_pool_;

    DBClient db_client_;
    memcached_pool_st *pool;
    int32_t ttl_ = 0;
    std::atomic<int32_t> cache_hits_{0};
    std::atomic<int32_t> cache_miss_{0};
    std::atomic<int32_t> num_invalidates_{0};
    std::atomic<int32_t> num_updates_{0};
};

void RunServer()
{
    std::string db_address("10.128.0.33:50051");
    std::shared_ptr<Channel> channel = grpc::CreateChannel(db_address, grpc::InsecureChannelCredentials());
    if (!channel)
    {
        std::cerr << "Failed to create channel to DB server." << std::endl;
        return;
    }

    CacheServiceImpl service(channel);
    service.Run();

    // Wait for server shutdown
    // std::cout << "Press Enter to stop the server..." << std::endl;
    // std::cin.get();

    service.Shutdown();
}

int main(int argc, char **argv)
{
    RunServer();
    return 0;
}