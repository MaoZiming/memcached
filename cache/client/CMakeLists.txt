#
# Dependencies
#
find_package(Threads)

#
# Sources
#
set(SOURCES
    src/policy.hpp
    src/policy.cpp
    src/cache_client.cpp
    src/db_client.cpp
    src/client.cpp
    src/client.hpp
    include/tqdm.hpp
    include/zipf.hpp
    include/benchmark.hpp
    include/workload.hpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/client/src
    ${CMAKE_SOURCE_DIR}/client/include
    /usr/local/include/libmemcached
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})


#
# Target
#
add_executable(
    client
    src/main.cpp     
    ${SOURCES}
)

add_executable(
    ttl_bench
    bench/ttl.cpp
    ${SOURCES}
)

add_executable(
    invalidate_bench
    bench/invalidate.cpp
    ${SOURCES}
)

add_executable(
    update_bench
    bench/update.cpp
    ${SOURCES}
)

add_executable(
    adaptive_bench
    bench/adaptive.cpp
    ${SOURCES}
)

add_executable(
    stale_bench
    bench/stale.cpp
    ${SOURCES}
)

add_executable(
    dataset
    bench/dataset.cpp
    ${SOURCES}
)

add_executable(
    sketches
    bench/sketches.cpp
    ${SOURCES}
)

target_link_libraries(client
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(ttl_bench
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(invalidate_bench
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(update_bench
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(adaptive_bench
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(stale_bench
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)


target_link_libraries(dataset
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)

target_link_libraries(sketches
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)
