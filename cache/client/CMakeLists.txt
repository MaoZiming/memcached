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
)

include_directories(
    ${CMAKE_SOURCE_DIR}/client/src
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
    adaptive_bench
    bench/adaptive.cpp
    ${SOURCES}
)

target_link_libraries(client
    PRIVATE
    myproto
)

target_link_libraries(ttl_bench
    PRIVATE
    myproto
)

target_link_libraries(adaptive_bench
    PRIVATE
    myproto
)