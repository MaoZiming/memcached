#
# Dependencies
#
find_package(Threads)

# Add include directories
include_directories(
    /usr/local/include/libmemcached
    ${CMAKE_SOURCE_DIR}/client/src
)

#
# Sources
#
set(SOURCES
    src/main.cpp
)

set(HEADERS
    ${CMAKE_SOURCE_DIR}/client/src/client.hpp
    ${CMAKE_SOURCE_DIR}/client/src/policy.hpp
    ${CMAKE_SOURCE_DIR}/client/src/thread_pool.hpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

# Add the source files
add_executable(server ${SOURCES}  ${CMAKE_SOURCE_DIR}/client/src/thread_pool.cpp ${HEADERS})

# Link the libmemcached library
target_link_libraries(server
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
    /usr/local/lib/libmemcachedutil.so
)
