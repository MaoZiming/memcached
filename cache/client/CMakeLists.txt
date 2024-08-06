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
    src/client.hpp
    src/cache_client.cpp
    src/db_client.cpp
    src/main.cpp
    src/client.cpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})


#
# Target
#
add_executable(client ${SOURCES})
target_link_libraries(client
    PRIVATE
    myproto
)