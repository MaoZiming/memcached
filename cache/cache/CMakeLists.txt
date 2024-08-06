#
# Dependencies
#
find_package(Threads)

# Add include directories
include_directories(/usr/local/include/libmemcached)

#
# Sources
#
set(SOURCES
    src/main.cpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

# Add the source files
add_executable(MyMemcachedProject ${SOURCES})

# Link the libmemcached library
target_link_libraries(MyMemcachedProject
    PRIVATE
    myproto
    /usr/local/lib/libmemcached.so
)
