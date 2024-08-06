#!/bin/bash

# Exit on any error
set -e

export MY_INSTALL_DIR=$HOME/.local
export PATH="$MY_INSTALL_DIR/bin:$PATH"
echo 'export MY_INSTALL_DIR=$HOME/.local' >> ~/.bashrc
echo 'export PATH="$MY_INSTALL_DIR/bin:$PATH"' >> ~/.bashrc

mkdir -p $MY_INSTALL_DIR
sudo apt update
sudo apt install -y make g++ cmake build-essential autoconf libtool pkg-config

# gRPC.
cd ~
git clone --recurse-submodules --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make -j 4
sudo make install
popd


# Memcached
cd ~
git clone https://github.com/MaoZiming/memcached.git

# Change to the memcached directory
cd memcached

# Install necessary dependencies
echo "Installing dependencies..."
sudo apt-get update
sudo apt-get install -y build-essential libevent-dev

# libmemcached
sudo apt-get install -y flex bison

echo "Compiling and installing memcached..."
./autogen.sh
./configure
make
sudo make install

echo "Starting memcached..."
sudo memcached -d -m 64 -p 11211 -u maoziming

