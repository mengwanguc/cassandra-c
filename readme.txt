apt-get update
apt-get install build-essential cmake git

cd libuv-v1.23.0
sh autogen.sh
./configure
make install
apt-get install libssl-dev
