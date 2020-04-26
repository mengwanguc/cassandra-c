mkdir build
cd build
cmake ..
make -j 32
make install -j 32

ln -s /usr/local/lib/x86_64-linux-gnu/libcassandra.so.2 /usr/lib64/libcassandra.so.2
ln -s /usr/local/lib/x86_64-linux-gnu/libcassandra.so.2 /usr/lib/libcassandra.so.2
