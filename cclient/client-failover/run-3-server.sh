export CLIENT=/home/sda_mount/cassandra-c/cclient/client-failover

if [ $# -eq 0 ]; then
    echo "You should provide output file name"
    exit 1
fi

ssh node3 $CLIENT/multicore-async-simple node0 &
ssh node4 $CLIENT/multicore-async-simple node0 &
$CLIENT/multicore-async-simple node0 > $CLIENT/client3/$1 &
