export CLIENT=/home/sda_mount/cassandra-c/cclient/client-failover

if [ $# -eq 0 ]; then
    echo "You should provide output file name"
    exit 1
fi

ssh node3 $CLIENT/evaluate node0 &
ssh node4 $CLIENT/evaluate node0 &
$CLIENT/evaluate node0 > $CLIENT/client3/$1 &
