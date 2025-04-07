# A script to test quickly

killall {node} &> /dev/null
rm -rf /tmp/*.db &> /dev/null
vals=(27000 27100 27200 27300)

rand=$(shuf -i 1000-150000000 -n 1)
TESTDIR=${TESTDIR:="testdata/hyb_4"}
#TESTDIR=${TESTDIR:="testdata/hyb_16"}

TYPE=${TYPE:="release"}

# # Run the syncer now
 ./target/$TYPE/node \
     --config $TESTDIR/nodes-0.json \
     --ip ip_file \
     --protocol sync \
     --input 100 \
     --syncer $1 \
     --byzantine false > logs/syncer.log &

for((i=0;i<4;i++)); do
./target/$TYPE/node \
    --config $TESTDIR/nodes-$i.json \
    --ip ip_file \
    --protocol rbc \
    --input $2 \
    --syncer $1 \
    --byzantine $3 > logs/$i.log &
done


# #4 nodes with i byzantine node 
# for((i=0;i<4;i++)); do
#   if [[ $i -eq 3 ]]; then
#     # Make node 3 Byzantine (33% of 4 nodes)
#     byz=true
#   else
#     byz=false
#   fi
#
#   ./target/$TYPE/node \
#     --config $TESTDIR/nodes-$i.json \
#     --ip ip_file \
#     --protocol rbc \
#     --input $2 \
#     --syncer $1 \
#     --byzantine $byz > logs/$i.log &
# done

#check 16 nodes with 33% byzantine
# for((i=0;i<16;i++)); do
#   echo "Node $i -> Byzantine = $byz"
#   if [[ $i -eq 0 ]]; then
#     # Node 0 is the leader and must be honest
#     byz=false
#   elif [[ $i -le 5 ]]; then
#     # Make 5 nodes Byzantine (nodes 1 to 5)
#     byz=true
#   else
#     byz=false
#   fi
#
#   ./target/$TYPE/node \
#     --config $TESTDIR/nodes-$i.json \
#     --ip ip_file \
#     --protocol rbc \
#     --input $2 \
#     --syncer $1 \
#     --byzantine $byz > logs/$i.log &
# done


# Kill all nodes sudo lsof -ti:7000-7015 | xargs kill -9
