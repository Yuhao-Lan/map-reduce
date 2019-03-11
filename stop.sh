#!/bin/bash
#az login 
N=0
echo -n "Enter the number of VMs/node you want to delete: > "
read N

for i in `seq 1 $N`
do
        (az vm delete -g NetworkWatcherRG -n myVMDeployed$i --yes
          echo "delete" myVMDeployed$i "successfully...") &
done
wait
echo "All done"
