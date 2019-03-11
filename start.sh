# read N 
N=0
echo -n "Enter the number of VMs/node you want to create: > "
read N

# Deallocate the VM that you deprovisioned with az vm deallocate. The following example deallocates the VM named myVM in the resource group named myResourceGroup.

az vm deallocate \
  --resource-group NetworkWatcherRG \
  --name worker1

# Mark the VM as generalized with az vm generalize. The following example marks the VM named myVM in the resource group named myResourceGroup as generalized.

az vm generalize \
  --resource-group NetworkWatcherRG \
  --name worker1

# Create an image of the VM resource with az image create. The following example creates an image named myImage in the resource group named myResourceGroup using the VM resource named myVM.


az image create \
  --resource-group NetworkWatcherRG \
  --name myImage --source worker1

# Create N nodes 

for i in `seq 1 $N`
do
   (az vm create \
   --resource-group NetworkWatcherRG \
   --name myVMDeployed$i \
   --image myImage\
   --admin-username yuhaolan \
   --ssh-key-value ~/.ssh/id_rsa.pub \
   --private-ip-address 10.0.1.$((i+10)) \
   --size Standard_B1s \
   --location eastus \

   echo "create" myVMDeployed$i "successfully..."   
   ) &
done
wait
echo "All done"
