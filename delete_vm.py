import subprocess
from user_parameters import *


subprocess_list = []

for counter in range(1, number_node + 1):
    print("Deleting VM: " + "map-reduce-node-" + str(counter))
    subprocess_list.append(subprocess.Popen(["az", 
        "vm", "delete", 
        "--resource-group", resource_group_name,
        "--name", "map-reduce-node-" + str(counter),
        "--yes"]))

print("Waiting for deleting VM...")
for p in subprocess_list:
    p.wait()
print("Now " + str(number_node) + " VM instance(s) has been released!")

