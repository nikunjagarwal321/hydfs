#!/bin/bash

# --------------------------
# Configuration
# --------------------------
user="akashe2"   # Your NetID
vms=(3701 3702 3703 3704 3705 3706 3707 3708 3709 3710)
repo_dir="G37"  # The folder where the repo is located
log_path="G37/distributed_log_query/log"  # Path where log_generator.go should be executed

# --------------------------
# Execute log_generator.go in log directory on each VM
# --------------------------
for i in "${vms[@]}"; do
    vm="fa25-cs425-$i.cs.illinois.edu"
    echo ">>> Running log_generator.go in $log_path on $vm"

    ssh "$user@$vm" "bash -s" <<ENDSSH
# Navigate to the repo directory
if [ -d "$repo_dir" ]; then
    cd "$repo_dir"
    echo "Changed to directory: \$(pwd)"
    
    # Check if the log directory exists
    if [ -d "distributed_log_query/log" ]; then
        cd "distributed_log_query/log"
        echo "Changed to log directory: \$(pwd)"
        
        # Check if log_generator.go exists in this directory
        if [ -f "log_generator.go" ]; then
            echo "Found log_generator.go, executing..."
            
            # Run the Go program
            go run log_generator.go
            
            echo "Log generation completed on \$(hostname)"
            
            # Show what was created
            echo "Generated files in current directory:"
            ls -la
        else
            echo "ERROR: log_generator.go not found in distributed_log_query/log/"
            echo "Available files:"
            ls -la
            exit 1
        fi
    else
        echo "ERROR: distributed_log_query/log directory not found"
        echo "Available directories:"
        ls -la
        exit 1
    fi
else
    echo "ERROR: Repository directory $repo_dir not found"
    exit 1
fi
ENDSSH

done

echo "✅ Log generation completed on all VMs in $log_path directory."
