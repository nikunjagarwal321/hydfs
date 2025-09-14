#!/bin/bash

# --------------------------
# Configuration
# --------------------------
user="akashe2"   # Your NetID
vms=(3701 3702 3703 3704 3705 3706 3707 3708 3709 3710)
repo_url="https://akashe2:glpat--WvwgvR8y2qibWyD3sjq@gitlab.engr.illinois.edu/akashe2/G37.git"
repo_dir="G37"  # The folder where the repo is located

# --------------------------
# Git pull on each VM
# --------------------------
for i in "${vms[@]}"; do
    vm="fa25-cs425-$i.cs.illinois.edu"
    echo ">>> Updating repository on $vm"

    ssh "$user@$vm" "bash -s" <<ENDSSH
if [ -d "$repo_dir" ]; then
    echo "Repo exists. Pulling latest changes..."
    cd "$repo_dir"
    git pull origin main || git pull origin master
    echo "Updated $repo_dir on $(hostname)"
else
    echo "Repo does not exist. Cloning..."
    git clone "$repo_url"
    echo "Cloned $repo_dir on $(hostname)"
fi
ENDSSH

done

echo "✅ All repositories updated on all VMs."
