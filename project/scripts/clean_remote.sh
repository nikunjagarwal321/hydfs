#!/bin/bash

# --------------------------
# Configuration (mirrors generate_logs_remote.sh)
# --------------------------
user="akashe2"                # Your NetID
vms=(3701 3702 3703 3704 3705 3706 3707 3708 3709 3710)

# --------------------------
# Clean files/dirs on each VM
# --------------------------
for i in "${vms[@]}"; do
    vm="fa25-cs425-$i.cs.illinois.edu"
    echo ">>> Cleaning working directory on $vm"

    ssh "$user@$vm" "bash -s" <<'ENDSSH'
# Target directory is current working directory (PWD)
target_dir="$PWD"

# Ensure directory exists; if not, nothing to clean
if [ ! -d "$target_dir" ]; then
    echo "No directory to clean: $target_dir (skipping)"
    exit 0
fi

echo "Cleaning contents of: $target_dir"

# Prefer find to safely remove all entries (including dotfiles), not the directory itself
if command -v find >/dev/null 2>&1; then
    # Remove only non-hidden entries (exclude names beginning with '.')
    find "$target_dir" -mindepth 1 -maxdepth 1 -not -name '.*' -exec rm -rf -- {} +
else
    # Fallback: remove only non-hidden entries using shell glob (default excludes dotfiles)
    rm -rf -- "$target_dir"/* 2>/dev/null || true
fi

echo "Done."
ENDSSH

done

echo "✅ Cleanup done on all VMs."


