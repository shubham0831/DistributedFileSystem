#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "${script_dir}/nodes-2.sh"

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "starting ${node}..."
    echo "${node}" "/home/spareek/go/bin/node -sa orion09.cs.usfca.edu:25000 -la ${node}.cs.usfca.edu:25001"
    ssh "${node}" "cd /bigdata/spareek/ && /home/spareek/go/bin/node -sa orion09.cs.usfca.edu:25000 -la ${node}.cs.usfca.edu:25001" &
done

echo "Startup complete!"
