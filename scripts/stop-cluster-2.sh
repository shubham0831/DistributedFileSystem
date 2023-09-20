#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "${script_dir}/nodes-2.sh"

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" 'pkill -u "$(whoami)" node'
done

echo "Done!"
