
script_dir="$(cd "$(dirname "$0")" && pwd)"
source "${script_dir}/nodes.sh"


for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" 'rm /bigdata/spareek/node/*'
done

echo "Done!"