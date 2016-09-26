#!/bin/bash

container_host=$1
container_id=$2
container_port=$3
user=root

function generate_random_dport {
    min=32768
    max=$[ 61000 - 32768 + 1 ]
    random=$(date +%s%N)
    echo $(($random%$max+$min))
}

dport=`generate_random_dport`
nc -z $container_host $dport

while [ $? -eq 0 ]
do
    dport=`generate_random_dport`
    nc -z $container_host $dport
done

container_ip=`ssh $user@$container_host docker exec $container_id ip addr | grep eth1 | tail -1 | cut -b 10-19`
ssh $user@$container_host iptables -t nat -A DOCKER ! -i docker_gwbridge -p tcp -m tcp --dport $dport -j DNAT --to-destination $container_ip:$container_port

echo $dport