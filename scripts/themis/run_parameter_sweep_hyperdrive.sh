export MAVEN_OPTS="-XX:-UseGCOverheadLimit"
mkdir results/$1
mkdir configuration/cluster/$1

# Generate cluster configurations for Themis
for lease in 5 10 20 40 80 # Variation of lease time
do
	for d in $(seq 0 0.02 1) # Variation of fairness knob
	do
		for e in 1 # Variation of visibility knob
		do
			echo "Themis $lease $d $e"
			python scripts/generate_cluster_config.py --racks 2 --machines 2 --slots 2 --gpus 4 --policy Themis --fairness_knob $d --visibility_knob $e --lease_time $lease --config_file config_themis_lease$lease-d$d-e$e
			mv configuration/cluster/config_themis_lease$lease-d$d-e$e.json configuration/cluster/$1/ #Move config to separate file
			cluster_config_file=`echo config_themis_lease$lease-d$d-e$e.json`
			workload_config_file=configuration/jobs/hyperdrive_trace_workload.json
			mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/$1/$cluster_config_file $workload_config_file" > results/$1/themis_lease$lease-d$d-e$e.txt 
		done
	done
done

# Generate cluster configurations for other configurations
for lease in 5 10 20 40 80
do
	echo "Other Schemes $lease"
	python scripts/generate_cluster_config.py --racks 2 --machines 2 --slots 2 --gpus 4 --policy SLAQ --lease_time $lease --config_file config_slaq_lease$lease
	python scripts/generate_cluster_config.py --racks 2 --machines 2 --slots 2 --gpus 4 --policy Gandiva --lease_time $lease --config_file config_gandiva_lease$lease
	python scripts/generate_cluster_config.py --racks 2 --machines 2 --slots 2 --gpus 4 --policy Tiresias --lease_time $lease --config_file config_tiresias_lease$lease
	mv configuration/cluster/config_slaq_lease$lease.json configuration/cluster/$1/ 
	mv configuration/cluster/config_gandiva_lease$lease.json configuration/cluster/$1/
	mv configuration/cluster/config_tiresias_lease$lease.json configuration/cluster/$1/
	workload_config_file=configuration/jobs/hyperdrive_trace_workload.json
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/$1/config_slaq_lease$lease.json $workload_config_file" > results/$1/slaq_lease$lease.txt 
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/$1/config_gandiva_lease$lease.json $workload_config_file" > results/$1/gandiva_lease$lease.txt 
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/$1/config_tiresias_lease$lease.json $workload_config_file" > results/$1/tiresias_lease$lease.txt 
done
