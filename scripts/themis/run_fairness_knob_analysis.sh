export MAVEN_OPTS="-XX:-UseGCOverheadLimit"
mkdir results/$1
mkdir configuration/cluster/$1

# Generate cluster configurations for Themis
for lease in 20 # Variation of lease time
do
	for d in $(seq 0 0.1 1) # Variation of fairness knob
	do
		for e in 1 # Variation of visibility knob
		do
			echo "Themis $lease $d $e"
			python scripts/generate_cluster_config.py --racks 2 --machines 2 --slots 2 --gpus 4 --policy Themis --fairness_knob $d --visibility_knob $e --lease_time $lease --config_file config_themis_lease$lease-d$d-e$e
			mv configuration/cluster/config_themis_lease$lease-d$d-e$e.json configuration/cluster/$1/ #Move config to separate file
			cluster_config_file=`echo config_themis_lease$lease-d$d-e$e.json`
			workload_config_file=configuration/jobs/final_workload.json
			mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/$1/$cluster_config_file $workload_config_file" > results/$1/themis_lease$lease-d$d-e$e.txt 
		done
	done
done
