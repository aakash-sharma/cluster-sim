export MAVEN_OPTS="-XX:-UseGCOverheadLimit"
mkdir results/$1
mkdir configuration/cluster/$1

# Generate cluster configurations for other configurations
for contention in 1 0.1 0.01 0.001 0.0001 0.00001
do
	workload_config_file=configuration/jobs/poisson_workload_arr"$contention".json
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/medium_tiresias_work_conserving/config_themis_lease20-d0.00-e1.json $workload_config_file" > results/$1/themis_lease20_contention$contention.txt
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/medium_tiresias_work_conserving/config_slaq_lease20.json $workload_config_file" > results/$1/slaq_lease20_contention$contention.txt 
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/medium_tiresias_work_conserving/config_gandiva_lease20.json $workload_config_file" > results/$1/gandiva_lease20_contention$contention.txt 
	mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="configuration/cluster/medium_tiresias_work_conserving/config_tiresias_lease20.json $workload_config_file" > results/$1/tiresias_lease20_contention$contention.txt 
done
