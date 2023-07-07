package com.wisr.mlsched;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.events.JobArrivalEvent;
import com.wisr.mlsched.job.JobStatistics;
import com.wisr.mlsched.resources.Cluster;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Simulation {
	
	private static double mTime;
	private static final Level mLogLevel = Level.ALL;
	//private static final Level mLogLevel = Level.OFF;
	private static boolean admissionControl = false;
	
	public static void main(String args[]) {
		Integer racks = Integer.parseInt(args[0]);
		Integer machines = Integer.parseInt(args[1]);
		String cluster_policy = args[2];
		String cluster_config_file = args[3];
		String workload_config_file = args[4];

		Logger log = Logger.getLogger(Simulation.class.getSimpleName());
		log.setLevel(mLogLevel);
		
		// Get the configurations
		JSONObject clusterConfig = ConfigUtils.getClusterConfig(cluster_config_file);
		JSONArray workloadConfig = ConfigUtils.getWorkloadConfigs(workload_config_file);
		JSONObject networkConfig = null;
		JSONObject systemConfig = null;

		log.info("Starting Simulation");
		// Initialize simulator time
		mTime = 0;
		
		// Initialize cluster
		String network_config_file = args[5];
		String system_config_file = args[6];
		networkConfig = ConfigUtils.getNetworkConfigs(network_config_file);
		//systemConfig = ConfigUtils.getSystemConfigs(system_config_file);
		String run_name = args[7];
		Cluster.createCluster(racks, machines, cluster_policy, clusterConfig, networkConfig, system_config_file, run_name);

		ClusterEventQueue eventQueue = ClusterEventQueue.getInstance();

		// AS: change to account for arrival time

		for(Object object : workloadConfig) {
			JSONObject config = (JSONObject) object;
			eventQueue.enqueueEvent(new JobArrivalEvent
					(ConfigUtils.getJobStartTime(config), config));
		}
		
		// Start processing
		eventQueue.start();
		
		// Print out statistics
		JobStatistics.getInstance().printStats();
	}
	
	public static double getSimulationTime() {
		return mTime;
	}
	
	public static void setSimulationTime(double time) {
		mTime = time;
	}
	
	public static Level getLogLevel() {
		return mLogLevel;
	}

	public static boolean isAdmissionControlEnabled() {
		return admissionControl;
	}
}
