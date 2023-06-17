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
		String cluster_config_file = args[0];
		String workload_config_file = args[1];

		Logger log = Logger.getLogger(Simulation.class.getSimpleName());
		log.setLevel(mLogLevel);
		
		// Get the configurations
		JSONObject clusterConfig = ConfigUtils.getClusterConfig(cluster_config_file);
		JSONArray workloadConfig = ConfigUtils.getWorkloadConfigs(workload_config_file);
		JSONObject networkConfig = null;

		log.info("Starting Simulation");
		// Initialize simulator time
		mTime = 0;
		
		// Initialize cluster
		if (args.length > 2) {
			String network_config_file = args[2];
			networkConfig = ConfigUtils.getNetworkConfigs(network_config_file);
			String run_name = args[3];
			Cluster.createCluster(clusterConfig, networkConfig, run_name);

		}
		else {
			Cluster.createCluster(clusterConfig);
		}
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
