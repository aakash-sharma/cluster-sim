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
	private static final int DIMS = 6;

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
		String nw_overhead_file = args[7];
		String run_name = args[8];

		double rack_delay_wait = 1;
		double nw_delay_wait = 2;

		if (args.length > 9) {
			rack_delay_wait = Double.parseDouble(args[9]);
		}
		if (args.length > 10) {
			nw_delay_wait = Double.parseDouble(args[10]);
		}
		Cluster.createCluster(racks, machines, cluster_policy, clusterConfig, networkConfig, system_config_file,
				nw_overhead_file, run_name, nw_delay_wait, rack_delay_wait);

		ClusterEventQueue eventQueue = ClusterEventQueue.getInstance();

		// AS: change to account for arrival time

		for(Object object : workloadConfig) {
			JSONObject config = (JSONObject) object;
			eventQueue.enqueueEvent(new JobArrivalEvent
					(ConfigUtils.getJobStartTime(config), config));
		}
		
		try {
			// Start processing
			eventQueue.start();
		}
		catch (OutOfMemoryError oome) {
			//Log the info
			System.err.println("Max JVM memory: " + Runtime.getRuntime().maxMemory());
			System.err.println("Total JVM memory: " + Runtime.getRuntime().totalMemory());
			System.err.println("Free JVM memory: " + Runtime.getRuntime().freeMemory());
		}

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

	public static int getNumDims() {
		return DIMS;
	}
	public static boolean isAdmissionControlEnabled() {
		return admissionControl;
	}
}
