package com.wisr.mlsched.config;

import java.io.FileReader;
import java.io.IOException;

import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.IntraJobSchedulerFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ConfigUtils {
	/**
	 * Get cluster configuration JSON from configuration file
	 * @param configFile
	 * @return JSONObject
	 */
	public static JSONObject getClusterConfig(String configFile) {
		JSONParser parser = new JSONParser();
		try {
			return (JSONObject) parser.parse(new FileReader(configFile));
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Get workload configuration JSON from workload configuration file
	 * @param configFile
	 * @return JSONArray containing details are all workloads
	 */
	public static JSONArray getWorkloadConfigs(String workloadFile) {
		JSONParser parser = new JSONParser();
		try {
			return (JSONArray) parser.parse(new FileReader(workloadFile));
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Return a cluster configuration object from the given JSON configuration
	 * @param config
	 * @return ClusterConfiguration object
	 */
	public static ClusterConfiguration getClusterConfig(JSONObject config) {
		int racks = Integer.parseInt(getAttributeValue(config, "racks_in_cluster"));
		int machines = Integer.parseInt(getAttributeValue(config, "machines_per_rack"));
		int slots = Integer.parseInt(getAttributeValue(config, "slots_per_machine"));
		int gpus = Integer.parseInt(getAttributeValue(config, "gpus_per_slot"));
		int iter_granularity = Integer.parseInt(getAttributeValue(config, "iteration_granularity"));
		double lease_time = Double.parseDouble(getAttributeValue(config, "lease_time"));
		double fairness_threshold = Double.parseDouble(getAttributeValue(config, "fairness_threshold"));
		double epsilon = Double.parseDouble(getAttributeValue(config, "epsilon"));
		boolean shouldUseConfig = Boolean.parseBoolean(getAttributeValue(config, "should_use_config"));
		String policy = getClusterPolicy(config);
		String astra_sim_path = getAttributeValue(config, "astra_sim_path");
		String astra_sim_bin_path = getAttributeValue(config, "astra_sim_bin_path");
		return new ClusterConfiguration(racks, machines, slots, gpus, iter_granularity, policy, lease_time,
				fairness_threshold, epsilon, shouldUseConfig, astra_sim_path, astra_sim_bin_path);
	}
	
	/**
	 * Create a new IntraJobScheduler for the workload and cluster properties
	 * @param workload_config
	 * @param cluster_config
	 * @return instance of IntraJobScheduler
	 */
	public static IntraJobScheduler createJob(JSONObject workload_config,
			JSONObject cluster_config) {
		return IntraJobSchedulerFactory.createInstance(workload_config,
				getClusterPolicy(cluster_config));
	}
	
	/**
	 * Given the cluster configuration JSON, returns the policy
	 * @param config
	 * @return string indicating the policy
	 */
	public static String getClusterPolicy(JSONObject config) {
		return getAttributeValue(config, "cluster_policy");
	}
	
	/**
	 * Given a workload configuration for a job,
	 * return the start time for the job
	 * @param workload_config, JSON representing workload configuration.
	 * @return a double representing the start time.
	 */
	public static double getJobStartTime(JSONObject workload_config) {
		return Double.parseDouble(getAttributeValue
				(workload_config, "start_time"));
	}
	
	/**
	 * Return an attribute value given a key and a JSON configuration
	 */
	public static String getAttributeValue(JSONObject object, String attribute) {
		return (String) object.get(attribute);
	}
}