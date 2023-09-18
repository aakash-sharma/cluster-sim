package com.wisr.mlsched.config;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


import com.wisr.mlsched.Simulation;
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
	 * Get network configuration JSON from workload configuration file
	 * @param networkFile
	 * @return JSONArray containing details are all workloads
	 */
	public static JSONObject getNetworkConfigs(String networkFile) {
		JSONParser parser = new JSONParser();
		try {
			return (JSONObject) parser.parse(new FileReader(networkFile));
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
		boolean consolidate = Boolean.parseBoolean(getAttributeValue(config, "consolidate"));
		String astra_sim_path = getAttributeValue(config, "astra_sim_path");
		String astra_sim_bin_path = getAttributeValue(config, "astra_sim_bin_path");

		return new ClusterConfiguration(racks, machines, slots, gpus, iter_granularity, policy, lease_time,
				fairness_threshold, epsilon, shouldUseConfig, consolidate, astra_sim_path, astra_sim_bin_path);

	}
	public static ClusterConfiguration getClusterConfig(Integer cluster_racks, Integer cluster_machines,
														String cluster_policy, JSONObject config,
														JSONObject networkConfig, String system_config_file,
														String nw_overhead_file,String run_name, double nw_delay_wait,
														double rack_delay_wait) {
		int racks = cluster_racks;
		int machines = cluster_machines;
		int slots = 1;
		int dim1_slot = 0;
		int dim2_dim1 = 0;
		int gpus_dim2 = 0;

		int iter_granularity = Integer.parseInt(getAttributeValue(config, "iteration_granularity"));
		double lease_time = Double.parseDouble(getAttributeValue(config, "lease_time"));
		double fairness_threshold = Double.parseDouble(getAttributeValue(config, "fairness_threshold"));
		double epsilon = Double.parseDouble(getAttributeValue(config, "epsilon"));
		boolean shouldUseConfig = Boolean.parseBoolean(getAttributeValue(config, "should_use_config"));
		String policy = cluster_policy;
		boolean consolidate = Boolean.parseBoolean(getAttributeValue(config, "consolidate"));
		String astra_sim_path = getAttributeValue(config, "astra_sim_path");
		String astra_sim_bin_path = getAttributeValue(config, "astra_sim_bin_path");

		int dims = ((Long) networkConfig.get("dimensions-count")).intValue();
		String topo_name = (String) networkConfig.get("topology-name");
		JSONArray topo_per_dim_js = (JSONArray) networkConfig.get("topologies-per-dim");
		JSONArray dim_type_js = (JSONArray) networkConfig.get("dimension-type");
		JSONArray unit_count_js = (JSONArray) networkConfig.get("units-count");
		JSONArray links_count_js = (JSONArray) networkConfig.get("links-count");
		JSONArray link_latency_js = (JSONArray) networkConfig.get("link-latency");
		JSONArray link_bandwidth_js = (JSONArray) networkConfig.get("link-bandwidth");
		JSONArray nic_latency_js = (JSONArray) networkConfig.get("nic-latency");

		if (racks > 1) {
			dims += 2;
		} else if (machines > 1) {
			dims += 1;
		}

		Iterator<String> st_itr = topo_per_dim_js.iterator();
		String topo_per_dim[] = new String[dims];
		int i = 0;
		while(st_itr.hasNext()) {
			topo_per_dim[i] = st_itr.next();
			i += 1;
		}

		Iterator<Long> int_itr = unit_count_js.iterator();
		int unit_count[] = new int[dims];
		i = 0;
		while(int_itr.hasNext()) {
			unit_count[i] = (int_itr.next()).intValue();
			i += 1;
		}

		int_itr = links_count_js.iterator();
		float link_ratio[] = new float[dims];
		i = 0;
		while(int_itr.hasNext()) {
			link_ratio[i] = (int_itr.next()).intValue();
			link_ratio[i] /= unit_count[i];
			i += 1;
		}

		st_itr = dim_type_js.iterator();
		String dim_type[] = new String[dims];
		i = 0;
		Vector<Integer> intra_node_units = new Vector<Integer>();
		while(st_itr.hasNext()) {
			dim_type[i] = st_itr.next();
			if (unit_count[i] == 0) {
				System.out.println("Unit count cant be 0!");
				System.exit(-1);
			}
			if (dim_type[i].equals("N") || dim_type[i].equals("T")) {
				intra_node_units.add(unit_count[i]);
			}

			i += 1;
		}

		int_itr = link_latency_js.iterator();
		long link_latency[] = new long[dims];
		i = 0;
		while(int_itr.hasNext()) {
			link_latency[i] = int_itr.next();
			i += 1;
		}

		int_itr = link_bandwidth_js.iterator();
		int link_bandwidth[] = new int[dims];
		i = 0;
		while(int_itr.hasNext()) {
			link_bandwidth[i] = (int_itr.next()).intValue();
			i += 1;
		}

		int_itr = nic_latency_js.iterator();
		float nic_latency[] = new float[dims];
		i = 0;
		while(int_itr.hasNext()) {
			nic_latency[i] = int_itr.next();
			i += 1;
		}

		//AS: add sloty latency here

		if (machines > 1) {
			topo_per_dim[i] = "Ring";
			unit_count[i] = machines;
			link_ratio[i] = 1;
			dim_type[i] = "P";
			link_latency[i] = 5000;
			link_bandwidth[i] = 200;
			nic_latency[i] = 0;
		}
		if (racks > 1) {
			topo_per_dim[i + 1] = "Switch";
			unit_count[i + 1] = racks;
			link_ratio[i + 1] = 1;
			dim_type[i + 1] = "PP";
			link_latency[i + 1] = 50000; // ns
			link_bandwidth[i + 1] = 400;
			nic_latency[i + 1] = 0;
		}

		//gpus = intra_node_units.remove(0);

		int size = intra_node_units.size();

		if (size > 0) {
			slots = intra_node_units.remove(size - 1);
			size -= 1;
		}

		if (size > 0)
		{
			dim1_slot = intra_node_units.remove(size-1);
			size -= 1;
		}

		if (size > 0)
		{
			dim2_dim1 = intra_node_units.remove(size-1);
			size -= 1;
		}

		if (size > 0)
		{
			gpus_dim2 = intra_node_units.remove(size-1);
		}

		String[] all_reduce_impl = null;
		String[] all_gather_impl = null;
		String[] reduce_scatter_impl = null;
		String[] all_to_all_impl = null;
		String intra_dim_sched = null;
		String inter_dim_sched = null;

		String line = "";
		String splitBy = ": ";

		try {
			BufferedReader br = new BufferedReader(new FileReader(system_config_file));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] sysConfig = line.split(splitBy);

				if (sysConfig[0].equals("all-reduce-implementation")) {
					all_reduce_impl = sysConfig[1].split("_");
				}
				if (sysConfig[0].equals("all-gather-implementation")) {
					all_gather_impl = sysConfig[1].split("_");
				}
				if (sysConfig[0].equals("reduce-scatter-implementation")) {
					reduce_scatter_impl = sysConfig[1].split("_");
				}
				if (sysConfig[0].equals("all-to-all-implementation")) {
					all_to_all_impl = sysConfig[1].split("_");
				}
				if (sysConfig[0].equals("intra-dimension-scheduling")) {
					intra_dim_sched = sysConfig[1];
				}
				if (sysConfig[0].equals("inter-dimension-scheduling")) {
					inter_dim_sched = sysConfig[1];
				}
			}

		} catch (IOException e)
		{
			e.printStackTrace();
		}

		HashMap<String, Double> rackOverheads = new HashMap<String, Double>();
		HashMap<String, Double> nwOverheads = new HashMap<String, Double>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(nw_overhead_file));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] split_line = line.split(",");

				rackOverheads.put(split_line[0], Double.parseDouble(split_line[1]));
				nwOverheads.put(split_line[0], Double.parseDouble(split_line[2]));
			}

		} catch (IOException e)
		{
			e.printStackTrace();
		}

//		System.out.println(run_name);
//		System.out.println(racks);
//		System.out.println(machines);
//		System.out.println(slots);
//		System.out.println(dim1_slot);
//		System.out.println(dim2_dim1);
//		System.out.println(gpus_dim2);

		return new ClusterConfiguration(run_name, racks, machines, slots, dim1_slot, dim2_dim1, gpus_dim2, iter_granularity,
				policy, lease_time,
				fairness_threshold, epsilon, shouldUseConfig, consolidate, astra_sim_path, astra_sim_bin_path,
				topo_name, topo_per_dim, dim_type, link_ratio, link_latency, link_bandwidth, all_reduce_impl,
				all_gather_impl, reduce_scatter_impl, all_to_all_impl, intra_dim_sched, inter_dim_sched, rackOverheads,
				nwOverheads, nw_delay_wait,	rack_delay_wait);

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

	public static double[] getJobDelayTimes(JSONObject workload_config) {
		double delay_timers [] = new double[Simulation.getNumDims()];
		Arrays.fill(delay_timers, -1);
		if (workload_config.containsKey("delay_timers")){
			JSONArray delay_timers_js = (JSONArray) workload_config.get("delay_timers");
			Iterator<Long> double_itr = delay_timers_js.iterator();
			int i = Simulation.getNumDims() - delay_timers_js.size();
			while(double_itr.hasNext()) {
				delay_timers[i] = (double_itr.next()).doubleValue();
				i += 1;
			}
		}
		return delay_timers;
	}

	/**
	/**
	 * Return an attribute value given a key and a JSON configuration
	 */
	public static String getAttributeValue(JSONObject object, String attribute) {
		return (String) object.get(attribute);
	}
}