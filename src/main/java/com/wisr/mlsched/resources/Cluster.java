package com.wisr.mlsched.resources;

import java.util.*;
import java.util.logging.Logger;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.globalsched.InterJobScheduler;
import com.wisr.mlsched.globalsched.InterJobSchedulerFactory;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.JobGroupManager;

import org.json.simple.JSONObject;

/**
 * Cluster represents the set of GPUs, the global scheduler to manage the
 * distribution of GPUs to jobs and a set of jobs.
 */
public class Cluster {

	private List<GPU> mGpusInCluster; // List of GPUs belong to cluster
	private List<IntraJobScheduler> mRunningJobs; // List of running jobs in cluster
	private Set<IntraJobScheduler> mActiveJobs; // List of active jobs in cluster
	private InterJobScheduler mScheduler; // Instance of inter-job scheduler
	private String mPolicy; // Policy of Cluster
	private int mIterGranularity; // Number of iterations to be scheduled policy
	private double mLeaseTime; // Lease time policy for GPUs within cluster
	private double mFairnessThreshold; // To consider jobs having 1+fairnessThreshold for GPU allocation
	private double mEpsilon; // Fraction of jobs to consider having good loss potential
	private ClusterConfiguration mConfig; // Cluster configuration object

	private static Cluster sInstance = null; // Singleton Instance of Cluster
	private static Logger sLog; // Instance of logger
	private String mAstraSimPath; // Astra sim path
	private String mAstraSimBinPath; // Astra sim binary path

	/**
	 * Creates an instance of the required cluster from the given configuration.
	 * This methods creates GPU objects and initializes the running jobs and
	 * scheduler.
	 * 
	 * @param config
	 */
	private Cluster(ClusterConfiguration config) {
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		// Create GPUs based on configuration
		mGpusInCluster = new ArrayList<GPU>();
		mRunningJobs = new ArrayList<IntraJobScheduler>();
		mActiveJobs = new HashSet<IntraJobScheduler>();
		mConfig = config;
		mPolicy = config.getPolicy();
		mLeaseTime = config.getLeaseTime();
		mFairnessThreshold = config.getFairnessThreshold();
		mIterGranularity = config.getIterGranularity();
		mEpsilon = config.getEpsilon();
		mScheduler = InterJobSchedulerFactory.createInstance(config);
		mAstraSimPath = config.getmAstraSimPath();
		mAstraSimBinPath = config.getmAstraSimBinPath();

		System.out.println("========Network Configs========");
		System.out.println(config.getRacks());
		System.out.println(config.getMachinesPerRack());
		System.out.println(config.getSlotsPerMachine());
		System.out.println(config.getDim1PerSlot());
		System.out.println(config.getDim2sPerDim1());
		System.out.println(config.getGPUsDim2());
		System.out.println("================================");
		for (int i = 0; i < config.getRacks(); i++) {
			for (int j = 0; j < config.getMachinesPerRack(); j++) {
				for (int k = 0; k < config.getSlotsPerMachine(); k++) {
					if (config.getDim1PerSlot() > 0) {
						for (int l = 0; l < config.getDim1PerSlot(); l++) {
							if (config.getDim2sPerDim1() > 0) {
								for (int m = 0; m < config.getDim2sPerDim1(); m++) {
									if (config.getGPUsDim2() > 0) {
										for (int n = 0; n < config.getGPUsDim2(); n++) {
											mGpusInCluster.add(new GPU(new GPULocation(n, m, l, k, j, i)));
										}
									}
									else {
										mGpusInCluster.add(new GPU(new GPULocation(m, -1, l, k, j, i)));
									}

								}
							} else {
									mGpusInCluster.add(new GPU(new GPULocation(l, -1, -1, k, j, i)));
							}
						}
					}
					else {
							mGpusInCluster.add(new GPU(new GPULocation(k, -1, -1, -1, j, i)));
						}
					}
				}
			}

		System.out.println("Created cluster with " + Integer.toString(mGpusInCluster.size()) + " GPUs and with " + mPolicy
				+ " policy");
	}

	/**
	 * Clients will call this API to get the singleton instance of the Cluster
	 * object
	 * 
	 * @return An instance of the Cluster object
	 */
	public static Cluster getInstance() {
		if (sInstance == null) {
			sLog.severe("ERROR: Cluster not instantiated.");
			return null;
		}
		return sInstance;
	}

	/**
	 * Client will call this API to create a cluster with given GPU configuration.
	 * This method will internally create a singleton instance of the cluster.
	 * 
	 * @param config
	 */
	public static Cluster createCluster(JSONObject config) {
		ClusterConfiguration clusterConfig = ConfigUtils.getClusterConfig(config);
		sInstance = new Cluster(clusterConfig);
		return sInstance;
	}

	public static Cluster createCluster(Integer racks, Integer machines, String cluster_policy,
										JSONObject config, JSONObject networkConfig, String system_config_file,
										String nw_overhead_file, String run_name, double nw_delay_wait,
										double rack_delay_wait, int delay_hist) {
		ClusterConfiguration clusterConfig = ConfigUtils.getClusterConfig(racks, machines, cluster_policy,
				config, networkConfig, system_config_file, nw_overhead_file, run_name, nw_delay_wait, rack_delay_wait,
				delay_hist);
		sInstance = new Cluster(clusterConfig);
		return sInstance;
	}
	/**
	 * Get the set of GPUs in the cluster.
	 * 
	 * @return A List consisting of GPU objects
	 */
	public List<GPU> getGPUsInCluster() {
		return mGpusInCluster;
	}

	/**
	 * Get the list of jobs currently running on the cluster
	 * 
	 * @return A list consisting of IntraJobScheduler objects
	 */
	public List<IntraJobScheduler> getRunningJobs() {
		return mRunningJobs;
	}

	/**
	 * Add a job to list of running jobs
	 * 
	 * @param job
	 */
	public void addJob(IntraJobScheduler job) {
		sLog.info("Adding job " + Integer.toString(job.getJobId()) + " to cluster");
		job.setmIterGranularity(mIterGranularity);
		job.setmAstraSimPath(mAstraSimPath, mAstraSimBinPath);
		mRunningJobs.add(job);
		JobGroupManager.getInstance().trackJob(job);
	}

	/**
	 * Remove a job from list of running jobs
	 * 
	 * @param job
	 */
	public void removeJob(IntraJobScheduler job) {
		sLog.info("Removing job " + Integer.toString(job.getJobId()) + " from cluster");
		mRunningJobs.remove(job);
		removeActiveJob(job);
		JobGroupManager.getInstance().untrackJob(job);
	}

	/**
	 * Get an instance of the inter-job scheduler
	 * 
	 * @return the singleton instance of InterJobScheduler
	 */
	public InterJobScheduler getScheduler() {
		return mScheduler;
	}

	/**
	 * Returns the policy with which the cluster is operating
	 * 
	 * @return
	 */
	public String getPolicy() {
		return mPolicy;
	}

	/**
	 * Returns the lease time policy for this cluster
	 * 
	 * @return double representing the lease time
	 */
	public double getLeaseTime() {
		return mLeaseTime;
	}

	/**
	 * Returns the Fairness Threshold value from configuration
	 * 
	 * @return double representing fairness threshold
	 */
	public double getFairnessThreshold() {
		return mFairnessThreshold;
	}

	/**
	 * Returns the Epsilon value from configuration
	 * 
	 * @return double representing epsilon
	 */
	public double getEpsilon() {
		return mEpsilon;
	}

	/**
	 * Returns the Cluster Configuration object for this cluster.
	 * 
	 * @return ClusterConfiguration object
	 */
	public ClusterConfiguration getConfiguration() {
		return mConfig;
	}

	public Set<IntraJobScheduler> getActiveJobs() {
		return mActiveJobs;
	}

	public void addActiveJob(IntraJobScheduler job) {
		mActiveJobs.add(job);
	}

	public void removeActiveJob(IntraJobScheduler job) {
		mActiveJobs.remove(job);
	}

}
