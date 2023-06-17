package com.wisr.mlsched.resources;

import java.util.ArrayList;
import java.util.List;
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
		mConfig = config;
		mPolicy = config.getPolicy();
		mLeaseTime = config.getLeaseTime();
		mFairnessThreshold = config.getFairnessThreshold();
		mIterGranularity = config.getIterGranularity();
		mEpsilon = config.getEpsilon();
		mScheduler = InterJobSchedulerFactory.createInstance(config);
		mAstraSimPath = config.getmAstraSimPath();
		mAstraSimBinPath = config.getmAstraSimBinPath();
		System.out.println(config.getRacks());
		System.out.println(config.getMachinesPerRack());
		System.out.println(config.getSlotsPerMachine());
		System.out.println(config.getGPUsPerSlot());
		if (config.getUseConfig()) {
			for (int i = 0; i < config.getRacks(); i++) {
				for (int j = 0; j < config.getMachinesPerRack(); j++) {
					for (int k = 0; k < config.getSlotsPerMachine(); k++) {
						for (int l = 0; l < config.getGPUsPerSlot(); l++) {
							mGpusInCluster.add(new GPU(new GPULocation(l, k, j, i)));
						}
					}
				}
			}
		} else {
			initHeterogenousCluster();
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

	public static Cluster createCluster(JSONObject config, JSONObject networkConfig, String run_name) {
		ClusterConfiguration clusterConfig = ConfigUtils.getClusterConfig(config, networkConfig, run_name);
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

	private void initHeterogenousCluster() {
		System.out.println("Initialzing heterogenous cluster");
		for (int rack_id = 0; rack_id < mConfig.getRacks(); rack_id++) {
			// 6 m/c with 4 GPUs per machine
			for (int mc = 0; mc < 6; mc++) {
				for(int slot=0;slot<2;slot++) {
					for(int gpu=0;gpu<2;gpu++) {
						mGpusInCluster.add(new GPU(new GPULocation(gpu, slot, mc, rack_id)));
					}
				}
			}
			// 3 m/c with 2 GPUs per machine
			for (int mc = 6; mc < 9; mc++) {
				for(int gpu=0;gpu<2;gpu++) {
					mGpusInCluster.add(new GPU(new GPULocation(gpu, 0, mc, rack_id)));
				}
			}
			// 2 m/c with 1 GPU per machine
			for (int mc = 9; mc < 11; mc++) {
				mGpusInCluster.add(new GPU(new GPULocation(0, 0, mc, rack_id)));
			}
		}
		/*for (int rack_id = 2; rack_id < 4; rack_id++) {
			// 2 m/c with 4 GPUs per machine
			for (int mc = 0; mc < 2; mc++) {
				for(int slot=0;slot<2;slot++) {
					for(int gpu=0;gpu<2;gpu++) {
						mGpusInCluster.add(new GPU(new GPULocation(gpu, slot, mc, rack_id)));
					}
				}
			}
			// 2 m/c with 2 GPUs per machine
			for (int mc = 2; mc < 4; mc++) {
				for(int gpu=0;gpu<2;gpu++) {
					mGpusInCluster.add(new GPU(new GPULocation(gpu, 0, mc, rack_id)));
				}
			}
			// 3 m/c with 1 GPU per machine
			for (int mc = 4; mc < 7; mc++) {
				mGpusInCluster.add(new GPU(new GPULocation(0, 0, mc, rack_id)));
			}
		}*/
	}
}
