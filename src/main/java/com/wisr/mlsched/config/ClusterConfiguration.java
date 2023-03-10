package com.wisr.mlsched.config;

/**
 * Representation of the cluster's configuration
 */
public class ClusterConfiguration {
	private int mRacks;
	private int mMachinesPerRack;
	private int mSlotsPerMachine;
	private int mGPUsPerSlot;
	private int mIterGranularity;
	private String mPolicy;
	private double mLeaseTime;
	private double mFairnessThreshold;
	private double mEpsilon;
	private boolean mUseConfig;
	private String mAstraSimPath;
	private String mAstraSimBinPath;

	public ClusterConfiguration(int racks, int machines_per_rack,
                                int slots_per_machine, int gpus_per_slot, int iter_granularity, String policy, double lease,
                                double fairness_threshold, double epsilon, boolean useConfig, String astra_sim_path, String astra_sim_bin_path) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mGPUsPerSlot = gpus_per_slot;
		mIterGranularity = iter_granularity;
		mPolicy = policy;
		mLeaseTime = lease;
		mFairnessThreshold = fairness_threshold;
		mEpsilon = epsilon;
		mUseConfig = useConfig;
		mAstraSimPath = astra_sim_path;
		mAstraSimBinPath = astra_sim_bin_path;
	}
	
	public boolean getUseConfig() {
		return mUseConfig;
	}
	
	public double getLeaseTime() {
		return mLeaseTime;
	}
	
	public int getRacks() {
		return mRacks;
	}

	public int getMachinesPerRack() {
		return mMachinesPerRack;
	}

	public int getSlotsPerMachine() {
		return mSlotsPerMachine;
	}

	public int getGPUsPerSlot() {
		return mGPUsPerSlot;
	}

	public int getIterGranularity() {
		return mIterGranularity;
	}

	public String getPolicy() {
		return mPolicy;
	}
	
	public double getFairnessThreshold() {
		return mFairnessThreshold;
	}
	
	public double getEpsilon() {
		return mEpsilon;
	}
	public String getmAstraSimPath() {
		return mAstraSimPath;
	}
	public String getmAstraSimBinPath() {
		return mAstraSimBinPath;
	}
}