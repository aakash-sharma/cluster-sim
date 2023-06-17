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
	private boolean mConsolidate;
	private String mAstraSimPath;
	private String mAstraSimBinPath;
	private String mTopoName;
	private String[] mTopoPerDim;
	private String[] mDimType;
	private int[] mLinkCount;
	private long[] mLinkLatency;
	private int[] mLinkBandwidth;
	private String mRunName;

	public ClusterConfiguration(int racks, int machines_per_rack, int slots_per_machine, int gpus_per_slot,
								int iter_granularity, String policy, double lease, double fairness_threshold,
								double epsilon, boolean useConfig, boolean consolidate, String astra_sim_path,
								String astra_sim_bin_path) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mGPUsPerSlot = gpus_per_slot;
		mIterGranularity = iter_granularity;
		mPolicy = policy;
		mLeaseTime = lease;
		mFairnessThreshold = fairness_threshold;
		mEpsilon = epsilon;
		mConsolidate = consolidate;
		mUseConfig = useConfig;
		mAstraSimPath = astra_sim_path;
		mAstraSimBinPath = astra_sim_bin_path;
	}

	public ClusterConfiguration(String run_name, int racks, int machines_per_rack, int slots_per_machine, int gpus_per_slot,
								int iter_granularity, String policy, double lease, double fairness_threshold,
								double epsilon, boolean useConfig, boolean consolidate, String astra_sim_path,
								String astra_sim_bin_path, String topo_name, String[] topo_per_dim, String[] dim_type,
								int[] link_count, long[] link_latency, int[] link_bandwidth) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mGPUsPerSlot = gpus_per_slot;
		mIterGranularity = iter_granularity;
		mPolicy = policy;
		mLeaseTime = lease;
		mFairnessThreshold = fairness_threshold;
		mEpsilon = epsilon;
		mConsolidate = consolidate;
		mUseConfig = useConfig;
		mAstraSimPath = astra_sim_path;
		mAstraSimBinPath = astra_sim_bin_path;
		mTopoPerDim = topo_per_dim;
		mDimType = dim_type;
		mLinkCount = link_count;
		mLinkLatency = link_latency;
		mLinkBandwidth = link_bandwidth;
		mTopoName = topo_name;
		mRunName = run_name;
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
	public String getmAstraSimBinPath() { return mAstraSimBinPath; }

	public boolean getmConsolidate() { return mConsolidate; }

	public String getmTopoName(){
		return mTopoName;
	}

	public String[] getmTopoPerDim(){
		return mTopoPerDim;
	}

	public String[] getmDimType() {
		return mDimType;
	}

	public int[] getmLinkBandwidth() {
		return mLinkBandwidth;
	}

	public long[] getmLinkLatency() {
		return mLinkLatency;
	}
	public int[] getmLinkCount() {
		return mLinkCount;
	}
	public String getmRunName() {
		return mRunName;
	}
}