package com.wisr.mlsched.config;

/**
 * Representation of the cluster's configuration
 */
public class ClusterConfiguration {
	private int mRacks;
	private int mMachinesPerRack;
	private int mSlotsPerMachine;
	private int mGPUsPerSlot;
	private int mGPUsDim1;
	private int mGPUsDim2;
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
	private float[] mLinkRatio;
	private long[] mLinkLatency;
	private int[] mLinkBandwidth;
	private String[] mAllReduceImpl;
	private String[] mAllGatherImpl;
	private String[] mReduceScatterImpl;
	private String[] mAllToAllImpl;
	String mIntraDimSched;
	String mInterDimSched;
	private String mRunName;

	private double mNwDelayWait;
	private double mRackDelayWait;


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

	public ClusterConfiguration(String run_name, int racks, int machines_per_rack, int slots_per_machine,
								int gpus_per_slot, int gpusDim1, int gpusDim2, int iter_granularity, String policy,
								double lease, double fairness_threshold, double epsilon, boolean useConfig,
								boolean consolidate, String astra_sim_path,
								String astra_sim_bin_path, String topo_name, String[] topo_per_dim, String[] dim_type,
								float[] link_ratio, long[] link_latency, int[] link_bandwidth, String[] all_reduce_impl,
								String[] all_gather_impl, String[] reduce_scatter_impl, String[] all_to_all_impl,
								String intra_dim_sched,	String inter_dim_sched, double nw_delay_wait,
								double rack_delay_wait) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mGPUsPerSlot = gpus_per_slot;
		mGPUsDim1 = gpusDim1;
		mGPUsDim2 = gpusDim2;
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
		mLinkRatio = link_ratio;
		mLinkLatency = link_latency;
		mLinkBandwidth = link_bandwidth;
		mTopoName = topo_name;
		mAllReduceImpl = all_reduce_impl;
		mAllGatherImpl = all_gather_impl;
		mReduceScatterImpl = reduce_scatter_impl;
		mAllToAllImpl = all_to_all_impl;
		mIntraDimSched = intra_dim_sched;
		mInterDimSched = inter_dim_sched;
		mRunName = run_name;
		mNwDelayWait = nw_delay_wait;
		mRackDelayWait = rack_delay_wait;
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

	public int getGPUsDim1() {
		return mGPUsDim1;
	}

	public int getGPUsDim2() {
		return mGPUsDim2;
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
	public float[] getmLinkRatio() {
		return mLinkRatio;
	}
	public String getmRunName() {
		return mRunName;
	}
	public String[] getmAllReduceImpl(){
		return mAllReduceImpl;
	}
	public String[] getmAllGatherImpl(){
		return mAllGatherImpl;
	}
	public String[] getmReduceScatterImpl(){
		return mReduceScatterImpl;
	}
	public String[] getmAllToAllImpl() {
		return mAllToAllImpl;
	}
	public String getmIntraDimSched(){
		return mIntraDimSched;
	}
	public String getmInterDimSched(){
		return mInterDimSched;
	}
	public double getmNwDelayWait() {
		return mNwDelayWait;
	}

	public double getmRackDelayWait() {
		return mRackDelayWait;
	}

}