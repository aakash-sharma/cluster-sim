package com.wisr.mlsched.config;

import java.util.HashMap;

/**
 * Representation of the cluster's configuration
 */
public class ClusterConfiguration {
	private int mRacks;
	private int mMachinesPerRack;
	private int mSlotsPerMachine;
	private int mDim1sPerSlot;
	private int mDim2sPerDim1;
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
	private HashMap<String, Double> mRackOverheads;
	private HashMap<String, Double> mNwOverheads;
	private double mNwDelayWait;
	private double mRackDelayWait;
	private int mDelayHist;

	public double getmNwDelayWait() {
		return mNwDelayWait;
	}

	public double getmRackDelayWait() {
		return mRackDelayWait;
	}


	public ClusterConfiguration(int racks, int machines_per_rack, int slots_per_machine, int gpus_per_slot,
								int iter_granularity, String policy, double lease, double fairness_threshold,
								double epsilon, boolean useConfig, boolean consolidate, String astra_sim_path,
								String astra_sim_bin_path) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mDim1sPerSlot = gpus_per_slot;
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
								int dim1_slot, int dim2_dim1, int gpus_dim2, int iter_granularity, String policy,
								double lease, double fairness_threshold, double epsilon, boolean useConfig,
								boolean consolidate, String astra_sim_path, String astra_sim_bin_path, String topo_name,
								String[] topo_per_dim, String[] dim_type, float[] link_ratio, long[] link_latency,
								int[] link_bandwidth, String[] all_reduce_impl, String[] all_gather_impl,
								String[] reduce_scatter_impl, String[] all_to_all_impl, String intra_dim_sched,
								String inter_dim_sched, HashMap<String, Double> rackOverheads,
								HashMap<String, Double> nwOverheads, double nw_delay_wait, double rack_delay_wait,
								int delay_hist) {
		mRacks = racks;
		mMachinesPerRack = machines_per_rack;
		mSlotsPerMachine = slots_per_machine;
		mDim1sPerSlot = dim1_slot;
		mDim2sPerDim1 = dim2_dim1;
		mGPUsDim2 = gpus_dim2;
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
		mRackOverheads = rackOverheads;
		mNwOverheads = nwOverheads;
		mNwDelayWait = nw_delay_wait;
		mRackDelayWait = rack_delay_wait;
		mDelayHist = delay_hist;
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

	public int getDim1PerSlot() {
		return mDim1sPerSlot;
	}

	public int getDim2sPerDim1() {
		return mDim2sPerDim1;
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

	public HashMap<String, Double> getmRackOverheads() {
		return mRackOverheads;
	}

	public void setmRackOverheads(HashMap<String, Double> mRackOverheads) {
		this.mRackOverheads = mRackOverheads;
	}

	public HashMap<String, Double> getmNwOverheads() {
		return mNwOverheads;
	}

	public void setmNwOverheads(HashMap<String, Double> mNwOverheads) {
		this.mNwOverheads = mNwOverheads;
	}

	public int getDelayHist() {
		return mDelayHist;
	}
}