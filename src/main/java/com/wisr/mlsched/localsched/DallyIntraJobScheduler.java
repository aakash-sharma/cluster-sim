package com.wisr.mlsched.localsched;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DallyIntraJobScheduler extends IntraJobScheduler {

	private static Logger sLog; // Instance of logger
	private double mGPUServiceForJob; // Measurement of GPU time made available to job
	private double nwDelayWait[];
	private double rackDelayWait[];

	public DallyIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mGPUServiceForJob = 0.0;
		nwDelayWait = new double[2];
		rackDelayWait = new double[2];
		nwDelayWait[0] = Cluster.getInstance().getLeaseTime() * Cluster.getInstance().getmNwDelayWait();
		rackDelayWait[0] = Cluster.getInstance().getLeaseTime() * Cluster.getInstance().getmRackDelayWait();
	}

	@Override
	public List<Bid> prepareBid(List<GPU> offeredGPUs) {
		// We should get a bid only 1 GPU at a time
		if (getGPUsAvailableForNextIteration().size() >= mMaxParallelism) {
			// Already have enough GPUs. No need to bid
			return null;
		}
		if (offeredGPUs.size() != 1) {
			sLog.severe("Offered incorrect # GPUs: " + Integer.toString(offeredGPUs.size()));
			return null;
		}
		
		List<Bid> bidList = new ArrayList<Bid>();
		// Added negative of GPUService since we want job with min value to win
		double bidValue = mGPUServiceForJob; 
		sLog.info("JobGroup:" + Integer.toString(getJobGroupId())
		+ " Job:" + Integer.toString(getJobId()) + 
		" Bid:" + Double.toString(bidValue));
		bidList.add(new Bid(offeredGPUs, -1*bidValue, this));
		return bidList;
	}

	public List<Bid> prepareMultiBid(List<GPU> offeredGPUs) {

		List<Bid> bidList = new ArrayList<Bid>();
		// Added negative of GPUService since we want job with min value to win
		double bidValue = mGPUServiceForJob;
		sLog.info("JobGroup:" + Integer.toString(getJobGroupId())
				+ " Job:" + Integer.toString(getJobId()) +
				" Bid:" + Double.toString(bidValue));
		bidList.add(new Bid(offeredGPUs, -1*bidValue, this));
		return bidList;
	}

	public void startIteration() {
		super.startIteration();
		mGPUServiceForJob += mCurrentIterationGPUs.size()*(mTimePerIteration/getJobSpeedup()) * mIterGranularity;
	}

	public double[] getNwDelayWait(){
		return nwDelayWait;
	}

	public void setNwDelayWait(double time, int idx){
		nwDelayWait[idx] = time;
	}

	public double[] getRackDelayWait(){
		return rackDelayWait;
	}

	public void setRackDelayWait(double time, int idx){
		rackDelayWait[idx] = time;
	}
}
