package com.wisr.mlsched.localsched;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ConfigUtils;
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
	private double mWorkCompleted;
	private double[] mNwSensitivity;
	private double[] mNwStall;
	private double[] mGPUServiceDim; // rename it
	private double[] mIdealJctDim; // ideal jct per dim
	private double nwDelayWait;
	private double rackDelayWait;

	public DallyIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mGPUServiceForJob = 0.0;
		mNwSensitivity = new double[Simulation.getNumDims()];
		mNwStall = new double[Simulation.getNumDims()];
		mGPUServiceDim = new double[Simulation.getNumDims()];
		mIdealJctDim = new double[Simulation.getNumDims()];
		setDelayTimers(config);
	}

	private void setDelayTimers(JSONObject config) {
		double[] delay_timers = ConfigUtils.getJobDelayTimes(config);

		if (delay_timers[5] != -1) {
			nwDelayWait = Cluster.getInstance().getLeaseTime() * delay_timers[5];
		}
		else {
			nwDelayWait = Cluster.getInstance().getLeaseTime() *
					Cluster.getInstance().getConfiguration().getmNwDelayWait();
		}

		System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(nwDelayWait));

		if (delay_timers[4] != -1) {
			rackDelayWait = Cluster.getInstance().getLeaseTime() * delay_timers[4];
		}
		else {
			rackDelayWait = Cluster.getInstance().getLeaseTime() *
					Cluster.getInstance().getConfiguration().getmRackDelayWait();
		}
		System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(rackDelayWait));
	}

	public void tuneDelayTimers(){
		/*if (mNwSensitivity[mCurrSlwstDim] > .9) {
			rackDelayWait = 0;
			nwDelayWait = 0;
		}*/
		System.out.println("Nw Sensitivity, Job id: " + String.valueOf(this.getJobId()) + " model: " + mModelName
						+ " dim " + String.valueOf(mCurrSlwstDim) + " : " +
						String.valueOf(mNwSensitivity[mCurrSlwstDim]));
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
		/*sLog.info("JobGroup:" + Integer.toString(getJobGroupId())
				+ " Job:" + Integer.toString(getJobId()) +
				" Bid:" + Double.toString(bidValue));*/
		bidList.add(new Bid(offeredGPUs, -1*bidValue, this));
		return bidList;
	}

//	public void startIteration() {
//		super.startIteration();
//		//mGPUServiceForJob += mCurrentIterationGPUs.size()*(mTimePerIteration/getJobSpeedup()) * mIterGranularity;
//		//mGPUServiceForJob = (double)getmTotalIterationsRemaining() / getmTotalExpectedIterations();
//	}

	public void endIteration() {
		super.endIteration();
		mGPUServiceForJob += mCurrentIterationGPUs.size() * (mTimePerIteration/getJobSpeedup()) * mIterGranularity;

		mWorkCompleted = 1 - (double) getmTotalIterationsRemaining() / mTotalExpectedIterations;
		double ideal_jct = mTimePerIteration * mTotalExpectedIterations;
		mNwSensitivity[mCurrSlwstDim] = mWorkCompleted / (mGpuTime / ideal_jct);
		mNwStall[mCurrSlwstDim] = mCommTimeItr / mGpuTimeItr;
		mGPUServiceDim[mCurrSlwstDim] = mCurrentIterationGPUs.size() * (mTimePerIteration/getJobSpeedup()) * mIterGranularity;
		System.out.println("work completed = " + String.valueOf(mWorkCompleted));
		System.out.println("mGpuTime/ideal_jct = " + String.valueOf(mGpuTime/ideal_jct));
		System.out.println("nw stall = " + String.valueOf(mNwStall[mCurrSlwstDim]));
		tuneDelayTimers();
	}

	public double getNwDelayWait(){
		return nwDelayWait;
	}

	public void setNwDelayWait(double time){
		nwDelayWait = time;
	}

	public double getRackDelayWait(){
		return rackDelayWait;
	}

	public void setRackDelayWait(double time, int idx){
		rackDelayWait = time;
	}
}
