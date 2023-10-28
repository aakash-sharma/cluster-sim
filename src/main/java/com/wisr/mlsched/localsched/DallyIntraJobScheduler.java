package com.wisr.mlsched.localsched;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.globalsched.DallyInterJobScheduler;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DallyIntraJobScheduler extends IntraJobScheduler {

	private static Logger sLog; // Instance of logger
	private double mGPUServiceForJob; // Measurement of GPU service made available to job
	private double mWorkCompleted;
	private double mNwSlowdown;
	private double[] mNwStall;
	private double[] mGpuTimeDim;
	private double[] mIdealGpuTimeDim; // ideal time per dim
	private double nwDelayWait;
	private double rackDelayWait;
	private boolean tuneDelayFlag;

	public DallyIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mGPUServiceForJob = 0.0;
		mNwSlowdown = 0.0;
		mNwStall = new double[Simulation.getNumDims()];
		mGpuTimeDim = new double[Simulation.getNumDims()];
		mIdealGpuTimeDim = new double[Simulation.getNumDims()];
		tuneDelayFlag = true;
		setDelayTimers(config);
	}

	private void setDelayTimers(JSONObject config) {
		double[] delay_timers = ConfigUtils.getJobDelayTimes(config);

		if (delay_timers[5] != -1) {
			nwDelayWait = delay_timers[5];
			tuneDelayFlag = false;
		}
		else {
			nwDelayWait = Cluster.getInstance().getConfiguration().getmNwDelayWait();  // default 1
		}

		System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(nwDelayWait));

		if (delay_timers[4] != -1) {
			rackDelayWait = delay_timers[4];
			tuneDelayFlag = false;
		}
		else {
			rackDelayWait = Cluster.getInstance().getConfiguration().getmRackDelayWait(); // default 1
		}
		System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(rackDelayWait));
	}

	public void tuneDelayTimers(int gpu_demand){
		/*if (mWorkCompleted >= .75) {
			return;
		}

		System.out.println("Nw slowdown, Job id: " + String.valueOf(this.getJobId()) + " model: " + mModelName
				+ " dim " + String.valueOf(mCurrSlwstDim) + " : " +
				String.valueOf(mNwSlowdown) + " Nw stall: " + String.valueOf(mNwStall[mCurrSlwstDim])); */

		DallyInterJobScheduler sched = (DallyInterJobScheduler) Cluster.getInstance().getScheduler();

		//if (mNwStall[4] > .2) {
			rackDelayWait = Math.min(10, Math.max(1, Math.ceil(sched.getMcDemandDelay(gpu_demand))));
			//rackDelayWait = Math.min(6, Math.max(1, Math.ceil(sched.getMcDemandDelay(gpu_demand))));
			System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
					+ String.valueOf(rackDelayWait));
		//}
		//if (mNwStall[5] > .2) {
			//nwDelayWait = Math.min(10, Math.max(1, Math.ceil(sched.getRackDemandDelay(gpu_demand))));
			nwDelayWait = Math.min(12, Math.max(2, Math.ceil(sched.getRackDemandDelay(gpu_demand))));
			System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
					+ String.valueOf(nwDelayWait));
		//}
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
//		sLog.info("JobGroup:" + Integer.toString(getJobGroupId())
//				+ " Job:" + Integer.toString(getJobId()) +
//				" Bid:" + Double.toString(bidValue));
		bidList.add(new Bid(offeredGPUs, -1*bidValue, this));
		return bidList;
	}

//	public void startIteration() {
//		super.startIteration();
//		//mGPUServiceForJob += mCurrentIterationGPUs.size()*(mTimePerIteration/getJobSpeedup()) * mIterGranularity;
//		//mGPUServiceForJob = (double)getmTotalIterationsRemaining() / getmTotalExpectedIterations();
//		mGPUServiceForJob += mCurrentIterationGPUs.size() * (mTimePerIteration/getPlacementSlowdown(mCurrentIterationGPUs))
//				* mIterGranularity;
//	}

	public void endIteration() {
		//mGPUServiceForJob += mCurrentIterationGPUs.size() * (mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs))
		//		* mIterGranularity;
		//int demand = mCurrentIterationGPUs.size();
		super.endIteration();
//		System.out.println("endIteration");
//		System.out.println(mCurrentIterationGPUs.size());
//		System.out.println(mTimePerIteration);
//		System.out.println(getPlacementSlowdown(mCurrentIterationGPUs));
//		System.out.println(mIterGranularity);
//		System.out.println(mCurrentIterationGPUs.size() * (mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs))
//				* mIterGranularity);


		mWorkCompleted = 1 - (double) getmTotalIterationsRemaining() / mTotalExpectedIterations;
		double ideal_jct = mTimePerIteration * mTotalExpectedIterations / mMaxParallelism;
		mNwSlowdown = mWorkCompleted / (mGpuTime / ideal_jct);
		mGPUServiceForJob = mNwSlowdown;
		mNwStall[mCurrSlwstDim] = mCommTimeItr / mGpuTimeItr;
		//mGpuTimeDim[mCurrSlwstDim] +=  mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs) * mIterGranularity;
		//mIdealGpuTimeDim[mCurrSlwstDim] +=  mTimePerIteration * mIterGranularity;
		//System.out.println("JobId: " + String.valueOf(getJobId()) + " endIteration");
		//System.out.println("nw slowdown: " + String.valueOf(mNwSlowdown));
		//System.out.println("work completed = " + String.valueOf(mWorkCompleted));
		//System.out.println("mGpuTime/ideal_jct = " + String.valueOf(mGpuTime/ideal_jct));
		//System.out.println("nw stall = " + String.valueOf(mNwStall[mCurrSlwstDim]));

//		if (tuneDelayFlag) {
//			tuneDelayTimers(demand);
//		}
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

	public boolean isTuneDelayFlag() {
		return tuneDelayFlag;
	}
}
