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
	private boolean delayFlag;

	public DallyIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mGPUServiceForJob = 0.0;
		mNwSlowdown = 0.0;
		mNwStall = new double[Simulation.getNumDims()];
		mGpuTimeDim = new double[Simulation.getNumDims()];
		mIdealGpuTimeDim = new double[Simulation.getNumDims()];
		delayFlag = true;
		setDelayTimers(config);
	}

	private void setDelayTimers(JSONObject config) {
		double[] delay_timers = ConfigUtils.getJobDelayTimes(config);

		if (delay_timers[5] != -1) {
			nwDelayWait = delay_timers[5];
			delayFlag = false;
		}
		else {
			nwDelayWait = Cluster.getInstance().getConfiguration().getmNwDelayWait();  // default 1
		}

		System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(nwDelayWait));

		if (delay_timers[4] != -1) {
			rackDelayWait = delay_timers[4];
			delayFlag = false;
		}
		else {
			rackDelayWait = Cluster.getInstance().getConfiguration().getmRackDelayWait(); // default 1
		}
		System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(rackDelayWait));
	}

	public void tuneDelayTimers(int gpu_demand){
		int delay = 0;

		if (mWorkCompleted >= .75  || mCurrSlwstDim < 4) {
			return;
		}

		System.out.println("Nw slowdown, Job id: " + String.valueOf(this.getJobId()) + " model: " + mModelName
				+ " dim " + String.valueOf(mCurrSlwstDim) + " : " +
				String.valueOf(mNwSlowdown) + " Nw stall: " + String.valueOf(mNwStall[mCurrSlwstDim]));

		DallyInterJobScheduler sched = (DallyInterJobScheduler) Cluster.getInstance().getScheduler();
		rackDelayWait = Math.ceil(sched.getMcDemandDelay(gpu_demand));
		nwDelayWait = Math.ceil(sched.getRackDemandDelay(gpu_demand));
//		System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(rackDelayWait));
//			System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(nwDelayWait));
//		if (sched.getMcDemandDelayMap().containsKey(gpu_demand)) {
//			rackDelayWait = Math.ceil(sched.getMcDemandDelayMap().get(gpu_demand));
//			System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(rackDelayWait));
//		}
//		if (sched.getRackDemandDelayMap().containsKey(gpu_demand)) {
//			nwDelayWait = Math.ceil(sched.getRackDemandDelayMap().get(gpu_demand));
//			System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(nwDelayWait));
//		}

		/*if (mCurrSlwstDim == 4 && rackDelayWait > 0){
			return;
		}
		if (mCurrSlwstDim == 5 && nwDelayWait > 0){
			return;
		}

		if (mNwStall[mCurrSlwstDim] >= .8) {
			delay = 5;
		}
		else if (mNwStall[mCurrSlwstDim] >= .5) {
			delay = 2;
		}*/
		/*
		else if (mNwStall[mCurrSlwstDim] >= .4) {
			delay = 6;
		}
		else if (mNwStall[mCurrSlwstDim] >= .3) {
			delay = 4;
		}
		else if (mNwStall[mCurrSlwstDim] >= .2) {
			delay = 2;
		}*/

//		if (mCurrSlwstDim == 4) {
//			/*if (mNwStall[mCurrSlwstDim] >= .8) {
//				delay = 5;
//			}
//			else*/ if (mNwStall[mCurrSlwstDim] >= .5) {
//				delay = 1;
//			}
//			rackDelayWait = delay;
//			System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(rackDelayWait));
//		}
//		else {
//			/*if (mNwStall[mCurrSlwstDim] >= .8) {
//				delay = 10;
//			}
//			else */if (mNwStall[mCurrSlwstDim] >= .5) {
//				delay = 5;
//			}
//			nwDelayWait = delay;
//			System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
//					+ String.valueOf(nwDelayWait));
//		}
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
		mGPUServiceForJob += mCurrentIterationGPUs.size() * (mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs))
				* mIterGranularity;
		int demand = mCurrentIterationGPUs.size();
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

		if (delayFlag) {
			tuneDelayTimers(demand);
		}
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
