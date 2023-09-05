package com.wisr.mlsched.localsched;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.events.StartIterationEvent;
import com.wisr.mlsched.globalsched.DallyInterJobScheduler;
import com.wisr.mlsched.globalsched.DallyInterJobScheduler;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.job.JobStatistics;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DallyIntraJobScheduler extends IntraJobScheduler {

	private static Logger sLog; // Instance of logger
	private double mGPUServiceForJob; // Measurement of GPU time made available to job
	private double nwDelayWait;
	private double rackDelayWait;
	private int mJobQ;

	public DallyIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mGPUServiceForJob = 0.0;
		mJobQ = 1;
		DallyInterJobScheduler sched = (DallyInterJobScheduler) Cluster.getInstance().getScheduler();
		sched.getBgPriorityQ().add(this);
		setDelayTimers(config);
	}

	private void setDelayTimers(JSONObject config) {
		double[] delay_timers = ConfigUtils.getJobDelayTimes(config);

		if (delay_timers[5] != -1) {
			//nwDelayWait = Cluster.getInstance().getLeaseTime() * delay_timers[5];
			nwDelayWait = delay_timers[5];
		}
		else {
			//nwDelayWait = Cluster.getInstance().getLeaseTime() *
			//		Cluster.getInstance().getConfiguration().getmNwDelayWait();
			nwDelayWait = Cluster.getInstance().getConfiguration().getmNwDelayWait();
		}

		System.out.println("Setting nw delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(nwDelayWait));

		if (delay_timers[4] != -1) {
			//rackDelayWait = Cluster.getInstance().getLeaseTime() * delay_timers[4];
			rackDelayWait = delay_timers[4];
		}
		else {
			//rackDelayWait = Cluster.getInstance().getLeaseTime() *
			//		Cluster.getInstance().getConfiguration().getmRackDelayWait();
			rackDelayWait = Cluster.getInstance().getConfiguration().getmRackDelayWait();
		}
		System.out.println("Setting rack delay for job: " + String.valueOf(this.getJobId()) + " to: "
				+ String.valueOf(rackDelayWait));
	}

	public void tuneDelayTimers(){
		double remain_ratio = (double)getmTotalIterationsRemaining() / getmTotalExpectedIterations();
		int num_dims = Simulation.getNumDims();

		if (mSlowdownDims[num_dims-1] != -1) {
			if (remain_ratio <= .3)
			{
				System.out.println("Reducing nw delay by 1");
				//nwDelayWait -= 1;
			}
		}

		if (mSlowdownDims[num_dims-2] != -1) {
			if (remain_ratio <= .3)
			{
				System.out.println("Reducing rack delay to 0");
				rackDelayWait = 0;
			}
		}
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
		//mGPUServiceForJob += mCurrentIterationGPUs.size()*(mTimePerIteration/getJobSpeedup()) * mIterGranularity;
	}

	public void endIteration() {
		mGPUServiceForJob = (double)getmTotalIterationsRemaining() / getmTotalExpectedIterations();
		DallyInterJobScheduler sched = (DallyInterJobScheduler) Cluster.getInstance().getScheduler();
		long itr_remain = getmTotalIterationsRemaining();
		setmTotalIterationsRemaining(itr_remain  - (mIterGranularity * mCurrentIterationGPUs.size()/mMaxParallelism));
		if (itr_remain % 10000 == 0) {
			sLog.log(Level.ALL, "End iteration for job " + Integer.toString(mJobId));
			sLog.info("Iterations Remaining: " + Long.toString(itr_remain));
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());
		}
		oldRatio = getCurrentEstimate()/getIdealEstimate();
		themisTs = getCurrentEstimate();
		if (getmTotalIterationsRemaining() == 0) {
			// Job is done
			System.out.println("Job " + Integer.toString(mJobId) + " done");
			sLog.info("Job " + Integer.toString(mJobId) + " done");
			List<GPU> relinquished_resources = relinquishAllResources();
			// Make all relinquished resources available
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime()
							+ CHECKPOINTING_OVERHEAD_PER_GPU, relinquished_resources));
			sched.removeJob(this);
			Cluster.getInstance().removeJob(this);
			JobStatistics.getInstance().recordJobEnd(mJobId, Simulation.getSimulationTime(), mJobStartTime,
					getIdealEstimate(), mIsLeader, mGpuTime, mCompTime, mCommTime, mMaxParallelism, queueDelay, mAllocs);

			System.out.println("Allocs: " + Arrays.toString(mAllocs));
			System.out.println("slowdowns: " + Arrays.toString(mSlowdownDims));
			System.out.println("Max JVM memory: " + Runtime.getRuntime().maxMemory());
			System.out.println("Total JVM memory: " + Runtime.getRuntime().totalMemory());
			System.out.println("Free JVM memory: " + Runtime.getRuntime().freeMemory());
			return;
		}

		// Job has iterations left

		if (sched.checkJobPreempted(this)) {
			Iterator<GPU> currentGPUIterator = mCurrentIterationGPUs.iterator();
			while (currentGPUIterator.hasNext()) {
				GPU gpu = currentGPUIterator.next();
				gpu.markLeaseEnd();
			}
			List<GPU> expiredResources = new ArrayList<GPU>(mCurrentIterationGPUs);
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
							CHECKPOINTING_OVERHEAD_PER_GPU*mCurrentIterationGPUs.size(), expiredResources));
			mIsWaiting = true;
			mTimeLastResourceAssignment = Simulation.getSimulationTime();
			return;
		}

		mNextIterationGPUs = mCurrentIterationGPUs;
		ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
		ClusterEventQueue.getInstance()
				.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime()+1, new ArrayList<GPU>()));
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
	public double getGPUServiceForJob(){
		return mGPUServiceForJob;
	}

	public int getJobQ() {
		return mJobQ;
	}

	public void setJobQ(int jobQ) {
		this.mJobQ = jobQ;
	}
}
