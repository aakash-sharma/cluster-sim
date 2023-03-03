package com.wisr.mlsched.localsched;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.events.EndIterationEvent;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.events.StartIterationEvent;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.job.JobStatistics;
import com.wisr.mlsched.job.LossFunction;
import com.wisr.mlsched.job.LossFunctionFactory;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;

import org.json.simple.JSONObject;

public abstract class IntraJobScheduler {
	// List of Job configurations
	private int mJobGroupId; // Unique identifier for job group to which this job belongs
	private int mJobId; // Unique identifier for this job
	protected double mJobStartTime; // Job start time
	protected long mTotalExpectedIterations; // Total number of iterations job is expected to run
	protected double mTimePerIteration; // Amount of time for a single iteration of job on 1 GPU
	protected int mIterGranularity; // Number of iterations to schedule at once
	protected int mMaxParallelism; // Represents max GPUs job can request
	private int mRandomSeed; // Random seed for loss curve
	private LossFunction mLossCurve; // Representation of loss curve
	private double mCrossSlotSlowdown; // Slowdown due to network b/w GPUs across slots
	private double mCrossMachineSlowdown; // Slowdown due to network b/w GPUs across machines
	private double mCrossRackSlowdown; // Slowdown due to network b/w GPUs across slots

	private final double CHECKPOINTING_OVERHEAD_PER_GPU = 0.0; // 6 seconds overhead
	
	// State management for job
	private boolean mIsLeader; // Whether this job is the leader in it's job group
	protected long mTotalIterationsRemaining; // Number of iterations of job remaining
	protected Set<GPU> mCurrentIterationGPUs; // GPUs for current iteration
	private Set<GPU> mNextIterationExpectedGPUs; // GPUs we expect from current iteration to be used for next iteration
	protected Set<GPU> mNextIterationGPUs; // GPUs allocated for next iteration
	private boolean mIsWaiting; // Represents if job is waiting for resources
	protected double oldRatio; // Old value of Ts/Ti
	protected double themisTs; // Themis Ts
	private double mTimeLastResourceAssignment; 
	private static Logger sLog; // Instance of logger
	private double mGpuTime;
	private double queueDelay; // State to maintain with admission control
	private boolean mIsQueued; // Every job starts with getting queued 

	public IntraJobScheduler(JSONObject config) {
		initFromConfig(config);
		mJobStartTime = Simulation.getSimulationTime();
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		sLog.info("Starting job " + Integer.toString(mJobId));
		mCurrentIterationGPUs = new HashSet<GPU>();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		mNextIterationGPUs = new HashSet<GPU>();
		mIsWaiting = true;
		oldRatio = Double.POSITIVE_INFINITY;
		themisTs = Double.POSITIVE_INFINITY;
		mGpuTime = 0;
		mTimeLastResourceAssignment = Simulation.getSimulationTime()-1;
		mIsLeader = true; // By default, everyone is a leader unless told otherwise
		JobStatistics.getInstance().recordJobStart(mJobId, Simulation.getSimulationTime(), mMaxParallelism);
		List<GPU> availableResources = getResourcesAvailableInCluster();
		if (!availableResources.isEmpty()) {
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime(), availableResources));
		}
		queueDelay = 0;
		mIsQueued = true;
	}
	
	/**
	 * Set the role of job and number of iterations it needs to run.
	 * @param is_leader
	 * @param iterations
	 */
	public void setRole(boolean is_leader, long iterations) {
		mIsLeader = is_leader;
		mTotalIterationsRemaining = iterations;
		mTotalExpectedIterations = iterations;
	}

	/**
	 * @return the mTotalIterationsRemaining
	 */
	public long getmTotalIterationsRemaining() {
		return mTotalIterationsRemaining;
	}

	/**
	 * @param mTotalIterationsRemaining the mTotalIterationsRemaining to set
	 */
	public void setmTotalIterationsRemaining(long mTotalIterationsRemaining) {
		if (mTotalIterationsRemaining < 0) {
			this.mTotalIterationsRemaining = 0;
		}
		else {
			this.mTotalIterationsRemaining = mTotalIterationsRemaining;
		}
	}
	
	public double getLastResourceAssignment() {
		return mTimeLastResourceAssignment;
	}

	public double getmJobStartTime() {
		return mJobStartTime;
	}
	
	public double getCurrentEstimateForThemis() {
		// Do update if we do not have resources
		if(mCurrentIterationGPUs.size() == 0) {
			//return (Simulation.getSimulationTime() - mJobStartTime) + mTotalIterationsRemaining*mTimePerIteration;
			//System.out.println("Job " + Integer.toString(mJobId) + " : " + Double.toString(oldRatio) + " " + 
			 //   Double.toString(mTimeLastResourceAssignment));
			return oldRatio*(Simulation.getSimulationTime() - mTimeLastResourceAssignment + 1);
		} else {
			//System.out.println("Current iteration GPUs = " + Integer.toString(mCurrentIterationGPUs.size()));
			//System.out.println(getCurrentEstimate());
			return getCurrentEstimate()/getIdealEstimate();
		}
	}

	/**
	 * @return the mTotalExpectedIterations
	 */
	public long getmTotalExpectedIterations() {
		return mTotalExpectedIterations;
	}

	/**
	 * @param mTotalExpectedIterations the mTotalExpectedIterations to set
	 */
	public void setmTotalExpectedIterations(long mTotalExpectedIterations) {
		this.mTotalExpectedIterations = mTotalExpectedIterations;
	}

	public void setmIterGranularity(int mIterGranularity) {
		this.mIterGranularity = mIterGranularity;
	}


	// Aakash: Call astra sim here
	public void startIteration() {
		sLog.log(Level.INFO, "Starting iteration for job " + Integer.toString(mJobId));
		mCurrentIterationGPUs = new HashSet<GPU>(mNextIterationGPUs);
		mNextIterationGPUs = new HashSet<GPU>();
		mIsWaiting = false;
		assert(mCurrentIterationGPUs.size() > 0);
		/*System.out.println("Placement Job " + Integer.toString(mJobId) + ":" 
				+ " Time: " + Double.toString(Simulation.getSimulationTime())
				+ " Iteration: " + Integer.toString(mTotalExpectedIterations - mTotalIterationsRemaining)
				+ " NumGPUs: " + Integer.toString(mCurrentIterationGPUs.size())
				+ " Score: " + Double.toString(getPlacementSlowdown(mCurrentIterationGPUs))
				+ " Number_jobs_running: " + Integer.toString(Cluster.getInstance().getRunningJobs().size()));*/
		ClusterEventQueue.getInstance().enqueueEvent(
				new EndIterationEvent(Simulation.getSimulationTime() + (mTimePerIteration / getJobSpeedup() * mIterGranularity), this));
		// Aakash: augment this
		mGpuTime = mGpuTime + (mTimePerIteration/getJobSpeedup())*mCurrentIterationGPUs.size() * mIterGranularity;
		Iterator<GPU> gpuIter = mCurrentIterationGPUs.iterator();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		while(gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			if(gpu.getLeaseEnd() > Simulation.getSimulationTime() + mTimePerIteration / getJobSpeedup()) {
				mNextIterationExpectedGPUs.add(gpu);
			}
		}
	}

	public void endIteration() {
		setmTotalIterationsRemaining(getmTotalIterationsRemaining() - mIterGranularity);
		long itr_remain = getmTotalIterationsRemaining();
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
							+ CHECKPOINTING_OVERHEAD_PER_GPU*relinquished_resources.size(), relinquished_resources));
			Cluster.getInstance().removeJob(this);
			JobStatistics.getInstance().recordJobEnd(mJobId, Simulation.getSimulationTime(), mJobStartTime,
					getIdealEstimate(), mIsLeader, mGpuTime, mMaxParallelism, queueDelay);
			return;
		}
		// Job has iterations left
		List<GPU> expiredResources = new ArrayList<GPU>();
		Iterator<GPU> currentGPUIterator = mCurrentIterationGPUs.iterator();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		while (currentGPUIterator.hasNext()) {
			GPU gpu = currentGPUIterator.next();
			if (gpu.hasLeaseExpired()) {
				expiredResources.add(gpu);
				gpu.markLeaseEnd();
			} else {
				mNextIterationGPUs.add(gpu);
				mNextIterationExpectedGPUs.add(gpu);
			}
		}
		
		// check if this iteration can finish within the least lease end time
		while(true) {
			boolean converged = true;
			double timeForIteration = mTimePerIteration*getPlacementSlowdown(mNextIterationGPUs)/mNextIterationGPUs.size();
			Iterator<GPU> it = mNextIterationGPUs.iterator();
			while(it.hasNext()) {
				GPU gpu = it.next();
				if(Simulation.getSimulationTime() + timeForIteration > gpu.getLeaseEnd()) {
					// cannot use this GPU anymore
					//System.out.println("Cannot use this GPU anymore " + Integer.toString(mJobId));
					mNextIterationExpectedGPUs.remove(gpu);
					expiredResources.add(gpu);
					gpu.markLeaseEnd();
					it.remove();
					converged = false;
				}
			}
			if(converged) {
				break;	
			}
		}
		
		if(!expiredResources.isEmpty()) {
			ClusterEventQueue.getInstance()
			.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
					CHECKPOINTING_OVERHEAD_PER_GPU*mCurrentIterationGPUs.size(), expiredResources));
		}
		
		if (mNextIterationGPUs.isEmpty()) {
			mCurrentIterationGPUs = new HashSet<GPU>();
			mIsWaiting = true;
			mTimeLastResourceAssignment = Simulation.getSimulationTime();
		} else {
			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
		}
	}
	
	public void resetOldRatio() { // short-circuit this API
		//oldRatio = Double.POSITIVE_INFINITY;
	}

	public double getCurrentEstimate() {
		return (Simulation.getSimulationTime() - mJobStartTime)
				+ (getmTotalIterationsRemaining() * mTimePerIteration) / getJobSpeedup();
	}
	
	public double getEstimateAfterAllocation() {
		return (Simulation.getSimulationTime() - mJobStartTime)
				+ (getmTotalIterationsRemaining() * mTimePerIteration) / getJobSpeedup1();
	}

	public double getIdealEstimate() {
		return 1.0 * mTimePerIteration * getmTotalExpectedIterations() / mMaxParallelism;
	}

	public double getLossGradient() {
		return mLossCurve.getSlope(getmTotalExpectedIterations() - getmTotalIterationsRemaining());
	}
	
	public double getLoss(long iteration) {
		return mLossCurve.getValue(iteration);
	}

	public void notifyResourceAssignment(List<GPU> assignment) {
		sLog.info("Job " + Integer.toString(mJobId) + " got resources"); 
		System.out.println("Job " + Integer.toString(mJobId) + " got resources " + assignment);
		mNextIterationGPUs.addAll(assignment);
		//themisTs = getEstimateAfterAllocation();
	}
	
	public void notifyResourceAvailable() {
		mIsWaiting = false;
		mTimeLastResourceAssignment = Simulation.getSimulationTime();
	}
	
	public double getJobSpeedup1() {
		return getGPUsAvailableForNextIteration().size()* getPlacementSlowdown(getGPUsAvailableForNextIteration());
	}

	public double getJobSpeedup() {
		return mCurrentIterationGPUs.size() * getPlacementSlowdown(mCurrentIterationGPUs);
	}
	
	public int getJobId() {
		return mJobId;
	}
	
	public boolean willParticipateInBid() {
		return true;
	}
	
	public int getJobGroupId() {
		return mJobGroupId;
	}


	// Aakash: change here
	public double getPlacementSlowdown(Set<GPU> gpus) {
		HashSet<Integer> map = new HashSet<Integer>();
		Iterator<GPU> gpuIter = gpus.iterator();
		// Check if across racks
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getRackId());
			if (map.size() > 1) {
				return mCrossRackSlowdown;
			}
		}
		// Check if across machines
		map = new HashSet<Integer>();
		gpuIter = gpus.iterator();
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getMachineId());
			if (map.size() > 1) {
				return mCrossMachineSlowdown;
			}
		}
		// Check if across slots
		map = new HashSet<Integer>();
		gpuIter = gpus.iterator();
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getSlotId());
			if (map.size() > 1) {
				return mCrossSlotSlowdown;
			}
		}
		return 1.0;
	}

	public double getQueueDelay() {
		return queueDelay;
	}

	public boolean isQueued() {
		return mIsQueued;
	}

	public void dequeue(double time) {
		mIsQueued = false;
		queueDelay = time - mJobStartTime;
		mJobStartTime = mJobStartTime + time;
	}
	
	public boolean isWaitingForResources() {
		return mIsWaiting;
	}
	
	public boolean hasResourcesForNextIteration() {
		return mNextIterationGPUs.size() > 0;
	}
	
	public Set<GPU> getGPUsAvailableForNextIteration() {
		Set<GPU> set = new HashSet<GPU>(mNextIterationGPUs);
		// Now add all GPUs to this set which will not expire after the iteration
		set.addAll(mNextIterationExpectedGPUs);
		return set;
	}
	
	/**
	 * Returns the maximum number of GPUs this job can take
	 * @return
	 */
	public int getMaxParallelism() {
		return mMaxParallelism;
	}
	
	public double getOldBenefit() {
		double oldSpeedup = getGPUsAvailableForNextIteration().size() * getPlacementSlowdown(getGPUsAvailableForNextIteration());
		double oldTs = (Simulation.getSimulationTime() - mJobStartTime) + 
				(mTotalIterationsRemaining*mTimePerIteration)/oldSpeedup;
		double oldratio = oldTs/getIdealEstimate();
		System.out.println("Job " + mJobId + " ");
		return oldratio;
	}

	public abstract List<Bid> prepareBid(List<GPU> offeredGPUs);

	private List<GPU> relinquishAllResources() {
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		List<GPU> releasedResources = new ArrayList<GPU>();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (gpu.getJob() != null && gpu.getJob().equals(this)) {
				releasedResources.add(gpu);
				gpu.markLeaseEnd();
			}
		}
		return releasedResources;
	}

	private List<GPU> getResourcesAvailableInCluster() {
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		List<GPU> availableResources = new ArrayList<GPU>();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (!gpu.isLeased()) {
				availableResources.add(gpu);
			}
		}
		return availableResources;
	}

	private void initFromConfig(JSONObject config) {
		mJobGroupId = Integer.parseInt(ConfigUtils.getAttributeValue(config, "job_group_id"));
		mJobId = Integer.parseInt(ConfigUtils.getAttributeValue(config, "job_id"));
		//System.out.println(ConfigUtils.getAttributeValue(config, "total_iterations"));
		setmTotalExpectedIterations(Long.parseLong(ConfigUtils.getAttributeValue(config, "total_iterations")));
		mTimePerIteration = Double.parseDouble(ConfigUtils.getAttributeValue(config, "time_per_iteration"));
		mMaxParallelism = Integer.parseInt(ConfigUtils.getAttributeValue(config, "max_parallelism"));
		mRandomSeed = Integer.parseInt(ConfigUtils.getAttributeValue(config, "random_seed"));
		mCrossSlotSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_slot_slowdown"));
		mCrossMachineSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_machine_slowdown"));
		mCrossRackSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_rack_slowdown"));
		mLossCurve = LossFunctionFactory.createInstance(ConfigUtils.getAttributeValue(
				config, "loss_function_type"), getmTotalExpectedIterations(), mRandomSeed);
		setmTotalIterationsRemaining(getmTotalExpectedIterations());
		//mCrossSlotSlowdown = 1.0;
		//mCrossMachineSlowdown = random(0.6, 0.98);
		//mCrossRackSlowdown = mCrossMachineSlowdown/0.7;
	}
	
	private double random( double min, double max ) {
	  double diff = max - min;
	  return min + Math.random( ) * diff;
	}
}
