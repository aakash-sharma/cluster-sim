package com.wisr.mlsched.localsched;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.events.StartIterationEvent;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.job.JobStatistics;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.json.simple.JSONObject;

public class GandivaIntraJobScheduler extends IntraJobScheduler {

	private static Logger sLog; // Instance of logger
	private boolean mSwap;
	private Map<Integer, Double> nwSlowdown;
	private Map<Integer, Double> rackSlowdown;
	private Map<Integer, Double> mcSlowdown;
	private Map<Integer, Double> slotSlowdown;

	public GandivaIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		mSwap = false;
		nwSlowdown = new HashMap<Integer, Double>();
		rackSlowdown = new HashMap<Integer, Double>();
		mcSlowdown = new HashMap<Integer, Double>();
		slotSlowdown = new HashMap<Integer, Double>();
	}

	@Override
	public List<Bid> prepareBid(List<GPU> offeredGPUs) {
		// We should get a bid only 1 GPU at a time
		if(getGPUsAvailableForNextIteration().size() >= mMaxParallelism) {
			// Already have enough GPUs. No need to bid
			return null;
		}
		if(offeredGPUs.size() != 1) {
			sLog.severe("Offered incorrect # GPUs: " + 
					Integer.toString(offeredGPUs.size()));
			return null;
		}
		Set<GPU> potentialNewGPUSet = new HashSet<GPU>(getGPUsAvailableForNextIteration());
		potentialNewGPUSet.addAll(offeredGPUs);
		// Prepare a bid with new placement score
		double placementScore = potentialNewGPUSet.size() * getPlacementSlowdown(potentialNewGPUSet);
		List<Bid> bidList = new ArrayList<Bid>();
		sLog.info("JobGroup:" + Integer.toString(getJobGroupId())
		+ " Job:" + Integer.toString(getJobId()) + 
		" Bid:" + Double.toString(placementScore));
		bidList.add(new Bid(offeredGPUs, placementScore, this));
		return bidList;
	}

	public List<Bid> prepareMultiBid(List<GPU> offeredGPUs) {

		// Prepare a bid with new placement score
		List<Bid> bidList = new ArrayList<Bid>();

		bidList.add(new Bid(offeredGPUs, mJobStartTime, this));
		return bidList;
	}

	public void endIteration() {

		mGpuTime += mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs) * mIterGranularity;
		mCompTime += mTimePerIteration * mIterGranularity;
		mCommTime = mGpuTime - mCompTime;

		mGpuTimeItr = mTimePerIteration / getPlacementSlowdown(mCurrentIterationGPUs) * mIterGranularity;
		mCompTimeItr = mTimePerIteration * mIterGranularity;
		mCommTimeItr = mGpuTimeItr - mCompTimeItr;

		if (mCommTimeItr < 0) {
			System.out.println("negative comm time, gpu time: " + String.valueOf(mGpuTimeItr) + " comp time: " + String.valueOf(mCompTimeItr)
					+ " job speedup: " + String.valueOf(getPlacementSlowdown(mCurrentIterationGPUs)));
		}

		long itr_remain = getmTotalIterationsRemaining();
		setmTotalIterationsRemaining(itr_remain  - (mIterGranularity * mCurrentIterationGPUs.size()));
		if (itr_remain % 1000 == 0) {
			sLog.log(Level.ALL, "End iteration for job " + Integer.toString(mJobId));
			sLog.info("Iterations Remaining: " + Long.toString(itr_remain));
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());
		}
		oldRatio = getCurrentEstimate()/getIdealEstimate();
		themisTs = getCurrentEstimate();
		if (getmTotalIterationsRemaining() == 0) {
			// Job is done
			System.out.println("JobId: " + Integer.toString(mJobId) + " done");
			//sLog.info("Job " + Integer.toString(mJobId) + " done");
			List<GPU> relinquished_resources = relinquishAllResources();
			// Make all relinquished resources available
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime()
							+ relinquished_resources.size(), relinquished_resources));
			Cluster.getInstance().removeJob(this);
			JobStatistics.getInstance().recordJobEnd(mJobId, Simulation.getSimulationTime(), mJobStartTime,
					getIdealEstimate(), mIsLeader, mGpuTime, mCompTime, mCommTime, mMaxParallelism, queueDelay, mAllocs
					, mJobArrivalTime);

			System.out.println("Allocs: " + Arrays.toString(mAllocs));
			System.out.println("slowdowns: " + Arrays.toString(mSlowdownDims));
			//System.out.println("Max JVM memory: " + Runtime.getRuntime().maxMemory());
			//System.out.println("Total JVM memory: " + Runtime.getRuntime().totalMemory());
			//System.out.println("Free JVM memory: " + Runtime.getRuntime().freeMemory());
			return;
		}
		// Job has iterations left
//		Iterator<GPU> currentGPUIterator = mCurrentIterationGPUs.iterator();
//
//		if (mSwap) {
//			mSwap = false;
//		}

//			List<GPU> expiredResources = new ArrayList<GPU>();
//			while (currentGPUIterator.hasNext()) {
//				GPU gpu = currentGPUIterator.next();
//				expiredResources.add(gpu);
//				gpu.markLeaseEnd();
//			}
//
//			mCurrentIterationGPUs = new HashSet<GPU>();
//
//			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
//			ClusterEventQueue.getInstance()
//					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
//							mCurrentIterationGPUs.size(), expiredResources));
//		}
//		else {
//			while (currentGPUIterator.hasNext()) {
//				GPU gpu = currentGPUIterator.next();
//				mNextIterationGPUs.add(gpu);
//			}
//
//			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
//		}
		if (mSwap) {
			mSwap = false;
			System.out.println("Swapping job, next iteration gpu size: " + String.valueOf(mNextIterationGPUs.size()));
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());

			List<GPU> expiredResources = new ArrayList<GPU>();
			Iterator<GPU> currentGPUIterator = mCurrentIterationGPUs.iterator();
			while (currentGPUIterator.hasNext()) {
				GPU gpu = currentGPUIterator.next();
					expiredResources.add(gpu);
					gpu.markLeaseEnd();
				}
			ClusterEventQueue.getInstance()
				.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
						mCurrentIterationGPUs.size(), expiredResources));
			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
			return;
		}

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

		if (expiredResources.isEmpty()) {
			mNextIterationGPUs = mCurrentIterationGPUs;
		}

		// check if this iteration can finish within the least lease end time
		while (true) {
			boolean converged = true;
			double timeForIterations = mTimePerIteration * mIterGranularity /
					getPlacementSlowdown(mNextIterationGPUs) / mNextIterationGPUs.size();
			Iterator<GPU> it = mNextIterationGPUs.iterator();
			while (it.hasNext()) {
				GPU gpu = it.next();
				if (Simulation.getSimulationTime() + timeForIterations > gpu.getLeaseEnd()) {
					// cannot use this GPU anymore
					//System.out.println("Cannot use this GPU anymore " + Integer.toString(mJobId));
					mNextIterationExpectedGPUs.remove(gpu);
					expiredResources.add(gpu);
					gpu.markLeaseEnd();
					it.remove();
					converged = false;
				}
			}
			if (converged) {
				break;
			}
		}

		if (mNextIterationGPUs.size() > mNextIterationExpectedGPUs.size()) {
			mNextIterationGPUs = mNextIterationExpectedGPUs;
		}

		if(!expiredResources.isEmpty()) {
			System.out.println("releasing resources: " + String.valueOf(expiredResources.size()));
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
							mCurrentIterationGPUs.size(), expiredResources));
		}

		if (mNextIterationGPUs.isEmpty()) {
			System.out.println("Job GPU lease expired");
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());
			mCurrentIterationGPUs = new HashSet<GPU>();
			mIsWaiting = true;
			mTimeLastResourceAssignment = Simulation.getSimulationTime();
		} else {
			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
		}

	}

	public double getPlacementSlowdown(Set<GPU> gpus) {
		if (gpus.isEmpty())
		{
			return 0;
		}

		if (gpus.size() == 1) {
			return 1;
		}

		/*if (mSlowdown.containsKey(gpus)){
			return mSlowdown.get(gpus);
		}*/

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();
		MultiKeyMap slotMap = new MultiKeyMap();

		for (GPU gpu: gpus) {
			Integer rack = gpu.getLocation().getRackId();
			Integer machine = gpu.getLocation().getMachineId();
			Integer slot = gpu.getLocation().getSlotId();

			Integer count = rackMap.get(rack);
			rackMap.merge(rack, 1, Integer::sum);

			if (!machineMap.containsKey(rack, machine)) {
				machineMap.put(rack, machine, 1);
			} else {
				count = (Integer) machineMap.get(rack, machine);
				machineMap.put(rack, machine, count + 1);
			}

			if (!slotMap.containsKey(rack, machine, slot)) {
				slotMap.put(rack, machine, slot, 1);
			} else {
				count = (Integer) slotMap.get(rack, machine, slot);
				slotMap.put(rack, machine, slot, count + 1);
			}
		}

		double slowdown = 1 ;
		int num_gpus = gpus.size();

		if (rackMap.size() > 1) {
			if (nwSlowdown.containsKey(num_gpus)) {
				//System.out.println("Returning cached nw slowdown");
				return nwSlowdown.get(num_gpus);
			}
			slowdown = super.getPlacementSlowdown(gpus);
			nwSlowdown.put(num_gpus, slowdown);
			return slowdown;
		}
		else if (machineMap.size() > 1) {
			if (rackSlowdown.containsKey(num_gpus)) {
				//System.out.println("Returning cached rack slowdown");
				return rackSlowdown.get(num_gpus);
			}
			slowdown = super.getPlacementSlowdown(gpus);
			rackSlowdown.put(num_gpus, slowdown);
			return slowdown;
		}
		else if (slotMap.size() > 1) {
			if (mcSlowdown.containsKey(num_gpus)) {
				//System.out.println("Returning cached machine slowdown");
				return mcSlowdown.get(num_gpus);
			}
			slowdown = super.getPlacementSlowdown(gpus);
			mcSlowdown.put(num_gpus, slowdown);
			return slowdown;
		}

		if (slotSlowdown.containsKey(num_gpus)) {
			//System.out.println("Returning cached slot slowdown");
			return slotSlowdown.get(num_gpus);
		}

		slowdown = super.getPlacementSlowdown(gpus);
		slotSlowdown.put(num_gpus, slowdown);
		return slowdown;
	}

	public void setSwap(boolean mSwap) {
		this.mSwap = mSwap;
	}

	public void setNextIterationGPUs(Set<GPU> nextIterationGPUs) {
		this.mNextIterationGPUs = nextIterationGPUs;
	}
}