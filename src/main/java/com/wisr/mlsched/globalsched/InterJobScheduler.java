package com.wisr.mlsched.globalsched;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.events.StartIterationEvent;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;

/**
 * Interface for InterJobScheduler types
 */
public abstract class InterJobScheduler {
	
	protected Random mRand;
	protected ClusterConfiguration mConfig;
	public InterJobScheduler() {
		mRand = new Random(0); // seed to make it deterministic
	}
	public InterJobScheduler(ClusterConfiguration config) {
		mRand = new Random(0); // seed to make it deterministic
		mConfig = config;
	}
	public void onResourceAvailable(List<GPU> gpu_set) {


	}
	
	protected void startWaitingJobs() {
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();
		for(IntraJobScheduler job : jobs) {
			if(job.isWaitingForResources() && job.hasResourcesForNextIteration()) {
				ClusterEventQueue.getInstance().enqueueEvent(
						new StartIterationEvent(Simulation.getSimulationTime(), job));
				job.notifyResourceAvailable();
			} else if(job.isWaitingForResources()) {
				//job.resetOldRatio(); // do not reset, instead we do prioritization based on time_waiting*old_Ts/Ti
			}
		}
	}
	
	protected void perGPUResourceAllocator(List<GPU> gpu_set) {
		if(Cluster.getInstance().getRunningJobs().size() == 0) {
			// Means no jobs are running, hence nothing to do
			return;
		}
		for(GPU gpu : gpu_set) {
			// Put this GPU up for auction by getting bids from jobs
			if(gpu.isLeased()) {
				// Already leased.
				continue;
			}
			List<Bid> bids = new ArrayList<Bid>();
			List<GPU> gpuList = new ArrayList<GPU>();
			gpuList.add(gpu);
			List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();
			for(IntraJobScheduler job : jobs) {
				List<Bid> bidsFromJob = job.prepareBid(gpuList);
				if(bidsFromJob != null && !bidsFromJob.isEmpty()) {
					bids.addAll(bidsFromJob);
				}
			}
			if(bids.size() == 0) {
				// No bids. All jobs must be running at full capacity
				continue;
			}
			Collections.sort(bids, new PerGPUBidComparator());
			Bid winningBid = bids.get(0); // Select the winner
			winningBid.getJob().notifyResourceAssignment(gpuList);
			gpu.assignGPU(Cluster.getInstance().getLeaseTime(), winningBid.getJob());
		}
		startWaitingJobs();
	}

	protected void multiGPUResourceAllocator(List<GPU> gpu_set) {
		if(Cluster.getInstance().getRunningJobs().size() == 0) {
			// Means no jobs are running, hence nothing to do
			return;
		}
		List<GPU> gpuList = new ArrayList<GPU>();

		for(GPU gpu : gpu_set) {
			// Put this GPU up for auction by getting bids from jobs
			if (gpu.isLeased()) {
				// Already leased.
				continue;
			}
			gpuList.add(gpu);
		}

		List<Bid> bids = new ArrayList<Bid>();
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();

		/*
		for(IntraJobScheduler job : jobs) {

			List<GPU> gpuAllocation = consolidatedGPUAllocation(gpuList, job);

			if (gpuAllocation.isEmpty()){
				continue;
			}

			System.out.println("Allocated GPUs to job");
			for (GPU gpu: gpuAllocation){
				System.out.println(gpu.getLocation().getGPUId() + " "  + gpu.getLocation().getDim2Id()
						 + " " + gpu.getLocation().getDim1Id() + " " + gpu.getLocation().getSlotId() + " " +
						  gpu.getLocation().getMachineId() + " " + gpu.getLocation().getRackId() + "\n");
			}

			List<Bid> bidsFromJob = job.prepareMultiBid(gpuAllocation);
			if(bidsFromJob != null && !bidsFromJob.isEmpty()) {
				bids.addAll(bidsFromJob);
			}
		}
		if(bids.size() == 0) {
			// No bids. All jobs must be running at full capacity
			return;
		}
		Collections.sort(bids, new PerGPUBidComparator());

		Bid bid = bids.get(0); // Select the winner
		bid.getJob().notifyResourceAssignment(bid.getGPUList());

		for (GPU gpu : bid.getGPUList())
			gpu.assignGPU(Cluster.getInstance().getLeaseTime(), bid.getJob());

		int gpusLeft = gpu_set.size() - bid.getGPUList().size();

		for (int i = 1; i < bids.size(); i++) {

			if (gpusLeft <= 0)
				break;

			boolean flag = false;
			bid = bids.get(i); // Select the winner

			for (GPU gpu : bid.getGPUList()) {
				if (gpu.isLeased()){
					flag = true;
					break;
				}
			}

		 */

		for(IntraJobScheduler job : jobs) {

			if (job.getGPUsAvailableForNextIteration().size() >= job.getMaxParallelism()) {
				// Already have enough GPUs. No need to bid
				continue;
			}

			List<Bid> bidsFromJob = job.prepareMultiBid(null);
			if (bidsFromJob != null && !bidsFromJob.isEmpty()) {
				bids.addAll(bidsFromJob);
			}
		}

		if(bids.size() == 0) {
			// No bids. All jobs must be running at full capacity
			return;
		}

		Collections.sort(bids, new PerGPUBidComparator());

		for (int i = 0; i < bids.size(); i++) {

			if (gpuList.isEmpty())
				break;

			Bid bid = bids.get(i); // Select the winner
			IntraJobScheduler job = bid.getJob();

			List<GPU> gpuAllocation = consolidatedGPUAllocation(gpuList, job);

			if (gpuAllocation.isEmpty()){
				continue;
			}

			System.out.println("Allocated GPUs to job");
			for (GPU gpu: gpuAllocation){
				System.out.println(gpu.getLocation().getGPUId() + " "  + gpu.getLocation().getDim2Id()
						+ " " + gpu.getLocation().getDim1Id() + " " + gpu.getLocation().getSlotId() + " " +
						gpu.getLocation().getMachineId() + " " + gpu.getLocation().getRackId() + "\n");
			}

			bid.getJob().notifyResourceAssignment(gpuAllocation);

			for (GPU gpu : gpuAllocation) {
				gpu.assignGPU(Cluster.getInstance().getLeaseTime(), bid.getJob());
				if (!gpuList.remove(gpu)) {
					System.out.println("Unable to remove GPU from gpu list!");
					System.exit(-1);
				}
			}
		}
		startWaitingJobs();
	}

	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job){
		return null;
	}

	//protected abstract List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, int gpuDemand);

	protected class PerGPUBidComparator implements Comparator<Bid> {

		@Override
		public int compare(Bid bid1, Bid bid2) {
			int comp = Double.compare(bid1.getExpectedBenefit(), bid2.getExpectedBenefit());
			if(comp != 0) {
				return -1*comp;
			}
			if(bid1.getJob().getGPUsAvailableForNextIteration().size() !=
					bid2.getJob().getGPUsAvailableForNextIteration().size()) {
				return bid1.getJob().getGPUsAvailableForNextIteration().size() -
						bid2.getJob().getGPUsAvailableForNextIteration().size();
			}
			comp = Double.compare(bid1.getJob().getLastResourceAssignment(), bid2.getJob().getLastResourceAssignment());
			if(comp != 0) {
				return comp;
			}
			return bid1.getJob().getJobId() - bid2.getJob().getJobId();
		}
		
	}
}