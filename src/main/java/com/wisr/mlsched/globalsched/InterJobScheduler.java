package com.wisr.mlsched.globalsched;

import java.util.*;

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

		for(IntraJobScheduler job : jobs) {

			if (!job.getGPUsAvailableForNextIteration().isEmpty()) {
				// Already have enough GPUs. No need to bid
				continue;
			}

			List<Bid> bidsFromJob = job.prepareMultiBid(gpu_set);
			if (bidsFromJob != null && !bidsFromJob.isEmpty()) {
				bids.addAll(bidsFromJob);
			}
		}

		if(bids.size() == 0) {
			// No bids. All jobs must be running at full capacity
			return;
		}

		Collections.sort(bids, new PerGPUBidComparator());

		System.out.println("Total running jobs: " + String.valueOf(Cluster.getInstance().getRunningJobs().size()));
		System.out.println("Total active jobs: " + String.valueOf(Cluster.getInstance().getActiveJobs().size()));

		for (int i = 0; i < bids.size(); i++) {

			if (gpuList.isEmpty())
				break;

			Bid bid = bids.get(i); // Select the winner
			IntraJobScheduler job = bid.getJob();

			List<GPU> gpuAllocation = consolidatedGPUAllocation(gpuList, job);

			if (gpuAllocation.isEmpty()){
				continue;
			}

			System.out.println("Allocated + " + String.valueOf(gpuAllocation.size()) + " GPUs to JobId: " +
					String.valueOf(job.getJobId()));
			/*for (GPU gpu: gpuAllocation){
				System.out.println(gpu.getLocation().getGPUId() + " "  + gpu.getLocation().getDim2Id()
						+ " " + gpu.getLocation().getDim1Id() + " " + gpu.getLocation().getSlotId() + " " +
						gpu.getLocation().getMachineId() + " " + gpu.getLocation().getRackId() + "\n");
			}*/

			bid.getJob().notifyResourceAssignment(gpuAllocation);

			for (GPU gpu : gpuAllocation) {
				gpu.assignGPU(Cluster.getInstance().getLeaseTime(), bid.getJob());
				if (!gpuList.remove(gpu)) {
					System.out.println("Unable to remove GPU from gpu list!");
					System.exit(-1);
				}
			}
			System.out.println("Inside Total running jobs: " + String.valueOf(Cluster.getInstance().getRunningJobs().size()));
			System.out.println("Inside Total active jobs: " + String.valueOf(Cluster.getInstance().getActiveJobs().size()));
		}
		startWaitingJobs();
		System.out.println("Outside Total running jobs: " + String.valueOf(Cluster.getInstance().getRunningJobs().size()));
		System.out.println("Outside Total active jobs: " + String.valueOf(Cluster.getInstance().getActiveJobs().size()));
		print_util();
	}

	void print_util() {
		double used_gpus = 0, cluster_util = 0;
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (gpu.getJob() != null && gpu.isLeased()) {
				used_gpus += 1;
			}
		}
		cluster_util = used_gpus / Cluster.getInstance().getGPUsInCluster().size();

		System.out.println("Cluster util: " + String.valueOf(cluster_util));
	}

	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job){
		return null;
	}

	protected void allocateGPU(List<GPU> allocatedGpus, List<GPU> gpuList, int gpuDemand, int allocRack, int allocMac, int allocSlot,
							 int allocDim1, int allocDim2) {
		for (GPU gpu: gpuList) {
			Integer rack = gpu.getLocation().getRackId();
			Integer machine = gpu.getLocation().getMachineId();
			Integer slot = gpu.getLocation().getSlotId();
			Integer dim1 = gpu.getLocation().getDim1Id();
			Integer dim2 = gpu.getLocation().getDim2Id();

			if ((rack == allocRack || allocRack == -1) && (machine == allocMac || allocMac == -1) &&
					(slot == allocSlot || allocSlot == -1 )	&& (dim1 == allocDim1 || allocDim1 == -1)
					&& (dim2 == allocDim2 || allocDim2 == -1)) {
				allocatedGpus.add(gpu);
				gpuDemand -= 1;
			}

			if (gpuDemand == 0) {
				break;
			}
		}
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