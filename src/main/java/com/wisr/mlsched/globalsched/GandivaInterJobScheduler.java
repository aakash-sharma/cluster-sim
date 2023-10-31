package com.wisr.mlsched.globalsched;

import java.util.*;

import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.localsched.GandivaIntraJobScheduler;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;
import java.util.*;

public class GandivaInterJobScheduler extends InterJobScheduler {

	protected boolean mConsolidate;
	private Map<Integer, Set<Integer>> machineAffinity;
	private Set<Integer> affinitizedNodes;

	public GandivaInterJobScheduler(ClusterConfiguration config) {
		super(config);
		mConsolidate = config.getmConsolidate();
		machineAffinity = new HashMap<>();
		affinitizedNodes = new HashSet<Integer>();
	}

	@Override
	public void onResourceAvailable(List<GPU> gpu_set) {
		super.onResourceAvailable(gpu_set);
		if (mConsolidate) {
			multiGPUResourceAllocator(gpu_set);
		} else {
			perGPUResourceAllocator(gpu_set);
		}
//		perGPUResourceAllocator(gpu_set);
	}

	protected void multiGPUResourceAllocator(List<GPU> gpu_set) {
		if (Cluster.getInstance().getRunningJobs().size() == 0) {
			// Means no jobs are running, hence nothing to do
			return;
		}
		List<GPU> gpuList = new ArrayList<GPU>();

		for (GPU gpu : gpu_set) {
			// Put this GPU up for auction by getting bids from jobs
			if (gpu.isLeased()) {
				// Already leased.
				continue;
			}
			gpuList.add(gpu);
		}

		List<Bid> bids = new ArrayList<Bid>();
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();

		for (IntraJobScheduler job : jobs) {

			List<Bid> bidsFromJob = job.prepareMultiBid(gpu_set);
			if (bidsFromJob != null && !bidsFromJob.isEmpty()) {
				bids.addAll(bidsFromJob);
			}
		}

		if (bids.size() == 0) {
			// No bids. All jobs must be running at full capacity
			return;
		}

		Collections.sort(bids, new PerGPUBidComparator());

		for (int i = 0; i < bids.size(); i++) {

			if (gpuList.isEmpty())
				break;

			Bid bid = bids.get(i); // Select the winner
			GandivaIntraJobScheduler job = (GandivaIntraJobScheduler) bid.getJob();

			if (!job.isWaitingForResources() && job.getCurrSlwstDim() <= 3) {
				continue;
			}

			/*if (!job.isWaitingForResources()) {
				continue;
			}*/

			List<GPU> gpuAllocation = consolidatedGPUAllocation(gpuList, job);

			if (gpuAllocation.isEmpty()) {
				continue;
			}

			System.out.println("Allocated + " + String.valueOf(gpuAllocation.size()) + " GPUs to JobId: " +
					String.valueOf(job.getJobId()));
			/*for (GPU gpu: gpuAllocation){
				System.out.println(gpu.getLocation().getGPUId() + " "  + gpu.getLocation().getDim2Id()
						+ " " + gpu.getLocation().getDim1Id() + " " + gpu.getLocation().getSlotId() + " " +
						gpu.getLocation().getMachineId() + " " + gpu.getLocation().getRackId() + "\n");
			}*/

			job.notifyResourceAssignment(gpuAllocation);

			if (!job.isQueued() && !job.isWaitingForResources()) {
				System.out.println("Setting swap flag for job: " + String.valueOf(job.getJobId()));
				System.out.println(String.valueOf(job.isQueued()));
				System.out.println(String.valueOf(job.isWaitingForResources()));
				job.setSwap(true);
			}

			for (GPU gpu : gpuAllocation) {
				gpu.assignGPU(Cluster.getInstance().getLeaseTime(), bid.getJob());
				if (!gpuList.remove(gpu)) {
					System.out.println("Unable to remove GPU from gpu list!");
					System.exit(-1);
				}
			}
		}
		startWaitingJobs();
		System.out.println("Total running jobs: " + String.valueOf(Cluster.getInstance().getRunningJobs().size()));
		System.out.println("Total active jobs: " + String.valueOf(Cluster.getInstance().getActiveJobs().size()));

		double used_gpus = 0, cluster_util = 0;
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (gpu.getJob() != null) {
				used_gpus += 1;
			}
		}

		cluster_util = used_gpus / Cluster.getInstance().getGPUsInCluster().size();

		System.out.println("Cluster util: " + String.valueOf(cluster_util));
	}

	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();
		int gpuDemand = job.getMaxParallelism();

		System.out.println("====== Gandiva Consolidated gpu allocation ======");
		System.out.println("JobId: " + String.valueOf(job.getJobId()) + " GPU list: " + String.valueOf(gpuList.size())
		+ " GPU demand: " + String.valueOf(gpuDemand) + " slowest dim: " + String.valueOf(job.getCurrSlwstDim()));



		if (gpuDemand > gpuList.size()) {
			return allocatedGpus;
		}

		double gpusPerMachine = 1;

		if (Cluster.getInstance().getConfiguration().getSlotsPerMachine() > 1) {
			gpusPerMachine *= Cluster.getInstance().getConfiguration().getSlotsPerMachine();
		}
		if (Cluster.getInstance().getConfiguration().getDim1PerSlot() > 1) {
			gpusPerMachine *= Cluster.getInstance().getConfiguration().getDim1PerSlot();
		}
		if (Cluster.getInstance().getConfiguration().getDim2sPerDim1() > 1) {
			gpusPerMachine *= Cluster.getInstance().getConfiguration().getDim2sPerDim1();
		}
		if (Cluster.getInstance().getConfiguration().getGPUsDim2() > 1) {
			gpusPerMachine *= Cluster.getInstance().getConfiguration().getGPUsDim2();
		}

		double gpusPerRack = gpusPerMachine * Cluster.getInstance().getConfiguration().getMachinesPerRack();

		Integer allocatedRack = -1;
		Integer allocatedMachine = -1;
		Integer minGPUAllocation = gpuList.size() + 1;
		Integer maxGPUAllocation = 0;
		Integer gpus = 0;
		Integer rack = -1;
		Integer machine = -1;

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();

		for (GPU gpu : gpuList) {
			rack = gpu.getLocation().getRackId();
			machine = gpu.getLocation().getMachineId();

			Integer count = rackMap.get(rack);
			rackMap.merge(rack, 1, Integer::sum);

			if (!machineMap.containsKey(rack, machine)) {
				machineMap.put(rack, machine, 1);
			} else {
				count = (Integer) machineMap.get(rack, machine);
				machineMap.put(rack, machine, count + 1);
			}
		}

		/*if (job.getJobId() == 433531) {
			System.out.println("hit1");
		}*/

		Collections.shuffle(gpuList);
		if (job.isQueued() || job.isWaitingForResources()) {

			if (gpuDemand > gpusPerRack) {
				System.out.println(("GPUs per rack = " + String.valueOf(gpusPerRack)));

				allocateGPU(allocatedGpus, gpuList, gpuDemand, -1, -1, -1,
						-1, -1);
				return allocatedGpus;
			}

			if (gpuDemand > gpusPerMachine) {
				System.out.println(("GPUs per machine = " + String.valueOf(gpusPerMachine)));

				allocateGPU(allocatedGpus, gpuList, gpuDemand, -1, -1, -1,
						-1, -1);
				return allocatedGpus;
			}

			/*

			if (gpuDemand > gpusPerMachine) {
				System.out.println(("GPUs per machine = " + String.valueOf(gpusPerMachine)));

				for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
					rack = entry.getKey();
					gpus = entry.getValue();
					if (gpus >= gpuDemand) {
						//minGPUAllocation = gpus;
						allocatedRack = rack;
						break;
					}
				}

				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, -1, -1,
							-1, -1);
				}
				return allocatedGpus;
			}*/

			Set<Integer> affinity_nodes = machineAffinity.get(gpuDemand);

			if (affinity_nodes != null) {
				for (Object o : machineMap.keySet()) {
					MultiKey key = (MultiKey) o;
					Object key2 = key.getKey(0);
					Object key3 = key.getKey(1);
					gpus = (Integer) machineMap.get(key);

					// Find the largest number of free GPUs
					if (gpus >= gpuDemand && gpus > maxGPUAllocation && affinity_nodes.contains((Integer) key3)) {
						allocatedRack = (Integer) key2;
						allocatedMachine = (Integer) key3;
						maxGPUAllocation = gpus;
					}
				}
				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, -1,
							-1, -1);
					return allocatedGpus;
				}
			}
			else {
				for (Object o : machineMap.keySet()) {
					MultiKey key = (MultiKey) o;
					Object key2 = key.getKey(0);
					Object key3 = key.getKey(1);
					gpus = (Integer) machineMap.get(key);

					// Find the largest number of free GPUs in unaffinitized nodes
					if (gpus >= gpuDemand && gpus > maxGPUAllocation && !affinitizedNodes.contains((Integer) key3)) {
						allocatedRack = (Integer) key2;
						allocatedMachine = (Integer) key3;
						maxGPUAllocation = gpus;
					}
				}
				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, -1,
							-1, -1);

					if (affinity_nodes == null) {
						machineAffinity.put(gpuDemand, new HashSet<Integer>());
					}
					affinity_nodes = machineAffinity.get(gpuDemand);
					affinity_nodes.add(allocatedMachine);
					affinitizedNodes.add(allocatedMachine);
					return allocatedGpus;
				}

				for (Object o : machineMap.keySet()) {
					MultiKey key = (MultiKey) o;
					Object key2 = key.getKey(0);
					Object key3 = key.getKey(1);
					gpus = (Integer) machineMap.get(key);

					// Find the largest number of free GPUs
					if (gpus >= gpuDemand && gpus > maxGPUAllocation) {
						allocatedRack = (Integer) key2;
						allocatedMachine = (Integer) key3;
						maxGPUAllocation = gpus;
					}
				}
				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, -1,
							-1, -1);
				}
				return allocatedGpus;
			}
		}

		else if (job.getCurrSlwstDim() > 3){
			System.out.println("Trying to swap allocation");

			if (job.getJobId() == 407009) {
				System.out.println("hit2");
			}

			if (gpuDemand > gpusPerRack) {
				return allocatedGpus;
			}

			if (gpuDemand > gpusPerMachine && job.getCurrSlwstDim() == 5) {
				System.out.println(("GPUs per machine = " + String.valueOf(gpusPerMachine)));

				for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
					rack = entry.getKey();
					gpus = entry.getValue();
					if (gpus >= gpuDemand) {
						//minGPUAllocation = gpus;
						allocatedRack = rack;
						break;
					}
				}

				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, -1, -1,
							-1, -1);
				}
				return allocatedGpus;
			}

			else if (gpuDemand <= gpusPerMachine && job.getCurrSlwstDim() > 3) {
				for (Object o : machineMap.keySet()) {
					MultiKey key = (MultiKey) o;
					Object key2 = key.getKey(0);
					Object key3 = key.getKey(1);
					gpus = (Integer) machineMap.get(key);

					// Find the largest number of free GPUs
					if (gpus >= gpuDemand && gpus > maxGPUAllocation) {
						allocatedRack = (Integer) key2;
						allocatedMachine = (Integer) key3;
						maxGPUAllocation = gpus;
					}
				}
				if (gpus >= gpuDemand) {
					allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, -1,
							-1, -1);
				}
			}
		}

		return allocatedGpus;
	}
}