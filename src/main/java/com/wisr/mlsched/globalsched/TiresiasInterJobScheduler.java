package com.wisr.mlsched.globalsched;

import java.util.*;

import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.TiresiasIntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.collections4.keyvalue.MultiKey;

import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.resources.GPU;
import org.apache.commons.lang3.tuple.MutablePair;

//import java.util.HashMap;

public class TiresiasInterJobScheduler extends InterJobScheduler {

	protected boolean mConsolidate;
	private Vector<TiresiasIntraJobScheduler> mFgPriorityQ;
	private Vector<TiresiasIntraJobScheduler> mBgPriorityQ;
	private double mMinPriority;
	private double mMaxPriority;
	private int mGPUusage;

	public TiresiasInterJobScheduler(ClusterConfiguration config){
		super(config);
		mFgPriorityQ = new Vector<TiresiasIntraJobScheduler>();
		mBgPriorityQ = new Vector<TiresiasIntraJobScheduler>();
		mConsolidate = config.getmConsolidate();
		mMinPriority = Double.MAX_VALUE;
		mMaxPriority = 0;

	}
	public TiresiasInterJobScheduler(){
		mConsolidate = false;
	}
	@Override
	public void onResourceAvailable(List<GPU> gpu_set) {
		super.onResourceAvailable(gpu_set);
		if (mConsolidate){
			multiGPUResourceAllocator(gpu_set);
		}
		else {
			perGPUResourceAllocator(gpu_set);
		}
	}

	public Vector<TiresiasIntraJobScheduler> getFgPriorityQ(){
		return mFgPriorityQ;
	}

	public Vector<TiresiasIntraJobScheduler> getBgPriorityQ(){
		return mBgPriorityQ;
	}
	@Override
	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();
		int gpuDemand = job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size();

		if (gpuDemand <= 0) {
			return allocatedGpus;
		}

		System.out.println("====== Tiresias Consolidated gpu allocation ======");
		System.out.println(gpuList.size());
		System.out.println(gpuDemand);

		Integer allocatedRack = -1;
		Integer allocatedMachine = -1;
		Integer minGPUAllocation = gpuList.size() + 1;
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

		for (Object o : machineMap.keySet()) {
			MultiKey key = (MultiKey) o;
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			gpus = (Integer) machineMap.get(key);

			// Find the smallest consolidated slots available
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
			}
		}

		if (gpus >= gpuDemand) {
			allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, -1,
					-1, -1);
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

		if (gpuDemand > gpusPerMachine) {
			System.out.println(("GPUs per machine = " + String.valueOf(gpusPerMachine)));

			for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
				rack = entry.getKey();
				gpus = entry.getValue();
				if (gpus >= gpuDemand && gpus < minGPUAllocation) {
					minGPUAllocation = gpus;
					allocatedRack = rack;
				}
			}

			if (gpus >= gpuDemand) {
				allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, -1, -1,
						-1, -1);
			}
			return allocatedGpus;
		}

		double gpusPerRack = gpusPerMachine * Cluster.getInstance().getConfiguration().getMachinesPerRack();

		if (gpuDemand > gpusPerRack) {
			System.out.println(("GPUs per rack = " + String.valueOf(gpusPerRack)));

			allocateGPU(allocatedGpus, gpuList, gpuDemand, -1, -1, -1,
					-1, -1);
		}

		return allocatedGpus;
	}

	private void allocateGPU(List<GPU> allocatedGpus, List<GPU> gpuList, int gpuDemand, int allocRack, int allocMac, int allocSlot,
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

	public void removeJob(TiresiasIntraJobScheduler job) {
		mGPUusage -= job.getCurrentParallelism();
		System.out.println("Removing job: " + String.valueOf(job.getJobId()));
		if (!mFgPriorityQ.remove(job)) {
			if (!mBgPriorityQ.remove(job)){
				System.out.println("Job not found in any Q!!!");
				System.exit(-1);
			}
		}

		if (job.getGPUServiceForJob() == mMinPriority || job.getGPUServiceForJob() == mMaxPriority) {
			mMinPriority = Double.MAX_VALUE;
			mMaxPriority = 0;

			for (TiresiasIntraJobScheduler job2 : mFgPriorityQ) {
				mMinPriority = Math.min(mMinPriority, job2.getGPUServiceForJob());
				mMaxPriority = Math.max(mMaxPriority, job2.getGPUServiceForJob());
			}
			for (TiresiasIntraJobScheduler job2 : mBgPriorityQ) {
				mMinPriority = Math.min(mMinPriority, job2.getGPUServiceForJob());
				mMaxPriority = Math.max(mMaxPriority, job2.getGPUServiceForJob());
			}

		}
	}

	public boolean checkJobPreempted(TiresiasIntraJobScheduler job) {

		mMinPriority = Math.min(mMinPriority, job.getGPUServiceForJob());
		mMaxPriority = Math.max(mMaxPriority, job.getGPUServiceForJob());
		double priority_gradient = (mMaxPriority - mMinPriority) / 2;

		if (job.getGPUServiceForJob() <= mMinPriority + priority_gradient) {
			if (job.getJobQ() == 1) {
				if(!mBgPriorityQ.remove(job)) {
					System.out.println("Job not found in BgQ!!! : " + String.valueOf(job.getJobId()));
					System.exit(-1);
				}
				mFgPriorityQ.add(job);
				job.setJobQ(0);
				Collections.sort(mFgPriorityQ, new IntraJobSchedComparator());
			}
			return false;
		}

		if (job.getJobQ() == 0) {
			if(!mFgPriorityQ.remove(job)) {
				System.out.println("Job not found in FgQ!!! : " + String.valueOf(job.getJobId()));
				System.exit(-1);
			}
			mBgPriorityQ.add(job);
			job.setJobQ(1);
			Collections.sort(mBgPriorityQ, new IntraJobSchedComparator());
		}


		if (Cluster.getInstance().getGPUsInCluster().size() > mGPUusage) {
			return false;
		}

		mGPUusage -= job.getCurrentParallelism();
		return true;
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

		/*System.out.println("Printing FgQ");
		for(TiresiasIntraJobScheduler job : mFgPriorityQ) {
			System.out.println(job.getJobId());
		}
		System.out.println("Printing BgQ");
		for(TiresiasIntraJobScheduler job : mBgPriorityQ) {
			System.out.println(job.getJobId());
		}*/

		for(TiresiasIntraJobScheduler job : mFgPriorityQ) {

			if (gpuList.isEmpty())
				break;

			if (!job.isWaitingForResources() || job.getMaxParallelism() > gpuList.size()) {
				continue;
			}

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

			job.notifyResourceAssignment(gpuAllocation);

			for (GPU gpu : gpuAllocation) {
				gpu.assignGPU(Cluster.getInstance().getLeaseTime(), job);
				mGPUusage += 1;
				if (!gpuList.remove(gpu)) {
					System.out.println("Unable to remove GPU from gpu list!");
					System.exit(-1);
				}
			}
		}

		for (TiresiasIntraJobScheduler job : mBgPriorityQ) {

			if (gpuList.isEmpty())
				break;

			if (!job.isWaitingForResources() || job.getMaxParallelism() > gpuList.size()) {
				continue;
			}

			List<GPU> gpuAllocation = consolidatedGPUAllocation(gpuList, job);

			if (gpuAllocation.isEmpty()) {
				continue;
			}

			System.out.println("Allocated GPUs to job");
			for (GPU gpu : gpuAllocation) {
				System.out.println(gpu.getLocation().getGPUId() + " " + gpu.getLocation().getDim2Id()
						+ " " + gpu.getLocation().getDim1Id() + " " + gpu.getLocation().getSlotId() + " " +
						gpu.getLocation().getMachineId() + " " + gpu.getLocation().getRackId() + "\n");
			}

			job.notifyResourceAssignment(gpuAllocation);

			for (GPU gpu : gpuAllocation) {
				gpu.assignGPU(Cluster.getInstance().getLeaseTime(), job);
				mGPUusage += 1;
				if (!gpuList.remove(gpu)) {
					System.out.println("Unable to remove GPU from gpu list!");
					System.exit(-1);
				}
			}
		}

		startWaitingJobs();
	}
}
