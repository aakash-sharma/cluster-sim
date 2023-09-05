package com.wisr.mlsched.globalsched;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.DallyIntraJobScheduler;
import com.wisr.mlsched.localsched.DallyIntraJobScheduler;
import com.wisr.mlsched.localsched.DallyIntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.*;
//import java.util.HashMap;

public class DallyInterJobScheduler extends InterJobScheduler {

	protected boolean mConsolidate;
	private Vector<DallyIntraJobScheduler> mFgPriorityQ;
	private Vector<DallyIntraJobScheduler> mBgPriorityQ;
	private double mMinPriority;
	private double mMaxPriority;
	private int mGPUusage;

	public DallyInterJobScheduler(ClusterConfiguration config){
		super(config);
		mConsolidate = config.getmConsolidate();
		mFgPriorityQ = new Vector<DallyIntraJobScheduler>();
		mBgPriorityQ = new Vector<DallyIntraJobScheduler>();
		mMinPriority = Double.MAX_VALUE;
		mMaxPriority = 0;
	}

	public DallyInterJobScheduler(){
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

	public Vector<DallyIntraJobScheduler> getFgPriorityQ(){
		return mFgPriorityQ;
	}

	public Vector<DallyIntraJobScheduler> getBgPriorityQ() {
		return mBgPriorityQ;
	}

	@Override
	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();
		job = (DallyIntraJobScheduler) job;
		int gpuDemand = job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size();

		if (gpuDemand <= 0 || gpuList.size() < gpuDemand){
			return allocatedGpus;
		}

		System.out.println("====== Dally Consolidated gpu allocation ======");
		System.out.println("JobId: " + String.valueOf(job.getJobId()) + " GPU list: " + String.valueOf(gpuList.size())
				+ " GPU demand: " + String.valueOf(gpuDemand));

		Integer allocatedRack = -1;
		Integer allocatedMachine = -1;
		Integer allocatedSlot = -1;
		Integer allocatedDim1 = -1;
		Integer allocatedDim2 = -1;
		Integer minGPUAllocation = gpuList.size() + 1;
		Integer gpus = 0;

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();
		MultiKeyMap slotMap = new MultiKeyMap();
		MultiKeyMap dim1Map = new MultiKeyMap();
		MultiKeyMap dim2Map = new MultiKeyMap();

		for (GPU gpu: gpuList) {
			Integer rack = gpu.getLocation().getRackId();
			Integer machine = gpu.getLocation().getMachineId();
			Integer slot = gpu.getLocation().getSlotId();
			Integer dim1 = gpu.getLocation().getDim1Id();
			Integer dim2 = gpu.getLocation().getDim2Id();

			Integer count = rackMap.get(rack);
			rackMap.merge(rack, 1, Integer::sum);

			if (!machineMap.containsKey(rack, machine)) {
				machineMap.put(rack, machine, 1);
			}
			else {
				count = (Integer) machineMap.get(rack, machine);
				machineMap.put(rack, machine, count+1);
			}

			if (slot != -1) {
				if (!slotMap.containsKey(rack, machine, slot)) {
					slotMap.put(rack, machine, slot, 1);
				} else {
					count = (Integer) slotMap.get(rack, machine, slot);
					slotMap.put(rack, machine, slot, count + 1);
				}
			}

			if (dim1 != -1) {
				if (!dim1Map.containsKey(rack, machine, slot, dim1)) {
					dim1Map.put(rack, machine, slot, dim1, 1);
				}
				else {
					count = (Integer) dim1Map.get(rack, machine, slot, dim1);
					dim1Map.put(rack, machine, slot, dim1, count + 1);
				}
			}

			if (dim2 != -1) {
				if (!dim2Map.containsKey(rack, machine, slot, dim1, dim2)) {
					dim2Map.put(rack, machine, slot, dim1, dim2, 1);
				} else {
					count = (Integer) dim2Map.get(rack, machine, slot, dim1, dim2);
					dim2Map.put(rack, machine, slot, dim1, dim2, count + 1);
				}
			}
		}

		for (Object o : dim2Map.keySet()) {
			MultiKey key = (MultiKey) o;
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			Object key4 = key.getKey(2);
			Object key5 = key.getKey(3);
			Object key6 = key.getKey(4);
			gpus = (Integer) dim2Map.get(key);

			// Find the smallest consolidated slots available
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
				allocatedDim1 = (Integer) key5;
				allocatedDim2 = (Integer) key6;
			}
		}

		if (gpus >= gpuDemand) {
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
		}

		for (Object o : dim1Map.keySet()) {
			MultiKey key = (MultiKey) o;
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			Object key4 = key.getKey(2);
			Object key5 = key.getKey(3);
			gpus = (Integer) dim1Map.get(key);

			// Find the smallest consolidated slots available
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
				allocatedDim1 = (Integer) key5;
			}
		}

		if (gpus >= gpuDemand){
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
		}

		for (Object o : slotMap.keySet()) {
			MultiKey key = (MultiKey) o;
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			Object key4 = key.getKey(2);
			gpus = (Integer) slotMap.get(key);

			// Find the smallest consolidated slots available
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
			}
		}

		if (gpus >= gpuDemand){
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
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

		if (gpus >= gpuDemand){
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
		}



		/*
		if (rack_delay_wait[1] == -1) {
			System.out.println(job.getJobId() + ": Rack delay timer -1, setting to "  +
					String.valueOf(job.getLastResourceAssignment()));
			rack_delay_wait[1] = job.getLastResourceAssignment();
		}
		else if (Simulation.getSimulationTime() - rack_delay_wait[1] >= rack_delay_wait[0]) {
			for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
				Integer rack = entry.getKey();
				gpus = entry.getValue();
				if (gpus >= gpuDemand && gpus < minGPUAllocation) {
					minGPUAllocation = gpus;
					allocatedRack = rack;
				}
			}

			if (gpus >= gpuDemand) {
				return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
						allocatedDim1, allocatedDim2);
			}
		}

		double [] nw_delay_wait = ((DallyIntraJobScheduler) job).getNwDelayWait();

		if (nw_delay_wait[1] == -1) {
			System.out.println(job.getJobId() + ": Network delay timer -1, setting to "  +
					String.valueOf(job.getLastResourceAssignment()));
			nw_delay_wait[1] = job.getLastResourceAssignment();
		}
		else if (Simulation.getSimulationTime() - nw_delay_wait[1] >= nw_delay_wait[0]) {
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
		}
		 */

		double rack_delay_wait = ((DallyIntraJobScheduler) job).getRackDelayWait();
		if (Simulation.getSimulationTime() - job.getLastResourceAssignment() >= rack_delay_wait) {
			for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
				Integer rack = entry.getKey();
				gpus = entry.getValue();
				if (gpus >= gpuDemand && gpus < minGPUAllocation) {
					minGPUAllocation = gpus;
					allocatedRack = rack;
				}
			}

			if (gpus >= gpuDemand) {
				return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
						allocatedDim1, allocatedDim2);
			}
		}

		double nw_delay_wait = ((DallyIntraJobScheduler) job).getNwDelayWait();
		if (Simulation.getSimulationTime() - job.getLastResourceAssignment() >= nw_delay_wait) {
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
		}

		return allocatedGpus;
	}

	private List<GPU> allocateGPU(List<GPU> allocatedGpus, List<GPU> gpuList, int gpuDemand, int allocRack, int allocMac, int allocSlot,
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
		return allocatedGpus;
	}

	public void removeJob(DallyIntraJobScheduler job) {
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

			for (DallyIntraJobScheduler job2 : mFgPriorityQ) {
				mMinPriority = Math.min(mMinPriority, job2.getGPUServiceForJob());
				mMaxPriority = Math.max(mMaxPriority, job2.getGPUServiceForJob());
			}
			for (DallyIntraJobScheduler job2 : mBgPriorityQ) {
				mMinPriority = Math.min(mMinPriority, job2.getGPUServiceForJob());
				mMaxPriority = Math.max(mMaxPriority, job2.getGPUServiceForJob());
			}

		}
	}

	public boolean checkJobPreempted(DallyIntraJobScheduler job) {

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
		for(DallyIntraJobScheduler job : mFgPriorityQ) {
			System.out.println(job.getJobId());
		}
		System.out.println("Printing BgQ");
		for(DallyIntraJobScheduler job : mBgPriorityQ) {
			System.out.println(job.getJobId());
		}*/

		for(DallyIntraJobScheduler job : mFgPriorityQ) {

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

		for (DallyIntraJobScheduler job : mBgPriorityQ) {

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