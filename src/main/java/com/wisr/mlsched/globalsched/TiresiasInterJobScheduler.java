package com.wisr.mlsched.globalsched;

import java.util.*;

import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.collections4.keyvalue.MultiKey;

import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.resources.GPU;

//import java.util.HashMap;

public class TiresiasInterJobScheduler extends InterJobScheduler {

	protected boolean mConsolidate;

	public TiresiasInterJobScheduler(ClusterConfiguration config){
		super(config);
		mConsolidate = config.getmConsolidate();

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

	@Override
	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();
		int gpuDemand = job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size();

		if (gpuDemand > gpuList.size()) {
			return allocatedGpus;
		}

		System.out.println("====== Tiresias Consolidated gpu allocation ======");
		System.out.println("JobId: " + String.valueOf(job.getJobId()) + " GPU list: " + String.valueOf(gpuList.size())
				+ " GPU demand: " + String.valueOf(gpuDemand));

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

		if (gpuDemand > gpusPerMachine || !isConsolidate(job)) {
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
				return allocatedGpus;
			}
		}

		double gpusPerRack = gpusPerMachine * Cluster.getInstance().getConfiguration().getMachinesPerRack();

		if (gpuDemand > gpusPerRack || !isConsolidate(job)) {
			System.out.println(("GPUs per rack = " + String.valueOf(gpusPerRack)));

			allocateGPU(allocatedGpus, gpuList, gpuDemand, -1, -1, -1,
					-1, -1);
		}

		return allocatedGpus;
	}

	boolean isConsolidate(IntraJobScheduler job) {
		String model = job.getModelName();
		//if (model.equals("ResNet50") || model.equals("ResNet18") || model.equals("MobileNetV3")) {
		if (model.equals("ResNet50") || model.equals("ResNet18")) {
			return false;
		}
		else {
			return true;
		}
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
}
