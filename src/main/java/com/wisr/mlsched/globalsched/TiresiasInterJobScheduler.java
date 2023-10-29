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
			if (gpus >= gpuDemand) {
				//minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				break;
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

		if (minGPUAllocation > gpusPerMachine || !shouldConsol(job)) {
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
				return allocatedGpus;
			}
		}

		double gpusPerRack = gpusPerMachine * Cluster.getInstance().getConfiguration().getMachinesPerRack();

		System.out.println(("GPUs per rack = " + String.valueOf(gpusPerRack)));
		if (gpuDemand > gpusPerRack || !shouldConsol(job)) {
			System.out.println(("GPUs per rack = " + String.valueOf(gpusPerRack)));

			allocateGPU(allocatedGpus, gpuList, gpuDemand, -1, -1, -1,
					-1, -1);
		}

		return allocatedGpus;
	}

	boolean shouldConsol(IntraJobScheduler job) {
		String model = job.getModelName();
		//if (model.equals("ResNet50") || model.equals("ResNet18") || model.equals("MobileNetV3")) {
		if (model.equals("ResNet50") || model.equals("ResNet18")) {
			return false;
		}
		else {
			return true;
		}
	}
}
