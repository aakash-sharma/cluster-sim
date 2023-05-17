package com.wisr.mlsched.globalsched;

import java.util.*;
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
	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, int gpuDemand) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();

		if (gpuDemand <=0){
			return allocatedGpus;
		}

		Integer allocatedRack = -1;
		Integer allocatedMachine = -1;
		Integer gpusRack = gpuList.size() + 1;
		Integer minGPUAllocation = gpuList.size() + 1;

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();
		MultiKeyMap slotMap = new MultiKeyMap();


		for (GPU gpu: gpuList) {
			Integer rack = gpu.getLocation().getRackId();
			Integer machine = gpu.getLocation().getMachineId();
			Integer slot = gpu.getLocation().getSlotId();

			Integer count = rackMap.get(rack);
			rackMap.merge(rack, 1, Integer::sum);

			if (!machineMap.containsKey(rack, machine)) {
				machineMap.put(rack, machine, 1);
			}
			else {
				machineMap.put(rack, machine, count+1);
			}
			if (!slotMap.containsKey(rack, machine, slot)) {
				slotMap.put(rack, machine, slot, 1);
			}
			else {
				slotMap.put(rack, machine, slot, count + 1);
			}
		}

		for (Object o : machineMap.keySet()) {
			MultiKey key = (MultiKey) o;
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			Integer gpus = (Integer) machineMap.get(key);

			// Find the smallest consolidated slots available
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
			}
		}

		if (allocatedRack != -1 && allocatedMachine != -1){
			for (GPU gpu: gpuList) {
				Integer rack = gpu.getLocation().getRackId();
				Integer machine = gpu.getLocation().getMachineId();

				if (rack == allocatedRack && machine == allocatedMachine) {
					allocatedGpus.add(gpu);
					gpuDemand -= 1;
				}

				if (gpuDemand == 0) {
					break;
				}
			}

			return allocatedGpus;
		}

		allocatedRack = -1;

		for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
			Integer rack = entry.getKey();
			Integer gpus = entry.getValue();
			if (gpus >= gpuDemand && gpus < minGPUAllocation) {
				minGPUAllocation = gpus;
				allocatedRack = rack;
			}
		}

		if (allocatedRack != -1) {
			for (GPU gpu : gpuList) {
				Integer rack = gpu.getLocation().getRackId();

				if (rack == allocatedRack) {
					allocatedGpus.add(gpu);
					gpuDemand -= 1;
				}

				if (gpuDemand == 0) {
					break;
				}
			}
		}

		return allocatedGpus;
	}
	
}