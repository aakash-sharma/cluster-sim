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
		Integer gpusMachine = gpuList.size() + 1;

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();
		MultiKeyMap slotMap = new MultiKeyMap();


		for (GPU gpu: gpuList) {
			Integer rack = gpu.getLocation().getRackId();
			Integer machine = gpu.getLocation().getMachineId();
			Integer slot = gpu.getLocation().getSlotId();
			//Integer gpu_ = gpu.getLocation().getGPUId();

			Integer count = rackMap.get(rack);
			if (count == null) {
				rackMap.put(rack, 1);
			}
			else {
				rackMap.put(rack, count+1);
			}

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

		for (Iterator it = machineMap.keySet().iterator(); it.hasNext();) {
			MultiKey key = (MultiKey) it.next();
			Object key2 = key.getKey(0);
			Object key3 = key.getKey(1);
			Integer gpus = (Integer)machineMap.get(key);

			if (gpus >= gpuDemand && gpus < gpusMachine) {
				gpusMachine = gpus;
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

		for (Iterator it = rackMap.keySet().iterator(); it.hasNext();) {
			Map.Entry<Integer, Integer> entry = (Map.Entry) it.next();
			Object key = entry.getKey();
			Integer gpus = (Integer)rackMap.get(key);

			if (gpus >= gpuDemand && gpus < gpusMachine)
			{
				gpusMachine = gpus;
				allocatedRack = (Integer) key;
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

			return allocatedGpus;
		}

		/*for (Map.Entry<MultiKey<? extends Integer>, ? extends Integer> entry : machineMap.entrySet()) {
			MultiKey<? extends Integer> keys = entry.getKey();
			Integer gpus = entry.getValue();
			System.out.println(keys.getKey(0) + ", " + keys.getKey(1) + " => " + gpus);

			if (gpus >= gpuDemand )
			{
				gpusMachine = Math.min(gpusMachine, gpus);
			}

		}*/

		/*

		machineMap.forEach((rack, machine) -> {
			Integer gpus = (Integer)machineMap.get(rack, machine);
			if (gpus >= gpuDemand )
			{
				gpusMachine = Math.min(gpusMachine, gpus);
			}
		});

		int gpuPerMachine = mConfig.getSlotsPerMachine() * mConfig.getGPUsPerSlot();
		 */


		return allocatedGpus;
	}
	
}