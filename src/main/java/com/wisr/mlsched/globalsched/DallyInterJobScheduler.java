package com.wisr.mlsched.globalsched;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ClusterConfiguration;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.DallyIntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.Queue;
//import java.util.HashMap;

public class DallyInterJobScheduler extends InterJobScheduler {

	protected boolean mConsolidate;

	private Map<Integer, Queue> mcDemandDelayMap;
	private Map<Integer, Queue> rackDemandDelayMap;
	private static final int MAX_Q = 10;

	public DallyInterJobScheduler(ClusterConfiguration config){
		super(config);
		mConsolidate = config.getmConsolidate();
		//mcDemandDelayMap = new HashMap<>();
		//rackDemandDelayMap = new HashMap<>();
		mcDemandDelayMap = new HashMap<>();
		rackDemandDelayMap = new HashMap<>();
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
	public double getMcDemandDelay(int demand) {
		LinkedList<Double> list = (LinkedList<Double>) mcDemandDelayMap.get(demand);
		double sum = 0;
		for (int i = 0; i < list.size(); i++) {
			sum += list.get(i);
		}

		if (sum == 0) {
			return 1;
		}

		return sum/list.size();
	}

	public double getRackDemandDelay(int demand) {
		LinkedList<Double> list = (LinkedList<Double>) rackDemandDelayMap.get(demand);
		double sum = 0;
		for (int i = 0; i < list.size(); i++) {
			sum += list.get(i);
		}

		if (sum == 0) {
			return 1;
		}

		return sum/list.size();
	}


	@Override
	protected List<GPU> consolidatedGPUAllocation(List<GPU> gpuList, IntraJobScheduler job) {

		List<GPU> allocatedGpus = new ArrayList<GPU>();
		job = (DallyIntraJobScheduler) job;
		int gpuDemand = job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size();

		if (gpuDemand > gpuList.size()){
			return allocatedGpus;
		}

		System.out.println("====== Dally Consolidated gpu allocation ======");
		System.out.println("JobId: " + String.valueOf(job.getJobId()) + " GPU list: " + String.valueOf(gpuList.size())
				+ " GPU demand: " + String.valueOf(gpuDemand));

		if (!mcDemandDelayMap.containsKey(gpuDemand)) {
			mcDemandDelayMap.put(gpuDemand, new LinkedList<Double>());
		}
		if (!rackDemandDelayMap.containsKey(gpuDemand)) {
			rackDemandDelayMap.put(gpuDemand, new LinkedList<Double>());
		}

		double starvation_time = Simulation.getSimulationTime() - job.getLastResourceAssignment();
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
			if (gpus >= gpuDemand) {
				//minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
				allocatedDim1 = (Integer) key5;
				allocatedDim2 = (Integer) key6;
				break;
			}
		}

		if (gpus >= gpuDemand) {
			LinkedList<Double> list = (LinkedList<Double>) mcDemandDelayMap.get(gpuDemand);
			list.add(starvation_time/Cluster.getInstance().getLeaseTime());
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
			if (gpus >= gpuDemand) {
				//minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
				allocatedDim1 = (Integer) key5;
				break;
			}
		}

		if (gpus >= gpuDemand){
			LinkedList<Double> list = (LinkedList<Double>) mcDemandDelayMap.get(gpuDemand);
			list.add(starvation_time/Cluster.getInstance().getLeaseTime());
			//mcDemandDelayMap.put(gpuDemand, starvation_time/Cluster.getInstance().getLeaseTime());
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
			if (gpus >= gpuDemand) {
				//minGPUAllocation = gpus;
				allocatedRack = (Integer) key2;
				allocatedMachine = (Integer) key3;
				allocatedSlot = (Integer) key4;
				break;
			}
		}

		if (gpus >= gpuDemand){
			LinkedList<Double> list = (LinkedList<Double>) mcDemandDelayMap.get(gpuDemand);
			list.add(starvation_time/Cluster.getInstance().getLeaseTime());
			//mcDemandDelayMap.put(gpuDemand, starvation_time/Cluster.getInstance().getLeaseTime());
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
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

		if (gpus >= gpuDemand){
			LinkedList<Double> list = (LinkedList<Double>) mcDemandDelayMap.get(gpuDemand);
			list.add(starvation_time/Cluster.getInstance().getLeaseTime());
			//mcDemandDelayMap.put(gpuDemand, starvation_time/Cluster.getInstance().getLeaseTime());
			return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
					allocatedDim1, allocatedDim2);
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

		double rack_delay_wait = ((DallyIntraJobScheduler) job).getRackDelayWait();

		if (starvation_time >= (Cluster.getInstance().getLeaseTime() * rack_delay_wait) ||
				gpuDemand > gpusPerMachine) {
			for (Map.Entry<Integer, Integer> entry : rackMap.entrySet()) {
				Integer rack = entry.getKey();
				gpus = entry.getValue();
				if (gpus >= gpuDemand) {
					//minGPUAllocation = gpus;
					allocatedRack = rack;
					break;
				}
			}

			if (gpus >= gpuDemand) {
				LinkedList<Double> list = (LinkedList<Double>) rackDemandDelayMap.get(gpuDemand);
				list.add(starvation_time/Cluster.getInstance().getLeaseTime());
				//rackDemandDelayMap.put(gpuDemand, starvation_time/Cluster.getInstance().getLeaseTime());
				return allocateGPU(allocatedGpus, gpuList, gpuDemand, allocatedRack, allocatedMachine, allocatedSlot,
						allocatedDim1, allocatedDim2);
			}
		}

		double gpusPerRack = gpusPerMachine * Cluster.getInstance().getConfiguration().getMachinesPerRack();
		double nw_delay_wait = ((DallyIntraJobScheduler) job).getNwDelayWait();

		if (Simulation.getSimulationTime() - job.getLastResourceAssignment() >=
				(Cluster.getInstance().getLeaseTime() * nw_delay_wait) ||
				gpuDemand > gpusPerRack) {
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
}