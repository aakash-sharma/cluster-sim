package com.wisr.mlsched.localsched;

import java.util.*;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;


import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.config.ConfigUtils;
import com.wisr.mlsched.events.EndIterationEvent;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.events.StartIterationEvent;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.job.JobStatistics;
import com.wisr.mlsched.job.LossFunction;
import com.wisr.mlsched.job.LossFunctionFactory;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;

public abstract class IntraJobScheduler {
	// List of Job configurations
	private int mJobGroupId; // Unique identifier for job group to which this job belongs
	private int mJobId; // Unique identifier for this job
	protected double mJobStartTime; // Job start time
	protected long mTotalExpectedIterations; // Total number of iterations job is expected to run
	protected double mTimePerIteration; // Amount of time for a single iteration of job on 1 GPU
	protected int mIterGranularity; // Number of iterations to schedule at once
	protected int mMaxParallelism; // Represents max GPUs job can request
	private int mRandomSeed; // Random seed for loss curve
	private LossFunction mLossCurve; // Representation of loss curve
	private double mCrossSlotSlowdown; // Slowdown due to network b/w GPUs across slots
	private double mCrossMachineSlowdown; // Slowdown due to network b/w GPUs across machines
	private double mCrossRackSlowdown; // Slowdown due to network b/w GPUs across slots
	private String mUserName; // User of the job
	private String mModelName; // Model name of the job
	private String mAstraSimPath;
	private String mAstraSimBinPath;

	private final double CHECKPOINTING_OVERHEAD_PER_GPU = 0.0; // 6 seconds overhead
	
	// State management for job
	private boolean mIsLeader; // Whether this job is the leader in it's job group
	protected long mTotalIterationsRemaining; // Number of iterations of job remaining
	protected Set<GPU> mCurrentIterationGPUs; // GPUs for current iteration
	private Set<GPU> mNextIterationExpectedGPUs; // GPUs we expect from current iteration to be used for next iteration
	protected Set<GPU> mNextIterationGPUs; // GPUs allocated for next iteration
	private boolean mIsWaiting; // Represents if job is waiting for resources
	protected double oldRatio; // Old value of Ts/Ti
	protected double themisTs; // Themis Ts
	private double mTimeLastResourceAssignment; 
	private static Logger sLog; // Instance of logger
	private double mGpuTime;
	private double queueDelay; // State to maintain with admission control
	private double mJobArrivalTime; // Arrival time of the job
	private boolean mIsQueued; // Every job starts with getting queued
	//private Map<Set<GPU>, Double> mSlowdown;
	private Map<Vector<Integer>, Double> mSlowdown;

	public IntraJobScheduler(JSONObject config) {
		initFromConfig(config);
		mJobStartTime = Simulation.getSimulationTime();
		mJobArrivalTime = ConfigUtils.getJobStartTime(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		sLog.info("Starting job " + Integer.toString(mJobId));
		mCurrentIterationGPUs = new HashSet<GPU>();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		mNextIterationGPUs = new HashSet<GPU>();
		mIsWaiting = true;
		oldRatio = Double.POSITIVE_INFINITY;
		themisTs = Double.POSITIVE_INFINITY;
		mGpuTime = 0;
		mTimeLastResourceAssignment = Simulation.getSimulationTime();
		mIsLeader = true; // By default, everyone is a leader unless told otherwise
		mSlowdown = new HashMap<>();
		JobStatistics.getInstance().recordJobStart(mJobId, Simulation.getSimulationTime(), mMaxParallelism);
		List<GPU> availableResources = getResourcesAvailableInCluster();
		if (!availableResources.isEmpty()) {
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime(), availableResources));
		}
		queueDelay = mJobStartTime - mJobArrivalTime;
		mIsQueued = true;
	}
	
	/**
	 * Set the role of job and number of iterations it needs to run.
	 * @param is_leader
	 * @param iterations
	 */
	public void setRole(boolean is_leader, long iterations) {
		mIsLeader = is_leader;
		mTotalIterationsRemaining = iterations;
		mTotalExpectedIterations = iterations;
	}

	/**
	 * @return the mTotalIterationsRemaining
	 */
	public long getmTotalIterationsRemaining() {
		return mTotalIterationsRemaining;
	}

	/**
	 * @param mTotalIterationsRemaining the mTotalIterationsRemaining to set
	 */
	public void setmTotalIterationsRemaining(long mTotalIterationsRemaining) {
		if (mTotalIterationsRemaining < 0) {
			this.mTotalIterationsRemaining = 0;
		}
		else {
			this.mTotalIterationsRemaining = mTotalIterationsRemaining;
		}
	}
	
	public double getLastResourceAssignment() {
		return mTimeLastResourceAssignment;
	}

	public double getmJobStartTime() {
		return mJobStartTime;
	}
	
	public double getCurrentEstimateForThemis() {
		// Do update if we do not have resources
		if(mCurrentIterationGPUs.size() == 0) {
			//return (Simulation.getSimulationTime() - mJobStartTime) + mTotalIterationsRemaining*mTimePerIteration;
			//System.out.println("Job " + Integer.toString(mJobId) + " : " + Double.toString(oldRatio) + " " + 
			 //   Double.toString(mTimeLastResourceAssignment));
			return oldRatio*(Simulation.getSimulationTime() - mTimeLastResourceAssignment + 1);
		} else {
			//System.out.println("Current iteration GPUs = " + Integer.toString(mCurrentIterationGPUs.size()));
			//System.out.println(getCurrentEstimate());
			return getCurrentEstimate()/getIdealEstimate();
		}
	}

	/**
	 * @return the mTotalExpectedIterations
	 */
	public long getmTotalExpectedIterations() {
		return mTotalExpectedIterations;
	}

	/**
	 * @param mTotalExpectedIterations the mTotalExpectedIterations to set
	 */
	public void setmTotalExpectedIterations(long mTotalExpectedIterations) {
		this.mTotalExpectedIterations = mTotalExpectedIterations;
	}

	public void setmIterGranularity(int mIterGranularity) {
		this.mIterGranularity = mIterGranularity;
	}

	public void setmAstraSimPath(String astra_sim_path, String astra_sim_bin_path) {
		this.mAstraSimPath = astra_sim_path;
		this.mAstraSimBinPath = astra_sim_bin_path;
	}

	// Aakash: Call astra sim here
	public void startIteration() {
		//sLog.log(Level.INFO, "Starting iteration for job " + Integer.toString(mJobId));
		mCurrentIterationGPUs = new HashSet<GPU>(mNextIterationGPUs);
		mNextIterationGPUs = new HashSet<GPU>();
		mIsWaiting = false;
		assert(mCurrentIterationGPUs.size() > 0);
		/*System.out.println("Placement Job " + Integer.toString(mJobId) + ":" 
				+ " Time: " + Double.toString(Simulation.getSimulationTime())
				+ " Iteration: " + Integer.toString(mTotalExpectedIterations - mTotalIterationsRemaining)
				+ " NumGPUs: " + Integer.toString(mCurrentIterationGPUs.size())
				+ " Score: " + Double.toString(getPlacementSlowdown(mCurrentIterationGPUs))
				+ " Number_jobs_running: " + Integer.toString(Cluster.getInstance().getRunningJobs().size()));*/
		ClusterEventQueue.getInstance().enqueueEvent(
				new EndIterationEvent(Simulation.getSimulationTime() + (mTimePerIteration / getJobSpeedup() *
						mIterGranularity), this));
		// Aakash: augment this
		mGpuTime += mTimePerIteration / getJobSpeedup() * mCurrentIterationGPUs.size() * mIterGranularity;
		Iterator<GPU> gpuIter = mCurrentIterationGPUs.iterator();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		while(gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			if(gpu.getLeaseEnd() > Simulation.getSimulationTime() +
					(mTimePerIteration * mIterGranularity / getJobSpeedup())) {
				mNextIterationExpectedGPUs.add(gpu);
			}
		}
	}

	public void endIteration() {
		long itr_remain = getmTotalIterationsRemaining();
		setmTotalIterationsRemaining(itr_remain - mIterGranularity);
		if (itr_remain % 10000 == 0) {
			sLog.log(Level.ALL, "End iteration for job " + Integer.toString(mJobId));
			sLog.info("Iterations Remaining: " + Long.toString(itr_remain));
			System.out.println("End iteration for job " + Integer.toString(mJobId) + " remaining iterations="
					+ Long.toString(getmTotalIterationsRemaining()) + " Time:" + Simulation.getSimulationTime());
		}
		oldRatio = getCurrentEstimate()/getIdealEstimate();
		themisTs = getCurrentEstimate();
		if (getmTotalIterationsRemaining() == 0) {
			// Job is done
			System.out.println("Job " + Integer.toString(mJobId) + " done");
			sLog.info("Job " + Integer.toString(mJobId) + " done");
			List<GPU> relinquished_resources = relinquishAllResources();
			// Make all relinquished resources available
			ClusterEventQueue.getInstance()
					.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime()
							+ CHECKPOINTING_OVERHEAD_PER_GPU*relinquished_resources.size(), relinquished_resources));
			Cluster.getInstance().removeJob(this);
			JobStatistics.getInstance().recordJobEnd(mJobId, Simulation.getSimulationTime(), mJobStartTime,
					getIdealEstimate(), mIsLeader, mGpuTime, mMaxParallelism, queueDelay);
			return;
		}
		// Job has iterations left
		List<GPU> expiredResources = new ArrayList<GPU>();
		Iterator<GPU> currentGPUIterator = mCurrentIterationGPUs.iterator();
		mNextIterationExpectedGPUs = new HashSet<GPU>();
		while (currentGPUIterator.hasNext()) {
			GPU gpu = currentGPUIterator.next();
			if (gpu.hasLeaseExpired()) {
				expiredResources.add(gpu);
				gpu.markLeaseEnd();
			} else {
				mNextIterationGPUs.add(gpu);
				mNextIterationExpectedGPUs.add(gpu);
			}
		}
		
		// check if this iteration can finish within the least lease end time
		while(true) {
			boolean converged = true;
			double timeForIterations = mTimePerIteration * mIterGranularity /
					getPlacementSlowdown(mNextIterationGPUs)/mNextIterationGPUs.size();
			Iterator<GPU> it = mNextIterationGPUs.iterator();
			while(it.hasNext()) {
				GPU gpu = it.next();
				if(Simulation.getSimulationTime() + timeForIterations > gpu.getLeaseEnd()) {
					// cannot use this GPU anymore
					//System.out.println("Cannot use this GPU anymore " + Integer.toString(mJobId));
					mNextIterationExpectedGPUs.remove(gpu);
					expiredResources.add(gpu);
					gpu.markLeaseEnd();
					it.remove();
					converged = false;
				}
			}
			if(converged) {
				break;	
			}
		}
		
		if(!expiredResources.isEmpty()) {
			ClusterEventQueue.getInstance()
			.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() +
					CHECKPOINTING_OVERHEAD_PER_GPU*mCurrentIterationGPUs.size(), expiredResources));
		}
		
		if (mNextIterationGPUs.isEmpty()) {
			mCurrentIterationGPUs = new HashSet<GPU>();
			mIsWaiting = true;
			mTimeLastResourceAssignment = Simulation.getSimulationTime();
		} else {
			ClusterEventQueue.getInstance().enqueueEvent(new StartIterationEvent(Simulation.getSimulationTime(), this));
		}
	}
	
	public void resetOldRatio() { // short-circuit this API
		//oldRatio = Double.POSITIVE_INFINITY;
	}

	public double getCurrentEstimate() {
		return (Simulation.getSimulationTime() - mJobStartTime)
				+ (getmTotalIterationsRemaining() * mTimePerIteration) / getJobSpeedup();
	}
	
	public double getEstimateAfterAllocation() {
		return (Simulation.getSimulationTime() - mJobStartTime)
				+ (getmTotalIterationsRemaining() * mTimePerIteration) / getJobSpeedup1();
	}

	public double getIdealEstimate() {
		return 1.0 * mTimePerIteration * getmTotalExpectedIterations() / mMaxParallelism;
	}

	public double getLossGradient() {
		return mLossCurve.getSlope(getmTotalExpectedIterations() - getmTotalIterationsRemaining());
	}
	
	public double getLoss(long iteration) {
		return mLossCurve.getValue(iteration);
	}

	public void notifyResourceAssignment(List<GPU> assignment) {
		//sLog.info("Job " + Integer.toString(mJobId) + " got resources");
		//System.out.println("Job " + Integer.toString(mJobId) + " got resources " + assignment);
		mNextIterationGPUs.addAll(assignment);
		//themisTs = getEstimateAfterAllocation();
	}
	
	public void notifyResourceAvailable() {
		mIsWaiting = false;
		queueDelay += Simulation.getSimulationTime() - mTimeLastResourceAssignment;
		mTimeLastResourceAssignment = Simulation.getSimulationTime();
	}
	
	public double getJobSpeedup1() {
		return getGPUsAvailableForNextIteration().size()* getPlacementSlowdown(getGPUsAvailableForNextIteration());
	}

	public double getJobSpeedup() {
		return mCurrentIterationGPUs.size() * getPlacementSlowdown(mCurrentIterationGPUs);
	}
	
	public int getJobId() {
		return mJobId;
	}
	
	public boolean willParticipateInBid() {
		return true;
	}
	
	public int getJobGroupId() {
		return mJobGroupId;
	}

	protected double astra_sim(Set<GPU> gpus, Vector<Integer> dimVec, Vector<String> dimType) {

		String[] topoPerDim = Cluster.getInstance().getConfiguration().getmTopoPerDim();
		String[] mDimType = Cluster.getInstance().getConfiguration().getmDimType();
		float[] mLinkRatio = Cluster.getInstance().getConfiguration().getmLinkRatio();
		long[] mLinkLatency = Cluster.getInstance().getConfiguration().getmLinkLatency();
		int[] mLinkBandwidth = Cluster.getInstance().getConfiguration().getmLinkBandwidth();
		String[] mAllReduceImpl = Cluster.getInstance().getConfiguration().getmAllReduceImpl();
		String[] mAllGatherImpl = Cluster.getInstance().getConfiguration().getmAllGatherImpl();
		String[] mReduceScatterImpl = Cluster.getInstance().getConfiguration().getmReduceScatterImpl();
		String[] mAllToAllImpl = Cluster.getInstance().getConfiguration().getmAllToAllImpl();
		String mIntraDimSched = Cluster.getInstance().getConfiguration().getmIntraDimSched();
		String mInterDimSched = Cluster.getInstance().getConfiguration().getmInterDimSched();
		String runName = Cluster.getInstance().getConfiguration().getmRunName();

		String PATH = mAstraSimPath + "/runs/" + runName + "/";
		File directory = new File(PATH);
		directory.mkdirs();
		directory = new File(PATH + "network");
		directory.mkdir();
		directory = new File(PATH + "system");
		directory.mkdir();
		directory = new File(PATH + "workload");
		directory.mkdir();
		directory = new File(PATH + "results");
		directory.mkdir();

		double computeTime = 0;
		double commTime = 0;
		double computeScale = 1;

		if (mModelName == null) {
			mModelName = "ResNet50";
		}

		if (mModelName == "ResNet50") {
			computeScale = 1;
		}

		if (mModelName == "VGG") {
			computeScale = 1.2;
		}

		JSONObject jsonObject = new JSONObject();
		JSONArray topologiesPerDim = new JSONArray();
		JSONArray dimensionType = new JSONArray();
		JSONArray unitsCount = new JSONArray();
		JSONArray linksCount = new JSONArray();
		JSONArray linkLatency = new JSONArray();
		JSONArray linkBW = new JSONArray();
		JSONArray nicLatency = new JSONArray();
		JSONArray routerLatency = new JSONArray();
		JSONArray hbmLatency = new JSONArray();
		JSONArray hbmBW = new JSONArray();
		JSONArray hbmScale = new JSONArray();
		int links = 0;

		int diff = topoPerDim.length - dimVec.size();

		System.out.println("==============astra-sim configs========");
		System.out.println(String.join(" ", dimType));
		//System.out.println(String.join(" ", topoPerDim));
		//System.out.println(String.join(" ", mDimType));
		System.out.println(diff);

		//for (int idx = diff; idx < topoPerDim.length; idx++)
		for (int idx = 0; idx < dimVec.size(); idx++)
		{
			topologiesPerDim.add(topoPerDim[idx+diff]);
			dimensionType.add(dimType.get(idx));
			linkLatency.add(mLinkLatency[idx+diff]);
			linkBW.add(mLinkBandwidth[idx+diff]);

			unitsCount.add(dimVec.get(idx));

			links = (int) Math.ceil(mLinkRatio[idx+diff] * dimVec.get(idx));
			System.out.println(topoPerDim[idx]);
			System.out.println(links);

			if (topoPerDim[idx].equals("Ring")) {
				if (links % 2 != 0) {
					links += 1;
				}
			}

			if (topoPerDim[idx].equals("FullyConnected")) {
				links += links % (dimVec.get(idx-diff) - 1);
			}

			linksCount.add(links);

			nicLatency.add(0);
			routerLatency.add(0);
			hbmLatency.add(500);
			hbmBW.add(370);
			hbmScale.add(0);
		}

		jsonObject.put("topology-name", "Hierarchical");
		jsonObject.put("topologies-per-dim", topologiesPerDim);
		jsonObject.put("dimension-type", dimensionType);
		jsonObject.put("dimensions-count", dimVec.size());
		jsonObject.put("units-count", unitsCount);
		jsonObject.put("links-count", linksCount);
		jsonObject.put("link-latency", linkLatency);
		jsonObject.put("link-bandwidth", linkBW);
		jsonObject.put("nic-latency", nicLatency);
		jsonObject.put("router-latency", routerLatency);
		jsonObject.put("hbm-latency", hbmLatency);
		jsonObject.put("hbm-bandwidth", hbmBW);
		jsonObject.put("hbm-scale", hbmScale);

		try {
			FileWriter file = new FileWriter(PATH + "/network/" + mJobId +".json");
			file.write(jsonObject.toJSONString());
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(PATH + "/system/" + mJobId +".txt"));
			writer.write("scheduling-policy: LIFO\n");
			writer.append("endpoint-delay: 1\n");
			writer.append("active-chunks-per-dimension: 1\n");
			writer.append("preferred-dataset-splits: 1\n");
			writer.append("boost-mode: 1\n");
			writer.append("all-reduce-implementation: " + mAllReduceImpl[0]);

			for (int idx = diff + 1; idx < dimVec.size(); idx++) {
				if (idx >= mAllReduceImpl.length) {
					writer.append("_" + mAllReduceImpl[mAllReduceImpl.length-1]);
				}
				else {
					writer.append("_" + mAllReduceImpl[idx]);
				}
			}
			writer.append("\n");
			writer.append("all-gather-implementation: " + mAllGatherImpl[0]);
			for (int idx = diff + 1; idx < dimVec.size(); idx++) {
				if (idx >= mAllGatherImpl.length) {
					writer.append("_" + mAllGatherImpl[mAllGatherImpl.length-1]);
				}
				else {
					writer.append("_" + mAllGatherImpl[idx]);
				}
			}
			writer.append("\n");
			writer.append("reduce-scatter-implementation: " + mReduceScatterImpl[0]);
			for (int idx = diff + 1; idx < dimVec.size(); idx++) {
				if (idx >= mReduceScatterImpl.length) {
					writer.append("_" + mReduceScatterImpl[mReduceScatterImpl.length-1]);
				}
				else {
					writer.append("_" + mReduceScatterImpl[idx]);
				}
			}
			writer.append("\n");
			writer.append("all-to-all-implementation: " + mAllToAllImpl[0]);
			for (int idx = diff + 1; idx < dimVec.size(); idx++) {
				if (idx >= mAllToAllImpl.length) {
					writer.append("_" + mAllToAllImpl[mAllToAllImpl.length-1]);
				}
				else {
					writer.append("_" + mAllToAllImpl[idx]);
				}
			}
			writer.append("\n");
			writer.append("collective-optimization: localBWAware\n");

			if (mIntraDimSched != null) {
				writer.append("intra-dimension-scheduling: " + mIntraDimSched + "\n");
			}
			if (mInterDimSched != null) {
				writer.append("inter-dimension-scheduling: " + mInterDimSched + "\n");
			}

			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<String> cmd = new ArrayList<String>();

		cmd.add(mAstraSimBinPath);
		cmd.add("--network-configuration=" + PATH + "network/" + mJobId + ".json");
		cmd.add("--system-configuration=" + PATH + "system/" + mJobId +".txt");
		cmd.add("--workload-configuration=" + mAstraSimPath + "/workload/" + mModelName + ".txt");
		cmd.add("--path=" + PATH + "results/");
		cmd.add("--run-name=" + mJobId);
		cmd.add("--compute-scale=" + String.valueOf(computeScale));

		try {
			ProcessBuilder pb = new ProcessBuilder(cmd);
			pb.directory(new File(PATH)); //Set current directory
			pb.redirectError(new File(PATH + "err.log")); //Log errors in specified log file.
			pb.redirectOutput(new File(PATH + "out.log")); //Log errors in specified log file.

			Process process = pb.start();
			int exitVal = process.waitFor();
			System.out.println("Ran command: " + String.join(" ", cmd));
			if (exitVal != 0) {
				System.out.println("Abnormal Behaviour! Something bad happened with astra sim.");
				//System.out.println("Ran command: " + String.join(" ", cmd));
				System.out.println("Printing stack trace:");
				StackTraceElement[] elements = Thread.currentThread().getStackTrace();
				for (int i = 1; i < elements.length; i++) {
					StackTraceElement s = elements[i];
					System.out.println("\tat " + s.getClassName() + "." + s.getMethodName()
							+ "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
				}
				System.exit(-1);
			}

			BufferedReader reader = new BufferedReader(new FileReader(PATH + "/results/EndToEnd.csv"));
			reader.readLine();

			String line = reader.readLine();
			String[] vals = line.split(",");

			computeTime = Float.parseFloat(vals[12]);
			commTime = Float.parseFloat(vals[13]);

			//System.out.println("A " + dim);
			System.out.println("===================");
			System.out.println("compute time: " + computeTime);
			System.out.println("comm time: " + commTime);
			System.out.println("Comp time fraction: " + computeTime / (commTime + computeTime));
			System.out.println("===================");
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		return computeTime/ (commTime + computeTime);
	}

/*
	public double getPlacementSlowdown_astra(Set<GPU> gpus) {
		HashSet<Integer> map = new HashSet<Integer>();
		Vector<Integer> dimVec = new Vector<Integer>();
		Iterator<GPU> gpuIter = gpus.iterator();

		// Check if across slots
		map = new HashSet<Integer>();
		gpuIter = gpus.iterator();
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getSlotId());
		}
		dimVec.add(map.size());

		// Check if across machines
		map = new HashSet<Integer>();
		gpuIter = gpus.iterator();
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getMachineId());
		}
		dimVec.add(map.size());

		// Check if across racks
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getRackId());
			//System.out.println("GPU location" + gpu.getLocation().getPrettyString());
		}
		dimVec.add(map.size());
		return astra_sim(gpus, dimVec);
	}
*/
	public double getPlacementSlowdown(Set<GPU> gpus) {

		if (gpus.isEmpty())
		{
			return Double.MIN_VALUE;
		}


		/*
		HashSet<Integer> map = new HashSet<Integer>();
		Vector<Integer> dimVec = new Vector<Integer>();
		Iterator<GPU> gpuIter = gpus.iterator();
		int gpu_loc;

		// Check if across dim2
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			gpu_loc = gpu.getLocation().getDim2Id();
			if (gpu_loc != -1) {
				map.add(gpu_loc);
			}
		}

		if (map.size() > 0) {
			dimVec.add(map.size());
			map.clear();
		}

		// Check if across dim1
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getDim1Id());
		}

		if (map.size() > 0) {
			dimVec.add(map.size());
			map.clear();
		}

		// Check if across slots
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getSlotId());
		}
		dimVec.add(map.size());
		map.clear();

		// Check if across machines
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getMachineId());
		}
		dimVec.add(map.size());
		map.clear();

		// Check if across racks
		while (gpuIter.hasNext()) {
			GPU gpu = gpuIter.next();
			map.add(gpu.getLocation().getRackId());
		}
		dimVec.add(map.size());
		map.clear();
		*/

		Map<Integer, Integer> rackMap = new HashMap<>();
		MultiKeyMap machineMap = new MultiKeyMap();
		MultiKeyMap slotMap = new MultiKeyMap();
		MultiKeyMap dim1Map = new MultiKeyMap();
		MultiKeyMap dim2Map = new MultiKeyMap();

		for (GPU gpu: gpus) {
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
			if (!slotMap.containsKey(rack, machine, slot)) {
				slotMap.put(rack, machine, slot, 1);
			}
			else {
				count = (Integer) slotMap.get(rack, machine, slot);
				slotMap.put(rack, machine, slot, count + 1);
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

			if (dim1 != -1 && dim2 != -1) {
				if (!dim2Map.containsKey(rack, machine, slot, dim1, dim2)) {
					dim2Map.put(rack, machine, slot, dim1, dim2, 1);
				} else {
					count = (Integer) dim2Map.get(rack, machine, slot, dim1, dim2);
					dim2Map.put(rack, machine, slot, dim1, dim2, count + 1);
				}
			}
		}

		int num_gpus = gpus.size();
		Vector<Integer> dimVec = new Vector<Integer>();
		Vector<String> dimType = new Vector<String>();
		double slowdown = 1.0;

		int rack_size = rackMap.size();

		if (rack_size > 1) {
			dimType.insertElementAt("PP", 0);
			if (num_gpus % rack_size != 0) {
				dimVec.insertElementAt(num_gpus, 0);
				if (mSlowdown.containsKey(dimVec)){
					return mSlowdown.get(dimVec);
				}
				slowdown = astra_sim(gpus, dimVec, dimType);
				mSlowdown.put(dimVec, slowdown);
				return slowdown;
			}
			else {
				dimVec.insertElementAt(rackMap.size(), 0);
			}
			num_gpus /= rack_size;
		}

		int machine_size = machineMap.size();

		if (machine_size > 1) {
			dimType.insertElementAt("P", 0);
			if (num_gpus % machine_size != 0) {
				dimVec.insertElementAt(num_gpus, 0);
				if (mSlowdown.containsKey(dimVec)){
					return mSlowdown.get(dimVec);
				}
				slowdown = astra_sim(gpus, dimVec, dimType);
				mSlowdown.put(dimVec, slowdown);
				return slowdown;
			}
			else {
				dimVec.insertElementAt(machineMap.size(), 0);
			}
			num_gpus /= machine_size;
		}

		int slot_size = slotMap.size();

		if (slot_size > 0) {
			dimType.insertElementAt("N", 0);
			if (num_gpus % slot_size != 0) {
				dimVec.insertElementAt(num_gpus, 0);
				if (mSlowdown.containsKey(dimVec)){
					return mSlowdown.get(dimVec);
				}
				slowdown = astra_sim(gpus, dimVec, dimType);
				mSlowdown.put(dimVec, slowdown);
				return slowdown;
			} else {
				dimVec.insertElementAt(slotMap.size(), 0);
			}
			num_gpus /= slot_size;
		}

		int dim1_size = dim1Map.size();

		if (dim1_size > 0) {
			dimType.insertElementAt("N", 0);
			if (num_gpus % dim1_size != 0) {
				dimVec.insertElementAt(num_gpus, 0);
				if (mSlowdown.containsKey(dimVec)){
					return mSlowdown.get(dimVec);
				}
				slowdown = astra_sim(gpus, dimVec, dimType);
				mSlowdown.put(dimVec, slowdown);
				return slowdown;
			} else {
				dimVec.insertElementAt(dim1Map.size(), 0);
			}
			num_gpus /= dim1_size;
		}

		int dim2_size = dim2Map.size();

		if (dim2_size > 0) {
			dimType.insertElementAt("N", 0);
			if (num_gpus % dim2_size != 0) {
				dimVec.insertElementAt(num_gpus, 0);
				if (mSlowdown.containsKey(dimVec)){
					return mSlowdown.get(dimVec);
				}
				slowdown = astra_sim(gpus, dimVec, dimType);
				mSlowdown.put(dimVec, slowdown);
				return slowdown;
			} else {
				dimVec.insertElementAt(dim2Map.size(), 0);
			}
			num_gpus /= dim2_size;
		}

		dimVec.insertElementAt(num_gpus, 0);
		dimType.insertElementAt("N", 0);

		if (mSlowdown.containsKey(dimVec)){
			return mSlowdown.get(dimVec);
		}
		slowdown = astra_sim(gpus, dimVec, dimType);
		mSlowdown.put(dimVec, slowdown);
		return slowdown;
	}

	public double getQueueDelay() {
		return queueDelay;
	}

	public boolean isQueued() {
		return mIsQueued;
	}

	public void dequeue(double time) {
		mIsQueued = false;
		queueDelay = time - mJobStartTime;
		mJobStartTime = mJobStartTime + time;
	}
	
	public boolean isWaitingForResources() {
		return mIsWaiting;
	}
	
	public boolean hasResourcesForNextIteration() {
		return mNextIterationGPUs.size() > 0;
	}
	
	public Set<GPU> getGPUsAvailableForNextIteration() {
		Set<GPU> set = new HashSet<GPU>(mNextIterationGPUs);
		// Now add all GPUs to this set which will not expire after the iteration
		set.addAll(mNextIterationExpectedGPUs);
		return set;
	}
	
	/**
	 * Returns the maximum number of GPUs this job can take
	 * @return
	 */
	public int getMaxParallelism() {
		return mMaxParallelism;
	}
	
	public double getOldBenefit() {
		double oldSpeedup = getGPUsAvailableForNextIteration().size() * getPlacementSlowdown(getGPUsAvailableForNextIteration());
		double oldTs = (Simulation.getSimulationTime() - mJobStartTime) + 
				(mTotalIterationsRemaining*mTimePerIteration)/oldSpeedup;
		double oldratio = oldTs/getIdealEstimate();
		System.out.println("Job " + mJobId + " ");
		return oldratio;
	}

	public abstract List<Bid> prepareBid(List<GPU> offeredGPUs);
	public List<Bid> prepareMultiBid(List<GPU> offeredGPUs) {
		List<Bid> bids = null;
		return bids;
	}

	private List<GPU> relinquishAllResources() {
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		List<GPU> releasedResources = new ArrayList<GPU>();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (gpu.getJob() != null && gpu.getJob().equals(this)) {
				releasedResources.add(gpu);
				gpu.markLeaseEnd();
			}
		}
		return releasedResources;
	}

	private List<GPU> getResourcesAvailableInCluster() {
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		List<GPU> availableResources = new ArrayList<GPU>();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (!gpu.isLeased()) {
				availableResources.add(gpu);
			}
		}
		return availableResources;
	}

	private void initFromConfig(JSONObject config) {
		mJobGroupId = Integer.parseInt(ConfigUtils.getAttributeValue(config, "job_group_id"));
		mJobId = Integer.parseInt(ConfigUtils.getAttributeValue(config, "job_id"));
		mUserName = ConfigUtils.getAttributeValue(config, "user");
		mModelName = ConfigUtils.getAttributeValue(config, "model");
		setmTotalExpectedIterations(Long.parseLong(ConfigUtils.getAttributeValue(config, "total_iterations")));
		mTimePerIteration = Double.parseDouble(ConfigUtils.getAttributeValue(config, "time_per_iteration"));
		mMaxParallelism = Integer.parseInt(ConfigUtils.getAttributeValue(config, "max_parallelism"));
		mRandomSeed = Integer.parseInt(ConfigUtils.getAttributeValue(config, "random_seed"));
		mCrossSlotSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_slot_slowdown"));
		mCrossMachineSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_machine_slowdown"));
		mCrossRackSlowdown = Double.parseDouble(ConfigUtils.getAttributeValue(config, "cross_rack_slowdown"));
		mLossCurve = LossFunctionFactory.createInstance(ConfigUtils.getAttributeValue(
				config, "loss_function_type"), getmTotalExpectedIterations(), mRandomSeed);
		setmTotalIterationsRemaining(getmTotalExpectedIterations());
		//mCrossSlotSlowdown = 1.0;
		//mCrossMachineSlowdown = random(0.6, 0.98);
		//mCrossRackSlowdown = mCrossMachineSlowdown/0.7;
	}
	
	private double random( double min, double max ) {
	  double diff = max - min;
	  return min + Math.random( ) * diff;
	}
}
