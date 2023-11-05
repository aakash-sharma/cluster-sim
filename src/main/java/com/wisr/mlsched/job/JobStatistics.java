package com.wisr.mlsched.job;

import java.io.IOException;
import java.util.*;
import java.io.File;

import com.wisr.mlsched.resources.GPU;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileOutputStream;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.events.JobStatisticEvent;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.JobGroupManager;
import com.wisr.mlsched.resources.Cluster;

public class JobStatistics {
	
	private static JobStatistics sInstance = null; // Holder of singleton
	private HashMap<Integer, SingleJobStat> mJobStats; // Job start and end time per job
	private List<FairnessIndex> mFairnessIndices; // List of fairness indices measured over time
	private List<LossValue> mLossValues; // List of cumulative loss values measured over time
	private List<Double> mFinishTimeFairness; // List of Ts/Ti for leader jobs
	private List<ContentionValue> mContention; // List of contention numbers over time
	private TreeMap<Double,Integer> mGPUContention; // Map of contention over time  AS: buggy
	private int[] mAllocs; // Number of allocations in the cluster
	private XSSFWorkbook mWorkbook;
	Map<Integer, Vector<Double>> mSimResults;
	private static int lastContention = 0;
	
	/**
	 * Private constructor to enforce singleton
	 */
	private JobStatistics() {
		mJobStats = new HashMap<Integer, SingleJobStat>();
		mFairnessIndices = new ArrayList<FairnessIndex>();
		mLossValues = new ArrayList<LossValue>();
		mFinishTimeFairness = new ArrayList<Double>();
		mContention = new ArrayList<ContentionValue>();
		mGPUContention = new TreeMap<Double, Integer>();
		mAllocs = new int[Simulation.getNumDims()];
		mWorkbook = new XSSFWorkbook();
		mSimResults = new TreeMap<Integer, Vector<Double>>();
		ClusterEventQueue.getInstance().enqueueEvent(new 
				JobStatisticEvent(Simulation.getSimulationTime() + 1));
	}
	
	/**
	 * Get the instance of JobStatistics.
	 */
	public static JobStatistics getInstance() {
		if(sInstance == null) {
			sInstance = new JobStatistics();
		}
		return sInstance;
	}
	
	/**
	 * Record the start of job
	 * @param jobid
	 * @param timestamp
	 */
	public void recordJobStart(int jobid, double timestamp, int gpu_demand) {
		mJobStats.put(jobid, new SingleJobStat(timestamp));
		if(mGPUContention.get(timestamp) == null) {
			mGPUContention.put(timestamp, 0);
		}
		lastContention += gpu_demand;
		if(lastContention <= Cluster.getInstance().getGPUsInCluster().size()) {
			mGPUContention.put(timestamp, 1);	
		} else {
			mGPUContention.put(timestamp, lastContention/Cluster.getInstance().getGPUsInCluster().size());
		}
	}
	
	/**
	 * Record the end of job
	 * @param jobid
	 * @param timestamp
	 */
	public void recordJobEnd(int jobid, double timestamp, double start_time, double ideal_running_time,
			boolean isLeader, double gpu_time, double comp_time, double comm_time, int gpu_demand,
							 double queue_delay, int [] allocs) {
		if(mGPUContention.get(timestamp) == null) {
			mGPUContention.put(timestamp, 0);
		}

		if(lastContention <= Cluster.getInstance().getGPUsInCluster().size()) {
			mGPUContention.put(timestamp, 1);	
		} else {
			mGPUContention.put(timestamp, lastContention/Cluster.getInstance().getGPUsInCluster().size());
		}
		mGPUContention.put(timestamp, lastContention);
		mJobStats.get(jobid).setStartTime(start_time);
		mJobStats.get(jobid).setEndTime(timestamp);
		mJobStats.get(jobid).setGpuTime(gpu_time);
		mJobStats.get(jobid).setCompTime(comp_time);
		mJobStats.get(jobid).setCommTime(comm_time);
		mJobStats.get(jobid).setQueueDelay(queue_delay);
		mJobStats.get(jobid).setAvgGPUcontention(avg_contention_for_job(jobid));
		mJobStats.get(jobid).setAllocs(allocs);
		mFinishTimeFairness.add((timestamp-start_time)/(ideal_running_time*avg_contention_for_job(jobid)));
		//lastContention -= gpu_demand;
	}

	public void setAlloc(int alloc, int idx) {
		mAllocs[idx] = alloc;
	}

	public int[] getAllocs() {
		return mAllocs;
	}

	private double avg_contention_for_job(int jobid) {
		double numerator = 0.0;
		double denominator = 0.0;
		double startTime = mJobStats.get(jobid).getStartTime();
		double endTime = mJobStats.get(jobid).getEndTime();
		
		double t1 = -1;
		double t2 = -1;
		boolean started = false;
		
		//System.out.println(mGPUContention);
		
		for(Double time : mGPUContention.keySet()) {
			if (Double.compare(time, startTime) < 0) {
				// no op
			}
		    else if(Double.compare(time, startTime) == 0) { // started
				t1 = startTime;
			} else if(Double.compare(time, endTime) == 0) { // ended
				numerator += mGPUContention.get(t1)*(time-t1);
				denominator += time-t1;
				break;
			} else { // middle
				numerator += mGPUContention.get(t1)*(time-t1);
				denominator += time-t1;
				t1=time;
			}
		}
		//System.out.println("Job id " + Integer.toString(jobid) + " con:" + Double.toString(numerator/denominator));
		return numerator/denominator;
	}
	
	public void recordJobStatistics() {
		mFairnessIndices.add(new FairnessIndex(Simulation.getSimulationTime(), computeJainFairness()));
		mLossValues.add(new LossValue(Simulation.getSimulationTime(), computeCumulativeLoss()));

		double used_gpus = 0, cluster_util = 0;
		List<GPU> gpus = Cluster.getInstance().getGPUsInCluster();
		Iterator<GPU> gpuIterator = gpus.iterator();
		while (gpuIterator.hasNext()) {
			GPU gpu = gpuIterator.next();
			if (gpu.getJob() != null && gpu.isLeased()) {
				used_gpus += 1;
			}
		}

		cluster_util = used_gpus / (double) Cluster.getInstance().getGPUsInCluster().size();

		mContention.add(new ContentionValue(Simulation.getSimulationTime(), computeCurrentContention(), mAllocs,
				Cluster.getInstance().getRunningJobs().size(), Cluster.getInstance().getActiveJobs().size(),
				cluster_util));

		System.out.println("Adding contention at time: " + String.valueOf(Simulation.getSimulationTime()));
		System.out.println("contention: " + String.valueOf(computeCurrentContention()));
		if(ClusterEventQueue.getInstance().getNumberEvents() > 0) {
			ClusterEventQueue.getInstance().enqueueEvent(new 
					JobStatisticEvent(Simulation.getSimulationTime() + Cluster.getInstance().getLeaseTime()
					* 0.1));
		}
	}
	
	private double computeCurrentContention() {
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();
		long cumulativeReq = 0;
		for(IntraJobScheduler job : jobs) {
			cumulativeReq += job.getMaxParallelism();
		}
		int num_gpus = Cluster.getInstance().getGPUsInCluster().size();
		return (double)cumulativeReq*1.0/num_gpus;
	}

	private double computeJobClusterUtil() {
		HashSet<IntraJobScheduler> jobs = (HashSet<IntraJobScheduler>) Cluster.getInstance().getActiveJobs();
		long cumulativeReq = 0;
		for(IntraJobScheduler job : jobs) {
			cumulativeReq += job.getMaxParallelism();
		}
		int num_gpus = Cluster.getInstance().getGPUsInCluster().size();
		return (double)cumulativeReq*1.0/num_gpus;
	}

	private double computeCumulativeLoss() {
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();
		double loss = 0.0;
		for(IntraJobScheduler job : jobs) {
			loss += job.getLoss(job.getmTotalExpectedIterations()-job.getmTotalIterationsRemaining());
		}
		return loss;
	}
	
	private double computeJainFairness() {
		List<IntraJobScheduler> jobs = Cluster.getInstance().getRunningJobs();
		List<Double> ratios = new ArrayList<Double>();
		List<Integer> jobGroups = new ArrayList<Integer>();
		for(IntraJobScheduler job: jobs) {
			if(!jobGroups.contains(job.getJobGroupId())) {
				// Have not processed this job group yet
				jobGroups.add(job.getJobGroupId());
				List<IntraJobScheduler> jobsInGroup = JobGroupManager.getInstance().getJobsInGroup(job);
				double total = 0;
				for(IntraJobScheduler jg : jobsInGroup) {
					total += jg.getIdealEstimate()/jg.getCurrentEstimate();
				}
				ratios.add(total/jobsInGroup.size());
			}
		}
		return Math.pow(sum(ratios), 2)/(ratios.size() * sumOfSquares(ratios));
	}
	
	private double sum(List<Double> values) {
		double sum = 0.0;
		for(Double val : values) {
			sum += val;
		}
		return sum;
	}
	
	private double sumOfSquares(List<Double> values) {
		double sum = 0.0;
		for(Double val : values) {
			sum += val*val;
		}
		return sum;
	}
	
	/**
	 * Print out individual JCTs, average JCT, and makespan
	 */
	public void printStats() {
		double makespan = 0.0;
		for(Integer key : mJobStats.keySet()) {
			mSimResults.put(key, new Vector<Double>());
		}
//		printJCT();
//		//printFairnessIndex();
//		//printLosses();
//		printQueueDelay();
//		printGpuTime();
//		printAllocs();
		makespan = printJobStats();
		printContentions();
		//printFinishTimeFairness();

		XSSFSheet sheet1 = mWorkbook.createSheet("job-stats ");
		XSSFSheet sheet2 = mWorkbook.createSheet("cluster-timeline-stats");
		XSSFSheet sheet3 = mWorkbook.createSheet("cluster-cum-stats");
		XSSFRow row;
		int rowid = 0;
		int cellid = 0;
		Cell cell;
		String[] headers = {"JobId", "Start-time", "End-time", "JCT", "Queue-delay", "%Queue-delay", "GPU-time", "%GPU-time", "Comp-time",
				"%Comp-Time", "Comm-time", "%Comm-time", "Avg-GPU-contention",
				"dim2Alloc", "dim1Alloc", "slotAlloc", "machineAlloc", "rackAlloc", "nwAlloc", "makespan"};
		row = sheet1.createRow(rowid++);

		for (String str : headers) {
			cell = row.createCell(cellid++);
			cell.setCellValue(str);
		}

		int total_jct = 0;
		int total_gpu = 0;
		int total_compute = 0;
		int total_queueing = 0;
		int total_comm = 0;
		int total_jobs = mJobStats.size();

		for(Integer key : mJobStats.keySet()) {

			row = sheet1.createRow(rowid++);
			//Vector<Double> values = mSimResults.get(key);
			cellid = 0;
			cell = row.createCell(cellid++);
			cell.setCellValue(key);

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getStartTime());

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getEndTime());

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getJobTime());
			total_jct += mJobStats.get(key).getJobTime();

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getQueueDelay());
			total_queueing += mJobStats.get(key).getQueueDelay();

			cell = row.createCell(cellid++);
			cell.setCellValue(mSimResults.get(key).get(1) / mSimResults.get(key).get(0) * 100);

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getGpuTime());
			total_gpu += mJobStats.get(key).getGpuTime();

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getGpuTime() / mJobStats.get(key).getJobTime() * 100);

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getCompTime());
			total_compute += mJobStats.get(key).getCompTime();

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getCompTime() / mJobStats.get(key).getJobTime() * 100);

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getCommTime());
			total_comm += mJobStats.get(key).getCommTime();

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getCommTime() / mJobStats.get(key).getJobTime() * 100);

			cell = row.createCell(cellid++);
			cell.setCellValue(mJobStats.get(key).getAvgGPUcontention());

			int allocs[] = mJobStats.get(key).getAllocs();

			for (int val: allocs) {
				cell = row.createCell(cellid++);
				cell.setCellValue(val);
			}

			cell = row.createCell(cellid++);
			cell.setCellValue(makespan);
		}

		rowid = 0;
		cellid = 0;
		String[] cluster_headers = {"Timestamp", "Contention", "NumRunningJobs", "NumActiveJobs", "ClusterUtil",
				"dim2Alloc", "dim1Alloc", "slotAlloc", "machineAlloc", "rackAlloc", "nwAlloc"};

		row = sheet2.createRow(rowid++);

		for (String str : cluster_headers) {
			cell = row.createCell(cellid++);
			cell.setCellValue(str);
		}

		for(ContentionValue val : mContention) {
			row = sheet2.createRow(rowid++);
			cell = row.createCell(0);
			cell.setCellValue(val.mTimestamp);
			cell = row.createCell(1);
			cell.setCellValue(val.mContention);
			cell = row.createCell(2);
			cell.setCellValue(val.mRunningJobs);
			cell = row.createCell(3);
			cell.setCellValue(val.mActiveJobs);
			cell = row.createCell(4);
			cell.setCellValue(val.mClusterUtil);
			int i = 5;
			for (int alloc: val.mAllocs){
				cell = row.createCell(i);
				cell.setCellValue(alloc);
				i += 1;
			}
		}

		rowid = 0;
		cellid = 0;
		int col = 0;
		cluster_headers = new String[]{"Makespan", "Total_JCT", "Total_GPU", "Total_compute", "Total_queueing",
				"Total_comm", "Avg_JCT", "Avg_compute", "Avg_queueing", "Avg_comm"};
		row = sheet3.createRow(rowid++);

		for (String str : cluster_headers) {
			cell = row.createCell(cellid++);
			cell.setCellValue(str);
		}

		row = sheet3.createRow(rowid++);

		cell = row.createCell(col++);
		cell.setCellValue(makespan);

		cell = row.createCell(col++);
		cell.setCellValue(total_jct);

		cell = row.createCell(col++);
		cell.setCellValue(total_gpu);

		cell = row.createCell(col++);
		cell.setCellValue(total_compute);

		cell = row.createCell(col++);
		cell.setCellValue(total_queueing);

		cell = row.createCell(col++);
		cell.setCellValue(total_comm);

		cell = row.createCell(col++);
		cell.setCellValue(total_jct/total_jobs);

		cell = row.createCell(col++);
		cell.setCellValue(total_compute/total_jobs);

		cell = row.createCell(col++);
		cell.setCellValue(total_queueing/total_jobs);

		cell = row.createCell(col++);
		cell.setCellValue(total_comm/total_jobs);

		String policy = Cluster.getInstance().getConfiguration().getPolicy();
		String topo_name = Cluster.getInstance().getConfiguration().getmTopoName();
		String runName = Cluster.getInstance().getConfiguration().getmRunName();
		Integer racks = Cluster.getInstance().getConfiguration().getRacks();
		Integer machines = Cluster.getInstance().getConfiguration().getMachinesPerRack();

		String PATH = "results/" + runName + "/";

		File directory = new File(PATH);
		directory.mkdir();
		File file = new File( PATH + policy + "_" + topo_name + "_" + machines.toString() + "m_" + racks.toString() + "r"
				+  ".xlsx");

		try {
			FileOutputStream out = new FileOutputStream(file);
			mWorkbook.write(out);
			out.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	private double printJobStats(){
		double total_jct = 0.0;
		double jct = 0.0;

		double queue_delay = 0.0;
		double total_queue_delay = 0.0;

		double gpu_time = 0.0;
		double total_gpu_time = 0.0;

		double comp_time = 0.0;
		double total_comp_time = 0.0;

		double comm_time = 0.0;
		double total_comm_time = 0.0;

		double avg_gpu_contention = 0;

		int allocs[];

		double earliest_start_time = Double.MAX_VALUE;
		double latest_end_time = Double.MIN_VALUE;
		double makespan = 0.0;

		for(Integer key : mJobStats.keySet()) {

			jct = mJobStats.get(key).getJobTime();
			total_jct += jct;
			mSimResults.get(key).add(jct);

			queue_delay = mJobStats.get(key).getQueueDelay();
			total_queue_delay += queue_delay;
			mSimResults.get(key).add(queue_delay);

			gpu_time = mJobStats.get(key).getGpuTime();
			mSimResults.get(key).add(gpu_time);
			total_gpu_time += gpu_time;

			comp_time = mJobStats.get(key).getCompTime();
			mSimResults.get(key).add(comp_time);
			total_comp_time += comp_time;

			comm_time = mJobStats.get(key).getCommTime();
			mSimResults.get(key).add(comm_time);
			total_comm_time += comm_time;

			avg_gpu_contention = mJobStats.get(key).getAvgGPUcontention();
			mSimResults.get(key).add(avg_gpu_contention);

			allocs = mJobStats.get(key).getAllocs();

			for (int val: allocs) {
				mSimResults.get(key).add(Double.valueOf(val));
			}

			double start_time = mJobStats.get(key).getStartTime();
			double end_time = mJobStats.get(key).getEndTime();
			if(start_time < earliest_start_time) {
				earliest_start_time = start_time;
			}
			if(end_time > latest_end_time) {
				latest_end_time = end_time;
			}
		}

		double avg_jct = total_jct/mJobStats.keySet().size();
		double avg_queue_delay = total_queue_delay/mJobStats.keySet().size();
		double avg_gpu_time = total_gpu_time/mJobStats.keySet().size();
		double avg_comp_time = total_comp_time/mJobStats.keySet().size();
		double avg_comm_time = total_comm_time/mJobStats.keySet().size();

		System.out.println("Average JCT: " + Double.toString(avg_jct));
		System.out.println("Average queue delay: " + Double.toString(avg_queue_delay));
		System.out.println("Average GPU time: " + Double.toString(avg_gpu_time));
		System.out.println("Average Compute time: " + Double.toString(avg_comp_time));
		System.out.println("Average Communication time: " + Double.toString(avg_comm_time));
		System.out.println("Total GPU Time: " + total_gpu_time);
		System.out.println("Total Compute Time: " + total_comp_time);
		System.out.println("Total Communication Time: " + total_comm_time);
		System.out.println("Total queue delay: " + total_queue_delay);
		System.out.println("Total slot allocs: " + mAllocs[0]);
		//System.out.println("Total dim1 allocs: " + mAllocs[1]);
		//System.out.println("Total slot allocs: " + mAllocs[2]);
		System.out.println("Total machine allocs: " + mAllocs[3]);
		System.out.println("Total rack allocs: " + mAllocs[4]);
		System.out.println("Total network allocs: " + mAllocs[5]);

		makespan = latest_end_time-earliest_start_time;
		System.out.println("Makespan: " + Double.toString(makespan));
		return makespan;
	}

//	private void printJCT() {
//		double total_jct = 0.0;
//		double jct = 0.0;
//		for(Integer key : mJobStats.keySet()) {
//			jct = mJobStats.get(key).getJobTime();
//			total_jct += jct;
//			mSimResults.get(key).add(jct);
//			System.out.println("Job " + Integer.toString(key) + " ran for time: " +
//					Double.toString(jct));
//		}
//		double avg_jct = total_jct/mJobStats.keySet().size();
//		System.out.println("Average JCT: " + Double.toString(avg_jct));
//	}
//
//	private double printMakespan() {
//		double earliest_start_time = Double.MAX_VALUE;
//		double latest_end_time = Double.MIN_VALUE;
//		double makespan = 0.0;
//
//		for(Integer key : mJobStats.keySet()) {
//			double start_time = mJobStats.get(key).getStartTime();
//			double end_time = mJobStats.get(key).getEndTime();
//			if(start_time < earliest_start_time) {
//				earliest_start_time = start_time;
//			}
//			if(end_time > latest_end_time) {
//				latest_end_time = end_time;
//			}
//		}
//
//		makespan = latest_end_time-earliest_start_time;
//		System.out.println("Makespan: " + Double.toString(makespan));
//		return makespan;
//	}
//	private double printQueueDelay() {
//		double queue_delay = 0.0;
//		double cum_queue_delay = 0.0;
//		for(Integer key : mJobStats.keySet()) {
//			queue_delay = mJobStats.get(key).getQueueDelay();
//			cum_queue_delay += queue_delay;
//			mSimResults.get(key).add(queue_delay);
//			System.out.println("Queue Delay Job " + Integer.toString(key) + ": " + Double.toString(queue_delay));
//		}
//		return cum_queue_delay;
//	}
//
//	private void printGpuTime() {
//		double total_time = 0.0;
//		double gpu_time = 0.0;
//		for(Integer key : mJobStats.keySet()) {
//			gpu_time = mJobStats.get(key).getGpuTime();
//			System.out.println("GPU Time Job " + Integer.toString(key) + ": " + Double.toString(gpu_time));
//			mSimResults.get(key).add(gpu_time);
//			total_time += gpu_time;
//		}
//		System.out.println("Total GPU Time: " + total_time);
//	}
//
//	private void printAllocs() {
//
//		int dim2Allocs = 0;
//		int dim1Allocs = 0;
//		int slotAllocs = 0;
//		int machineAllocs = 0;
//		int rackAllocs = 0;
//
//		int allocs[];
//
//		for(Integer key : mJobStats.keySet()) {
//			allocs = mJobStats.get(key).getAllocs();
//			dim2Allocs += allocs[0];
//			dim1Allocs += allocs[1];
//			slotAllocs += allocs[2];
//			machineAllocs += allocs[3];
//			rackAllocs += allocs[4];
//
//			for (int val: allocs) {
//				mSimResults.get(key).add((double) val);
//			}
//			//total_time += gpu_time;
//		}
//		System.out.println("Total dim2 allocs: " + dim2Allocs);
//		System.out.println("Total dim1 allocs: " + dim1Allocs);
//		System.out.println("Total slot allocs: " + slotAllocs);
//		System.out.println("Total machine allocs: " + machineAllocs);
//		System.out.println("Total rack allocs: " + rackAllocs);
//	}
	// end here

	private void printFinishTimeFairness() {
		double max = 0.0;
		double sum = 0;
		double sum_of_squares = 0;
		for(double d : mFinishTimeFairness) {
			sum += d;
			sum_of_squares += d*d;
			if(d > max) {
				max = d;
			}
			System.out.println("Job FFT: " + Double.toString(d));
		}
		double jf = (sum*sum)/(mFinishTimeFairness.size()*sum_of_squares);
		System.out.println("Finish Time Fairness : " + Double.toString(jf));
		System.out.println("Max Fairness : " + Double.toString(max));
	}



	private void printFairnessIndex() {
		for(FairnessIndex f : mFairnessIndices) {
			System.out.println("JF " + Double.toString(f.getTimestamp()) + 
					" " + Double.toString(f.getFairness()));
		}
	}
	
	private void printLosses() {
		for(LossValue l : mLossValues) {
			System.out.println("Loss " + Double.toString(l.getTimestamp()) + 
					" " + Double.toString(l.getLoss()));
		}
	}

	private void printContentions() {
		double sum = 0.0;
		double max = 0.0;
		for(ContentionValue l : mContention) {
			//System.out.println("Contention " + Double.toString(l.getTimestamp()) +
			//		" " + Double.toString(l.getContention()));
			sum += l.getContention();
			if(l.getContention() > max) {
				max = l.getContention();
			}
		}
		double mean = (double)sum*1.0/(mContention.size()-1);
		double standard_deviation = 0.0;
		for(ContentionValue l : mContention) {
			standard_deviation += Math.pow((l.getContention()-mean), 2);
		}
		standard_deviation = Math.sqrt((standard_deviation)/(mContention.size()-1));
		System.out.println("Mean Contention: " + Double.toString(mean));
		System.out.println("Standard Deviation Contention: " + Double.toString(standard_deviation));
		System.out.println("Peak Contention: " + Double.toString(max));
	}
	
	private class SingleJobStat {
		private double mStartTime;
		private double mEndTime;
		private double mGpuTime;
		private double mCompTime;
		private double mCommTime;
		private double mQueueDelay;
		private double mAvgGPUcontention;
		private int mAllocs[];
		public SingleJobStat(double start_time) {
			mStartTime = start_time;
			mEndTime = -1; // indicates not set
			mGpuTime = 0;
			mCommTime = 0;
			mCompTime = 0;
			mAllocs = new int[6];
		}

		public void setStartTime(double start_time) {
			mStartTime = start_time;
		}
		
		public void setEndTime(double end_time) {
			mEndTime = end_time;
		}

		public void setQueueDelay(double queue_delay) {	mQueueDelay = queue_delay; }

		public double getQueueDelay() {
			return mQueueDelay;
		}
		
		public void setGpuTime(double gpu_time) {
			mGpuTime = gpu_time;
		}


		public void setCompTime(double mCompTime) {
			this.mCompTime = mCompTime;
		}

		public void setCommTime(double mCommTime) {
			this.mCommTime = mCommTime;
		}


		public double getCompTime() {
			return mCompTime;
		}

		public double getCommTime() {
			return mCommTime;
		}

		public double getGpuTime() { return mGpuTime; }

		public double getStartTime() {
			return mStartTime;
		}
		
		public double getEndTime() {
			return mEndTime;
		}

		public void setAllocs(int[] allocs) {
			mAllocs = allocs;
		}

		public int[] getAllocs() {
			return mAllocs;
		}

		public void setAvgGPUcontention(double gpu_contention) { mAvgGPUcontention = gpu_contention; }

		public double getAvgGPUcontention() { return mAvgGPUcontention; }

		public double getJobTime() {
			if(mEndTime == -1) { // not set
				return -1; // invalid request
			}
			return mEndTime - mStartTime;
		}
	}
	
	private class FairnessIndex {
		double mTimestamp;
		double mFairness;
		
		public FairnessIndex(double timestamp, double fairness) {
			mTimestamp = timestamp;
			mFairness = fairness;
		}
		
		public double getTimestamp() {
			return mTimestamp;
		}
		
		public double getFairness() {
			return mFairness;
		}
	}
	
	private class LossValue {
		private double mTimestamp;
		private double mLoss;
		
		public LossValue(double timestamp, double loss) {
			mTimestamp = timestamp;
			mLoss = loss;
		}
		
		public double getTimestamp() {
			return mTimestamp;
		}
		
		public double getLoss() {
			return mLoss;
		}
	}
	
	private class ContentionValue {
		private double mTimestamp;
		private double mContention;
		private int[] mAllocs;
		private int mRunningJobs;
		private int mActiveJobs;
		private double mClusterUtil;

		public ContentionValue(double timestamp, double contention, int [] allocs, int runningJobs, int activeJobs,
							   double clusterUtil) {
			mTimestamp = timestamp;
			mContention = contention;
			mAllocs = new int[6];
			mAllocs[0] = allocs[0];
			mAllocs[1] = allocs[1];
			mAllocs[2] = allocs[2];
			mAllocs[3] = allocs[3];
			mAllocs[4] = allocs[4];
			mAllocs[5] = allocs[5];
			mRunningJobs = runningJobs;
			mActiveJobs = activeJobs;
			mClusterUtil = clusterUtil;
		}
		
		public double getTimestamp() {
			return mTimestamp;
		}
		
		public double getContention() {
			return mContention;
		}
	}
}