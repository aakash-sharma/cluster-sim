package com.wisr.mlsched.job;

import java.io.IOException;
import java.util.*;
import java.io.File;
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
	private HashMap<Integer, SingleJobStat> mJobTime; // Job start and end time per job
	private List<FairnessIndex> mFairnessIndices; // List of fairness indices measured over time
	private List<LossValue> mLossValues; // List of cumulative loss values measured over time
	private List<Double> mFinishTimeFairness; // List of Ts/Ti for leader jobs
	private List<ContentionValue> mContention; // List of contention numbers over time
	private TreeMap<Double,Integer> mGPUContention; // Map of contention over time
	private XSSFWorkbook mWorkbook;
	Map<Integer, Vector<Double>> mSimResults;
	private static int lastContention = 0;
	
	/**
	 * Private constructor to enforce singleton
	 */
	private JobStatistics() {
		mJobTime = new HashMap<Integer, SingleJobStat>();
		mFairnessIndices = new ArrayList<FairnessIndex>();
		mLossValues = new ArrayList<LossValue>();
		mFinishTimeFairness = new ArrayList<Double>();
		mContention = new ArrayList<ContentionValue>();
		mGPUContention = new TreeMap<Double, Integer>();
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
		mJobTime.put(jobid, new SingleJobStat(timestamp));
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
			boolean isLeader, double gpu_time, int gpu_demand, double queue_delay) {
		if(mGPUContention.get(timestamp) == null) {
			mGPUContention.put(timestamp, 0);
		}
		if(lastContention <= Cluster.getInstance().getGPUsInCluster().size()) {
			mGPUContention.put(timestamp, 1);	
		} else {
			mGPUContention.put(timestamp, lastContention/Cluster.getInstance().getGPUsInCluster().size());
		}
		mGPUContention.put(timestamp, lastContention);
		mJobTime.get(jobid).setStartTime(start_time);
		mJobTime.get(jobid).setEndTime(timestamp);
		mJobTime.get(jobid).setGpuTime(gpu_time);
		mJobTime.get(jobid).setQueueDelay(queue_delay);
		mFinishTimeFairness.add((timestamp-start_time)/(ideal_running_time*avg_contention_for_job(jobid)));
	}
	
	private double avg_contention_for_job(int jobid) {
		double numerator = 0.0;
		double denominator = 0.0;
		double startTime = mJobTime.get(jobid).getStartTime();
		double endTime = mJobTime.get(jobid).getEndTime();
		
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
		mContention.add(new ContentionValue(Simulation.getSimulationTime(), computeCurrentContention()));
		if(ClusterEventQueue.getInstance().getNumberEvents() > 0) {
			ClusterEventQueue.getInstance().enqueueEvent(new 
					JobStatisticEvent(Simulation.getSimulationTime() + Cluster.getInstance().getLeaseTime()));
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
		for(Integer key : mJobTime.keySet()) {
			mSimResults.put(key, new Vector<Double>());
		}
		printJCT();
		printGpuTime();
		//printFairnessIndex();
		//printLosses();
		//printContentions();
		printQueueDelay();
		printFinishTimeFairness();
		makespan = printMakespan();

		XSSFSheet sheet1 = mWorkbook.createSheet(" Sim-stats ");
		XSSFRow row;
		int rowid = 0;
		int cellid = 0;
		Cell cell;
		String[] headers = {"JobId", "JCT", "GPU-time", "Queue-delay", "makespan"};
		row = sheet1.createRow(rowid++);

		for (String str : headers) {
			cell = row.createCell(cellid++);
			cell.setCellValue(str);
		}
		row = sheet1.createRow(rowid++);
		for (int i = 0; i < headers.length-1; i++) {
			cell = row.createCell(cellid++);
			cell.setCellValue("");
		}

		cell = row.createCell(headers.length-1);
		cell.setCellValue(makespan);

		for(Integer key : mJobTime.keySet()) {

			row = sheet1.createRow(rowid++);
			Vector<Double> values = mSimResults.get(key);
			cellid = 0;
			cell = row.createCell(cellid++);
			cell.setCellValue(key);

			for (Double val : values) {
				cell = row.createCell(cellid++);
				cell.setCellValue(val);
			}
		}

		String policy = Cluster.getInstance().getConfiguration().getPolicy();
		String topo_name = Cluster.getInstance().getConfiguration().getmTopoName();

		try {
			FileOutputStream out = new FileOutputStream(new File("results/results_" + policy + "_" + topo_name
					+ ".xlsx"));
			mWorkbook.write(out);
			out.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void printQueueDelay() {
		double queue_delay = 0.0;
		for(Integer key : mJobTime.keySet()) {
			queue_delay = mJobTime.get(key).getQueueDelay();
			mSimResults.get(key).add(queue_delay);
			System.out.println("Queue Delay Job " + Integer.toString(key) + ": " + Double.toString(queue_delay));
		}

	}
	
	private void printGpuTime() {
		double total_time = 0.0;
		double gpu_time = 0.0;
		for(Integer key : mJobTime.keySet()) {
			gpu_time = mJobTime.get(key).getGpuTime();
			System.out.println("GPU Time Job " + Integer.toString(key) + ": " + Double.toString(gpu_time));
			mSimResults.get(key).add(gpu_time);
			total_time += gpu_time;
		}
		System.out.println("Total GPU Time: " + total_time);
	}
	
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
	
	private void printJCT() {
		double total_jct = 0.0;
		double jct = 0.0;
		for(Integer key : mJobTime.keySet()) {
			jct = mJobTime.get(key).getJobTime();
			total_jct += jct;
			mSimResults.get(key).add(jct);
			System.out.println("Job " + Integer.toString(key) + " ran for time: " +
					Double.toString(jct));
		}
		double avg_jct = total_jct/mJobTime.keySet().size();
		System.out.println("Average JCT: " + Double.toString(avg_jct));
	}
	
	private double printMakespan() {
		double earliest_start_time = Double.MAX_VALUE;
		double latest_end_time = Double.MIN_VALUE;
		double makespan = 0.0;

		for(Integer key : mJobTime.keySet()) {
			double start_time = mJobTime.get(key).getStartTime();
			double end_time = mJobTime.get(key).getEndTime();
			if(start_time < earliest_start_time) {
				earliest_start_time = start_time;
			}
			if(end_time > latest_end_time) {
				latest_end_time = end_time;
			}
		}

		makespan = latest_end_time-earliest_start_time;
		System.out.println("Makespan: " + Double.toString(makespan));
		return makespan;
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
			System.out.println("Contention " + Double.toString(l.getTimestamp()) + 
					" " + Double.toString(l.getContention()));
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
		private double mQueueDelay;
		
		public SingleJobStat(double start_time) {
			mStartTime = start_time;
			mEndTime = -1; // indicates not set
		}

		public void setStartTime(double start_time) {
			mStartTime = start_time;
		}
		
		public void setEndTime(double end_time) {
			mEndTime = end_time;
		}

		public void setQueueDelay(double queue_delay) {
			mQueueDelay = queue_delay;
		}

		public double getQueueDelay() {
			return mQueueDelay;
		}
		
		public void setGpuTime(double gpu_time) {
			mGpuTime = gpu_time;
		}
		
		public double getStartTime() {
			return mStartTime;
		}
		
		public double getEndTime() {
			return mEndTime;
		}
		
		public double getGpuTime() {
			return mGpuTime;
		}
		
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
		
		public ContentionValue(double timestamp, double contention) {
			mTimestamp = timestamp;
			mContention = contention;
		}
		
		public double getTimestamp() {
			return mTimestamp;
		}
		
		public double getContention() {
			return mContention;
		}
	}
}