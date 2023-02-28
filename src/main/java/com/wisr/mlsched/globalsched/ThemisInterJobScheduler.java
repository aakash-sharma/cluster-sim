package com.wisr.mlsched.globalsched;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.wisr.mlsched.ClusterEventQueue;
import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.job.Bid;
import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.resources.Cluster;
import com.wisr.mlsched.resources.GPU;

import gurobi.*;

public class ThemisInterJobScheduler extends InterJobScheduler {

	public List<Bid> pickWinningBids(List<GPU> gpu_set, List<Bid> bid_set) {
		List<Bid> winners = new ArrayList<Bid>();
		GRBEnv env;
		GRBModel solver;
		try {
			System.out.println("Picking Winning Bids");
			env = new GRBEnv("themis_gurobi.log");
			env.set(GRB.IntParam.LogToConsole, 0);
			solver = new GRBModel(env);
			solver.set(GRB.DoubleParam.TimeLimit, 300.0);
			// create helpers and gurobi variables per bid
			HashMap<GPU, Set<Bid>> bidsPerGPU = new HashMap<GPU, Set<Bid>>();
			HashMap<Bid, GRBVar> bidVariables = new HashMap<Bid, GRBVar>();
			HashMap<IntraJobScheduler, GRBVar> jobVariables = new HashMap<IntraJobScheduler, GRBVar>();
			HashMap<IntraJobScheduler, Set<Bid>> bidsPerJob = new HashMap<IntraJobScheduler, Set<Bid>>();
			for (Bid bid: bid_set) {
				bidVariables.put(bid, solver.addVar(0.0, 1.0, 0.0, GRB.BINARY, ""));
				for (GPU gpu: bid.getGPUList()) {
					if (bidsPerGPU.containsKey(gpu)) {
						bidsPerGPU.get(gpu).add(bid);
					} else {
						Set<Bid> bids = new HashSet<Bid>();
						bids.add(bid);
						bidsPerGPU.put(gpu, bids);
					}
				}
				if (bidsPerJob.containsKey(bid.getJob())) {
					bidsPerJob.get(bid.getJob()).add(bid);
				} else {
					Set<Bid> bids = new HashSet<Bid>();
					bids.add(bid);
					bidsPerJob.put(bid.getJob(), bids);
					jobVariables.put(bid.getJob(), solver.addVar(0.0, 1.0, 0.0, GRB.BINARY, ""));
				}
			}
			solver.update();
			// create gurobi constraints per gpu, per job
			for (GPU gpu: bidsPerGPU.keySet()) {
				GRBLinExpr packingConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerGPU.get(gpu)) {
					packingConstraint.addTerm(1.0, bidVariables.get(bid));
				}
				solver.addConstr(packingConstraint, GRB.LESS_EQUAL, 1.0, "");
			}
			for (IntraJobScheduler job: bidsPerJob.keySet()) {
				GRBLinExpr jobMaxParallelConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerJob.get(job)) {
					jobMaxParallelConstraint.addTerm(bid.getGPUList().size(), bidVariables.get(bid));
				}
				solver.addConstr(jobMaxParallelConstraint, GRB.LESS_EQUAL, 
						job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size(),  "");
				GRBLinExpr jobOneBidOnlyConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerJob.get(job)) {
					jobOneBidOnlyConstraint.addTerm(1.0, bidVariables.get(bid));
				}
				solver.addConstr(jobOneBidOnlyConstraint, GRB.EQUAL, jobVariables.get(job), "");
			}
			// create gurobi objective for maximizing product of bid valuations
			GRBLinExpr valuationObjective = new GRBLinExpr();
			for (Bid bid: bid_set) {
//				// potential problem in logarithm
//				double logExpectedBenefit = -1000;
//				if (Double.compare(bid.getExpectedBenefit(), 1) < 0) {
//					logExpectedBenefit = Math.log10(bid.getExpectedBenefit());
//				}
//				if (Double.compare(bid.getExpectedBenefit(), 0) == 0) {
//					logExpectedBenefit = -1000;
				//double logExpectedBenefit = 0.00000001;
				//if (Double.compare(bid.getExpectedBenefit(), 1) > 0) {
					//logExpectedBenefit = Math.log(bid.getExpectedBenefit()); // log(T_s_new/T_i_new)
				//}
				double logExpectedBenefit = 0.00001;
				if(Double.compare(bid.getExpectedBenefit(),1) > 0) {
					logExpectedBenefit = Math.log(bid.getExpectedBenefit());
				}
//				double logOldBenefit = 1000;
//				if (!Double.isInfinite(bid.getOldBenefit())) {
//					logOldBenefit = Math.log(bid.getOldBenefit());  // log(T_s_old/T_i_old)	
//				}
				
				//if (Double.compare(bid.getExpectedBenefit(), 1) > 0) {
				//	logExpectedBenefit = Math.log(bid.getExpectedBenefit());
				//}
				valuationObjective.addTerm(logExpectedBenefit, bidVariables.get(bid));
//				valuationObjective.addTerm(-logOldBenefit, bidVariables.get(bid));
//				valuationObjective.addConstant(logOldBenefit);
			}
			/*for (IntraJobScheduler job: bidsPerJob.keySet()) {
				double logOldBenefit = 1000;
				if (!Double.isInfinite(job.getOldBenefit())) {
					logOldBenefit = Math.log(job.getOldBenefit());  // log(T_s_old/T_i_old)	
				}
				valuationObjective.addTerm(-logOldBenefit, jobVariables.get(job));
				valuationObjective.addConstant(logOldBenefit);
			}*/
			//solver.setObjective(valuationObjective, GRB.MINIMIZE);
			solver.setObjective(valuationObjective, GRB.MAXIMIZE);
			solver.update();
			// optimize
			System.out.println("Beginning to Solve...");
			solver.optimize();
			double maxValuation =  0.0;
			if (solver.get(GRB.IntAttr.Status) == GRB.Status.OPTIMAL
					|| solver.get(GRB.IntAttr.Status) == GRB.Status.TIME_LIMIT ) {
				maxValuation = solver.get(GRB.DoubleAttr.ObjVal);
				for (Bid bid: bidVariables.keySet()) {
					GRBVar var = bidVariables.get(bid);
					if (var.get(GRB.DoubleAttr.X) == 1) {
						winners.add(bid);
					}
				}

				// for (IntraJobScheduler job: jobVariables.keySet()) {
				// 	double maxValuationAbsentJob = pickWinningBidsTruthTelling(gpu_set, bid_set, job);
				// 	// double maxValuationPFJob = maxValuation/jobWinningValuation.get(job);
				// 	double hiddenPayment = 1 - maxValuation/maxValuationAbsentJob;
				// 	System.out.println("Hidden Payment: " + hiddenPayment);
				// }
			} else {
				solver.computeIIS();
	            solver.write(".quark-unsat.ilp");
				System.out.println("UNSOLVABLE.");
			}
			solver.dispose();
			env.dispose();
		} catch (GRBException e) {
			System.out.println("Gurobi error " + e.getErrorCode() + ": " + e.getMessage());
			// Get current size of heap in bytes
			long heapSize = Runtime.getRuntime().totalMemory();

// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
			long heapMaxSize = Runtime.getRuntime().maxMemory();

			// Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
			long heapFreeSize = Runtime.getRuntime().freeMemory();

			System.out.println("heap size = " + heapSize + "\nheap max size = " + heapMaxSize + "\nheap free size = " + heapFreeSize);
			e.printStackTrace();
		}
		return winners;
	}

	public double pickWinningBidsTruthTelling(List<GPU> gpu_set, List<Bid> bid_set, IntraJobScheduler jobEx) {
		List<Bid> winners = new ArrayList<Bid>();
		GRBEnv env;
		GRBModel solver;
		double maxValuation = 0.0;
		try {
			env = new GRBEnv("themis_gurobi_tt.log");
			env.set(GRB.IntParam.LogToConsole, 0);
			solver = new GRBModel(env);
			solver.set(GRB.DoubleParam.TimeLimit, 300.0);
			// create helpers and gurobi variables per bid
			HashMap<GPU, Set<Bid>> bidsPerGPU = new HashMap<GPU, Set<Bid>>();
			HashMap<Bid, GRBVar> bidVariables = new HashMap<Bid, GRBVar>();
			HashMap<IntraJobScheduler, GRBVar> jobVariables = new HashMap<IntraJobScheduler, GRBVar>();
			HashMap<IntraJobScheduler, Set<Bid>> bidsPerJob = new HashMap<IntraJobScheduler, Set<Bid>>();
			for (Bid bid: bid_set) {
				if (bid.getJob() == jobEx) {
					continue;
				}
				bidVariables.put(bid, solver.addVar(0.0, 1.0, 0.0, GRB.BINARY, ""));
				for (GPU gpu: bid.getGPUList()) {
					if (bidsPerGPU.containsKey(gpu)) {
						bidsPerGPU.get(gpu).add(bid);
					} else {
						Set<Bid> bids = new HashSet<Bid>();
						bids.add(bid);
						bidsPerGPU.put(gpu, bids);
					}
				}
				if (bidsPerJob.containsKey(bid.getJob())) {
					bidsPerJob.get(bid.getJob()).add(bid);
				} else {
					Set<Bid> bids = new HashSet<Bid>();
					bids.add(bid);
					bidsPerJob.put(bid.getJob(), bids);
					jobVariables.put(bid.getJob(), solver.addVar(0.0, 1.0, 0.0, GRB.BINARY, ""));
				}
			}
			solver.update();
			// create gurobi constraints per gpu, per job
			for (GPU gpu: bidsPerGPU.keySet()) {
				GRBLinExpr packingConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerGPU.get(gpu)) {
					packingConstraint.addTerm(1.0, bidVariables.get(bid));
				}
				solver.addConstr(packingConstraint, GRB.LESS_EQUAL, 1.0, "");
			}
			for (IntraJobScheduler job: bidsPerJob.keySet()) {
				GRBLinExpr jobMaxParallelConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerJob.get(job)) {
					jobMaxParallelConstraint.addTerm(bid.getGPUList().size(), bidVariables.get(bid));
				}
				solver.addConstr(jobMaxParallelConstraint, GRB.LESS_EQUAL, 
						job.getMaxParallelism() - job.getGPUsAvailableForNextIteration().size(),  "");
				GRBLinExpr jobOneBidOnlyConstraint = new GRBLinExpr();
				for (Bid bid: bidsPerJob.get(job)) {
					jobOneBidOnlyConstraint.addTerm(1.0, bidVariables.get(bid));
				}
				solver.addConstr(jobOneBidOnlyConstraint, GRB.EQUAL, jobVariables.get(job), "");
			}
			// create gurobi objective for maximizing product of bid valuations
			GRBLinExpr valuationObjective = new GRBLinExpr();
			for (Bid bid: bid_set) {
				if (bid.getJob() == jobEx) {
					continue;
				}
//				// potential problem in logarithm
//				double logExpectedBenefit = -1000;
//				if (Double.compare(bid.getExpectedBenefit(), 1) < 0) {
//					logExpectedBenefit = Math.log10(bid.getExpectedBenefit());
//				}
//				if (Double.compare(bid.getExpectedBenefit(), 0) == 0) {
//					logExpectedBenefit = -1000;
				//double logExpectedBenefit = 0.00000001;
				//if (Double.compare(bid.getExpectedBenefit(), 1) > 0) {
					//logExpectedBenefit = Math.log(bid.getExpectedBenefit()); // log(T_s_new/T_i_new)
				//}
				double logExpectedBenefit = 0.00001;
				if(Double.compare(bid.getExpectedBenefit(),1) > 0) {
					logExpectedBenefit = Math.log(bid.getExpectedBenefit());
				}
//				double logOldBenefit = 1000;
//				if (!Double.isInfinite(bid.getOldBenefit())) {
//					logOldBenefit = Math.log(bid.getOldBenefit());  // log(T_s_old/T_i_old)	
//				}
				
				//if (Double.compare(bid.getExpectedBenefit(), 1) > 0) {
				//	logExpectedBenefit = Math.log(bid.getExpectedBenefit());
				//}
				valuationObjective.addTerm(logExpectedBenefit, bidVariables.get(bid));
//				valuationObjective.addTerm(-logOldBenefit, bidVariables.get(bid));
//				valuationObjective.addConstant(logOldBenefit);
			}
			/*for (IntraJobScheduler job: bidsPerJob.keySet()) {
				double logOldBenefit = 1000;
				if (!Double.isInfinite(job.getOldBenefit())) {
					logOldBenefit = Math.log(job.getOldBenefit());  // log(T_s_old/T_i_old)	
				}
				valuationObjective.addTerm(-logOldBenefit, jobVariables.get(job));
				valuationObjective.addConstant(logOldBenefit);
			}*/
			//solver.setObjective(valuationObjective, GRB.MINIMIZE);
			solver.setObjective(valuationObjective, GRB.MAXIMIZE);
			solver.update();
			// optimize
			solver.optimize();
			if (solver.get(GRB.IntAttr.Status) == GRB.Status.OPTIMAL
					|| solver.get(GRB.IntAttr.Status) == GRB.Status.TIME_LIMIT ) {
				maxValuation = solver.get(GRB.DoubleAttr.ObjVal);
				for (Bid bid: bidVariables.keySet()) {
					GRBVar var = bidVariables.get(bid);
					if (var.get(GRB.DoubleAttr.X) == 1) {
						winners.add(bid);
					}
				}
			} else {
				solver.computeIIS();
	            solver.write(".quark-unsat.ilp");
				System.out.println("UNSOLVABLE.");
			}
		} catch (GRBException e) {
			e.printStackTrace();
		}
		return maxValuation;
	}

	private void admissionControl() {
		double maxRho = 0.0;
		double minStartTimeQueuedJob = Double.MAX_VALUE;
		IntraJobScheduler minJob = null;
		List<IntraJobScheduler> allJobsInCluster = Cluster.getInstance().getRunningJobs();
	    // implementing FIFO
		for(IntraJobScheduler job: allJobsInCluster) {
			if (job.isQueued()) {
				double jobStartTime = job.getmJobStartTime();
				if (jobStartTime < minStartTimeQueuedJob) {
					minJob = job;
					minStartTimeQueuedJob = jobStartTime;
				}
			} else {
				double jobRho = job.getCurrentEstimateForThemis();
				if (jobRho > maxRho) {
					maxRho = jobRho;
				}
			}
		}
		// implementing Admission Control Threshold
		if (maxRho < 1.5) {
			minJob.dequeue(Simulation.getSimulationTime());
		}
	}

	@Override
	public void onResourceAvailable(List<GPU> gpu_set) {
		if(Cluster.getInstance().getRunningJobs().size() == 0) {
			// Means no jobs are running, hence nothing to do
			System.out.println("No jobs");
			return;
		}
		if (Simulation.isAdmissionControlEnabled()) {
			admissionControl();
		}
		List<IntraJobScheduler> allJobsInCluster = Cluster.getInstance().getRunningJobs();
		List<Integer> jobsToConsider = new ArrayList<>();
		for(IntraJobScheduler job : allJobsInCluster) {
			if (Simulation.isAdmissionControlEnabled() && job.isQueued()) {
				continue;
			} 
			jobsToConsider.add(job.getJobId());
		}
		gpu_set = gpu_set.stream().filter(g -> !g.isLeased()).collect(Collectors.toList());
		while(gpu_set.size() > 0) {
			List<JobFairness> fairnessValues = new ArrayList<JobFairness>();
			List<IntraJobScheduler> runningJobs = Cluster.getInstance().getRunningJobs();
			for(IntraJobScheduler job: runningJobs) {
				if(jobsToConsider.contains(job.getJobId()) && job.willParticipateInBid()) {
					fairnessValues.add(new JobFairness(job, job.getCurrentEstimateForThemis()));
					System.out.println("Job: " + job.getJobId() + " Fairness: " + 
					    Double.toString(job.getCurrentEstimateForThemis()));
				}
			}
			if(fairnessValues.size() == 0) {
				System.out.println("No fairness values");
				break;
			}
			// Sort in descending order of fairness - Most unfair to most fair
			Collections.sort(fairnessValues, new JobFairnessComparator());
			double d = Cluster.getInstance().getFairnessThreshold();
			
			// We need to now check which jobs to consider for bidding
			int num_jobs_to_consider = Math.max((int) (d*fairnessValues.size()), 1);
			double lowest_value_to_consider = fairnessValues.get(num_jobs_to_consider-1).getFairnessValue();
			// These are jobs we will definitely consider for bidding.
			List<JobFairness> fairnessValuesDefinite = new ArrayList<>();
			// These are jobs that are tied at the lowest Ts/Ti. We will randomly pick jobs from this list.
			List<JobFairness> fairnessValuesProbable = new ArrayList<>();
			for(JobFairness job: fairnessValues) {
				if(Double.compare(job.getFairnessValue(), lowest_value_to_consider) > 0) {
					fairnessValuesDefinite.add(job);
				} else if(Double.compare(job.getFairnessValue(), lowest_value_to_consider) == 0) {
					fairnessValuesProbable.add(job);
				}
			}
			List<JobFairness> allJobs = new ArrayList<>();
			// First add in all definite jobs
			allJobs.addAll(fairnessValuesDefinite);
			// Shuffle and add in probable jobs
			Collections.shuffle(fairnessValuesProbable, new Random(0));
			allJobs.addAll(fairnessValuesProbable);
			// Take number of jobs stipulated by fairness knob
			allJobs = allJobs.subList(0, num_jobs_to_consider);
			
			// Get bids from above jobs
			List<Bid> bids = new ArrayList<Bid>();
			for(JobFairness job: allJobs) {
				System.out.println("Bid from job: " + job.getJob().getJobId());
				List<Bid> bidsFromJob = job.getJob().prepareBid(gpu_set);
				if(bidsFromJob != null && !bidsFromJob.isEmpty()) {
					bids.addAll(bidsFromJob);
				}
			}

			Collections.sort(bids, new PerGPUBidComparator());
			//System.out.println(bids);
			
			// If no bids, let's see if others not selected have any resources requirements
			if(bids.size() == 0) {
				System.out.println("No bids");
				for(JobFairness job : allJobs) {
					jobsToConsider.remove(new Integer(job.getJob().getJobId()));
				}
				continue;
			}
			List<GPU> remainingGPUSet = new ArrayList<>(gpu_set);
			
			List<Bid> winningBids = pickWinningBids(gpu_set, bids);//finalBids);
			//System.out.println(winningBids);
			if(winningBids.size() == 0) {
				System.out.println("Have a problem!");
				for(Bid bid : bids) {
					System.out.println("Bid: " + bid);
				}
				for (GPU gpu: gpu_set) {
					System.out.println("GPU: " + gpu.getLocation().getPrettyString());
				}
				System.exit(0);
			}
			for(Bid bid : winningBids) {
				//System.out.println("Winning Bids: " + bid.toString());
				bid.getJob().notifyResourceAssignment(bid.getGPUList());
				remainingGPUSet.removeAll(bid.getGPUList());
				//System.out.println(remainingGPUSet.size());
				for(GPU g: bid.getGPUList()) {
					g.assignGPU(Cluster.getInstance().getLeaseTime(), bid.getJob());
				}
				if(remainingGPUSet.size() == 0) {
					break;
				}
			}
			gpu_set = remainingGPUSet;
		}
		startWaitingJobs();
	}
	
	private boolean isGpuPresent(GPU gpu, List<GPU> gpuList) {
		for(GPU g: gpuList) {
			if(g.getLocation().compareTo(gpu.getLocation())) {
				return true;
			}
		}
		return false;
	}
	
	private class JobLossGradient {
		private IntraJobScheduler mJob;
		private double mGradient;
		
		public JobLossGradient(IntraJobScheduler job, double gradient) {
			mJob = job;
			mGradient = gradient;
		}
		
		public IntraJobScheduler getJob() {
			return mJob;
		}
		
		public double getLossGradient() {
			return mGradient;
		}
	}
	
	private class JobFairness {
		private IntraJobScheduler mJob;
		private Double mFairnessValue;
		
		public JobFairness(IntraJobScheduler job, double value) {
			mJob = job;
			mFairnessValue = value;
		}
		
		public IntraJobScheduler getJob() {
			return mJob;
		}
		
		public double getFairnessValue() {
			return mFairnessValue;
		}
	}
	
	private class JobFairnessComparator implements Comparator<JobFairness> {

		@Override
		public int compare(JobFairness j1, JobFairness j2) {
			int comp = Double.compare(j1.getFairnessValue(), j2.getFairnessValue());
			if(comp != 0) {
				return -1*comp;
			}
			return j1.getJob().getJobId() - j2.getJob().getJobId();
		}
		
	}
	
	private class LossGradientComparator implements Comparator<JobLossGradient> {

		@Override
		public int compare(JobLossGradient j1, JobLossGradient j2) {
			int comp = Double.compare(j1.getLossGradient(), j2.getLossGradient());
			if(comp != 0) {
				return -1*comp;
			}
			return j1.getJob().getJobId() - j2.getJob().getJobId();
		}
		
	}
}
