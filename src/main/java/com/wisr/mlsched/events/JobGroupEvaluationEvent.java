package com.wisr.mlsched.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.wisr.mlsched.localsched.IntraJobScheduler;
import com.wisr.mlsched.localsched.JobGroupManager;

/**
 * Implementation of event when iteration ends for a job
 */
public class JobGroupEvaluationEvent extends ClusterEvent {

	private Integer mJobGroupId;
	public JobGroupEvaluationEvent(double timestamp, int job_group_id) {
		super(timestamp);
		mJobGroupId = job_group_id;
		setPriority(ClusterEvent.EventType.JOB_GROUP_EVALUATION);
	}

	@Override
	public void handleEvent() {
		super.handleEvent();
		List<IntraJobScheduler> jobs = JobGroupManager.getInstance().getJobsInGroup(mJobGroupId);
		Collections.sort(jobs, new JobIterationComparator()); // sort in ascending order of iterations
		long max_iterations = jobs.get(jobs.size()-1).getmTotalExpectedIterations();
		if(jobs.size() == 1) {
			return;
		}
		long iteration_step = max_iterations/(int)(Math.log10(jobs.size())/Math.log10(2));
		int index = 0;
		int count = 1;
		while(index < jobs.size()-1) {
			int num_jobs = (jobs.size()-index)/2;
			for(int i=index;i<index+num_jobs;i++) {
				jobs.get(i).setRole(false, Math.min(iteration_step*count, 
						jobs.get(i).getmTotalExpectedIterations()));
			}
			index +=  num_jobs;
			count++;
		}
	}

	private class JobIterationComparator implements Comparator<IntraJobScheduler> {

		@Override
		public int compare(IntraJobScheduler arg0, IntraJobScheduler arg1) {
			if(arg0.getmTotalExpectedIterations() != arg1.getmTotalExpectedIterations()) {
				return (int)(arg0.getmTotalExpectedIterations() - arg1.getmTotalExpectedIterations());
			}
			return arg0.getJobId() - arg1.getJobId();
		}
		
	}
}