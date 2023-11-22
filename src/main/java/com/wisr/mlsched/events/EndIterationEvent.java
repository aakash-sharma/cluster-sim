package com.wisr.mlsched.events;

import com.wisr.mlsched.localsched.IntraJobScheduler;

/**
 * Implementation of event when iteration ends for a job
 */
public class EndIterationEvent extends ClusterEvent {

	private IntraJobScheduler mJob;
	public EndIterationEvent(double timestamp, IntraJobScheduler job) {
		super(timestamp);
		mJob = job;
		setPriority(ClusterEvent.EventType.END_ITERATION);
	}

	@Override
	public void handleEvent() {
		super.handleEvent();
		mJob.endIteration();
	}
	
}