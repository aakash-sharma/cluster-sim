package com.wisr.mlsched;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.wisr.mlsched.events.ClusterEvent;
import com.wisr.mlsched.events.ResourceAvailableEvent;
import com.wisr.mlsched.resources.Cluster;

/**
 * Global event queue to manage cluster level events.
 */
public class ClusterEventQueue {
	
	private static ClusterEventQueue sInstance = null;
	public static TreeSet<ClusterEvent> mEventQueue;
	private static Logger sLog; // Instance of logger
	
	
	/**
	 * Private constructor
	 */
	private ClusterEventQueue() {
		mEventQueue = new TreeSet<ClusterEvent>(new ClusterEventComparator());
		sLog = Logger.getLogger(ClusterEventQueue.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
	}
	
	/**
	 * Get an instance of the cluster event queue
	 * @return instance of ClusterEventQueue
	 */
	public static ClusterEventQueue getInstance() {
		if(sInstance == null) {
			sInstance = new ClusterEventQueue();
		}
		return sInstance;
	}
	
	/**
	 * Enqueue a cluster event to the cluster event queue
	 * @param event
	 */
	public void enqueueEvent(ClusterEvent event) {
		sLog.log(Level.ALL, "Enqueuing event " + event.toString() + " with timestamp " + event.getTimestamp());
		mEventQueue.add(event);
	}
	
	/**
	 * Return the current size of event size
	 * @return integer, correponding to number of events
	 */
	public int getNumberEvents() {
		return mEventQueue.size();
	}
	
	/**
	 * Start event processing
	 */
	public void start() {
		sLog.info("Starting cluster event queue");
		while(!mEventQueue.isEmpty()) {
			ClusterEvent event = mEventQueue.pollFirst();
			//sLog.info("Processing event " + event.toString() + " at " + Double.toString(
			//		event.getTimestamp()));
			event.handleEvent();

			if (mEventQueue.isEmpty() && !Cluster.getInstance().getRunningJobs().isEmpty()) {
				ClusterEventQueue.getInstance()
						.enqueueEvent(new ResourceAvailableEvent(Simulation.getSimulationTime() + 60,
								Cluster.getInstance().getGPUsInCluster()));

			}
		}
	}
	
	/**
	 * Comparator for cluster events
	 */
	private class ClusterEventComparator implements Comparator<ClusterEvent> {

		@Override
		public int compare(ClusterEvent e1, ClusterEvent e2) {
			// First check timestamps
			if(e1.getTimestamp() != e2.getTimestamp()) {
				if(e1.getTimestamp() < e2.getTimestamp()) {
					return -1;
				} else {
					return 1;
				}
			}
			// If timestamps are same, then priority comes into picture
			if(e1.getPriority() != e2.getPriority()) {
				return e1.getPriority() - e2.getPriority();
			}
			// If everything is same, then random ordering
			return 1;
		}
	}
}