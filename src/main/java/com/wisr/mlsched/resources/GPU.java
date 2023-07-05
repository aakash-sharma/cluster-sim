package com.wisr.mlsched.resources;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.wisr.mlsched.Simulation;
import com.wisr.mlsched.localsched.IntraJobScheduler;

/**
 * GPU represents the location of GPU and
 * information about which job holds the GPU.
 */
public class GPU {
	private final long NOT_LEASED = -1; // Constant for not leased
	
	private GPULocation mGpuLocation;
	private double mLeaseStart; // Start time of GPU lease
    private double mLeaseDuration; // Duration of GPU lease
    private IntraJobScheduler mJobUsingGPU; // Job currently using the GPU
    private static Logger sLog; // Instance of logger
    
    /**
     * Constructor for GPU object
     * @param location
     */
    public GPU(GPULocation location) {
    	mGpuLocation = location;
    	mLeaseStart = NOT_LEASED;
    	mLeaseDuration = NOT_LEASED;
    	mJobUsingGPU = null;
    	sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
		sLog.log(Level.ALL, "Created new GPU " + location.getPrettyString());
    }
    
    @Override
    public String toString() {
    	String out = mGpuLocation.getPrettyString();
    	if(mJobUsingGPU == null) {
    		out = out + " : unassigned"; 
    	} else {
    		out = out + " : " + Integer.toString(mJobUsingGPU.getJobId());
    	}
    	return out;
    }
    
    /**
     * Assign the GPU to a job for a time specified by lease
     * @param lease_duration_ms
     * @param job
     */
    public void assignGPU(double lease_duration, IntraJobScheduler job) {
    	// TODO: assert that already not leased to another job
    	//sLog.info("Assigning GPU " + mGpuLocation.getPrettyString() + " to job "
    	//		+ Integer.toString(job.getJobId()) + " for lease "
        //		+ Double.toString(lease_duration));
    	mLeaseDuration = lease_duration;
    	mLeaseStart = Simulation.getSimulationTime();
    	mJobUsingGPU = job;
    }
    
    /**
     * Check if the GPU has been leased to a job
     * @return true if GPU has been leased, false otherwise
     */
    public boolean isLeased() {
    	return mLeaseStart != NOT_LEASED;
    }
    
    /**
     * Check if the job's lease on the GPU has expired
     * @return true if leased has expired, false otherwise
     */
    public boolean hasLeaseExpired() {
    	if (Simulation.getSimulationTime() >= mLeaseStart + mLeaseDuration) {
    		return true;
    	}
    	return false;
    }
    
    /**
     * Returns the job that has leased this GPU
     * @return instance of IntraJobScheduler that holds lease to the GPU
     */
    public IntraJobScheduler getJob() {
    	return mJobUsingGPU;
    }
    
    /**
     * Returns the location for this GPU
     * @return instance of GPULocation
     */
    public GPULocation getLocation() {
    	return mGpuLocation;
    }
    
    /**
     * Mark this GPU as not belonging to any job.
     */
    public void markLeaseEnd() {
    	sLog.info("Marking lease ended for GPU " + mGpuLocation.getPrettyString()
    		+ " for job " + mJobUsingGPU.getJobId());
    	mLeaseStart = NOT_LEASED;
    	mLeaseDuration = NOT_LEASED;
    	mJobUsingGPU = null;
    }
    
    /**
     * Return the lease end time for this GPU.
     */
    public double getLeaseEnd() {
    	return mLeaseStart + mLeaseDuration;
    }

    public int getGPUId() {
      return mGpuLocation.getGPUId();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }

      if (!(o instanceof GPU)) {
        return false;
      }

      GPU gpu = (GPU) o;

    	return mGpuLocation.compareTo(gpu.getLocation());
    }
}