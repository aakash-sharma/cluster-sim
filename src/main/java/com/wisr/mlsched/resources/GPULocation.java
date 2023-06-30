package com.wisr.mlsched.resources;

/**
 * Represents the physical location coordinates of the GPU.
 */
public class GPULocation {
	private int mGpuId; // Unique GPU ID
    private int mDim2Id; // Dim2 Dim1 slot within a machine that GPU belongs to
    private int mDim1Id; // Dim1 Slot within a machine that GPU belongs to
    private int mSlotId; // Slot within a machine that GPU belongs to
    private int mMachineId; // Machine on rack that GPU belongs to
    private int mRackId; // Rack ID within cluster
    
    /**
     * Constructor for GPULocation object
     * @param gpu_id
     * @param slot_id
     * @param machine_id
     * @param rack_id
     */
    public GPULocation(int gpu_id, int dim2_id, int dim1_id, int slot_id, int machine_id, int rack_id) {
    	this.mGpuId = gpu_id;
        this.mDim2Id = dim2_id;
        this.mDim1Id = dim1_id;
    	this.mSlotId = slot_id;
    	this.mMachineId = machine_id;
    	this.mRackId = rack_id;
    }

    public GPULocation(int dim2_id, int dim1_id, int slot_id, int machine_id, int rack_id) {
        this.mGpuId = 0;
        this.mDim2Id = dim2_id;
        this.mDim1Id = dim1_id;
        this.mSlotId = slot_id;
        this.mMachineId = machine_id;
        this.mRackId = rack_id;
    }
    /**
     * Returns the GPU ID
     */
    public int getGPUId() {
    	return mGpuId;
    }

    /**
     * Returns the GPU's dim1 Slot ID
     */
    public int getDim1Id() {
        return mDim1Id;
    }

    /**
     * Returns the GPU's dim2 dim1 Slot ID
     */
    public int getDim2Id() {
        return mDim2Id;
    }

    /**
     * Returns the GPU's Slot ID
     */
    public int getSlotId() {
    	return mSlotId;
    }
    
    /**
     * Returns the GPU's Machine ID
     */
    public int getMachineId() {
    	return mMachineId;
    }
    
    /**
     * Returns the GPU's Rack ID
     */
    public int getRackId() {
    	return mRackId;
    }
    
    /**
     * Returns a pretty string representation of the GPU location
     */
    public String getPrettyString() {
    	return "(Rack,Machine,Slot,GPUID):" +
			Integer.toString(mRackId) + "," +
			Integer.toString(mMachineId) + "," +
			Integer.toString(mSlotId) + "," +
			Integer.toString(mGpuId);
    }
    
    public boolean compareTo(GPULocation location) {
    	if(mRackId == location.getRackId() && mMachineId == location.getMachineId()
    			&& mSlotId == location.getSlotId() && mGpuId == location.getGPUId()) {
    		return true;
    	}
    	return false;
    }
}