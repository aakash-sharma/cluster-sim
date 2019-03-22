package com.wisr.mlsched;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import org.json.simple.JSONObject;

public class AAMIntraJobScheduler extends IntraJobScheduler {

	private static Logger sLog; // Instance of logger

	public AAMIntraJobScheduler(JSONObject config) {
		super(config);
		sLog = Logger.getLogger(Cluster.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
	}

	@Override
	public List<Bid> prepareBid(List<GPU> offeredGPUs) {
		if (getGPUsAvailableForNextIteration().size() >= mMaxParallelism) {
			// Already have enough GPUs. No need to bid
			return null;
		}
		List<Bid> bids = new ArrayList<Bid>();
		int n = (int) Math.pow(2, offeredGPUs.size())-1;
		Queue<String> q = new LinkedList<String>();
		q.add("1");
		while (n-- > 0) {
			String s1 = q.peek();
			q.remove();
			Bid bid = prepareBidWithGPUs(s1, offeredGPUs);
			if(bid != null) {
				bids.add(bid);
			}
			String s2 = s1;
			q.add(s1 + "0");
			q.add(s2 + "1");
		}
		return bids;
	}
	
	private Bid prepareBidWithGPUs(String bitmask, List<GPU>offeredGPUs) {
		String paddedBitMask = padLeftZeros(bitmask, offeredGPUs.size());
		List<GPU> gpusForBid = new ArrayList<GPU>();
		char[] bitmaskArray = paddedBitMask.toCharArray();
		for(int i=0;i<bitmaskArray.length;i++) {
			if(bitmaskArray[i] == '1') {
				gpusForBid.add(offeredGPUs.get(i));
			}
		}
		if(getGPUsAvailableForNextIteration().size() + gpusForBid.size() > mMaxParallelism) {
			// No point of making this bid
			return null;
		}
		Set<GPU> potentialNewGPUSet = new HashSet<GPU>(getGPUsAvailableForNextIteration());
		potentialNewGPUSet.addAll(gpusForBid);
		double expectedJobSpeedUp = potentialNewGPUSet.size() * getPlacementSlowdown(potentialNewGPUSet);
		double newExpectedRunningTime = (Simulation.getSimulationTime() - mJobStartTime)
				+ (mTotalIterationsRemaining * mTimePerIteration) / expectedJobSpeedUp;
		double expectedGain = getCurrentEstimate() - newExpectedRunningTime;
		System.out.println(paddedBitMask + " " + Double.toString(expectedGain));
		return new Bid(gpusForBid, expectedGain, this);
	}
	
	private String padLeftZeros(String inputString, int length) {
	    if (inputString.length() >= length) {
	        return inputString;
	    }
	    StringBuilder sb = new StringBuilder();
	    while (sb.length() < length - inputString.length()) {
	        sb.append('0');
	    }
	    sb.append(inputString);
	 
	    return sb.toString();
	}
}