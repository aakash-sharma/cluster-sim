package com.wisr.mlsched.localsched;

import java.util.logging.Logger;

import org.json.simple.JSONObject;

/**
 * Factory for producing IntraJobScheduler objects
 */
public class IntraJobSchedulerFactory {
	private static Logger sLog = Logger.getLogger(IntraJobSchedulerFactory.class.getSimpleName());
	public static IntraJobScheduler createInstance(JSONObject workload_config,
			String policy) {
		switch(policy) {
			case "Themis":
				return new ThemisIntraJobScheduler(workload_config);
			case "Gandiva":
				return new GandivaIntraJobScheduler(workload_config);
			case "SLAQ":
				return new SLAQIntraJobScheduler(workload_config);
			case "Tiresias":
				return new TiresiasIntraJobScheduler(workload_config);
			case "SJF":
				return new SJFIntraJobScheduler(workload_config);
			case "Optimus":
				return new OptimusIntraJobScheduler(workload_config);
			case "SRSF":
				return new SRSFIntraJobScheduler(workload_config);
			case "Dally":
				return new DallyIntraJobScheduler(workload_config);
		}
		sLog.severe("Policy not defined");
		return null;
	}
}