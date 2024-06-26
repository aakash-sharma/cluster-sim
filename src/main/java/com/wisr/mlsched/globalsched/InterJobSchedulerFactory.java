package com.wisr.mlsched.globalsched;

import com.wisr.mlsched.config.ClusterConfiguration;

import java.util.logging.Logger;

/**
 * Factory for producing InterJobScheduler objects
 */
public class InterJobSchedulerFactory {
	private static Logger sLog = Logger.getLogger(InterJobSchedulerFactory.class.getSimpleName());
	
	public static InterJobScheduler createInstance(ClusterConfiguration config) {
		String policy = config.getPolicy();
		switch(policy) {
			case "Themis":
				return new ThemisInterJobScheduler();
			case "Gandiva":
				return new GandivaInterJobScheduler(config);
			case "SLAQ":
				return new SLAQInterJobScheduler();
			case "Tiresias":
				return new TiresiasInterJobScheduler(config);
			case "SJF":
				return new SJFInterJobScheduler();
			case "Optimus":
				return new OptimusInterJobScheduler();
			case "SRSF":
				return new SRSFInterJobScheduler();
			case "Dally":
				return new DallyInterJobScheduler(config);
		}
		sLog.severe("Policy not defined");
		return null;
	}
}