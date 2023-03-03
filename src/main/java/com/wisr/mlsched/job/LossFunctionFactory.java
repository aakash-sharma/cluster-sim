package com.wisr.mlsched.job;

import java.util.logging.Logger;

/**
 * Factory for producing IntraJobScheduler objects
 */
public class LossFunctionFactory {
	private static Logger sLog = Logger.getLogger(LossFunctionFactory.class.getSimpleName());
	public static LossFunction createInstance(String lossFunctionType, 
			long numIterations, int seed) {
		switch(lossFunctionType) {
			case "sublinear":
				return SubLinearLossFunction.getRandomSublinearFunction(numIterations, seed);
			case "superlinear":
				return SubLinearLossFunction.getRandomSublinearFunction(numIterations, seed);
		}
		sLog.severe("Loss function type not defined");
		return null;
	}
}