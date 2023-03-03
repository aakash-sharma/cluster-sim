package com.wisr.mlsched.job;

public interface LossFunction {
  // y = f(x), where y is typically the normalized loss function value and x is the iteration number
  public double getValue(long iteration);
  public double getSlope(long iteration);
  public double getDeltaValue(long iteration);
}
