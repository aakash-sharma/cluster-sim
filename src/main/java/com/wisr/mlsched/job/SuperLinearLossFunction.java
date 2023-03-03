package com.wisr.mlsched.job;

import java.util.Random;

public class SuperLinearLossFunction implements LossFunction {
  public long numIterations;
  public double valueBegin;
  public double valueEnd;
  public double u, b, c;

  private SuperLinearLossFunction(long numIterations, double valueBegin, double valueEnd,
                           double u, double b, double c) {
    this.numIterations = numIterations;
    this.valueBegin = valueBegin;
    this.valueEnd = valueEnd;
    this.u = u;
    this.b = b;
    this.c = c;
  }

  public static SuperLinearLossFunction getRandomSuperlinearFunction(long numIterations, int seed) {
    double tValueBegin = 1.0;
    double tValueEnd = 0.0;
    Random r = new Random(seed);
    double tU = r.nextDouble();
    double tC = Math.pow(tU, numIterations);
    tC = tC/(tC - 1);
    double tB = -Math.log(1 - tC)/Math.log(tU);
    return new SuperLinearLossFunction(numIterations, tValueBegin, tValueEnd,
        tU, tB, tC);
  }

  public double getValue(long iteration) {
    double value = Math.pow(u, iteration - b) + c;
    return value;
  }

  public double getSlope(long iteration) {
    double value = Math.pow(u, iteration - b);
    value = Math.log(u)*value;
    return value;
  }

  public double getDeltaValue(long iteration) {
    return getValue(iteration-1) - getValue(iteration);
  }
}