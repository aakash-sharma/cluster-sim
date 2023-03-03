package com.wisr.mlsched.job;

import java.util.Random;

public class SubLinearLossFunction implements LossFunction {
  public long numIterations;
  public double valueBegin;
  public double valueEnd;
  public double a, b, c, d;

  private SubLinearLossFunction(long numIterations, double valueBegin, double valueEnd,
                           double a, double b, double c, double d) {
    this.numIterations = numIterations;
    this.valueBegin = valueBegin;
    this.valueEnd = valueEnd;
    this.a = a;
    this.b = b;
    this.c = c;
    this.d = d;
  }

  public static SubLinearLossFunction getRandomSublinearFunction(long numIterations, int seed) {
    double tValueBegin = 1.0;
    double tValueEnd = 0.0;
    Random r = new Random(seed);
    double tA = r.nextDouble();
    double tB = r.nextDouble();
    double tT = tA*numIterations*numIterations + tB*numIterations;
    double tC = 1/(1 + 1/tT);
    double tD = -1/tT;
    return new SubLinearLossFunction(numIterations, tValueBegin, tValueEnd,
        tA, tB, tC, tD);
  }

  public double getValue(long iteration) {
    double value = a*iteration*iteration + b*iteration + c;
    value = 1/value + d;
    return value;
  }

  public double getSlope(long iteration) {
    double value = a*iteration*iteration + b*iteration + c;
    value = value*value;
    value = -1/value;
    value = value * (-2*a*iteration + b);
    return value;
  }

  public double getDeltaValue(long iteration) {
    return getValue(iteration) - getValue(iteration+1);
  }
}