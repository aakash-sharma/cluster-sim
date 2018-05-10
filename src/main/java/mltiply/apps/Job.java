package mltiply.apps;

import mltiply.resources.Resources;
import mltiply.utils.Function;
import mltiply.utils.Interval;
import mltiply.utils.SublinearFunction;
import mltiply.utils.SuperlinearFunction;

import java.util.Random;

public class Job {
  public int jobId;
  public int numIterations;
  public int currIterationNum;
  public int numWorkers;
  public double serialIterationDuration;
  public Stage currIteration;
  public Function lossFunction;
  public int numTasksUntilNow;

  public Job(int jobId, int numIterations) {
    this.jobId = jobId;
    this.numIterations = numIterations;
    this.currIterationNum = -1;
    Random r = new Random();
    int rn = r.nextInt(2);
    if (rn == 0) {
      lossFunction = SublinearFunction.getRandomSublinearFunction(numIterations);
    } else {
      lossFunction = SuperlinearFunction.getRandomSuperlinearFunction(numIterations);
    }
    numWorkers = 0;
    numTasksUntilNow = 0;
    serialIterationDuration = r.nextInt(91) + 10;
  }

  public void initNextIteration() {
    currIterationNum += 1;
    currIteration = new Stage(jobId, currIterationNum, serialIterationDuration/numWorkers,
        1, new Interval(numTasksUntilNow, numTasksUntilNow+numWorkers-1));
    numTasksUntilNow = numTasksUntilNow + numWorkers;
  }

  public boolean isFinished() {
    if (currIterationNum < numIterations-1)
      return false;
    else
      return true;
  }
  
  // public int jid; // unique job id
  // public int workers; // number of workers currently assigned to this job
  // public Function loss; // loss as a function of iteration
  // public int iterations; // current SGD iteration number for this job
  // public int iterationsMax; // limit on the maximum number for this iteration
  // public double miniBatchSizeIndicator; // mini-batch size
  // public double schedulingEpoch; // time interval when scheduling decision is taken
  //
  // public Job(int jid, double schedulingEpoch) {
  //   this.jid = jid;
  //   this.workers = 0;
  //   this.iterations = 0;
  //   this.loss = new Function();
  //   this.iterationsMax = 50;
  //   this.miniBatchSizeIndicator = 0.5;
  //   this.schedulingEpoch = schedulingEpoch;
  // }
  //
  // public double getIterationTime() {
  //   return miniBatchSizeIndicator/workers;
  // }
  //
  // public double getLossReduction() {
  //   int numIterations = (int) Math.floor(schedulingEpoch/getIterationTime());
  //   return loss.getDeltaLoss(iterations, iterations + numIterations);
  // }

}
