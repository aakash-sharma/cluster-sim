package mlsched.events;

import mlsched.simulator.Main;
import mlsched.workload.Job;
import mlsched.workload.Task;

public class StartIteration extends Event {

	public StartIteration(double timeStamp, Job j) {
		super(timeStamp, j);
	}

	@Override
	public void eventHandler() {
		j.currIterationNum++;
		
		if(j.currIterationNum == 1) {
			Main.jobStats.get(j.jobId).jobStartTime = Main.currentTime;
		}
		
		Main.jobStats.get(j.jobId).iterStartTimes.add(Main.currentTime);
		
		for(Task t : j.runningTasks) {
			Main.eventQueue.add(new EndTask(Main.currentTime + t.duration, j, t));
		}
		
	}
}