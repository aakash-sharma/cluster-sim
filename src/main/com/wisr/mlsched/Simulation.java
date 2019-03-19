import org.json.simple.JSONObject;

public class Simulation {
	public static void main(String args[]) {
		String cluster_config_file = args[0];
		String workload_config_file = args[1];
		
		// Get the configurations
		JSONObject clusterConfig = ConfigUtils.getClusterConfig(cluster_config_file);
		JSONObject[] workloadConfig = ConfigUtils.getWorkloadConfigs(workload_config_file);
		
		// Initialize cluster
		Cluster cluster = Cluster.createCluster(clusterConfig);
		
		ClusterEventQueue eventQueue = ClusterEventQueue.getInstance();
		
		// Queue startup of jobs by start time
		for(int i=0;i<workloadConfig.length;i++) {
			// TODO: Enqueue JA for each job here
		}
		
		// Start processing
		eventQueue.start();
	}
}