import argparse
import json
import math
import random

def get_args():
    parser = argparse.ArgumentParser(description="Themis Workload Configuration Script")
    parser.add_argument('--workload', dest='workload', type=str, required=True, help='Workload details separated by commas')
    parser.add_argument('--workload_rate', dest='workload_rate', type=float, required=True, help='Rate at which workload events arrive. Modeled as Poisson arrival')
    parser.add_argument('--config_file', dest='config_file', type=str, required=True, help='Config file to write workload configuration')
    return parser.parse_args()

def nextTime(rateParameter):
    return -math.log(1.0 - random.random()) / rateParameter

def main():
    args = get_args()
    # Get the models and how many instances of models are required to be trained
    models = args.workload.split(',')
    list_ml_jobs = []
    job_group_id = 1
    job_id = 1
    for model in models:
        model_name = model.split(':')[0]
        no_times = int(model.split(':')[1]) # Each kind of model will have a number of instances
        for i in range(0, no_times):
            model_file_name = "configuration/models/" + model_name + ".json" # Read the model's configuration
            training_job = []
            with open(model_file_name) as json_file:
                model_data = json.load(json_file)
                hyper_params = model_data['hyperparameter_jobs']
                for hyper_param in hyper_params:
                    job_conf = {}
                    job_conf['job_id'] = str(job_id)
                    job_conf['job_group_id'] = str(job_group_id)
                    job_conf['cross_slot_slowdown'] = model_data['cross_slot_slowdown']
                    job_conf['cross_machine_slowdown'] = model_data['cross_machine_slowdown']
                    job_conf['cross_rack_slowdown'] = model_data['cross_rack_slowdown']
                    job_conf = {key: value for (key, value) in (job_conf.items() + hyper_param.items())} # Construct entire job config
                    job_id = job_id+1
                    training_job.append(job_conf)
            list_ml_jobs.append(training_job)
            job_group_id = job_group_id+1
    # Now we have 'n' ML Training jobs. We need to pick them randomly and assign start times modeled as Poisson arrival times
    workloads = []
    start_time = 0
    while(len(list_ml_jobs) > 0):
        # Randomly select a job group
        r = random.randint(0,len(list_ml_jobs)-1)
        job_group = list_ml_jobs[r]
        # Generate start time from a Poisson distribution
        start_time = start_time + nextTime(args.workload_rate)
        for job in job_group:
            job['start_time'] = str(int(start_time)) # made it integer to make benchmarking easier
            workloads.append(job)
        del list_ml_jobs[r]
    with open("configuration/jobs/"+args.config_file+".json", "w") as f:
        f.write(json.dumps(workloads, indent=2, sort_keys=False))

if __name__ == "__main__":
    main()
