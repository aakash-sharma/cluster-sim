import csv
import json
import argparse

def get_args():
    parser = argparse.ArgumentParser(description="Themis Workload generation script")
    parser.add_argument('--time_scale', dest='scale', type=int, required=True, help='Time scaling to apply on traces')
    parser.add_argument('--dump_config', dest='config', type=str, required=True, help='Config file to dump to')
    return parser.parse_args()

def main():
    args = get_args()
    list_ml_jobs = []
    job_group_id = 1
    job_id = 1
    with open('traces/480_job_trace.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader) # Skip CSV header
        for data in csv_reader:
            training_job = []
            with open("configuration/models/" + data[4] + ".json") as json_file:
                model_data = json.load(json_file)
                hyper_params = model_data['hyperparameter_jobs']
                for hyper_param in hyper_params:
                    job_conf = {}
                    job_conf['job_id'] = str(job_id)
                    job_conf['job_group_id'] = str(job_group_id)
                    job_conf['cross_slot_slowdown'] = model_data['cross_slot_slowdown']
                    job_conf['cross_machine_slowdown'] = model_data['cross_machine_slowdown']
                    job_conf['cross_rack_slowdown'] = model_data['cross_rack_slowdown']
                    job_conf['start_time'] = str(int(data[2])*args.scale)
                    #job_conf['start_time'] = "0"
                    job_conf = {key: value for (key, value) in (job_conf.items() + hyper_param.items())} # Construct entire job config
                    job_id = job_id+1
                    training_job.append(job_conf)
            list_ml_jobs.append(training_job)
            job_group_id = job_group_id+1
    flat_list_ml_jobs = [item for sublist in list_ml_jobs for item in sublist]
    with open("configuration/jobs/"+args.config+".json", "w") as f:
        f.write(json.dumps(flat_list_ml_jobs, indent=2, sort_keys=False))

if __name__== "__main__":
    main()
