from parse_hyperdrive_logs import getJobGroupSizeDistribution
import csv
import json
import random

def get_model_stats(model_name):
    with open("configuration/model_stats/" + model_name + ".json") as json_file:
        model_stats_data = json.load(json_file)
        return model_stats_data['cross_slot_slowdown'],model_stats_data['cross_machine_slowdown'],model_stats_data['cross_rack_slowdown'],model_stats_data['loss_function_type']

def sample_number_from_distribution(distribution):
    rand = generate_random_number(len(distribution))
    return distribution[rand]

def generate_random_number(number):
    return random.randint(0,number-1)

def main():
    distribution = getJobGroupSizeDistribution()
    models = {} # Model with list of corresponding jobs from trace
    with open('traces/480_job_trace.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader) # Skip CSV header
        for data in csv_reader:
            if data[4] not in models:
                models[data[4]] = []
            models[data[4]].append(data)
    
    for key,value in models.items():
        # Get model stats
        cross_slot_slowdown, cross_machine_slowdown, cross_rack_slowdown, loss_curve_type = get_model_stats(key)
        # Decide how many jobs in this job group by sampling from distribution
        number_jobs_in_group = sample_number_from_distribution(distribution)
        model_conf = {}
        model_conf["cross_slot_slowdown"] = str(cross_slot_slowdown)
        model_conf["cross_machine_slowdown"] = str(cross_machine_slowdown)
        model_conf["cross_rack_slowdown"] = str(cross_rack_slowdown)
        model_conf["hyperparameter_jobs"] = []
        for i in range(0, number_jobs_in_group):
            job_conf = {}
            # Take a random index from the list available
            r = generate_random_number(len(value))
            job_conf["total_iterations"] = str(value[r][3])
            job_conf["time_per_iteration"] = str(float(value[r][5])*int(value[r][1])/int(value[r][3]))
            if value[r][1] >= 4:
                job_conf["max_parallelism"] = "4"
            else :
                job_conf["max_parallelism"] = str(value[r][1])
            job_conf["loss_function_type"] = loss_curve_type
            job_conf["random_seed"] = str(generate_random_number(100))
            model_conf["hyperparameter_jobs"].append(job_conf)
        with open("configuration/models/" + key + ".json", 'w') as cf:
            cf.write(json.dumps(model_conf, indent=2, sort_keys=False))

if __name__== "__main__":
    main()
