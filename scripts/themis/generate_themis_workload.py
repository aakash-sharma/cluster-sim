import csv
import random
import json

models = ['vgg19','vgg16','vgg11','alexnet','resnet152','resnet101','resnet50','inception4','inception3','googlenet']
model_config_folder = "configuration/model_stats/themis/"

prev_expt_id = -1
job_group_id = 0
job_id = 1

list_jobs = []
with open("traces/philly_hyperdrive_workload.csv", "r") as f:
    reader = csv.reader(f, delimiter=",")
    start_time = -100
    next(reader)
    for line in reader:
        expt_id = int(line[0])
        #start_time = int(line[2])
        running_time = float(line[3])
        num_gpus = int(line[4])
        if expt_id == prev_expt_id:
            # Continue in same job group
            job_id = job_id + 1
        else:
            # New Job Group
            prev_expt_id = expt_id
            job_group_id = job_group_id + 1
            job_id = job_id + 1
            model_to_use = models[random.randint(0,len(models)-1)]
            start_time += 100
            print("New job group " + str(job_group_id) + " " + model_to_use)
        with open(model_config_folder + model_to_use + ".json") as json_file:
            model_data = json.load(json_file)
            job_conf = {}
            job_conf["job_id"] = str(job_id)
            job_conf["job_group_id"] = str(job_group_id)
            job_conf["cross_slot_slowdown"] = model_data["cross_slot_slowdown"]
            job_conf["cross_machine_slowdown"] = model_data["cross_machine_slowdown"]
            job_conf["cross_rack_slowdown"] = model_data["cross_rack_slowdown"]
            job_conf["time_per_iteration"] = str(float(model_data["time_per_iteration"])/60)
            job_conf["random_seed"] = str(random.randint(0,99))
            job_conf["max_parallelism"] = str(num_gpus)
            #job_conf["start_time"] = str(start_time)
            job_conf["start_time"] = str(start_time)
            job_conf["loss_function_type"] = model_data["loss_function_type"] 
            job_conf["total_iterations"] = str(int(running_time/float(job_conf["time_per_iteration"])))
            list_jobs.append(job_conf)
with open("configuration/jobs/"+"nsdi_philly_hyperdrive"+".json", "w") as f:
    f.write(json.dumps(list_jobs, indent=2, sort_keys=False))
