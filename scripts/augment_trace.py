import sys
import json
import random

models = ["VGG", "ResNet50"]
demands =["1", "2", "4", "8", "16"]

#in_file= open(sys.argv[1], "r")
#out_file = open(sys.argv[2], "w")

with open(sys.argv[1]) as fd:
    traceJson = json.load(fd)

for trace in traceJson:
    if "model" not in trace.keys():
        trace["model"] = random.choice(models)
    trace["total_iterations"] = str(int(trace["total_iterations"]) * 100)
    trace["max_parallelism"] = random.choice(demands)

fd.close()


with open(sys.argv[2], "w") as fd:
    json.dump(traceJson, fd)


