from collections import defaultdict


f = open("../sensetime-gpus","r")
lines = f.readlines()

gpus = defaultdict(int)
total = 0

for line in lines:
    gpus[int(line)] += 1
    total += 1

print("Total GPU parallelism types =", len(gpus))

for k,v in gpus.items():
	print("Parallelism", k, "has %", gpus[k]/total*100)
