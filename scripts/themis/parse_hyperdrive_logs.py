import json

def getJobGroupSizeDistribution():
    distribution = []
    with open('traces/metrics_dedup_1530309592.634231.json') as json_file:
        data = json.load(json_file)
        for key,value in data.items():
            count = 0
            for v in value:
                count = count + 1
            distribution.append(count)
    distribution.sort()
    # Hack to get rid of all 1's
    distribution = [item for item in distribution if item != 1]
    return distribution

if __name__== "__main__":
    getJobGroupSizeDistribution()
