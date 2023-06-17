import argparse
import json

def get_args():
    parser = argparse.ArgumentParser(description="Themis Cluster Configuration Script")
    parser.add_argument('--racks', dest='racks', type=int, required=True, help='Number of racks in cluster')
    parser.add_argument('--machines', dest='machines', type=int, required=True, help='Number of machines per racks')
    parser.add_argument('--slots', dest='slots', type=int, required=True, help='Number of slots per machine')
    parser.add_argument('--gpus', dest='gpus', type=int, required=True, help='Number of GPUs per slot')
    parser.add_argument('--policy', dest='policy', type=str, required=True, help='Type of scheduling policy')
    parser.add_argument('--fairness_knob', dest='fairness_knob', type=float, default=0.0, required=False, help='Fairness Knob')
    parser.add_argument('--visibility_knob', dest='visibility_knob', type=float, default=1.0, required=False, help='Visibility Knob')
    parser.add_argument('--lease_time', dest='lease_time', type=float, required=True, help='GPU Lease Time')
    parser.add_argument('--config_file', dest='config_file', type=str, required=True, help='File name to dump cluster configuration')
    return parser.parse_args()

def main():
    args = get_args()
    cluster_conf = {}
    cluster_conf['racks_in_cluster'] = str(args.racks)
    cluster_conf['machines_per_rack'] = str(args.machines)
    cluster_conf['slots_per_machine'] = str(args.slots)
    cluster_conf['gpus_per_slot'] = str(args.gpus)
    cluster_conf['cluster_policy'] = args.policy
    cluster_conf['fairness_threshold'] = str(args.fairness_knob)
    cluster_conf['epsilon'] = str(args.visibility_knob)
    cluster_conf['lease_time'] = str(args.lease_time)
    cluster_conf['should_use_config'] = "True"
    with open("configuration/cluster/" + args.config_file + ".json", 'w') as cf:
        cf.write(json.dumps(cluster_conf, indent=2, sort_keys=False))

if __name__ == "__main__":
    main()
