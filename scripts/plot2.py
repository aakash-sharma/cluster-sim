import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re

num_paths = len(sys.argv) - 1
jct_colors = ['green', 'red', 'blue', 'purple']
q_delay_colors = ['orange', 'cyan', 'purple']
q_delay_pct_colors = ['black', 'red', 'blue']
pct_q_delay_colors = ["black", "brown", "grey"]

topologies = [file.split("/")[-1] for file in glob.glob(sys.argv[1] + "/*.xlsx")]
topologies = ["_".join(file.split("_")[1:]) for file in topologies]
topologies = [file.split(".")[0] for file in topologies]

makespans = []
titles = []
cluster_dfs = []

for topo in topologies:
    dfs = []
    print("reading topo: " + topo)
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        print("reading path: " + path)
        run_name = path.split("/")[-1]
        for file in glob.glob(path + "/*.xlsx"):
           match = re.search(topo, file)
           if match:
             print("Reading file = ", file)
             df = pd.read_excel(file)
             df["%Q-delay"] = df["Queue-delay"] / df["JCT"] * 100
             df["run"] = run_name
             df.rename(columns=lambda x: x + "_" + scheme if "JobId" not in x else "JobId", inplace=True)
             dfs.append(df)

    merged_df = dfs[0]

    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["JobId"])
    print(list(merged_df.columns.values))
    cluster_dfs.append(merged_df)


fig, axs = plt.subplots(len(cluster_dfs)+1, 3, figsize=(5, 5))
fig.tight_layout(pad=10)

print(len(cluster_dfs))
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]

    y_jct = []
    y_q_delay = []
    y_q_delay_pct = []
    y_allocs = []
    schemes = []
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1]
        scheme = path.split("/")[-1].split("_")[-1]
        schemes.append(scheme)
        y_jct.append("JCT" + "_" + scheme)
        y_q_delay.append("Queue-delay" + "_" + scheme)
        y_q_delay_pct.append("%Q-delay" + "_" + scheme)
        y_allocs.append("nwAlloc" + "_" + scheme)
        y_allocs.append("rackAlloc" + "_" + scheme)

    cluster_dfs[i].plot(ax=axs[i][0], y=y_jct,
        x="JobId", kind="bar", linewidth=3, logy=True, color=jct_colors[:len(y_jct)])

    handles1, labels1 = axs[i][0].get_legend_handles_labels()

    # Create a single legend with the combined handles and labels
    axs[i][0].legend(handles1, labels1)
    axs[i][0].set_title("JCT" + "_" + cluster_scheme)

    axs2 = axs[i][1].twinx()
    cluster_dfs[i].plot(ax=axs[i][1], y=y_q_delay,
    x="JobId", kind="bar", linewidth=3, logy=True, color=q_delay_colors[:len(y_q_delay)])
    cluster_dfs[i].plot(ax=axs2, y=y_q_delay_pct, x="JobId", kind="line",
    color=q_delay_pct_colors[:len(y_q_delay)], legend=None)

    handles1, labels1 = axs[i][1].get_legend_handles_labels()
    handles2, labels2 = axs2.get_legend_handles_labels()
    handles = handles1 + handles2
    labels = labels1 + labels2

    # Create a single legend with the combined handles and labels
    axs[i][1].legend(handles, labels)
    axs[i][1].set_title("Q delay" + "_" + cluster_scheme)

    cluster_dfs[i].plot(ax=axs[i][2], y=y_allocs, x="JobId", kind="line",
    color=jct_colors[:len(y_allocs)])
    handles3, labels3 = axs[i][2].get_legend_handles_labels()
    axs[i][2].legend(handles3, labels3)
    axs[i][2].set_title("Allocations" + "_" + cluster_scheme)

makespans = []
titles = []
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        makespans.append(cluster_dfs[i]["makespan" + "_" + scheme][0])
        print(cluster_dfs[i]["makespan" + "_" + scheme].head())
        titles.append(cluster_scheme + "_" + scheme)

axs[len(cluster_dfs)][0].bar(titles, makespans, color=['red', 'green', 'blue', 'cyan'])
axs[len(cluster_dfs)][0].set_xticklabels(titles)
axs[len(cluster_dfs)][0].set_xticklabels(axs[len(cluster_dfs)][0].get_xticklabels(), rotation=90)

fig.tight_layout(pad=50)
fig.subplots_adjust(bottom=1, right=2, top=5)
#plt.show()

schemes = []
for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   print(scheme)
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.savefig("results/results-" + schemes + ".pdf", format="pdf", bbox_inches="tight")




