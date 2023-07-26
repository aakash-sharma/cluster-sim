import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import scipy


num_paths = len(sys.argv) - 1
jct_colors = ['green', 'red', 'blue', 'purple', 'cyan', 'black', 'brown', 'orange']
q_delay_colors = ['orange', 'cyan', 'purple', 'red', 'black', 'brown','green', 'blue']
q_delay_pct_colors = ['black', 'red', 'blue', 'purple', 'cyan', 'blue', 'brown', 'orange']
pct_q_delay_colors = ["black", "brown", "grey"]

topologies = [file.split("/")[-1] for file in glob.glob(sys.argv[1] + "/*.xlsx")]
topologies = ["_".join(file.split("_")[1:]) for file in topologies]
topologies = [file.split(".")[0] for file in topologies]

makespans = []
titles = []
cluster_dfs = []

max_jct = 0
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
             #df["JCT_sorted"] = df["JCT"].copy().sort_values().reset_index(drop=True)
             #df['JCT_CDF'] = df["JCT_sorted"].rank(method = 'average', pct = True)
             #cdf_column = ranked_column.sort_values()# / len(df["JCT"])
             #df['JCT_CDF'] = cdf_column.reset_index(drop=True)
             #print(cdf_column)
             #print(cdf_column.keys())

             df.rename(columns=lambda x: x + "_" + scheme if "JobId" not in x else "JobId", inplace=True)
             dfs.append(df)

    merged_df = dfs[0]

    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["JobId"])
    cluster_dfs.append(merged_df)

fig, axs = plt.subplots(len(cluster_dfs)+1, 4, figsize=(30, 15))

print(len(cluster_dfs))
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    print(cluster_scheme)

    y_jct = []
    y_q_delay = []
    y_q_delay_pct = []
    y_allocs = []
    schemes = []
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1]
        scheme = path.split("/")[-1].split("_")[-1]
        schemes.append(scheme)
        print(scheme)
        y_jct.append("JCT" + "_" + scheme)
        y_q_delay.append("Queue-delay" + "_" + scheme)
        y_q_delay_pct.append("%Q-delay" + "_" + scheme)
        y_allocs.append("nwAlloc" + "_" + scheme)
        y_allocs.append("rackAlloc" + "_" + scheme)

    cluster_dfs[i].plot(ax=axs[i][0], y=y_jct,
        x="JobId", kind="line", linewidth=3, logy=True, color=jct_colors[:len(y_jct)])

    handles1, labels1 = axs[i][0].get_legend_handles_labels()

    # Create a single legend with the combined handles and labels
    axs[i][0].legend(handles1, labels1)
    axs[i][0].set_title("JCT" + "_" + cluster_scheme)

    axs2 = axs[i][1].twinx()
    cluster_dfs[i].plot(ax=axs[i][1], y=y_q_delay,
    x="JobId", kind="line", linewidth=3, logy=True, color=q_delay_colors[:len(y_q_delay)])

    handles1, labels1 = axs[i][1].get_legend_handles_labels()
    handles = handles1 #+ handles2
    labels = labels1 #+ labels2

    # Create a single legend with the combined handles and labels
    axs[i][1].legend(handles, labels)
    axs[i][1].set_title("Q delay" + "_" + cluster_scheme)

    cluster_dfs[i].plot(ax=axs[i][2], y=y_allocs, x="JobId", kind="line",
    color=jct_colors[:len(y_allocs)])
    handles3, labels3 = axs[i][2].get_legend_handles_labels()
    axs[i][2].legend(handles3, labels3)
    axs[i][2].set_title("Allocations" + "_" + cluster_scheme)

    j = 0
    Min = cluster_dfs[i][y_jct[0]].tolist()[0]
    Max = 0
    y_jct_cdf = []
    for jct in y_jct:
        jct_sorted = np.sort(np.array(cluster_dfs[i][jct].tolist()))
        print(y_jct[j])
        mean = np.mean(jct_sorted)
        std_dev = np.std(jct_sorted)
        jct_cdf = scipy.stats.norm.cdf(jct_sorted, loc=mean, scale=std_dev)
        y_jct_cdf.append(jct_cdf)
        for jct in jct_cdf:
            print(jct)

        Min = min(jct_sorted[0], Min)
        Max = max(jct_sorted[-1], Max)


    step = int((Max-Min)/len(y_jct_cdf[0]))
    x_axis = [x for x in range(int(Min), int(Max) - step, step)]

    for j in range(len(y_jct_cdf)):
        axs[i][3].plot(x_axis, y_jct_cdf[j], color=jct_colors[j], label=y_jct[j])

        j += 1

    handles4, labels4 = axs[i][3].get_legend_handles_labels()
    axs[i][3].legend(handles4, labels4)
    axs[i][3].set_title("JCT cdf" + "_" + cluster_scheme)

makespans = []
titles = []
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        makespans.append(cluster_dfs[i]["makespan" + "_" + scheme][0])
        #print(cluster_dfs[i]["makespan" + "_" + scheme].head())
        titles.append(cluster_scheme + "_" + scheme)

axs[len(cluster_dfs)][0].bar(titles, makespans, color=['red', 'green', 'blue', 'cyan'])
axs[len(cluster_dfs)][0].set_xticklabels(titles)
axs[len(cluster_dfs)][0].set_xticklabels(axs[len(cluster_dfs)][0].get_xticklabels(), rotation=90)

#fig.tight_layout(pad=50)
#fig.subplots_adjust(bottom=1, right=2, top=5)
#plt.show()

schemes = []
for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   print(scheme)
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.savefig("results/results-" + schemes + ".pdf", format="pdf", bbox_inches="tight")
#fig.savefig("results/results-delay_sched_sweep" + ".pdf", format="pdf", bbox_inches="tight")




