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
        print("reading scheme: " + scheme)
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

fig, axs = plt.subplots(9, len(cluster_dfs)+1, figsize=(15, 45))

print(len(cluster_dfs))

for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    print(cluster_scheme)

    y_jct = []
    y_comp = []
    y_comm = []
    y_q_delay = []
    y_q_delay_pct = []
    y_allocs = []
    schemes = []
    y_sum_allocs = [[] for x in range(len(sys.argv)-1)]
    j = 0
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1]
        scheme = path.split("/")[-1].split("_")[-1]
        schemes.append(scheme)
        print(scheme)
        y_jct.append("JCT" + "_" + scheme)
        y_comp.append("Compute-Time" + "_" + scheme)
        y_comm.append("Communication-Time" + "_" + scheme)
        y_q_delay.append("Queue-delay" + "_" + scheme)
        y_q_delay_pct.append("%Q-delay" + "_" + scheme)
        y_allocs.append("nwAlloc" + "_" + scheme)
        y_allocs.append("rackAlloc" + "_" + scheme)

        df = cluster_dfs[i]

        y_sum_allocs[j].append(df["dim2Alloc" + "_" + scheme].sum())
        y_sum_allocs[j].append(df["dim1Alloc" + "_" + scheme].sum())
        y_sum_allocs[j].append(df["slotAlloc" + "_" + scheme].sum())
        y_sum_allocs[j].append(df["machineAlloc" + "_" + scheme].sum())
        y_sum_allocs[j].append(df["rackAlloc" + "_" + scheme].sum())
        y_sum_allocs[j].append(df["nwAlloc" + "_" + scheme].sum())

        j += 1

    print(y_sum_allocs)

    cluster_dfs[i].plot(ax=axs[0][i], y=y_jct,
        x="JobId", kind="line", linewidth=1, logy=True, color=jct_colors[:len(y_jct)])

    handles1, labels1 = axs[0][i].get_legend_handles_labels()

    axs[0][i].legend(handles1, labels1)
    axs[0][i].set_title("JCT" + "_" + cluster_scheme)

    #axs2 = axs[0][i].twinx()
    cluster_dfs[i].plot(ax=axs[1][i], y=y_q_delay,
    x="JobId", kind="line", linewidth=1, logy=True, color=q_delay_colors[:len(y_q_delay)])

    # Create a single legend with the combined handles and labels
    handles1, labels1 = axs[1][i].get_legend_handles_labels()
    handles = handles1 #+ handles2
    labels = labels1 #+ labels2

    axs[1][i].legend(handles, labels)
    axs[2][i].set_title("Q delay" + "_" + cluster_scheme)

    cluster_dfs[i].plot(ax=axs[2][i], y=y_allocs, x="JobId", kind="line",
    color=jct_colors[:len(y_allocs)])
    handles3, labels3 = axs[2][i].get_legend_handles_labels()
    axs[2][i].legend(handles3, labels3) #, loc='upper center', bbox_to_anchor=(1, -1),
                                         #         fancybox=True, shadow=True, ncol=3)
    axs[2][i].set_title("Allocations" + "_" + cluster_scheme)

    j = 0
    Min = cluster_dfs[i][y_jct[0]].tolist()[0]
    Max = 0
    y_jct_cdf = []
    for jct in y_jct:
        jct_sorted = np.sort(np.array(cluster_dfs[i][jct].tolist()))
        #print(y_jct[j])
        mean = np.mean(jct_sorted)
        std_dev = np.std(jct_sorted)
        jct_cdf = scipy.stats.norm.cdf(jct_sorted, loc=mean, scale=std_dev)
        y_jct_cdf.append(jct_cdf)
        #for jct in jct_cdf:
         #   print(jct)

        Min = min(jct_sorted[0], Min)
        Max = max(jct_sorted[-1], Max)


    step = int((Max-Min)/len(y_jct_cdf[0]))
    x_axis = [x for x in range(int(Min), int(Max) - step, step)]

    for j in range(len(y_jct_cdf)):
        axs[3][i].plot(x_axis, y_jct_cdf[j], color=jct_colors[j], label=y_jct[j])

    handles4, labels4 = axs[3][i].get_legend_handles_labels()
    axs[3][i].legend(handles4, labels4)
    axs[3][i].set_title("JCT cdf" + "_" + cluster_scheme)

    j = 0
    Min_q_delay = cluster_dfs[i][y_q_delay[0]].tolist()[0]
    Min_comm = cluster_dfs[i][y_comm[0]].tolist()[0]
    Max_q_delay = 0
    Max_comm = 0
    y_q_delay_cdf = []
    y_comm_cdf = []
    for q_delay in y_q_delay:
        q_delay_sorted = np.sort(np.array(cluster_dfs[i][q_delay].tolist()))
        mean_q_delay = np.mean(q_delay_sorted)
        std_dev_q_delay = np.std(q_delay_sorted)
        q_delay_cdf = scipy.stats.norm.cdf(q_delay_sorted, loc=mean_q_delay, scale=std_dev_q_delay)

        y_q_delay_cdf.append(q_delay_cdf)
        #for jct in jct_cdf:
         #   print(jct)

        Min_q_delay = min(q_delay_sorted[0], Min_q_delay)
        Max_q_delay = max(q_delay_sorted[-1], Max_q_delay)

    for comm in y_comm:
        comm_sorted = np.sort(np.array(cluster_dfs[i][comm].tolist()))
        mean_comm = np.mean(comm_sorted)
        std_dev_comm = np.std(comm_sorted)
        comm_cdf = scipy.stats.norm.cdf(comm_sorted, loc=mean_comm, scale=std_dev_comm)
        y_comm_cdf.append(comm_cdf)
        Min_comm = min(comm_sorted[0], Min_q_delay)
        Max_comm = max(comm_sorted[-1], Max_comm)

    step_q_delay = int((Max_q_delay-Min_q_delay)/len(y_q_delay_cdf[0]))
    step_comm = int((Max_comm-Min_comm)/len(y_comm_cdf[0]))
    x_axis_q_delay = [x for x in range(int(Min_q_delay), int(Max_q_delay) - step_q_delay, step_q_delay)]
    x_axis_comm = [x for x in range(int(Min_comm), int(Max_comm) - step_comm, step_comm)]


    for j in range(len(y_q_delay_cdf)):
        axs[5][i].plot(x_axis_comm, y_comm_cdf[j], color=jct_colors[j], label=y_comm[j])
        axs[4][i].plot(x_axis_q_delay, y_q_delay_cdf[j], color=jct_colors[j], label=y_q_delay[j])

    handles5, labels5 = axs[4][i].get_legend_handles_labels()
    axs[4][i].legend(handles5, labels5)
    axs[4][i].set_title("Q_delay time cdf" + "_" + cluster_scheme)

    handles6, labels6 = axs[5][i].get_legend_handles_labels()
    axs[5][i].legend(handles6, labels6)
    axs[5][i].set_title("Communication time cdf" + "_" + cluster_scheme)

    cluster_dfs[i].plot(ax=axs[6][i], y=y_q_delay,
    x="JobId", kind="bar", linewidth=3, logy=True, color=q_delay_colors[:len(y_q_delay)])
    cluster_dfs[i].plot(ax=axs[7][i], y=y_comm,
    x="JobId", kind="line", linewidth=1, logy=True, color=q_delay_pct_colors[:len(y_q_delay)])

    handles7, labels7 = axs[6][i].get_legend_handles_labels()
    handles8, labels8 = axs[7][i].get_legend_handles_labels()

    axs[6][i].legend(handles7, labels7)
    axs[6][i].set_title("Q_delay time" + "_" + cluster_scheme)
    axs[7][i].legend(handles8, labels8)
    axs[7][i].set_title("Communication time" + "_" + cluster_scheme)


    x_allocs = []
    x_allocs.append("dim2")
    x_allocs.append("dim1")
    x_allocs.append("slot")
    x_allocs.append("mc")
    x_allocs.append("rack")
    x_allocs.append("nw")

    for j in range(len(y_sum_allocs)):
        axs[8][i].plot(x_allocs, y_sum_allocs[j], color=jct_colors[j], label=schemes[j])

    handles9, labels9 = axs[8][i].get_legend_handles_labels()
    axs[8][i].legend(handles5, labels5)
    axs[8][i].set_title("Dimension allocations" + "_" + cluster_scheme)

    handles9, labels9 = axs[8][i].get_legend_handles_labels()
    axs[8][i].legend(handles9, labels9)

makespans = []
titles = []
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        makespans.append(cluster_dfs[i]["makespan" + "_" + scheme][0])
        #print(cluster_dfs[i]["makespan" + "_" + scheme].head())
        titles.append(cluster_scheme + "_" + scheme)


axs[0][len(cluster_dfs)].bar(titles, makespans, color=jct_colors[:len(schemes)])#['red', 'green', 'blue', "purple", "brown", "magenta"])
axs[0][len(cluster_dfs)].set_xticklabels(titles)
axs[0][len(cluster_dfs)].set_xticklabels(axs[0][len(cluster_dfs)].get_xticklabels(), rotation=90)

#fig.tight_layout(pad=50)
#fig.subplots_adjust(bottom=1, right=2, top=5)
#plt.show()

schemes = []
for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   print(scheme)
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.tight_layout()
fig.savefig("results/results-" + schemes + ".pdf", format="pdf") #, bbox_inches="tight")
#fig.savefig("results/results-delay_sched_sweep" + ".pdf", format="pdf", bbox_inches="tight")




