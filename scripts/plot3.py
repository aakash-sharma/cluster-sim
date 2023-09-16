import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import scipy
import math


num_paths = len(sys.argv) - 1
jct_colors = ['green', 'red', 'blue', 'purple', 'cyan', 'black', 'brown', 'orange', 'yellow', 'magenta']
q_delay_colors = ['orange', 'cyan', 'purple', 'red', 'black', 'brown','green', 'blue']
q_delay_pct_colors = ['black', 'red', 'blue', 'purple', 'cyan', 'blue', 'brown', 'orange']
pct_q_delay_colors = ["black", "brown", "grey"]


topologies = [file.split("/")[-1] for file in glob.glob(sys.argv[1] + "/*.xlsx")]
topologies = ["_".join(file.split("_")[1:]) for file in topologies]
topologies = [file.split(".")[0] for file in topologies]

makespans = []
titles = []
cluster_dfs = []
cluster_cum_dfs = []

max_jct = 0
for topo in topologies:
    dfs = []
    dfs_cum = []
    #print("reading topo: " + topo)
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        #print("reading path: " + path)
        #print("reading scheme: " + scheme)
        run_name = path.split("/")[-1]
        files = glob.glob(path + "/*.xlsx")
        filtered_files = [f for f in files if "~" not in f]
        for file in filtered_files:
           match = re.search(topo, file)
           if match:
             print("Reading file = ", file)
#              df_dict = pd.read_excel(file, sheet_name=None)
#              df = pd.DataFrame()
#              for sheet_name, df_sheet in df_dict.items():
#                  df = df.append(df_sheet)
#              df = pd.concat(df_dict, ignore_index=True)
             df = pd.read_excel(file)
             df["%Q-delay"] = df["Queue-delay"] / df["JCT"] * 100
             df["run"] = run_name

             df_cum_stats = pd.read_excel(file, sheet_name="cluster-cum-stats")

             df.rename(columns=lambda x: x + "_" + scheme if "JobId" not in x else "JobId", inplace=True)
             df_cum_stats.rename(columns=lambda x: x + "_" + scheme, inplace=True)
             df_cum_stats["topo"] = topo
             dfs.append(df)
             dfs_cum.append(df_cum_stats)

    merged_df = dfs[0]
    merged_cum_df = dfs_cum[0]

    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["JobId"])
    for df in dfs_cum[1:]:
        merged_cum_df = pd.merge(merged_cum_df, df, on=["topo"])

    cluster_dfs.append(merged_df)
    cluster_cum_dfs.append(merged_cum_df)

    #print(merged_df.columns.values)

fig, axs = plt.subplots(12, max(4, len(cluster_dfs)), figsize=(30, 90))

#print(len(cluster_dfs))
idx = 0
schemes = []
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    print(cluster_scheme)
    idx = 0

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
        y_comp.append("Comp-time" + "_" + scheme)
        y_comm.append("Comm-time" + "_" + scheme)
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

    #print(y_sum_allocs)
    #print(len(cluster_dfs[i][y_jct[0]].tolist()))
    cluster_dfs[i].plot(ax=axs[idx][i], y=y_jct,
        x="JobId", kind="line", linewidth=1, logy=True, color=jct_colors[:len(y_jct)])

    handles1, labels1 = axs[idx][i].get_legend_handles_labels()

    axs[idx][i].legend(handles1, labels1)
    axs[idx][i].set_title("JCT" + "_" + cluster_scheme)
    idx += 1

    #axs2 = axs[0][i].twinx()
    cluster_dfs[i].plot(ax=axs[idx][i], y=y_q_delay,
    x="JobId", kind="line", linewidth=1, logy=True, color=q_delay_colors[:len(y_q_delay)])

    # Create a single legend with the combined handles and labels
    handles1, labels1 = axs[idx][i].get_legend_handles_labels()
    handles = handles1 #+ handles2
    labels = labels1 #+ labels2

    axs[idx][i].legend(handles, labels, loc='lower center')
    axs[idx][i].set_title("Q delay" + "_" + cluster_scheme)
    idx += 1

    cluster_dfs[i].plot(ax=axs[idx][i], y=y_allocs, x="JobId", kind="line",
    color=jct_colors[:len(y_allocs)])
    handles3, labels3 = axs[idx][i].get_legend_handles_labels()
    axs[idx][i].legend(handles3, labels3)
                        #, loc="lower center", bbox_to_anchor=(0.9, 1.3, 0, 0), fontsize="5") #, loc='upper center', bbox_to_anchor=(1, -1),
                                         #         fancybox=True, shadow=True, ncol=3)
    axs[idx][i].set_title("Allocations" + "_" + cluster_scheme)
    idx += 1

    Min = cluster_dfs[i][y_jct[0]].tolist()[0]
    Max = 0
    y_jct_cdf = []
    for jct in y_jct:
        jct_sorted = np.sort(np.array(cluster_dfs[i][jct].tolist()))
        mean = np.mean(jct_sorted)
        std_dev = np.std(jct_sorted)
        jct_cdf = scipy.stats.norm.cdf(jct_sorted, loc=mean, scale=std_dev)
        y_jct_cdf.append(jct_cdf)

        Min = min(jct_sorted[0], Min)
        Max = max(jct_sorted[-1], Max)


    step = int((Max-Min)/len(y_jct_cdf[0]))
    x_axis = [x for x in range(int(Min), int(Max) - step, step)]

    #print(len(y_jct_cdf))
    for j in range(len(y_jct_cdf)):
        axs[idx][i].plot(x_axis, y_jct_cdf[j], color=jct_colors[j], label=y_jct[j])

    handles4, labels4 = axs[idx][i].get_legend_handles_labels()
    axs[idx][i].legend(handles4, labels4)
    axs[idx][i].set_title("JCT cdf" + "_" + cluster_scheme)
    idx += 1

    Min_q_delay = cluster_dfs[i][y_q_delay[0]].tolist()[0]
    Min_comm = cluster_dfs[i][y_comm[0]].tolist()[0]
    Max_q_delay = 0
    Max_comm = 0
    y_q_delay_cdf = []
    y_q_delay_pdf = []
    y_q_delay_sorted = []
    y_comm_cdf = []
    y_comm_pdf = []
    y_comm_sorted = []
    for q_delay in y_q_delay:
        q_delay_sorted = np.sort(np.array(cluster_dfs[i][q_delay].tolist()))
        mean_q_delay = np.mean(q_delay_sorted)
        std_dev_q_delay = np.std(q_delay_sorted)
        q_delay_cdf = scipy.stats.norm.cdf(q_delay_sorted, loc=mean_q_delay, scale=std_dev_q_delay)
        q_delay_pdf = scipy.stats.norm.pdf(q_delay_sorted, loc=mean_q_delay, scale=std_dev_q_delay)
        q_delay_hist, bin_edges = scipy.histogram(q_delay_sorted, bins=range(5))

        y_q_delay_cdf.append(q_delay_cdf)
        y_q_delay_pdf.append(q_delay_pdf)
        y_q_delay_sorted.append(q_delay_sorted)

        Min_q_delay = min(q_delay_sorted[0], Min_q_delay)
        Max_q_delay = max(q_delay_sorted[-1], Max_q_delay)

    for comm in y_comm:
        comm_sorted = np.sort(np.array(cluster_dfs[i][comm].tolist()))
        #print("comm len: " + str(comm_sorted.size))
        mean_comm = np.mean(comm_sorted)
        std_dev_comm = np.std(comm_sorted)
        comm_cdf = scipy.stats.norm.cdf(comm_sorted, loc=mean_comm, scale=std_dev_comm)
        comm_pdf = scipy.stats.norm.pdf(comm_sorted, loc=mean_comm, scale=std_dev_comm)
        y_comm_cdf.append(comm_cdf)
        y_comm_pdf.append(comm_pdf)
        y_comm_sorted.append(comm_sorted)
        Min_comm = min(comm_sorted[0], Min_comm)
        Max_comm = max(comm_sorted[-1], Max_comm)

    step_q_delay = int((Max_q_delay-Min_q_delay)/len(y_q_delay_cdf[0]))
    step_comm = math.ceil((Max_comm-Min_comm)/len(y_comm_cdf[0]))
    #print(len(y_comm_cdf[0]))
    x_axis_q_delay = [x for x in range(int(Min_q_delay), int(Max_q_delay) - step_q_delay, step_q_delay)]
    x_axis_comm = [x for x in range(int(Min_comm), int(Max_comm), step_comm)]
    x_axis_sorted = [x for x in range(len(y_comm_sorted[0]))]


    for j in range(len(y_q_delay_cdf)):
        axs[idx][i].plot(x_axis_q_delay[:len(y_q_delay_cdf[j])], y_q_delay_cdf[j], color=jct_colors[j], label=y_q_delay[j])
        #axs[idx+1][i].plot(x_axis_q_delay, y_q_delay_pdf[j], color=jct_colors[j], label=y_q_delay[j])
        axs[idx+1][i].plot(x_axis_sorted, y_q_delay_sorted[j], color=jct_colors[j], label=y_q_delay[j])
        #print(len(x_axis_comm))
        #print(len(y_comm_cdf[j]))
        axs[idx+2][i].plot(x_axis_comm[:len(y_comm_cdf[j])], y_comm_cdf[j], color=jct_colors[j], label=y_comm[j])
        #axs[idx+3][i].plot(x_axis_comm, y_comm_pdf[j], color=jct_colors[j], label=y_comm[j])
        axs[idx+3][i].plot(x_axis_sorted, y_comm_sorted[j], color=jct_colors[j], label=y_comm[j])

    handles5, labels5 = axs[idx][i].get_legend_handles_labels()
    axs[idx][i].legend(handles5, labels5)
    axs[idx][i].set_title("Q_delay time CDF" + "_" + cluster_scheme)

    handles7, labels7 = axs[idx+1][i].get_legend_handles_labels()
    axs[idx+1][i].legend(handles7, labels7)
    axs[idx+1][i].set_title("Q_delay time sorted" + "_" + cluster_scheme)

    handles6, labels6 = axs[idx+2][i].get_legend_handles_labels()
    axs[idx+2][i].legend(handles6, labels6)
    axs[idx+2][i].set_title("Communication time CDF" + "_" + cluster_scheme)

    handles6, labels6 = axs[idx+3][i].get_legend_handles_labels()
    axs[idx+3][i].legend(handles6, labels6)
    axs[idx+3][i].set_title("Communication time sorted" + "_" + cluster_scheme)

    idx += 4

    cluster_dfs[i].plot(ax=axs[idx][i], y=y_q_delay,
    x="JobId", kind="bar", linewidth=3, logy=True, color=q_delay_colors[:len(y_q_delay)])
    cluster_dfs[i].plot(ax=axs[idx+1][i], y=y_comm,
    x="JobId", kind="line", linewidth=1, logy=True, color=q_delay_pct_colors[:len(y_q_delay)])

    handles7, labels7 = axs[idx][i].get_legend_handles_labels()
    handles8, labels8 = axs[idx+2][i].get_legend_handles_labels()

    axs[idx][i].legend(handles7, labels7)
    axs[idx][i].set_title("Q_delay time" + "_" + cluster_scheme)
    axs[idx+2][i].legend(handles8, labels8)
    axs[idx+2][i].set_title("Communication time" + "_" + cluster_scheme)
    idx += 2

    x_allocs = []
    x_allocs.append("dim2")
    x_allocs.append("dim1")
    x_allocs.append("slot")
    x_allocs.append("mc")
    x_allocs.append("rack")
    x_allocs.append("nw")

    for j in range(len(y_sum_allocs)):
        axs[idx][i].plot(x_allocs, y_sum_allocs[j], color=jct_colors[j], label=schemes[j])

    handles9, labels9 = axs[idx][i].get_legend_handles_labels()
    axs[idx][i].legend(handles5, labels5)
    axs[idx][i].set_title("Dimension allocations" + "_" + cluster_scheme)

    handles9, labels9 = axs[idx][i].get_legend_handles_labels()
    axs[idx][i].legend(handles9, labels9)

    idx += 1

makespans = []
avg_jct = []
avg_comm = []
avg_q = []
titles = []
for i in range(len(cluster_dfs)):
    cluster_scheme = topologies[i]
    for path in sys.argv[1:]:
        scheme = path.split("/")[-1].split("_")[-1]
        makespans.append(cluster_cum_dfs[i]["Makespan" + "_" + scheme][0])
        avg_jct.append(cluster_cum_dfs[i]["Avg_JCT" + "_" + scheme][0])
        avg_comm.append(cluster_cum_dfs[i]["Avg_comm" + "_" + scheme][0])
        avg_q.append(cluster_cum_dfs[i]["Avg_queueing" + "_" + scheme][0])
        titles.append(cluster_scheme + "_" + scheme)

i = 0

axs[idx][i].bar(titles, makespans, color=jct_colors[:len(schemes)])#['red', 'green', 'blue', "purple", "brown", "magenta"])
axs[idx][i].set_xticklabels(titles)
axs[idx][i].set_xticklabels(axs[idx][i].get_xticklabels(), rotation=90)
axs[idx][i].set_title("Makespans")
i += 1

axs[idx][i].bar(titles, avg_jct, color=jct_colors[:len(schemes)])#['red', 'green', 'blue', "purple", "brown", "magenta"])
axs[idx][i].set_xticklabels(titles)
axs[idx][i].set_xticklabels(axs[idx][i].get_xticklabels(), rotation=90)
axs[idx][i].set_title("Average JCT")
i += 1

axs[idx][i].bar(titles, avg_comm, color=jct_colors[:len(schemes)])#['red', 'green', 'blue', "purple", "brown", "magenta"])
axs[idx][i].set_xticklabels(titles)
axs[idx][i].set_xticklabels(axs[idx][i].get_xticklabels(), rotation=90)
axs[idx][i].set_title("Average Comm")
i += 1

axs[idx][i].bar(titles, avg_q, color=jct_colors[:len(schemes)])#['red', 'green', 'blue', "purple", "brown", "magenta"])
axs[idx][i].set_xticklabels(titles)
axs[idx][i].set_xticklabels(axs[idx][i].get_xticklabels(), rotation=90)
axs[idx][i].set_title("Average Q")
i += 1

#fig.tight_layout(pad=50)
#fig.subplots_adjust(bottom=1, right=2, top=5)
#plt.show()

schemes = []
for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   #print(scheme)
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.tight_layout(pad=5.0)

fig.savefig("results/2das/results-" + schemes + ".pdf", format="pdf") #, bbox_inches="tight")
#fig.savefig("results/results-delay_sched_sweep" + ".pdf", format="pdf", bbox_inches="tight")
#fig.savefig("results/results-delay_sched_sweep-2" + ".pdf", format="pdf", bbox_inches="tight")
