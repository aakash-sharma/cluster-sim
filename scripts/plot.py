import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

num_paths = len(sys.argv) - 1
jct_colors = ['green', 'red', 'blue']
q_delay_colors = ['orange', 'cyan', 'purple']
pct_q_delay_colors = ["black", "brown", "grey"]

filenames = [file.split("/")[-1] for file in glob.glob(sys.argv[1] + "/*.xlsx")]
#print(filenames)
cluster_dfs = []
schemes = []

for file in filenames:
    dfs = []
    #cluster_scheme = file.split(".")[0]
    for path in sys.argv[1:]:
       run_name = path.split("/")[-1]
       print("Reading file = ", path + "/" + file)
       df = pd.read_excel(path + "/" + file)
       df["%Q-delay"] = df["Queue-delay"] / df["JCT"] * 100
       df["run"] = run_name
       #df = df.add_suffix("_" + scheme)
       #df = df.rename(columns={"JobId" + "_" + scheme : "JobId"})
       dfs.append(df)

    cluster_dfs.append(dfs)

#     merged_df = dfs[0]
#
#     for df in dfs[1:]:
#         merged_df = pd.merge(merged_df, df, on=["JobId"])
#     merged_df["title"] = title
#     cluster_dfs.append(merged_df)


fig, axs = plt.subplots(len(cluster_dfs)+1, num_paths, figsize=(5, 5))
fig.tight_layout(pad=10)

for i in range(len(cluster_dfs)):
    cluster_scheme = filenames[i].split(".")[0]
    for j in range(len(cluster_dfs[i])):

        axs2 = axs[i][j].twinx()
        scheme = cluster_dfs[i][j]["run"][0]

        cluster_dfs[i][j].plot(ax=axs[i][j], y=["JCT", "Queue-delay"],
        x="JobId", kind="bar", linewidth=3, logy=True, color=[jct_colors[j], q_delay_colors[j]])
        cluster_dfs[i][j].plot(ax=axs2, y=["%Q-delay"], x="JobId", kind="line",
        color=pct_q_delay_colors[j], legend=None)

        handles1, labels1 = axs[i][j].get_legend_handles_labels()
        handles2, labels2 = axs2.get_legend_handles_labels()
        handles = handles1 + handles2
        labels = labels1 + labels2

        # Create a single legend with the combined handles and labels
        axs[i][j].legend(handles, labels)
        axs[i][j].set_title(cluster_scheme + '_' + scheme)

        j += 1


makespans = []
titles = []
for i in range(len(cluster_dfs)):
    cluster_scheme = filenames[i].split(".")[0]
    for j in range(len(cluster_dfs[i])):
        scheme = cluster_dfs[i][j]["run"][0]
        makespans.append(cluster_dfs[i][j]["makespan"][0])
        titles.append(cluster_scheme + "_" + scheme)

axs[len(cluster_dfs)][0].bar(titles, makespans, color=['red', 'green', 'blue', 'cyan'])
axs[len(cluster_dfs)][0].set_xticklabels(titles)
axs[len(cluster_dfs)][0].set_xticklabels(axs[len(cluster_dfs)][0].get_xticklabels(), rotation=90)


fig.tight_layout(pad=50)
fig.subplots_adjust(bottom=1, right=2, top=5)

for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.savefig("results/results-" + schemes + ".pdf", format="pdf", bbox_inches="tight")

#plt.show()
