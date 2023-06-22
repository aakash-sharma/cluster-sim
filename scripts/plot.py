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
    title = file.split(".")[0]
    for path in sys.argv[1:]:
       scheme = path.split("/")[-1]
       print("Reading file = ",path + "/" + file)
       df = pd.read_excel(path + "/" + file)
       df["%Q-delay"] = df["Queue-delay"] / df["JCT"] * 100
       df = df.add_suffix("_" + scheme)
       df = df.rename(columns={"JobId" + "_" + scheme : "JobId"})
       dfs.append(df)

    merged_df = dfs[0]

    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["JobId"])
    merged_df["title"] = title
    cluster_dfs.append(merged_df)


fig, axs = plt.subplots(len(cluster_dfs), figsize=(5, 10))
fig.tight_layout(pad=10)

for i in range(len(cluster_dfs)):

    title = cluster_dfs[i]["title"][0]

    axs2 = axs[i].twinx()

    j = 0
    for path in sys.argv[1:]:
       scheme = path.split("/")[-1]

       cluster_dfs[i].plot(ax=axs[i], y=["JCT" + "_" + scheme, "Queue-delay" + "_" + scheme],
       x="JobId", kind="bar", color=[jct_colors[j], q_delay_colors[j]])
       cluster_dfs[i].plot(ax=axs2, y=["%Q-delay" + "_" + scheme], x="JobId", kind="line",
       color=pct_q_delay_colors[j], legend=None)
       j += 1

    handles1, labels1 = axs[i].get_legend_handles_labels()
    handles2, labels2 = axs2.get_legend_handles_labels()
    handles = handles1 + handles2
    labels = labels1 + labels2

    # Create a single legend with the combined handles and labels
    axs[i].legend(handles, labels)
    axs[i].set_title(title)


fig.tight_layout(pad=50)
fig.subplots_adjust(bottom=1, right=2, top=5)

for path in sys.argv[1:]:
   scheme = path.split("/")[-1]
   schemes.append(scheme)
schemes = "_".join(schemes)
fig.savefig("results/results-" + schemes + ".pdf", format="pdf", bbox_inches="tight")

#plt.show()
