import sys
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

path=sys.argv[1]

filenames= glob.glob(path + "/*.xlsx")
print('File names:', filenames)
dfs= []

for file in filenames:
   print("Reading file = ",file)
   df = pd.read_excel(file)
   df["%Q-delay"] = df["Queue-delay"] / df["JCT"] * 100
   title = file.split("/")[1].split(".")[0]
   df["title"] = title
   #df = df.astype({'JobId':'int'})
   dfs.append(df)

fig, axs = plt.subplots(len(dfs), figsize=(5, 10))
fig.tight_layout(pad=10)

for i in range(len(dfs)):

   title = dfs[i]["title"][0]

   axs2 = axs[i].twinx()

   dfs[i].plot(ax=axs[i], y=["JCT", "Queue-delay"], x="JobId", kind="bar")
   dfs[i].plot(ax=axs2, y=["%Q-delay"], x="JobId", kind="line", color='green', legend=None)

   handles1, labels1 = axs[i].get_legend_handles_labels()
   handles2, labels2 = axs2.get_legend_handles_labels()
   handles = handles1 + handles2
   labels = labels1 + labels2

# Create a single legend with the combined handles and labels
   axs[i].legend(handles, labels)

   axs[i].set_title(title)

#fig.set_label(["JCT", "Queue-delay", "%Q-delay"])

fig.tight_layout(pad=50)
fig.subplots_adjust(bottom=1, right=2, top=5)
fig.savefig("results/results.pdf", format="pdf", bbox_inches="tight")

#plt.show()
