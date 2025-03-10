#!/usr/bin/env python3
import csv
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

def read_jobs_data(filename):
    data = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                data.append(float(row["jobs_per_sec"]))
            except (ValueError, KeyError):
                continue
    return data

fluxion_1000        = read_jobs_data("results_fluxion_1000.csv")
fluxion_10000       = read_jobs_data("results_fluxion_10000.csv")
fluxion_modded_1000 = read_jobs_data("results_fluxion_modded_1000.csv")
fluxion_modded_10000= read_jobs_data("results_fluxion_modded_10000.csv")


data = [fluxion_1000, fluxion_modded_1000, fluxion_10000, fluxion_modded_10000]
positions = [1, 2, 4, 5]

fig, ax = plt.subplots(figsize=(10, 6))

bp = ax.boxplot(data, positions=positions, widths=0.6, patch_artist=True)

colors = ['lightblue', 'lightgreen', 'lightblue', 'lightgreen']
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)

ax.set_xticks([1.5, 4.5])
ax.set_xticklabels(['1000 jobs', '10000 jobs'])
ax.set_xlabel("Job Count")
ax.set_ylabel("Jobs per Second")
ax.set_title("Job Throughput Comparison (Fluxion vs Fluxion With Emulator Additions)")

legend_handles = [
    mpatches.Patch(color='lightblue', label='Fluxion'),
    mpatches.Patch(color='lightgreen', label='Fluxion With Emulator Additions')
]
ax.legend(handles=legend_handles, loc='upper right')

plt.ylim(bottom=0, top=600)
plt.tight_layout()
plt.savefig("job_throughput_boxplot.png")
plt.show()
