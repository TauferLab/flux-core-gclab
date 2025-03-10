#!/usr/bin/env python
import csv
import sys
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
import matplotlib.patches as mpatches

# Define the phases (CSV header order)
phases = ["submit", "validate", "depend", "priority", "alloc", "start", "finish", "release", "free", "clean"]

# There are len(phases)-1 intervals; assign a color for each.
phase_interval_colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8']
# Labels for the legend: "submit -> validate", etc.
phase_interval_labels = [f"{phases[i]} -> {phases[i+1]}" for i in range(len(phases)-1)]

if len(sys.argv) < 2:
    print("Usage: {} <eventlog.csv>".format(sys.argv[0]))
    sys.exit(1)

csv_file = sys.argv[1]

# Read CSV file and parse event timestamps
jobs = []  # List of job dictionaries
with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        job = {"jobid": row["jobid"]}
        for phase in phases:
            # Convert to float if present; otherwise None
            job[phase] = float(row[phase]) if row[phase] != "" else None
        jobs.append(job)

# Determine the global minimum and maximum times among all events
global_min = min(
    job[phase] for job in jobs for phase in phases if job[phase] is not None
)
global_max = max(
    job[phase] for job in jobs for phase in phases if job[phase] is not None
)

# Convert each event timestamp to a relative time (seconds) starting at 0
for job in jobs:
    for phase in phases:
        if job[phase] is not None:
            job[phase] = job[phase] - global_min

# Build the vertices for the PolyCollection: one rectangle per phase interval per job.
verts = []
colors = []
# Each job will be plotted at a y-position equal to its index.
# We'll make each bar span vertically from (i - 0.4) to (i + 0.4)
for i, job in enumerate(jobs):
    for j in range(len(phases) - 1):
        start_time = job[phases[j]]
        end_time = job[phases[j+1]]
        # Only add a bar if both endpoints exist and the interval is positive
        if start_time is not None and end_time is not None and end_time > start_time:
            y_center = i
            y_bottom = y_center - 0.4
            y_top = y_center + 0.4
            # Define the vertices of the rectangle for this phase interval
            poly = [(start_time, y_bottom),
                    (start_time, y_top),
                    (end_time, y_top),
                    (end_time, y_bottom),
                    (start_time, y_bottom)]
            verts.append(poly)
            colors.append(phase_interval_colors[j])

# Create the PolyCollection with the computed vertices and colors.
pc = PolyCollection(verts, facecolors=colors, edgecolors='k', linewidths=0.5)

# Create the plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.add_collection(pc)
ax.set_xlim(0, global_max - global_min)
ax.set_ylim(-0.5, len(jobs) - 0.5)
ax.set_xlabel("Time (s, relative to earliest event)")
ax.set_title("Job Phases Timeline")

# Set the y-axis ticks and labels using the jobid (in CSV order, top to bottom)
y_positions = list(range(len(jobs)))
ax.set_yticks(y_positions)
ax.set_yticklabels([job["jobid"] for job in jobs])

# Build a legend for the phase intervals
legend_patches = [mpatches.Patch(color=phase_interval_colors[j],
                                 label=phase_interval_labels[j])
                  for j in range(len(phase_interval_labels))]
ax.legend(handles=legend_patches, bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()
plt.show()

plt.savefig("output.png")