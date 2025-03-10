#!/usr/bin/env python3
import csv
import sys
import matplotlib.pyplot as plt

config = "Fluxion, FCFS, 30 Nodes, First job 10 Nodes, Second 30 Nodes, Rest 3 Nodes"

if len(sys.argv) < 2:
    print("Usage: {} <job_transitions.csv>".format(sys.argv[0]))
    sys.exit(1)

csv_file = sys.argv[1]

# Read CSV file and convert times to floats.
jobs = []
with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        job = {
            "jobid": row["jobid"],
            "SUBMIT": float(row["SUBMIT"]) if row["SUBMIT"] else None,
            "START": float(row["START"]) if row["START"] else None,
            "FINISH": float(row["FINISH"]) if row["FINISH"] else None,
        }
        jobs.append(job)

# Compute the global minimum submit time and the maximum finish time.
min_submit = min(job["SUBMIT"] for job in jobs if job["SUBMIT"] is not None)
max_finish = max(job["FINISH"] for job in jobs if job["FINISH"] is not None)

# Compute relative start and finish times for each job.
for job in jobs:
    if job["START"] is not None:
        job["start_rel"] = job["START"] - min_submit
    else:
        job["start_rel"] = None
    if job["FINISH"] is not None:
        job["finish_rel"] = job["FINISH"] - min_submit
    else:
        job["finish_rel"] = None

# Create the plot.
fig, ax = plt.subplots(figsize=(10, 6))
y_positions = range(len(jobs))
bar_height = 0.8

for i, job in enumerate(jobs):
    if job["start_rel"] is not None and job["finish_rel"] is not None:
        start_rel = job["start_rel"]
        duration = job["finish_rel"] - job["start_rel"]
        ax.barh(i, duration, left=start_rel, height=bar_height,
                color="C0", edgecolor="k")

# Set up the y-axis with job IDs.
ax.set_yticks(list(y_positions))
ax.set_yticklabels([job["jobid"] for job in jobs])
ax.set_xlabel("Time (s) relative to earliest submission")
ax.set_ylabel("Job IDs")
ax.set_title(f"Job Execution Order ({config})")

# Set x-axis limits from 0 to (max_finish - min_submit).
ax.set_xlim(0, max_finish - min_submit)
plt.tight_layout()
plt.savefig("fig.png")
