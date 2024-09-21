import os
import re
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Path to the folder containing the log files
log_folder = 'sketches'

BIG_SIZE = 12
FIGRATIO = 1 / 2
FIGWIDTH = 6 # inches
FIGHEIGHT = FIGWIDTH * FIGRATIO
FIGSIZE = (FIGWIDTH, FIGHEIGHT)

plt.rcParams.update(
{
"figure.figsize": FIGSIZE,
"figure.dpi": 300,
# "text.usetex": True,
}
)

COLORS = sns.color_palette("Paired")
sns.set_style("ticks")
sns.set_palette(COLORS)

plt.rc("font", size=BIG_SIZE) # controls default text sizes
plt.rc("axes", titlesize=BIG_SIZE) # fontsize of the axes title
plt.rc("axes", labelsize=BIG_SIZE) # fontsize of the x and y labels
plt.rc("xtick", labelsize=BIG_SIZE) # fontsize of the tick labels
plt.rc("ytick", labelsize=BIG_SIZE) # fontsize of the tick labels
plt.rc("legend", fontsize=BIG_SIZE) # legend fontsize
plt.rc("figure", titlesize=BIG_SIZE) # fontsize of the figure title

# Regex pattern to extract data from the log files
pattern = re.compile(r"Total requests: (\d+).*?Read Ratio:.*?Average Value Size:.*?Min Value Size:.*?Max Value Size:.*?Average Interval:.*?Min Interval:.*?Max Interval:.*?Correct rate:.*?Correct pred rate: ([\d.]+).*?Storage serving: ([\d.]+).*?Average write latency: ([\d.]+).*?Average read latency: ([\d.]+).*?Average get_ew latency: ([\d.]+).*?gold_tracker: .*?", re.DOTALL)

# Initialize dictionaries to store data for each sketch and dataset
correct_pred_rate = {}
storage_serving = {}
write_time_ratio = {}
read_time_ratio = {}
ew_time_ratio = {}

# Process each log file in the folder
for log_file in os.listdir(log_folder):
    if log_file.endswith('.log'):
        dataset = log_file.split('_')[0]  # Extract dataset from filename
        sketch = log_file.split('_')[1].split('.')[0]  # Extract sketch from filename
        
        if dataset not in correct_pred_rate:
            correct_pred_rate[dataset] = {}
        if dataset not in storage_serving:
            storage_serving[dataset] = {}
        if dataset not in write_time_ratio:
            write_time_ratio[dataset] = {}
        if dataset not in read_time_ratio:
            read_time_ratio[dataset] = {}
        if dataset not in ew_time_ratio:
            ew_time_ratio[dataset] = {}
            
        with open(os.path.join(log_folder, log_file), 'r') as f:
            content = f.read()
            match = pattern.search(content)
            if match:
                total_requests = int(match.group(1))
                correct_pred_rate[dataset][sketch] = float(match.group(2)) * 100
                storage_serving[dataset][sketch] = float(match.group(3))
                write_time = float(match.group(4))
                read_time = float(match.group(5))
                ew_time = float(match.group(6))

                # write_time_ratio[dataset][sketch] = benchmark_time / total_requests * 1000
                write_time_ratio[dataset][sketch] = write_time * 1000
                read_time_ratio[dataset][sketch] = read_time * 1000
                ew_time_ratio[dataset][sketch] = ew_time * 1000

# Extract sketches and datasets
datasets = sorted(correct_pred_rate.keys())
sketches = sorted(next(iter(correct_pred_rate.values())).keys())

# Prepare data for plotting
correct_pred_values = np.array([[correct_pred_rate[ds].get(sk, 0) for sk in sketches] for ds in datasets])
storage_serving_values = np.array([[storage_serving[ds].get(sk, 0) for sk in sketches] for ds in datasets])
benchmark_write_time_values = np.array([[write_time_ratio[ds].get(sk, 0) for sk in sketches] for ds in datasets])
benchmark_read_time_values = np.array([[read_time_ratio[ds].get(sk, 0) for sk in sketches] for ds in datasets])
benchmark_ew_time_values = np.array([[ew_time_ratio[ds].get(sk, 0) for sk in sketches] for ds in datasets])

# Set up bar chart
x = np.arange(len(datasets))  # the label locations
width = 0.2  # the width of the bars


def convert_sketch(sketch):
    
    if sketch == "MinSketchTracker":
        sketch = "MinSketch"
    elif sketch == "ExactRWTracker":
        sketch = "ExactRW"
    elif sketch == "MinSketchConsTracker":
        sketch = "MinSketch (Cons.)"
    elif sketch == "TopKSketchTracker":
        sketch = "TopKSketch"
    elif sketch == "TopKSketchSampleTracker":
        sketch = "TopKSketchSample"
    return sketch

# Create the first figure for correct pred rate
fig1, ax1 = plt.subplots()
for i, sketch in enumerate(sketches):
    sketch = convert_sketch(sketch)
    bars1 = ax1.bar(x + i * width, correct_pred_values[:, i], width, label=sketch)

    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.0f}', 
                 ha='center', va='bottom', fontsize=9)

ax1.set_xlabel('Workloads')
ax1.set_ylabel('Accuracy Rate (%)')
ax1.set_xticks(x + width * (len(sketches) - 1) / 2)
ax1.set_xticklabels(datasets)
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3), ncol=4)
plt.tight_layout()

plt.savefig("tracker_pred_rate_multiple_sketches.pdf")


# Create the second figure for storage serving
fig2, ax2 = plt.subplots()
for i, sketch in enumerate(sketches):
    sketch = convert_sketch(sketch)
    bars2 = ax2.bar(x + i * width, storage_serving_values[:, i], width, label=sketch)

    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.0f}', 
                 ha='center', va='bottom', fontsize=9)

ax2.set_xlabel('Workloads')
ax2.set_ylabel('Storage Saving (x)')
# ax2.set_yscale("log")
ax2.set_xticks(x + width * (len(sketches) - 1) / 2)
ax2.set_xticklabels(datasets)
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3), ncol=4)
plt.yscale('log')

plt.tight_layout()
plt.savefig("tracker_storage_saving_multiple_sketches.pdf")

# Create the third figure for Benchmark time / Total requests
fig3, ax3 = plt.subplots()
for i, sketch in enumerate(sketches):
    sketch = convert_sketch(sketch)

    bars3 = ax3.bar(x + i * width, benchmark_write_time_values[:, i], width, label=sketch)
    
    for bar in bars3:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.1f}', 
                 ha='center', va='bottom', fontsize=9)
        
ax3.axhline(y=350, color='red', linestyle='--', linewidth=1)
ax3.text(len(x) - 1.5, 350 - 1.5, '350 us (Network)', color='red', ha='center', va='top', 
         fontsize=10, verticalalignment='top')
ax3.set_xlabel('Workloads')
ax3.set_ylabel('Overhead (us/req)')
ax3.set_xticks(x + width * (len(sketches) - 1) / 2)
ax3.set_xticklabels(datasets)
plt.yscale('log')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3), ncol=4)

plt.tight_layout()
plt.savefig("tracker_benchmark_write_time_multiple_sketches.pdf")

# Create the third figure for Benchmark time / Total requests
fig4, ax4 = plt.subplots()
for i, sketch in enumerate(sketches):
    sketch = convert_sketch(sketch)

    bars4 = ax4.bar(x + i * width, benchmark_read_time_values[:, i], width, label=sketch)
    for bar in bars4:
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.1f}', 
                 ha='center', va='bottom', fontsize=9)
        
ax4.axhline(y=350, color='red', linestyle='--', linewidth=1)
ax4.text(len(x) - 1.5, 350 - 1.5, '350 us (Network)', color='red', ha='center', va='top', 
         fontsize=10, verticalalignment='top')

ax4.set_xlabel('Workloads')
ax4.set_ylabel('Overhead (us/req)')
ax4.set_xticks(x + width * (len(sketches) - 1) / 2)
ax4.set_xticklabels(datasets)
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3), ncol=4)
plt.yscale('log')

plt.tight_layout()
plt.savefig("tracker_benchmark_read_time_multiple_sketches.pdf")

# Create the third figure for Benchmark time / Total requests
fig5, ax5 = plt.subplots()
for i, sketch in enumerate(sketches):
    sketch = convert_sketch(sketch)

    bars5 = ax5.bar(x + i * width, benchmark_ew_time_values[:, i], width, label=sketch)
    # Add data labels on top of each bar
    for bar in bars5:
        height = bar.get_height()
        ax5.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.1f}', 
                 ha='center', va='bottom', fontsize=9)
        
ax5.axhline(y=350, color='red', linestyle='--', linewidth=1)
ax5.text(len(x) - 1.5, 350 - 1.5, '350 us (Network)', color='red', ha='center', va='top', 
         fontsize=10, verticalalignment='top')

ax5.set_xlabel('Workloads')
ax5.set_ylabel('Overhead (us/req)')
ax5.set_xticks(x + width * (len(sketches) - 1) / 2)
ax5.set_xticklabels(datasets)
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3), ncol=4)
plt.yscale('log')

plt.tight_layout()
plt.savefig("tracker_benchmark_ew_time_multiple_sketches.pdf")

# Show plots (optional, if running interactively)
# plt.show()
