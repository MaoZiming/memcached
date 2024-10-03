import os
import matplotlib.pyplot as plt
import re
from datetime import datetime
import seaborn as sns
import pandas as pd

# Paths
log_dir = "/home/maoziming/memcached/cache/build/logs"
output_dir = "figures/"

BIG_SIZE = 12
FIGRATIO = 2/3
FIGWIDTH = 5
FIGHEIGHT = FIGWIDTH * FIGRATIO
FIGSIZE = (FIGWIDTH, FIGHEIGHT)

plt.rcParams.update({
    "figure.figsize": FIGSIZE,
    "figure.dpi": 300,
})

plt.rc("font", size=BIG_SIZE)
plt.rc("axes", titlesize=BIG_SIZE)
plt.rc("axes", labelsize=BIG_SIZE)
plt.rc("xtick", labelsize=BIG_SIZE)
plt.rc("ytick", labelsize=BIG_SIZE)
plt.rc("legend", fontsize=BIG_SIZE)
plt.rc("figure", titlesize=BIG_SIZE)

# Create output dir if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

BENCHMARKS = ["stale_bench",  "ttl_bench", "invalidate_bench", "update_bench", "adaptive_bench", "oracle_bench"]
DATASETS = ["PoissonMix", "Poisson", "PoissonWrite", "Tencent", "IBM", "Alibaba"]
SCALES = list(range(200))

dataset_to_reqs = {
    "Meta": 500000,
    "Twitter": 5000000,
    "IBM": 30000,
    "Tencent": 100000, 
    "Alibaba": 300000,
    "Poisson": 200000,
    "PoissonWrite": 200000,
    "PoissonMix": 200000
}

benchmark_to_print_name = {
    "stale_bench": "TTL (Inf.)",
    "ttl_bench": "TTL (1s)",
    "invalidate_bench": "Inv.", 
    "update_bench": "Upd.",
    "adaptive_bench": "Adpt.",
    "oracle_bench": "Oracle",
}


# Regex to match the log files
log_filename_pattern = re.compile(r"(\w+)_([^_]+)_scale(\d+)_([\d]{8}_[\d]{6})\.log.stats")

# Function to find the latest log for a given benchmark and dataset
def find_latest_log(benchmark, dataset, scale_):
    latest_time = None
    latest_log = None

    for log_file in os.listdir(log_dir):
        match = log_filename_pattern.match(log_file)
        if match:
            log_benchmark, log_dataset, scale, timestamp_str = match.groups()
            if int(scale) != scale_:
                continue
            if log_benchmark == benchmark and log_dataset == dataset:
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if latest_time is None or timestamp > latest_time:
                    latest_time = timestamp
                    latest_log = log_file
    return latest_log


log_pattern = re.compile(
    r"Average cache latency: ([\d.]+) us\s+"
    r"Average DB latency: ([\d.]+) us\s+"
    r"Median cache latency: ([\d.]+) us\s+"
    r"Median DB latency: ([\d.]+) us\s+"
    r"End-to-End Latency: ([\d.]+) ms\s+"
    r"Num operations: (\d+)"
)

data = {"Benchmark": [], "Dataset": [], "Scale": [], "Cache Latency": [], "DB Latency": [], "End-to-End Latency": [], "Throughput": []}

# Process each benchmark and dataset combination
for dataset in DATASETS:
    for benchmark in BENCHMARKS:
        for scale in SCALES:
            log_file = find_latest_log(benchmark, dataset, scale)
            if log_file:
                log_path = os.path.join(log_dir, log_file)
                with open(log_path, "r") as file:
                    log_content = file.read() 
                    
                # Search for the pattern in the entire log content
                match = log_pattern.search(log_content)
                if match:
                    # cache_latency = float(match.group(1))  # Average cache latency
                    # db_latency = float(match.group(2))     # Average DB latency
                    
                    cache_latency = float(match.group(3))  # Median cache latency
                    db_latency = float(match.group(4))     # Median DB latency
                    end_to_end_latency = float(match.group(5))
                    num_operations = int(match.group(6))

                    # Calculate throughput
                    throughput = num_operations / (end_to_end_latency / 1000)  # convert ms to seconds

                    # Append the extracted data to the data dictionary
                    data["Benchmark"].append(benchmark)
                    data["Dataset"].append(dataset)
                    data["Scale"].append(scale)
                    data["Cache Latency"].append(cache_latency / 1000)
                    data["DB Latency"].append(db_latency  / 1000)
                    data["End-to-End Latency"].append(end_to_end_latency)
                    data["Throughput"].append(throughput)

    if not data["Scale"]:
        continue

    # Convert the data to a DataFrame for easier handling
    df = pd.DataFrame(data)
    markers = ['o', 's', 'D', 'v', '^', 'P', '*', '+', 'x']
    benchmarks = df['Benchmark'].unique()
    max_throughput = df['Throughput'].max()

    def plot_figure(keyword, x_title, is_log=False):
        plt.figure(figsize=FIGSIZE)

        for i, benchmark in enumerate(benchmarks):
            # Filter data for the same benchmark
            subset = df[df['Benchmark'] == benchmark]
            subset = subset.sort_values(by='Scale')

            # Plot the line and points, connecting points with a line
            plt.plot(subset['Throughput'] / max_throughput * 100, subset[keyword], label=f'{benchmark_to_print_name[benchmark]}', marker=markers[i % len(markers)])
            
            # Add scale as text labels on top of the points
            for j, scale in enumerate(subset['Scale']):
                plt.text(subset['Throughput'].values[j] / max_throughput * 100, 
                        subset[keyword].values[j], 
                        str(scale), 
                        fontsize=8, 
                        ha='center', 
                        va='bottom')
                
        # Add labels and title
        plt.xlabel('Norm. Offered Load (%)')
        plt.ylabel(x_title)
        plt.legend()
        plt.tight_layout()
        # plt.ylim(2,10)
        if is_log:
            plt.yscale('log')

        # Show the plot
        plt.savefig(f'{dataset}_Throughput_vs_{keyword}.pdf')
        plt.figure(figsize=FIGSIZE)

    plot_figure('Cache Latency', 'Read Latency (ms)', False)
    plot_figure('DB Latency', 'Write Latency (ms)', False)

    data = {"Benchmark": [], "Dataset": [], "Scale": [], "Cache Latency": [], "DB Latency": [], "End-to-End Latency": [], "Throughput": []}
