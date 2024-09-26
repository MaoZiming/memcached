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

BENCHMARKS = ["stale_bench",  "ttl_bench", "invalidate_bench", "update_bench", "adaptive_bench"]
DATASETS = ["IBM"]
SCALES = list(range(10, 500, 10))

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
}


# Regex to match the log files
log_filename_pattern = re.compile(r"(\w+)_([^_]+)_scale(\d+)_([\d]{8}_[\d]{6})\.log$")

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


# Regular expression to parse the log line
log_pattern = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - CPU Utilization: ([\d.]+)% \| "
    r"usr: [\d.]+%, sys: [\d.]+%, idle: [\d.]+%, iowait: [\d.]+%, steal: [\d.]+% \| "
    r"Network recv: (\d+) bytes, send: (\d+) bytes \| Disk read: (\d+) bytes, write: (\d+) bytes, "
    r"DB current_rpcs: (\d+), Cache current_rpcs: (\d+)"
)

max_times_data = {"Benchmark": [], "Dataset": [], "Throughput": [], "Scale": [], "CPU": [], "NW": [], "Disk": [], "db_current_rpcs": [], "cache_current_rpcs": []}

def get_throughput(dataset, time): 
    return dataset_to_reqs[dataset] / time

# Process each benchmark and dataset combination
for dataset in DATASETS:
    for benchmark in BENCHMARKS:
        for scale in SCALES:
            log_file = find_latest_log(benchmark, dataset, scale)
            if log_file:
                log_path = os.path.join(log_dir, log_file)
                cpu_utilizations = []
                network_recv_mb = []
                network_send_mb = []
                network_total_mb = []
                disk_read_mb = []
                disk_write_mb = []
                disk_total_mb = []
                timestamps = []
                db_current_rpcs_list = []
                cache_current_rpcs_list = []

                with open(log_path, "r") as file:
                    for line in file:
                        match = log_pattern.match(line)
                        if match:
                            timestamp_str = match.group(1)
                            cpu_utilization = float(match.group(2))
                            recv_bytes = int(match.group(3)) / (1024 * 1024)  # Convert to MB
                            send_bytes = int(match.group(4)) / (1024 * 1024)  # Convert to MB
                            read_bytes = int(match.group(5)) / (1024 * 1024)  # Convert to MB
                            write_bytes = int(match.group(6)) / (1024 * 1024)  # Convert to MB
                            db_current_rpcs = int(match.group(7))
                            cache_current_rpcs = int(match.group(8))
                            
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

                            timestamps.append(timestamp)
                            cpu_utilizations.append(cpu_utilization)
                            network_recv_mb.append(recv_bytes)
                            network_send_mb.append(send_bytes)
                            network_total_mb.append(recv_bytes + send_bytes)
                            disk_read_mb.append(read_bytes)
                            disk_write_mb.append(write_bytes)
                            disk_total_mb.append(read_bytes + write_bytes)
                            cache_current_rpcs_list.append(cache_current_rpcs)
                            db_current_rpcs_list.append(db_current_rpcs)
                        else:
                            print(line)
                                                        
                if not timestamps:
                    print(f"No data found in {log_file}")
                    continue

                start_time = timestamps[0]
                relative_times = [(ts - start_time).total_seconds() for ts in timestamps]
                
                max_times_data['Benchmark'].append(benchmark)
                max_times_data['Dataset'].append(dataset)
                max_times_data['Throughput'].append(get_throughput(dataset, max(relative_times)))
                max_times_data['Scale'].append(scale)
                max_times_data['NW'].append(sum(network_total_mb[2:-2]) / len(network_total_mb[2:-2]))
                max_times_data["CPU"].append(sum(cpu_utilizations[2:-2]) / len(cpu_utilizations[2:-2]))
                max_times_data["Disk"].append(sum(disk_total_mb[2:-2]) / len(disk_total_mb[2:-2]))
                max_times_data["cache_current_rpcs"].append(max(1, sum(cache_current_rpcs_list[2:-2]) / len(cache_current_rpcs_list[2:-2])))
                max_times_data['db_current_rpcs'].append(max(1, sum(db_current_rpcs_list[2:-2]) / len(db_current_rpcs_list[2:-2])))
                
    print(max_times_data)

    # Convert the data to a DataFrame for easier handling
    df = pd.DataFrame(max_times_data)

    # Group by benchmark
    benchmarks = df['Benchmark'].unique()
    max_throughput = df['Throughput'].max()
    markers = ['o', 's', 'D', 'v', '^', 'P', '*', '+', 'x']


    def plot_figure(keyword, x_title, is_log=False):
        plt.figure(figsize=FIGSIZE)

        for i, benchmark in enumerate(benchmarks):
            # Filter data for the same benchmark
            subset = df[df['Benchmark'] == benchmark]
            subset = subset.sort_values(by='Scale')

            # print(keyword)
            # print(subset['Throughput'] / max_throughput * 100)
            # print(subset[keyword])

            # Plot the line and points, connecting points with a line
            plt.plot(subset['Throughput'] / max_throughput * 100, subset[keyword], label=f'{benchmark_to_print_name[benchmark]}',marker=markers[i % len(markers)])
            
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
        if is_log:
            plt.yscale('log')

        # Show the plot
        plt.savefig(f'{dataset}_Throughput_vs_{keyword}.pdf')
        plt.figure(figsize=FIGSIZE)
        
    
    # plot_figure('CPU', 'Avg. CPU Util. (%)')
    plot_figure('NW', 'Avg. NW Usage (MB/s)')
    # plot_figure('Disk', 'Avg. Disk Usage (MB/s)')
    # plot_figure('num_active_connections', '# active conn.')
    # plot_figure('db_current_rpcs', '# ongoing RPCs (to DB)')
    plot_figure('cache_current_rpcs', '# ongoing RPCs (to cache)', True)

    max_times_data = {"Benchmark": [], "Dataset": [], "Throughput": [], "Scale": [], "CPU": [], "NW": [], "Disk": [], "db_current_rpcs": [], "cache_current_rpcs": []}
