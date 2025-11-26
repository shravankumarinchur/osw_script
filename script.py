import os
import re
import gzip
import shutil
import contextlib
from datetime import datetime

def filter_files_by_timerange(directory, start_str, end_str):
    """
    Filters files in the given directory based on the provided start and end timestamps.
    Expected format: yy.mm.dd.hhmm (e.g., 25.09.08.0100)
    Works for oswtop, oswmeminfo, oswiostat, etc.
    """
    def parse_filename(filename):
        try:
            # Split on last "_" and strip .dat
            timestamp_str = filename.rsplit("_", 1)[-1].replace(".dat", "")
            return datetime.strptime(timestamp_str, "%y.%m.%d.%H%M")
        except Exception:
            return None

    start_dt = datetime.strptime(start_str, "%y.%m.%d.%H%M")
    end_dt   = datetime.strptime(end_str, "%y.%m.%d.%H%M")

    filtered_files = []
    for fname in sorted(os.listdir(directory)):
        if fname.endswith(".dat"):
            ftime = parse_filename(fname)
            if ftime and start_dt <= ftime <= end_dt:
                filtered_files.append(fname)

    return filtered_files



def get_oswarchive_path():
    while True:
        path = input("Enter the absolute path to the OSWatcher archive directory: ").strip()
        if os.path.isdir(path):
            return path
        else:
            print("Invalid directory path. Please try again.")

def unzip_gz_files(directory, silent=True):
    try:
        files = os.listdir(directory)
    except Exception as e:
        if not silent:
            print(f"Error accessing directory '{directory}': {e}")
        return False  # Signal failure to calling function

    success = True
    for file in files:
        if file.endswith(".gz"):
            gz_path = os.path.join(directory, file)
            dat_path = gz_path[:-3]
            if not os.path.exists(dat_path):
                if not silent:
                    print(f"Unzipping: {file}")
                try:
                    with gzip.open(gz_path, 'rb') as f_in:
                        with open(dat_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(gz_path)
                except Exception as e:
                    print(f"Error unzipping {file}: {e}")
                    success = False  # One file failed, still keep going

    return success  # True if all/unzipped or already extracted, False if any error occurred

def prepare_all_archives(base_dir, subdirectories):
    """
    Checks and unzips files for all required OSWatcher subdirectories.
    """
    print("Preparing all required archives...")
    for subdir in subdirectories:
        full_path = os.path.join(base_dir, subdir)
        print(f"\n--- Checking '{subdir}' directory ---")
        unzip_gz_files(full_path, silent=False)
    print("\nAll archives are ready for analysis.")


def run_cpu_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"cpu_analysis{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    cpu_cores = get_cpu_cores_from_vmstat(oswvmstat_dir)
    threshold_75 = 0.75 * cpu_cores

    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        process_oswtop_files(oswtop_dir, cpu_cores, threshold_75, file_list)

    print(f"CPU analysis written to: {output_path}")
    return True


def run_memory_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"memory_analysis{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        process_oswmeminfo_files(oswmeminfo_dir, file_list)

    print(f"Memory analysis written to: {output_path}")
    return True


def run_vmstat_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"vmstat_analysis{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    cpu_cores = get_cpu_cores_from_vmstat(oswvmstat_dir)

    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        process_oswvmstat_files(oswvmstat_dir, cpu_cores, file_list)

    print(f"vmstat analysis written to: {output_path}")
    return True


def run_dstate_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"dstate_and_high_resource_processes{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        analyze_oswtop_data(oswtop_dir, file_list)

    print(f"D-state and High Resource Process analysis written to: {output_path}")
    return True


def run_disk_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"disk_and_iowait_details{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        analyze_iostat_files(oswiostat_dir, file_list)

    print(f"Disk and IOwait analysis written to: {output_path}")
    return True


def run_netstat_analysis(file_list=None, output_suffix=""):
    
    output_filename = f"netstat_details{output_suffix}.txt"
    output_path = os.path.join(archive_dir, output_filename)
    with open(output_path, "w") as f, contextlib.redirect_stdout(f):
        analyze_netstat_files(oswnetstat_dir, file_list)

    print(f"Netstat analysis written to: {output_path}")
    return True

def get_cpu_cores_from_vmstat(vmstat_dir):
    #unzip_gz_files(vmstat_dir)
    for file in sorted(os.listdir(vmstat_dir)):
        if file.endswith(".dat"):
            file_path = os.path.join(vmstat_dir, file)
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("VCPUS"):
                        try:
                            cores = int(line.strip().split()[1])
                            print(f"\nDetected CPU Cores (VCPUS): {cores}")
                            return cores
                        except (IndexError, ValueError):
                            pass
    print("Could not determine CPU cores from vmstat data.")
    exit(1)

def extract_date_from_filename(filename):
    match = re.search(r"_(\d{2}\.\d{2}\.\d{2})\.\d{4}\.dat$", filename)
    return match.group(1) if match else "Unknown Date"

def detect_increasing_load_patterns(load_data, cpu_cores, min_consecutive=6):
    threshold_50 = 0.5 * cpu_cores
    increasing_patterns = []
    temp_pattern = []
    tracking = False

    for i in range(1, len(load_data)):
        prev_time, prev_date, prev_load_1m, _, _ = load_data[i - 1]
        curr_time, curr_date, curr_load_1m, _, _ = load_data[i]

        if curr_load_1m > threshold_50:
            if not tracking and prev_load_1m > threshold_50:
                tracking = True
                temp_pattern = [(prev_time, prev_date, prev_load_1m)]

            if curr_load_1m > prev_load_1m:
                temp_pattern.append((curr_time, curr_date, curr_load_1m))
            else:
                if len(temp_pattern) >= min_consecutive:
                    increasing_patterns.append(temp_pattern)
                temp_pattern = []
                tracking = False
        else:
            tracking = False
            temp_pattern = []

    if len(temp_pattern) >= min_consecutive:
        increasing_patterns.append(temp_pattern)

    if increasing_patterns:
        print("\n=== Detected Increasing Load Average Patterns (5+ consecutive) ===")
        for pattern in increasing_patterns:
            print("Pattern Detected:")
            for time_val, date_val, load in pattern:
                print(f"  [{date_val} {time_val}] Load: {load:.2f}")
            print("-" * 40)

def detect_decreasing_load_patterns(load_data, cpu_cores, min_consecutive=6):
    threshold_75 = 0.75 * cpu_cores
    decreasing_patterns = []
    temp_pattern = []
    tracking = False

    for i in range(len(load_data) - 1):
        curr_time, curr_date, curr_load_1m, _, _ = load_data[i]
        next_time, next_date, next_load_1m, _, _ = load_data[i + 1]

        if not tracking:
            if curr_load_1m > threshold_75:
                tracking = True
                temp_pattern = [(curr_time, curr_date, curr_load_1m)]
        else:
            if next_load_1m < curr_load_1m:
                temp_pattern.append((next_time, next_date, next_load_1m))
            else:
                if len(temp_pattern) >= min_consecutive:
                    decreasing_patterns.append(temp_pattern)
                temp_pattern = []
                tracking = False

    if len(temp_pattern) >= min_consecutive:
        decreasing_patterns.append(temp_pattern)

    if decreasing_patterns:
        print("\n=== Detected Decreasing Load Average Patterns (6+ consecutive) ===")
        for pattern in decreasing_patterns:
            print("Decreasing Pattern Detected:")
            for time_val, date_val, load in pattern:
                print(f"  [{date_val} {time_val}] Load: {load:.2f}")
            print("-" * 40)
    else:
        print("\nNo significant decreasing load average patterns detected.")

def process_oswtop_files(directory, cpu_cores, threshold_75, file_list=None):
    pattern = re.compile(r"^top - (\d{2}:\d{2}:\d{2}) .*load average: ([\d.]+), ([\d.]+), ([\d.]+)")
    highest, lowest = None, None
    load_data = []

    print(f"\n======== Analyzing Server instances where CPU crossed 75%+ usage=============\n")
    print(f"\n The total cpu cores : {cpu_cores}\n")

    # Either analyze all files or filtered ones
    files_to_process = file_list if file_list else sorted(os.listdir(directory))

    for filename in files_to_process:
        if filename.endswith(".dat"):
            filepath = os.path.join(directory, filename)
            date = extract_date_from_filename(filename)

            with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
                for line in file:
                    match = pattern.search(line)
                    if match:
                        timestamp, load_avg_1, load_avg_5, load_avg_15 = match.groups()
                        load_avg_1 = float(load_avg_1)
                        load_avg_5 = float(load_avg_5)
                        load_avg_15 = float(load_avg_15)
                        load_data.append((timestamp, date, load_avg_1, load_avg_5, load_avg_15))

                        if load_avg_1 > threshold_75:
                            print(f"{filename} - {timestamp} | Load Avg (1m: {load_avg_1}, 5m: {load_avg_5}, 15m: {load_avg_15})")

                        if highest is None or load_avg_1 > highest[0]:
                            highest = (load_avg_1, timestamp, date, filename)
                        if lowest is None or load_avg_1 < lowest[0]:
                            lowest = (load_avg_1, timestamp, date, filename)

    if highest:
        print(f"\n======= Peak Load Summary =======\n"
              f"Filename: {highest[3]}\nDate: {highest[2]}\nTime: {highest[1]}\nPeak Load Avg: {highest[0]}\n")

    if lowest:
        print(f"\n======= Lowest Load Summary =======\n"
              f"Filename: {lowest[3]}\nDate: {lowest[2]}\nTime: {lowest[1]}\nLowest Load Avg: {lowest[0]}\n")

    detect_increasing_load_patterns(load_data, cpu_cores, min_consecutive=6)
    detect_decreasing_load_patterns(load_data, cpu_cores, min_consecutive=6)


def detect_increasing_memory_patterns(mem_data, min_consecutive=6):
    pattern = []
    tracking = False
    increasing_patterns = []

    for i in range(1, len(mem_data)):
        prev = mem_data[i - 1]
        curr = mem_data[i]

        if curr[1] > 50:
            if not tracking and prev[1] > 50:
                tracking = True
                pattern = [prev]

            if curr[1] > prev[1]:
                pattern.append(curr)
            else:
                if len(pattern) >= min_consecutive:
                    increasing_patterns.append(pattern)
                pattern = []
                tracking = False
        else:
            pattern = []
            tracking = False

    if len(pattern) >= min_consecutive:
        increasing_patterns.append(pattern)

    if increasing_patterns:
        print("\n=== Detected Increasing Memory Usage Patterns (5+ consecutive) ===")
        for p in increasing_patterns:
            print("Pattern Detected:")
            for entry in p:
                ts, used_pct, used_gb, free_gb = entry
                print(f"  [{ts}] Used: {used_pct:.2f}% ({used_gb:.2f} GB), Free: {free_gb:.2f} GB")
            print("-" * 40)

def detect_decreasing_memory_patterns(mem_data, min_consecutive=6):
    pattern = []
    tracking = False
    decreasing_patterns = []

    for i in range(len(mem_data) - 1):
        curr = mem_data[i]
        next_ = mem_data[i + 1]

        if not tracking:
            if curr[1] > 75:
                tracking = True
                pattern = [curr]
        else:
            if next_[1] < curr[1]:
                pattern.append(next_)
            else:
                if len(pattern) >= min_consecutive:
                    decreasing_patterns.append(pattern)
                pattern = []
                tracking = False

    if len(pattern) >= min_consecutive:
        decreasing_patterns.append(pattern)

    if decreasing_patterns:
        print("\n=== Detected Decreasing Memory Usage Patterns (6+ consecutive) ===")
        for p in decreasing_patterns:
            print("Pattern Detected:")
            for entry in p:
                ts, used_pct, used_gb, free_gb = entry
                print(f"  [{ts}] Used: {used_pct:.2f}% ({used_gb:.2f} GB), Free: {free_gb:.2f} GB")
            print("-" * 40)
    else:
        print("\nNo significant decreasing memory usage patterns detected.")



def process_oswmeminfo_files(meminfo_dir, file_list=None):
    highest, lowest = None, None
    total_gb = None
    printed_total = False
    mem_data = []
    found_above_75 = False

    print("\n======== Analyzing Memory Usage above 75% =========\n")

    # Either analyze all files or only the filtered ones
    #if file_list:
       # files_to_process = file_list
    #else:
       # files_to_process = sorted(os.listdir(meminfo_dir))

    files_to_process = file_list if file_list else sorted(os.listdir(meminfo_dir))

    for filename in files_to_process:
        if filename.endswith(".dat"):
            filepath = os.path.join(meminfo_dir, filename)

            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                timestamp = None
                values = {}

                for line in f:
                    if line.startswith("zzz "):
                        if values:
                            try:
                                total = int(values["MemTotal"])
                                free = int(values["MemFree"])
                                buffers = int(values["Buffers"])
                                cached = int(values["Cached"])

                                free_mem_kb = free + buffers + cached
                                used_mem_kb = total - free_mem_kb
                                used_pct = (used_mem_kb / total) * 100
                                free_pct = 100 - used_pct

                                total_gb = total / (1024 * 1024)
                                used_gb = used_mem_kb / (1024 * 1024)
                                free_gb = free_mem_kb / (1024 * 1024)

                                if not printed_total:
                                    print(f"Total Memory on Server: {total_gb:.2f} GB\n")
                                    printed_total = True

                                if used_pct > 75:
                                    found_above_75 = True
                                    print(f"{timestamp} | Used: {used_pct:.2f}% "
                                          f"({used_gb:.2f} GB), Free: {free_pct:.2f}% "
                                          f"({free_gb:.2f} GB) | File: {filename}")

                                mem_data.append((timestamp, used_pct, used_gb, free_gb))

                                if highest is None or used_pct > highest[0]:
                                    highest = (used_pct, timestamp, used_gb, free_gb, filename)
                                if lowest is None or used_pct < lowest[0]:
                                    lowest = (used_pct, timestamp, used_gb, free_gb, filename)

                            except KeyError:
                                pass

                        values = {}
                        timestamp = line.strip().replace("zzz ", "").replace("***", "")

                    else:
                        parts = line.split()
                        if len(parts) >= 2 and parts[0].endswith(":"):
                            key = parts[0].rstrip(":")
                            val = parts[1]
                            if key in ["MemTotal", "MemFree", "Buffers", "Cached"]:
                                values[key] = val

    if not found_above_75:
        print("No occurrences found where memory usage > 75%.")

    if highest:
        print(f"\n======= Peak Memory Usage Summary =======")
        print(f"Filename: {highest[4]}")
        print(f"Timestamp: {highest[1]}")
        print(f"Used: {highest[0]:.2f}% ({highest[2]:.2f} GB), "
              f"Free: {100 - highest[0]:.2f}% ({highest[3]:.2f} GB)")

    if lowest:
        print(f"\n======= Lowest Memory Usage Summary =======")
        print(f"Filename: {lowest[4]}")
        print(f"Timestamp: {lowest[1]}")
        print(f"Used: {lowest[0]:.2f}% ({lowest[2]:.2f} GB), "
              f"Free: {100 - lowest[0]:.2f}% ({lowest[3]:.2f} GB)")

    detect_increasing_memory_patterns(mem_data, min_consecutive=6)
    detect_decreasing_memory_patterns(mem_data, min_consecutive=6)


def process_oswvmstat_files(vmstat_dir, cpu_cores, file_list=None):
    print("\n======== Analyzing vmstat output where 'r' > CPU cores ========\n")

    r_exceeds = []
    
    files_to_process = file_list if file_list else sorted(os.listdir(vmstat_dir))

    for filename in files_to_process:
        if filename.endswith(".dat"):
            filepath = os.path.join(vmstat_dir, filename)
            timestamp = None
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("zzz "):
                        timestamp = line.strip().replace("zzz ", "").replace("***", "")
                    elif re.match(r"\s*\d+", line):
                        columns = re.split(r"\s+", line.strip())
                        if len(columns) >= 6:
                            try:
                                r_val = int(columns[0])
                                b_val = int(columns[1])
                                if r_val > cpu_cores:
                                    r_exceeds.append((timestamp, r_val, b_val))
                            except ValueError:
                                continue

    if r_exceeds:
        print("Detected times where 'r' (running processes) > CPU cores:\n")
        for ts, r, b in r_exceeds:
            print(f"  [{ts}] r = {r}, b = {b}")
        print(f"\nTotal occurrences: {len(r_exceeds)}")
    else:
        print("No 'r' values exceeding CPU cores detected.")





def analyze_oswtop_data(oswtop_dir, file_list=None):
    print("analysing the D state processes.") 
    timestamp_header_pattern = re.compile(r'^zzz \*\*\*(.*?)$')

    # Pattern to match process lines
    process_line_pattern = re.compile(
        r'^\s*(\d+)\s+(\S+)\s+\d+\s+\S+\s+\S+\s+\S+\s+\S+\s+([RSDZTW])\s+([\d.]+)\s+([\d.]+)\s+[\d:.]+\s+(.+)$'
    )

    files_to_process = file_list if file_list else sorted(os.listdir(oswtop_dir))
    
    for file in files_to_process:
        filepath = os.path.join(oswtop_dir, file)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        current_timestamp = None
        process_list = []

        for line in lines:
            line = line.strip()

            match_ts = timestamp_header_pattern.match(line)
            if match_ts:
                if current_timestamp and process_list:
                    d_processes = [proc for proc in process_list if proc['state'] == 'D']
                    if d_processes:
                        print(f"\n[{current_timestamp}] D-state Processes (Count: {len(d_processes)}):")
                        for proc in d_processes:
                            print(f"PID={proc['pid']}, USER={proc['user']}, STATE={proc['state']}, CPU={proc['cpu']}%, MEM={proc['mem']}%, CMD={proc['cmd']}")
                current_timestamp = match_ts.group(1)
                process_list = []
                continue

            match_proc = process_line_pattern.match(line)
            if match_proc:
                proc_info = {
                    'pid': match_proc.group(1),
                    'user': match_proc.group(2),
                    'state': match_proc.group(3),
                    'cpu': float(match_proc.group(4)),
                    'mem': float(match_proc.group(5)),
                    'cmd': match_proc.group(6)
                }
                process_list.append(proc_info)

        # Final block for last timestamp
        if current_timestamp and process_list:
            d_processes = [proc for proc in process_list if proc['state'] == 'D']
            if d_processes:
                print(f"\n[{current_timestamp}] D-state Processes (Count: {len(d_processes)}):")
                for proc in d_processes:
                    print(f"PID={proc['pid']}, USER={proc['user']}, STATE={proc['state']}, CPU={proc['cpu']}%, MEM={proc['mem']}%, CMD={proc['cmd']}")






def analyze_iostat_files(directory, file_list=None):
    iowait_records = []  # To store tuples (timestamp, iowait)
    high_util_disks = []  # To store tuples (timestamp, disk, read_MBps, write_MBps, util%)
    
    def kb_to_mb(kb):
        return kb / 1024.0
    
    files_to_process = file_list if file_list else sorted(os.listdir(directory))
    
    for filename in files_to_process:
        if not filename.endswith(".dat"):
            continue
        filepath = os.path.join(directory, filename)
        with open(filepath, 'r') as f:
            timestamp = None
            header = None
            for line in f:
                line = line.strip()
                # Extract timestamp
                if line.startswith('zzz') or line.startswith('***'):
                    timestamp = line.split('***')[-1].strip()
                    continue
                # Handle avg-cpu section
                if line.startswith('avg-cpu:'):
                    try:
                        cpu_line = next(f).strip()
                        parts = cpu_line.split()
                        if len(parts) >= 4:
                            iowait = float(parts[3])
                            iowait_records.append((timestamp, iowait))
                    except StopIteration:
                        continue
                    continue
                # Look for header line to determine column indices
                if line.startswith('Device'):
                    header = line.split()
                    continue
                if line == '':
                    continue
                # Process device statistics lines
                if header and not line.startswith('avg-cpu:') and not line.startswith('zzz') and not line.startswith('***'):
                    parts = line.split()
                    if len(parts) < len(header):
                        continue
                    try:
                        device = parts[0]
                        # Dynamically find indices for rkB/s, wkB/s, and %util based on header
                        read_kBps_idx = header.index('rkB/s') if 'rkB/s' in header else -1
                        write_kBps_idx = header.index('wkB/s') if 'wkB/s' in header else -1
                        util_idx = header.index('%util') if '%util' in header else -1
                        
                        if read_kBps_idx == -1 or write_kBps_idx == -1 or util_idx == -1:
                            continue
                            
                        read_kBps = float(parts[read_kBps_idx])
                        write_kBps = float(parts[write_kBps_idx])
                        util = float(parts[util_idx])
                    except (ValueError, IndexError):
                        continue
                    if util > 50.0:
                        read_MBps = kb_to_mb(read_kBps)
                        write_MBps = kb_to_mb(write_kBps)
                        high_util_disks.append((timestamp, device, read_MBps, write_MBps, util))
    
    # Print top 20 iowait values
    print("Top 30 highest iowait values:")
    for ts, io in sorted(iowait_records, key=lambda x: x[1], reverse=True)[:30]:
        print(f"{ts} - iowait: {io:.2f}%")
    
    # Print high-utilization disks
    print("\nDisks with utilization > 50%:")
    for ts, dev, r_mb, w_mb, util in high_util_disks:
        print(f"{ts} - Device: {dev}, Read: {r_mb:.2f} MB/s, Write: {w_mb:.2f} MB/s, Utilization: {util:.2f}%")


def analyze_netstat_files(directory, file_list=None):
    packet_drop_records = []  # (timestamp, interface, direction, drops)
    error_records = []        # (timestamp, interface, direction, errors)

    files_to_process = file_list if file_list else sorted(os.listdir(directory))
    
    for filename in files_to_process:
        if not filename.endswith(".dat"):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, "r") as f:
            timestamp = None
            current_interface = None
            rx_line = None

            for line in f:
                line = line.strip()

                # Extract timestamp
                if line.startswith("zzz") or line.startswith("***"):
                    timestamp = line.split("***")[-1].strip()
                    current_interface = None
                    rx_line = None
                    continue

                if not timestamp or line.startswith("#kernel"):
                    continue

                if re.match(r"^\d+:\s+\S+:", line):
                    current_interface = line.split(":")[1].split("<")[0].strip()
                    continue

                # RX
                if line.startswith("RX:"):
                    rx_line = next(f).strip()
                    try:
                        rx_parts = rx_line.split()
                        rx_errors = float(rx_parts[2])
                        rx_drops = float(rx_parts[3])
                        if rx_drops > 0:
                            packet_drop_records.append((timestamp, current_interface, "RX", rx_drops))
                        if rx_errors > 0:
                            error_records.append((timestamp, current_interface, "RX", rx_errors))
                    except (ValueError, IndexError):
                        continue
                    continue

                # TX
                if line.startswith("TX:"):
                    tx_line = next(f).strip()
                    try:
                        tx_parts = tx_line.split()
                        tx_errors = float(tx_parts[2])
                        tx_drops = float(tx_parts[3])
                        if tx_drops > 0:
                            packet_drop_records.append((timestamp, current_interface, "TX", tx_drops))
                        if tx_errors > 0:
                            error_records.append((timestamp, current_interface, "TX", tx_errors))
                    except (ValueError, IndexError):
                        continue

    # ---------- DROP ANALYSIS ----------
    print("\n Top 10 Packet Drops (All):")
    if packet_drop_records:
        for ts, iface, direction, drops in sorted(packet_drop_records, key=lambda x: x[3], reverse=True)[:10]:
            print(f"{ts} - Interface: {iface} [{direction}], Drops: {drops:.0f}")
    else:
        print("No packet drops recorded.")

    print("\n Top 10 Unique Packet Drop Values:")
    unique_drops = {}
    for ts, iface, direction, drops in packet_drop_records:
        if drops not in unique_drops:
            unique_drops[drops] = (ts, iface, direction, drops)
    for drops, (ts, iface, direction, _) in sorted(unique_drops.items(), key=lambda x: x[0], reverse=True)[:10]:
        print(f"{ts} - Interface: {iface} [{direction}], Drops: {drops:.0f}")

    # ---------- ERROR ANALYSIS ----------
    print("\n Top 10 Packet Errors (All):")
    if error_records:
        for ts, iface, direction, errors in sorted(error_records, key=lambda x: x[3], reverse=True)[:10]:
            print(f"{ts} - Interface: {iface} [{direction}], Errors: {errors:.0f}")
    else:
        print("No packet errors recorded.")

    print("\n Top 10 Unique Packet Error Values:")
    unique_errors = {}
    for ts, iface, direction, errors in error_records:
        if errors not in unique_errors:
            unique_errors[errors] = (ts, iface, direction, errors)
    for errors, (ts, iface, direction, _) in sorted(unique_errors.items(), key=lambda x: x[0], reverse=True)[:10]:
        print(f"{ts} - Interface: {iface} [{direction}], Errors: {errors:.0f}")

if __name__ == "__main__":
    archive_dir = get_oswarchive_path()

    oswtop_dir = os.path.join(archive_dir, "oswtop")
    oswvmstat_dir = os.path.join(archive_dir, "oswvmstat")
    oswmeminfo_dir = os.path.join(archive_dir, "oswmeminfo")
    oswiostat_dir= os.path.join(archive_dir, "oswiostat")
    oswnetstat_dir= os.path.join(archive_dir, "oswnetstat")
    
   #unzip_gz_files(oswvmstat_dir)
   #unzip_gz_files(oswmeminfo_dir)
   # unzip_gz_files(oswiostat_dir)
    required_dirs = ["oswtop", "oswvmstat", "oswmeminfo", "oswiostat", "oswnetstat"]
    prepare_all_archives(archive_dir, required_dirs)

    
    while True:
        
        print("\n========== OSWatcher Analysis Menu ==========")
        print("1. Run All Analyses")
        print("2. Custom Time Range Analysis (any analysis type)")
        print("3. Exit")  

        choice = input("Enter your choice (1-3): ").strip()

        if choice == "1":
            print("\n  Running All Analyses...\n")
            run_cpu_analysis()
            run_memory_analysis()
            run_vmstat_analysis()
            run_dstate_analysis()
            run_disk_analysis()
            run_netstat_analysis()
            print("\n All analyses completed successfully!")            
        
        elif choice == "2":
            print("\n========== Custom Time Range Analysis ==========")
            print("Select analysis type for time range:")
        
            print("\n Enter time range in format yy.mm.dd.hhmm (e.g., 25.09.08.0100)")
            start_str = input("From: ").strip()
            end_str   = input("To: ").strip()

            # Filter files based on type
            selected_files_cpu = filter_files_by_timerange(oswtop_dir, start_str, end_str)
            selected_files_memory = filter_files_by_timerange(oswmeminfo_dir, start_str, end_str)
            selected_files_vmstat = filter_files_by_timerange(oswvmstat_dir, start_str, end_str)
            selected_files_disk = filter_files_by_timerange(oswiostat_dir, start_str, end_str)
            selected_files_netstat = filter_files_by_timerange(oswnetstat_dir, start_str, end_str)
            
            output_suffix = "_timerange"
            
            # Execute based on user choice
            
            print("\nRunning all analyses for custom time range...\n")
            analyses_run = False
                    
            if selected_files_cpu:
                 run_cpu_analysis(selected_files_cpu, output_suffix)
                 analyses_run = True
            else:
                print("No CPU files found in the given range.")
                        
            if selected_files_memory:
                 run_memory_analysis(selected_files_memory, output_suffix)
                 analyses_run = True
            else:
                 print("No Memory files found in the given range.")
                        
            if selected_files_vmstat:
                 run_vmstat_analysis(selected_files_vmstat, output_suffix)
                 analyses_run = True
            else:
                 print("No VMStat files found in the given range.")
                        
            if selected_files_cpu:
                     run_dstate_analysis(selected_files_cpu, output_suffix)
                     analyses_run = True
            else:
                print("No OSWtop files found in the given range for D-state analysis.")
                        
            if selected_files_disk:
                        run_disk_analysis(selected_files_disk, output_suffix)
                        analyses_run = True
            else:
                 print("No Disk (iostat) files found in the given range.")
                        
            if selected_files_netstat:
                      run_netstat_analysis(selected_files_netstat, output_suffix)
                      analyses_run = True
            else:
                 print("No Netstat files found in the given range.")
                    
            if analyses_run:
                      print("\nAll time-range analyses completed successfully!")
            else:
                print("\nNo files found in the given time range for any analysis type.")
           
        elif choice == "3":
            print(" Exiting.")
            break
