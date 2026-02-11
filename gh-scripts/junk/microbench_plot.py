#!/usr/bin/env python3

import matplotlib.pyplot as plt
import re
import os
from collections import defaultdict

def parse_benchmark_file(filename):
    """Advanced parser that handles all template parameter cases"""
    benchmarks = []
    times = []
    throughputs = []

    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line.startswith('BM_'):
                continue

            # Split into components while handling template parameters
            parts = re.split(r'\s+', line)
            if len(parts) < 3:
                continue

            # Reconstruct benchmark name by joining until we hit a numeric value
            name_parts = []
            i = 0
            while i < len(parts) and not re.match(r'^\d+\.?\d*$', parts[i]):
                name_parts.append(parts[i])
                i += 1

            if i >= len(parts):
                continue

            full_name = ' '.join(name_parts)
            time_ns = float(parts[i])

            # Extract throughput if present
            throughput = None
            for part in parts:
                if 'bytes_per_second=' in part:
                    throughput = part.split('=')[1]
                    break

            benchmarks.append(full_name)
            times.append(time_ns)
            throughputs.append(throughput)

    return benchmarks, times, throughputs

def group_benchmarks(benchmarks, times, throughputs):
    """Group benchmarks by their base name before template parameters"""
    groups = defaultdict(list)

    for name, time, throughput in zip(benchmarks, times, throughputs):
        # Extract base name before first '<'
        base_match = re.match(r'^(BM_[^<]+)', name)
        if base_match:
            base_name = base_match.group(1).strip()
            groups[base_name].append((name, time, throughput))
        else:
            # For benchmarks without templates
            groups[name].append((name, time, throughput))

    return groups

def plot_benchmark_group(group_name, benchmarks, output_dir='benchmark_plots'):
    """Create visualization for one benchmark group"""
    if not benchmarks:
        return

    # Sort by time (fastest first)
    benchmarks.sort(key=lambda x: x[1])
    names = [b[0] for b in benchmarks]
    times = [b[1] for b in benchmarks]
    throughputs = [b[2] for b in benchmarks]

    plt.figure(figsize=(14, max(6, len(benchmarks)*0.6)))
    bars = plt.barh(names, times, color='#2ca02c')

    # Add value labels
    for bar, time, throughput in zip(bars, times, throughputs):
        label = f'{time:.2f} ns'
        if throughput:
            label += f'\n{throughput}'
        plt.text(bar.get_width()*1.02,
                bar.get_y() + bar.get_height()/2,
                label,
                va='center', ha='left',
                fontsize=9)

    plt.xlabel('Time (ns) - log scale')
    plt.title(f'{group_name} Benchmark Results', pad=20)
    plt.xscale('log')
    plt.grid(axis='x', alpha=0.3)

    # Clean up borders
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Sanitize filename
    safe_name = re.sub(r'[<>:"/\\|?*]', '_', group_name)
    output_file = os.path.join(output_dir, f'{safe_name}.png')

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Saved: {output_file}')

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        print("Usage: python plot_benchmarks.py <benchmark_output.txt>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)

    benchmarks, times, throughputs = parse_benchmark_file(input_file)
    if not benchmarks:
        print("Error: No benchmark data found in file")
        sys.exit(1)

    benchmark_groups = group_benchmarks(benchmarks, times, throughputs)
    for group_name, group_data in benchmark_groups.items():
        plot_benchmark_group(group_name, group_data)
