#!/usr/bin/env python3
import subprocess
import time
from datetime import datetime
from typing import Tuple
import numpy as np
from scipy import stats

class PSQLExecutor:
    def __init__(self, host: str, database: str, user: str, port: int):
        self.host = host
        self.database = database
        self.user = user
        self.port = port

    def execute_update(self, sql: str, timeout: int = 30) -> Tuple[int, str, float]:
        """
        Execute SQL command and return (exit_code, output, duration)
        """
        cmd = [
            'psql',
            '-h', self.host,
            '-d', self.database,
            '-U', self.user,
            '-p', str(self.port),
            '-c', sql,
            '-v', 'ON_ERROR_STOP=1'
        ]

        start_time = time.time()

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False
            )

            duration = time.time() - start_time
            output = result.stdout

            # Include stderr in output if there's an error
            if result.stderr:
                output += f"\nSTDERR: {result.stderr}"

            return result.returncode, output.strip(), duration

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return 124, f"Timeout after {timeout} seconds", duration
        except Exception as e:
            duration = time.time() - start_time
            return -1, f"Execution failed: {str(e)}", duration

def main():
    # Setup here!
    config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'port': 6162,
        'sql': 'insert into t1 select a2.x * 1000 + a3.x, a2.x, a3.x from generate_series(1, 1000) as a2(x), generate_series(1, 1000) as a3(x);',
        'iterations': 30,
        'timeout': 30,
        # Optional: SQL to run after each iteration (not measured). E.g., 'delete from t1;'
        'post_iteration_sql': 'delete from t1;',
    }

    executor = PSQLExecutor(
        host=config['host'],
        database=config['database'],
        user=config['user'],
        port=config['port']
    )

    # Log file
    log_filename = f"psql_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    print(f"Starting {config['iterations']} iterations")
    print(f"Database: {config['host']}:{config['port']}/{config['database']}")
    print(f"SQL: {config['sql']}")
    if config.get('post_iteration_sql'):
        print(f"Post-iteration SQL (not measured): {config['post_iteration_sql']}")
    print("=" * 50)

    with open(log_filename, 'w') as log_file:
        # Write header to log
        log_file.write(f"Execution started at: {datetime.now()}\n")
        log_file.write(f"Configuration: {config}\n")
        log_file.write("=" * 50 + "\n\n")

        success_count = 0
        error_count = 0

        for i in range(1, config['iterations'] + 1):
            print(f"\n=== Iteration {i}/{config['iterations']} ===")
            log_file.write(f"\n=== Iteration {i}/{config['iterations']} ===\n")

            exit_code, output, duration = executor.execute_update(
                config['sql'],
                config['timeout']
            )

            # Print and log results
            result_line = f"Duration: {duration:.3f}s, Exit Code: {exit_code}"
            print(result_line)
            print("Output:", output)

            log_file.write(f"Start time: {datetime.now()}\n")
            log_file.write(f"{result_line}\n")
            log_file.write(f"Output:\n{output}\n")

            # Count results
            if exit_code == 0:
                success_count += 1
            else:
                error_count += 1

            # Execute post-iteration SQL if configured (not measured)
            if config.get('post_iteration_sql'):
                post_exit_code, post_output, _ = executor.execute_update(
                    config['post_iteration_sql'],
                    config['timeout']
                )
                if post_exit_code != 0:
                    print(f"Post-iteration SQL failed: {post_output}")
                    log_file.write(f"Post-iteration SQL failed: {post_output}\n")

            # Optional: Add delay between iterations
            # time.sleep(0.1)

            # Break on persistent non-timeout errors
            if exit_code not in (0, 124) and error_count >= 3:
                print(f"\nToo many errors ({error_count}). Stopping...")
                break

        # Summary
        summary = f"""
{'=' * 50}
Execution Summary:
Total iterations: {config['iterations']}
Successful: {success_count}
Failed: {error_count}
Completion time: {datetime.now()}
Log file: {log_filename}
{'=' * 50}
"""
        print(summary)
        log_file.write(summary)

def print_percentiles(vals, name, baseline=None):
    """Print percentiles and statistics, optionally with comparison to baseline"""
    arr = np.array(vals)
    if baseline is not None:
        baseline = np.array(baseline)

    p50 = np.percentile(arr, 50)
    p75 = np.percentile(arr, 75)
    p90 = np.percentile(arr, 90)
    p95 = np.percentile(arr, 95)
    p_max = np.max(arr)
    avg = np.mean(arr)

    print(f"\n{name}:")

    if baseline is None:
        # Just print values
        print(f"  p50:  {p50:.3f} ms")
        print(f"  p75:  {p75:.3f} ms")
        print(f"  p90:  {p90:.3f} ms")
        print(f"  p95:  {p95:.3f} ms")
        print(f"  max:  {p_max:.3f} ms")
        print(f"  avg:  {avg:.3f} ms")
        return {
            'p50': p50, 'p75': p75, 'p90': p90,
            'p95': p95, 'max': p_max, 'avg': avg
        }
    else:
        # Print with percentage difference
        def pct_diff(val, base_val):
            return ((val - base_val) / base_val) * 100

        print(f"  p50:  {p50:.3f} ms ({pct_diff(p50, np.percentile(baseline, 50)):+.2f}%)")
        print(f"  p75:  {p75:.3f} ms ({pct_diff(p75, np.percentile(baseline, 75)):+.2f}%)")
        print(f"  p90:  {p90:.3f} ms ({pct_diff(p90, np.percentile(baseline, 90)):+.2f}%)")
        print(f"  p95:  {p95:.3f} ms ({pct_diff(p95, np.percentile(baseline, 95)):+.2f}%)")
        print(f"  max:  {p_max:.3f} ms ({pct_diff(p_max, np.max(baseline)):+.2f}%)")
        print(f"  avg:  {avg:.3f} ms ({pct_diff(avg, np.mean(baseline)):+.2f}%)")

def get_results(fname):
    res = []
    with open(fname, "r") as f:
        for line in f.readlines():
            if not line.startswith("Duration"):
                continue
            rem = line[len("Duration: "):]
            i = -1
            for c in rem:
                i += 1
                if not (c.isdigit() or c == '.'):
                    break
            fl = float(rem[:i])
            res.append(fl)
    return res

def analyze2(oldf, newf, case):
    old_res = get_results(oldf)
    new_res = get_results(newf)
    print_percentiles(new_res, case, old_res)
    _, p_value = stats.ttest_rel(old_res, new_res)
    print(f"p-value: {p_value:.3f}")

def analyze(case):
    oldf = 'old.' + case
    newf = 'new.' + case
    analyze2(oldf, newf, case)


if __name__ == "__main__":
    # main()
    # analyze2('main.replace.log', 'my.replace.log', 'replace diff')
    analyze2('my.replace.log', 'my.do_nothing.log', 'replace vs do_nothing')
    analyze2('my.replace.log', 'my.emit_error.log', 'replace vs emit_error')
    analyze2('my.do_nothing.log', 'my.emit_error.log', 'do_nothing vs emit_error')
