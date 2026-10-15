#!/usr/bin/env python3
"""
Project PHANTOM: Automated Failure Reconstruction Engine.
Run all empirical benchmarks and generate comparison tables and charts.
"""

import sys
from phantom.benchmarks.runner import BenchmarkRunner

def main():
    print("================================================================================")
    print("PROJECT PHANTOM: EMPIRICAL BENCHMARK EVALUATION")
    print("Investigating: Trace-guided causal minimization vs. Delta Debugging & Random Search")
    print("================================================================================\n")
    runner = BenchmarkRunner(output_dir="benchmark_artifacts")
    records = runner.run_suite(verbose=True)
    chart_path = runner.generate_plots(records)
    print(f"\n[+] Generated benchmark comparison chart: {chart_path}")
    print("[+] Generated benchmark JSON: benchmark_artifacts/benchmark_results.json")
    print("[+] Generated benchmark CSV: benchmark_artifacts/benchmark_summary.csv")
    print("\nBenchmark completed successfully.")

if __name__ == "__main__":
    main()
