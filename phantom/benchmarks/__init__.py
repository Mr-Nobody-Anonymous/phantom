"""Benchmark suite package for Project PHANTOM."""

from phantom.benchmarks.base import BenchmarkCase
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases
from phantom.benchmarks.runner import BenchmarkRunner

__all__ = [
    "BenchmarkCase",
    "get_benchmark_a_cases",
    "get_benchmark_b_cases",
    "get_benchmark_c_cases",
    "BenchmarkRunner",
]
