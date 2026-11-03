"""Benchmarking and performance profiling suite for NexusKV."""

from __future__ import annotations

import argparse
import math
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from .cli import NexusClient


def calculate_percentile(data: List[float], percentile: float) -> float:
    if not data:
        return 0.0
    sorted_d = sorted(data)
    idx = int((len(sorted_d) - 1) * (percentile / 100.0))
    return sorted_d[idx]


def random_string(length: int = 16) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def run_benchmark_worker(
    host: str,
    port: int,
    command: str,
    num_requests: int,
    data_size: int,
) -> Tuple[int, List[float]]:
    client = NexusClient(host, port, timeout=10.0)
    latencies: List[float] = []
    success_count = 0
    payload = random_string(data_size)

    for i in range(num_requests):
        key = f"bench:{random.randint(1, 1000)}"
        t0 = time.perf_counter()
        try:
            if command == "SET":
                client.execute_command("SET", key, payload)
            elif command == "GET":
                client.execute_command("GET", key)
            elif command == "LPUSH":
                client.execute_command("LPUSH", "bench:list", payload)
            elif command == "LPOP":
                client.execute_command("LPOP", "bench:list")
            elif command == "PING":
                client.execute_command("PING")
            success_count += 1
        except Exception:
            pass
        latencies.append((time.perf_counter() - t0) * 1000.0)

    client.close()
    return success_count, latencies


def run_suite(
    host: str,
    port: int,
    total_requests: int = 5000,
    concurrency: int = 10,
    data_size: int = 64,
    test_commands: Optional[List[str]] = None,
) -> None:
    if not test_commands:
        test_commands = ["PING", "SET", "GET", "LPUSH", "LPOP"]

    print("=" * 76)
    print(f"  NexusKV High-Performance Benchmark Suite")
    print(f"  Target: {host}:{port} | Total Requests: {total_requests} | Concurrency: {concurrency}")
    print("=" * 76)
    print(f"{'COMMAND':<12} {'OPS/SEC':<14} {'AVG (ms)':<12} {'P50 (ms)':<12} {'P99 (ms)':<12}")
    print("-" * 76)

    reqs_per_worker = total_requests // concurrency

    for cmd in test_commands:
        start_time = time.perf_counter()
        all_latencies: List[float] = []
        total_success = 0

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(run_benchmark_worker, host, port, cmd, reqs_per_worker, data_size)
                for _ in range(concurrency)
            ]
            for f in as_completed(futures):
                succ, lats = f.result()
                total_success += succ
                all_latencies.extend(lats)

        elapsed = time.perf_counter() - start_time
        ops_sec = total_success / elapsed if elapsed > 0 else 0.0
        avg_lat = sum(all_latencies) / len(all_latencies) if all_latencies else 0.0
        p50 = calculate_percentile(all_latencies, 50.0)
        p99 = calculate_percentile(all_latencies, 99.0)

        print(f"{cmd:<12} {ops_sec:<14.1f} {avg_lat:<12.2f} {p50:<12.2f} {p99:<12.2f}")

    print("=" * 76)


def main() -> None:
    parser = argparse.ArgumentParser(description="NexusKV Benchmark Runner")
    parser.add_argument("-h", "--host", default="127.0.0.1", help="Target host")
    parser.add_argument("-p", "--port", type=int, default=6380, help="Target port")
    parser.add_argument("-n", "--requests", type=int, default=2000, help="Total requests per test")
    parser.add_argument("-c", "--clients", type=int, default=8, help="Number of concurrent clients")
    parser.add_argument("-d", "--data-size", type=int, default=64, help="Data size of payload bytes")
    parser.add_argument("-t", "--tests", default="PING,SET,GET,LPUSH,LPOP", help="Comma-separated commands")

    args = parser.parse_args()
    test_list = [x.strip().upper() for x in args.tests.split(",")]
    run_suite(
        host=args.host,
        port=args.port,
        total_requests=args.requests,
        concurrency=args.clients,
        data_size=args.data_size,
        test_commands=test_list,
    )


if __name__ == "__main__":
    main()
