#!/usr/bin/env python3
"""
Comprehensive concurrent performance test for KVStore
Tests various workloads: read-heavy, write-heavy, balanced, and mixed operations
"""

import socket
import threading
import time
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import sys

class KVStoreClient:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
    
    def _send_command(self, command):
        """Send a command and get response"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.host, self.port))
            sock.sendall((command + '\n').encode())
            
            response = b''
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                response += chunk
                if b'\n' in response:
                    break
            
            sock.close()
            return response.decode().strip()
        except Exception as e:
            return f"ERROR: {str(e)}"
    
    def set(self, key, value):
        return self._send_command(f"SET {key} {value}")
    
    def get(self, key):
        return self._send_command(f"GET {key}")
    
    def delete(self, key):
        return self._send_command(f"DEL {key}")


class PerformanceMetrics:
    def __init__(self):
        self.latencies = []
        self.successes = 0
        self.errors = 0
        self.lock = threading.Lock()
    
    def record(self, latency, success=True):
        with self.lock:
            self.latencies.append(latency)
            if success:
                self.successes += 1
            else:
                self.errors += 1
    
    def get_stats(self):
        with self.lock:
            if not self.latencies:
                return None
            return {
                'count': len(self.latencies),
                'successes': self.successes,
                'errors': self.errors,
                'min': min(self.latencies),
                'max': max(self.latencies),
                'mean': statistics.mean(self.latencies),
                'median': statistics.median(self.latencies),
                'p95': self._percentile(self.latencies, 95),
                'p99': self._percentile(self.latencies, 99),
            }
    
    def _percentile(self, data, percentile):
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


def worker_read_heavy(thread_id, num_ops, key_range, metrics, client):
    """Read-heavy workload: 90% reads, 10% writes"""
    for i in range(num_ops):
        key = f"key{thread_id % key_range}"
        start = time.perf_counter()
        
        if i % 10 == 0:  # 10% writes
            result = client.set(key, f"value_{thread_id}_{i}")
            success = result == "OK"
        else:  # 90% reads
            result = client.get(key)
            success = result != "ERROR"
        
        latency = (time.perf_counter() - start) * 1000  # Convert to ms
        metrics.record(latency, success)


def worker_write_heavy(thread_id, num_ops, key_range, metrics, client):
    """Write-heavy workload: 10% reads, 90% writes"""
    for i in range(num_ops):
        key = f"key{thread_id % key_range}"
        start = time.perf_counter()
        
        if i % 10 == 0:  # 10% reads
            result = client.get(key)
            success = result != "ERROR"
        else:  # 90% writes
            result = client.set(key, f"value_{thread_id}_{i}")
            success = result == "OK"
        
        latency = (time.perf_counter() - start) * 1000
        metrics.record(latency, success)


def worker_balanced(thread_id, num_ops, key_range, metrics, client):
    """Balanced workload: 50% reads, 50% writes"""
    for i in range(num_ops):
        key = f"key{thread_id % key_range}"
        start = time.perf_counter()
        
        if i % 2 == 0:
            result = client.set(key, f"value_{thread_id}_{i}")
            success = result == "OK"
        else:
            result = client.get(key)
            success = result != "ERROR"
        
        latency = (time.perf_counter() - start) * 1000
        metrics.record(latency, success)


def worker_mixed(thread_id, num_ops, key_range, metrics, client):
    """Mixed workload: 60% reads, 30% writes, 10% deletes"""
    for i in range(num_ops):
        key = f"key{thread_id % key_range}"
        start = time.perf_counter()
        
        op_type = i % 10
        if op_type < 6:  # 60% reads
            result = client.get(key)
            success = result != "ERROR"
        elif op_type < 9:  # 30% writes
            result = client.set(key, f"value_{thread_id}_{i}")
            success = result == "OK"
        else:  # 10% deletes
            result = client.delete(key)
            success = result in ("DELETED", "NOT_FOUND")
        
        latency = (time.perf_counter() - start) * 1000
        metrics.record(latency, success)


def worker_hot_keys(thread_id, num_ops, hot_key_ratio, metrics, client):
    """Workload with hot keys: some keys are accessed much more frequently"""
    hot_keys = [f"hot_key_{i}" for i in range(5)]
    cold_keys = [f"cold_key_{i}" for i in range(100)]
    
    for i in range(num_ops):
        # 80% of operations target hot keys
        if i % 100 < 80:
            key = hot_keys[i % len(hot_keys)]
        else:
            key = cold_keys[i % len(cold_keys)]
        
        start = time.perf_counter()
        if i % 2 == 0:
            result = client.set(key, f"value_{thread_id}_{i}")
            success = result == "OK"
        else:
            result = client.get(key)
            success = result != "ERROR"
        
        latency = (time.perf_counter() - start) * 1000
        metrics.record(latency, success)


def run_test(test_name, worker_func, num_threads, ops_per_thread, key_range=100, **kwargs):
    """Run a concurrent performance test"""
    print(f"\n{'='*60}")
    print(f"Test: {test_name}")
    print(f"Threads: {num_threads}, Ops per thread: {ops_per_thread}")
    print(f"{'='*60}")
    
    metrics = PerformanceMetrics()
    client = KVStoreClient()
    
    # Warmup phase
    print("Warming up...")
    warmup_metrics = PerformanceMetrics()
    warmup_threads = min(5, num_threads)
    with ThreadPoolExecutor(max_workers=warmup_threads) as executor:
        futures = []
        for t in range(warmup_threads):
            future = executor.submit(worker_func, t, ops_per_thread // 10, key_range, warmup_metrics, client)
            futures.append(future)
        for future in as_completed(futures):
            future.result()
    
    # Actual test
    print("Running test...")
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for t in range(num_threads):
            future = executor.submit(worker_func, t, ops_per_thread, key_range, metrics, client, **kwargs)
            futures.append(future)
        
        # Wait for all threads
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Worker error: {e}")
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Print results
    stats = metrics.get_stats()
    if stats:
        total_ops = stats['count']
        throughput = total_ops / elapsed
        
        print(f"\nResults:")
        print(f"  Total operations: {total_ops:,}")
        print(f"  Successful: {stats['successes']:,}")
        print(f"  Errors: {stats['errors']:,}")
        print(f"  Elapsed time: {elapsed:.2f}s")
        print(f"  Throughput: {throughput:,.0f} ops/sec")
        print(f"\nLatency (ms):")
        print(f"  Min: {stats['min']:.2f}")
        print(f"  Max: {stats['max']:.2f}")
        print(f"  Mean: {stats['mean']:.2f}")
        print(f"  Median: {stats['median']:.2f}")
        print(f"  P95: {stats['p95']:.2f}")
        print(f"  P99: {stats['p99']:.2f}")


def verify_consistency(num_threads, ops_per_thread):
    """Verify correctness under concurrent access"""
    print(f"\n{'='*60}")
    print("Consistency Test")
    print(f"{'='*60}")
    
    client = KVStoreClient()
    errors = []
    
    # Initialize test keys
    num_keys = 10
    for i in range(num_keys):
        client.set(f"consistency_key_{i}", f"initial_value_{i}")
    
    def consistency_worker(thread_id):
        local_errors = []
        for i in range(ops_per_thread):
            key_id = i % num_keys
            key = f"consistency_key_{key_id}"
            
            # Write operation
            value = f"value_t{thread_id}_op{i}"
            client.set(key, value)
            
            # Read back - should get our value or a later value
            result = client.get(key)
            if result == "ERROR" or result == "NOT_FOUND":
                local_errors.append(f"Thread {thread_id}, op {i}: Read failed")
            
            # Verify we can still read it
            result2 = client.get(key)
            if result2 != result:
                local_errors.append(f"Thread {thread_id}, op {i}: Inconsistent reads")
        
        return local_errors
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(consistency_worker, t) for t in range(num_threads)]
        for future in as_completed(futures):
            errors.extend(future.result())
    
    if errors:
        print(f"Found {len(errors)} consistency errors:")
        for error in errors[:10]:  # Show first 10
            print(f"  {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")
    else:
        print("✓ All consistency checks passed!")


def main():
    parser = argparse.ArgumentParser(description='KVStore Concurrent Performance Test')
    parser.add_argument('--threads', type=int, default=50, help='Number of concurrent threads')
    parser.add_argument('--ops', type=int, default=100, help='Operations per thread')
    parser.add_argument('--skip-consistency', action='store_true', help='Skip consistency test')
    parser.add_argument('--workload', choices=['all', 'read', 'write', 'balanced', 'mixed', 'hot'], 
                       default='all', help='Workload type to test')
    
    args = parser.parse_args()
    
    # Check if server is running
    try:
        client = KVStoreClient()
        result = client.set("test", "test")
        if "ERROR" in result and "Connection" in result:
            print("Error: Cannot connect to server. Make sure it's running on localhost:8080")
            sys.exit(1)
    except Exception as e:
        print(f"Error: Cannot connect to server: {e}")
        sys.exit(1)
    
    print("KVStore Concurrent Performance Test Suite")
    print(f"Testing with {args.threads} threads, {args.ops} ops per thread")
    
    # Run tests based on workload type
    if args.workload == 'all':
        run_test("Read-Heavy Workload (90% read, 10% write)", 
                worker_read_heavy, args.threads, args.ops)
        run_test("Write-Heavy Workload (10% read, 90% write)", 
                worker_write_heavy, args.threads, args.ops)
        run_test("Balanced Workload (50% read, 50% write)", 
                worker_balanced, args.threads, args.ops)
        run_test("Mixed Workload (60% read, 30% write, 10% delete)", 
                worker_mixed, args.threads, args.ops)
        run_test("Hot Keys Workload (80% hot, 20% cold)", 
                worker_hot_keys, args.threads, args.ops)
    elif args.workload == 'read':
        run_test("Read-Heavy Workload", worker_read_heavy, args.threads, args.ops)
    elif args.workload == 'write':
        run_test("Write-Heavy Workload", worker_write_heavy, args.threads, args.ops)
    elif args.workload == 'balanced':
        run_test("Balanced Workload", worker_balanced, args.threads, args.ops)
    elif args.workload == 'mixed':
        run_test("Mixed Workload", worker_mixed, args.threads, args.ops)
    elif args.workload == 'hot':
        run_test("Hot Keys Workload", worker_hot_keys, args.threads, args.ops)
    
    # Consistency test
    if not args.skip_consistency:
        verify_consistency(min(args.threads, 20), args.ops // 2)
    
    print(f"\n{'='*60}")
    print("All tests completed!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()