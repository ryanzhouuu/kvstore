#!/usr/bin/env python3
"""
Multi-client connection performance test for KVStore
Simulates multiple independent clients, each maintaining a persistent connection
Measures throughput, latency, and scalability under concurrent client load
"""

import socket
import threading
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import sys
from collections import defaultdict


class PersistentKVStoreClient:
    """Client that maintains a persistent connection"""
    def __init__(self, host='localhost', port=8080, client_id=0):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.sock = None
        self.lock = threading.Lock()
    
    def connect(self):
        """Establish connection to server"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(30.0)
            self.sock.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Client {self.client_id} connection error: {e}")
            return False
    
    def _send_command(self, command):
        """Send a command over persistent connection"""
        with self.lock:
            if not self.sock:
                if not self.connect():
                    return "ERROR: Connection failed"
            
            try:
                self.sock.sendall((command + '\n').encode())
                
                response = b''
                while True:
                    chunk = self.sock.recv(1024)
                    if not chunk:
                        # Connection closed, try to reconnect
                        self.sock = None
                        if not self.connect():
                            return "ERROR: Connection lost"
                        # Retry the command
                        self.sock.sendall((command + '\n').encode())
                        chunk = self.sock.recv(1024)
                        if not chunk:
                            return "ERROR: Retry failed"
                    
                    response += chunk
                    if b'\n' in response:
                        break
                
                return response.decode().strip()
            except socket.timeout:
                return "ERROR: Timeout"
            except Exception as e:
                self.sock = None
                return f"ERROR: {str(e)}"
    
    def set(self, key, value):
        return self._send_command(f"SET {key} {value}")
    
    def get(self, key):
        return self._send_command(f"GET {key}")
    
    def delete(self, key):
        return self._send_command(f"DEL {key}")
    
    def close(self):
        """Close the persistent connection"""
        with self.lock:
            if self.sock:
                try:
                    self.sock.close()
                except:
                    pass
                self.sock = None


class ClientMetrics:
    """Metrics collected per client"""
    def __init__(self, client_id):
        self.client_id = client_id
        self.latencies = []
        self.successes = 0
        self.errors = 0
        self.lock = threading.Lock()
    
    def record(self, latency_ms, success=True):
        with self.lock:
            self.latencies.append(latency_ms)
            if success:
                self.successes += 1
            else:
                self.errors += 1
    
    def get_stats(self):
        with self.lock:
            if not self.latencies:
                return None
            return {
                'client_id': self.client_id,
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


def client_worker_read_heavy(client, client_metrics, num_ops, key_range):
    """Read-heavy workload: 90% reads, 10% writes"""
    for i in range(num_ops):
        key = f"key_{client.client_id}_{i % key_range}"
        start = time.perf_counter()
        
        if i % 10 == 0:  # 10% writes
            result = client.set(key, f"value_{i}")
            success = result == "OK"
        else:  # 90% reads
            result = client.get(key)
            success = result != "ERROR" and result != "NOT_FOUND"
        
        latency_ms = (time.perf_counter() - start) * 1000
        client_metrics.record(latency_ms, success)


def client_worker_write_heavy(client, client_metrics, num_ops, key_range):
    """Write-heavy workload: 10% reads, 90% writes"""
    for i in range(num_ops):
        key = f"key_{client.client_id}_{i % key_range}"
        start = time.perf_counter()
        
        if i % 10 == 0:  # 10% reads
            result = client.get(key)
            success = result != "ERROR"
        else:  # 90% writes
            result = client.set(key, f"value_{i}")
            success = result == "OK"
        
        latency_ms = (time.perf_counter() - start) * 1000
        client_metrics.record(latency_ms, success)


def client_worker_balanced(client, client_metrics, num_ops, key_range):
    """Balanced workload: 50% reads, 50% writes"""
    for i in range(num_ops):
        key = f"key_{client.client_id}_{i % key_range}"
        start = time.perf_counter()
        
        if i % 2 == 0:
            result = client.set(key, f"value_{i}")
            success = result == "OK"
        else:
            result = client.get(key)
            success = result != "ERROR" and result != "NOT_FOUND"
        
        latency_ms = (time.perf_counter() - start) * 1000
        client_metrics.record(latency_ms, success)


def client_worker_mixed(client, client_metrics, num_ops, key_range):
    """Mixed workload: 60% reads, 30% writes, 10% deletes"""
    for i in range(num_ops):
        key = f"key_{client.client_id}_{i % key_range}"
        start = time.perf_counter()
        
        op_type = i % 10
        if op_type < 6:  # 60% reads
            result = client.get(key)
            success = result != "ERROR"
        elif op_type < 9:  # 30% writes
            result = client.set(key, f"value_{i}")
            success = result == "OK"
        else:  # 10% deletes
            result = client.delete(key)
            success = result in ("DELETED", "NOT_FOUND")
        
        latency_ms = (time.perf_counter() - start) * 1000
        client_metrics.record(latency_ms, success)


def run_multi_client_test(num_clients, ops_per_client, workload_func, workload_name, 
                         host, port, key_range=100):
    """Run a multi-client performance test"""
    print(f"\n{'='*70}")
    print(f"Multi-Client Test: {workload_name}")
    print(f"{'='*70}")
    print(f"Clients: {num_clients}")
    print(f"Operations per client: {ops_per_client:,}")
    print(f"Total operations: {num_clients * ops_per_client:,}")
    print(f"{'='*70}")
    
    # Create clients and metrics
    clients = []
    metrics_list = []
    
    print("Initializing clients...", end='', flush=True)
    for i in range(num_clients):
        client = PersistentKVStoreClient(host, port, client_id=i)
        if not client.connect():
            print(f"\nWarning: Client {i} failed to connect")
            continue
        clients.append(client)
        metrics_list.append(ClientMetrics(i))
    print(f" {len(clients)} clients connected")
    
    if not clients:
        print("Error: No clients could connect!")
        return None
    
    # Warmup phase
    print("Warming up...", end='', flush=True)
    warmup_clients = clients[:min(5, len(clients))]
    for client in warmup_clients:
        for i in range(min(10, ops_per_client // 10)):
            client.set(f"warmup_key_{i}", "warmup_value")
            client.get(f"warmup_key_{i}")
    print(" done")
    
    # Run test
    print("Running test...", end='', flush=True)
    start_time = time.perf_counter()
    
    def run_client(client, metrics):
        try:
            workload_func(client, metrics, ops_per_client, key_range)
        except Exception as e:
            print(f"\nClient {client.client_id} error: {e}")
        finally:
            client.close()
    
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = []
        for client, metrics in zip(clients, metrics_list):
            future = executor.submit(run_client, client, metrics)
            futures.append(future)
        
        # Wait for all clients to complete
        completed = 0
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % max(1, num_clients // 10) == 0:
                print(".", end='', flush=True)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(" done")
    
    # Aggregate results
    all_latencies = []
    total_successes = 0
    total_errors = 0
    total_ops = 0
    client_stats = []
    
    for metrics in metrics_list:
        stats = metrics.get_stats()
        if stats:
            client_stats.append(stats)
            all_latencies.extend(metrics.latencies)
            total_successes += stats['successes']
            total_errors += stats['errors']
            total_ops += stats['count']
    
    if not all_latencies:
        print("Error: No operations completed!")
        return None
    
    # Calculate aggregate statistics
    aggregate_stats = {
        'num_clients': num_clients,
        'ops_per_client': ops_per_client,
        'total_ops': total_ops,
        'total_successes': total_successes,
        'total_errors': total_errors,
        'elapsed': elapsed,
        'throughput': total_ops / elapsed,
        'latency_min': min(all_latencies),
        'latency_max': max(all_latencies),
        'latency_mean': statistics.mean(all_latencies),
        'latency_median': statistics.median(all_latencies),
        'latency_p95': statistics.quantiles(all_latencies, n=20)[18] if len(all_latencies) > 20 else max(all_latencies),
        'latency_p99': statistics.quantiles(all_latencies, n=100)[98] if len(all_latencies) > 100 else max(all_latencies),
        'throughput_per_client': (total_ops / elapsed) / num_clients,
        'client_stats': client_stats,
    }
    
    return aggregate_stats


def print_results(stats):
    """Print formatted test results"""
    print(f"\n{'='*70}")
    print("Results")
    print(f"{'='*70}")
    print(f"Clients: {stats['num_clients']}")
    print(f"Operations per client: {stats['ops_per_client']:,}")
    print(f"Total operations: {stats['total_ops']:,}")
    print(f"Successful: {stats['total_successes']:,}")
    print(f"Errors: {stats['total_errors']:,}")
    print(f"Elapsed time: {stats['elapsed']:.3f} seconds")
    print(f"\nThroughput:")
    print(f"  Aggregate: {stats['throughput']:,.0f} ops/sec ({stats['throughput']/1000:.2f} K ops/sec)")
    print(f"  Per client: {stats['throughput_per_client']:,.0f} ops/sec")
    print(f"\nLatency (ms):")
    print(f"  Min: {stats['latency_min']:.2f}")
    print(f"  Max: {stats['latency_max']:.2f}")
    print(f"  Mean: {stats['latency_mean']:.2f}")
    print(f"  Median: {stats['latency_median']:.2f}")
    print(f"  P95: {stats['latency_p95']:.2f}")
    print(f"  P99: {stats['latency_p99']:.2f}")
    
    if stats['total_errors'] > 0:
        error_rate = (stats['total_errors'] / stats['total_ops']) * 100
        print(f"\nError rate: {error_rate:.2f}%")
    
    # Show per-client statistics if not too many clients
    if stats['num_clients'] <= 10 and stats['client_stats']:
        print(f"\n{'='*70}")
        print("Per-Client Statistics (top 5 by throughput)")
        print(f"{'='*70}")
        print(f"{'Client':<10} {'Ops':<10} {'Success':<10} {'Errors':<10} {'Mean Lat(ms)':<15} {'P95(ms)':<12}")
        print("-" * 70)
        
        # Sort by throughput (operations / time, approximated by count)
        sorted_clients = sorted(stats['client_stats'], 
                               key=lambda x: x['count'], reverse=True)[:5]
        for client_stat in sorted_clients:
            throughput = client_stat['count'] / stats['elapsed']
            print(f"{client_stat['client_id']:<10} "
                  f"{client_stat['count']:<10} "
                  f"{client_stat['successes']:<10} "
                  f"{client_stat['errors']:<10} "
                  f"{client_stat['mean']:<15.2f} "
                  f"{client_stat['p95']:<12.2f}")
    
    print(f"{'='*70}")


def scalability_test(num_clients_list, ops_per_client, workload_func, 
                    workload_name, host, port):
    """Run scalability test with different numbers of clients"""
    print("\n" + "="*70)
    print(f"Scalability Test: {workload_name}")
    print("="*70)
    
    results = []
    for num_clients in num_clients_list:
        stats = run_multi_client_test(num_clients, ops_per_client, workload_func,
                                     f"{workload_name} ({num_clients} clients)",
                                     host, port)
        if stats:
            results.append(stats)
            print_results(stats)
    
    # Print scalability summary
    if len(results) > 1:
        print("\n" + "="*70)
        print("Scalability Summary")
        print("="*70)
        print(f"{'Clients':<10} {'Throughput (ops/sec)':<25} {'Latency Mean (ms)':<20} {'Per-Client (ops/sec)':<20}")
        print("-" * 70)
        for result in results:
            print(f"{result['num_clients']:<10} "
                  f"{result['throughput']:>20,.0f}  "
                  f"{result['latency_mean']:>15.2f}     "
                  f"{result['throughput_per_client']:>15,.0f}")
        print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description='Multi-client connection performance test for KVStore',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with 10 clients, 1000 ops each
  python3 multi_client_test.py --clients 10 --ops 1000

  # Test write-heavy workload
  python3 multi_client_test.py --clients 20 --ops 500 --workload write

  # Scalability test: 1, 5, 10, 20, 50 clients
  python3 multi_client_test.py --scalability --ops 200 --workload balanced

  # High concurrency test
  python3 multi_client_test.py --clients 100 --ops 100 --workload mixed
        """
    )
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=8080, help='Server port (default: 8080)')
    parser.add_argument('--clients', type=int, default=10, help='Number of concurrent clients (default: 10)')
    parser.add_argument('--ops', type=int, default=1000, help='Operations per client (default: 1000)')
    parser.add_argument('--workload', choices=['read', 'write', 'balanced', 'mixed'], 
                       default='balanced', help='Workload type (default: balanced)')
    parser.add_argument('--scalability', action='store_true', 
                       help='Run scalability test with multiple client counts')
    parser.add_argument('--key-range', type=int, default=100, 
                       help='Number of unique keys to use (default: 100)')
    
    args = parser.parse_args()
    
    # Check if server is running
    try:
        test_client = PersistentKVStoreClient(args.host, args.port)
        if not test_client.connect():
            print(f"Error: Cannot connect to server at {args.host}:{args.port}")
            sys.exit(1)
        test_client.set("connection_test", "test")
        test_client.close()
    except Exception as e:
        print(f"Error: Cannot connect to server: {e}")
        sys.exit(1)
    
    # Select workload function
    workload_map = {
        'read': (client_worker_read_heavy, "Read-Heavy (90% read, 10% write)"),
        'write': (client_worker_write_heavy, "Write-Heavy (10% read, 90% write)"),
        'balanced': (client_worker_balanced, "Balanced (50% read, 50% write)"),
        'mixed': (client_worker_mixed, "Mixed (60% read, 30% write, 10% delete)"),
    }
    
    workload_func, workload_name = workload_map[args.workload]
    
    print("="*70)
    print("KVStore Multi-Client Connection Performance Test")
    print("="*70)
    print(f"Server: {args.host}:{args.port}")
    print(f"Workload: {workload_name}")
    print(f"Key range: {args.key_range}")
    print("="*70)
    
    if args.scalability:
        # Test with increasing number of clients
        client_counts = [1, 5, 10, 20, 50, 100]
        # Limit to reasonable values if ops_per_client is high
        if args.ops > 1000:
            client_counts = [1, 5, 10, 20]
        scalability_test(client_counts, args.ops, workload_func, workload_name,
                        args.host, args.port, args.key_range)
    else:
        stats = run_multi_client_test(args.clients, args.ops, workload_func,
                                     workload_name, args.host, args.port, args.key_range)
        if stats:
            print_results(stats)
    
    print("\nTest completed!")


if __name__ == '__main__':
    main()