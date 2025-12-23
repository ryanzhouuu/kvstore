#!/usr/bin/env python3
"""
Single-threaded throughput test for KVStore
Measures pure throughput without concurrency overhead
"""

import socket
import time
import argparse
import sys


class KVStoreClient:
    def __init__(self, host='localhost', port=8080, reuse_connection=False):
        self.host = host
        self.port = port
        self.reuse_connection = reuse_connection
        self.sock = None
    
    def _get_socket(self):
        """Get a socket connection, reusing if enabled"""
        if self.reuse_connection and self.sock:
            try:
                # Test if socket is still alive
                self.sock.sendall(b'\n')
                return self.sock
            except:
                self.sock = None
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((self.host, self.port))
        
        if self.reuse_connection:
            self.sock = sock
        
        return sock
    
    def _send_command(self, command):
        """Send a command and get response"""
        sock = self._get_socket()
        try:
            sock.sendall((command + '\n').encode())
            
            response = b''
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                response += chunk
                if b'\n' in response:
                    break
            
            if not self.reuse_connection:
                sock.close()
            
            return response.decode().strip()
        except Exception as e:
            if not self.reuse_connection and sock:
                sock.close()
            if self.reuse_connection:
                self.sock = None
            return f"ERROR: {str(e)}"
    
    def close(self):
        """Close persistent connection if exists"""
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def set(self, key, value):
        return self._send_command(f"SET {key} {value}")
    
    def get(self, key):
        return self._send_command(f"GET {key}")
    
    def delete(self, key):
        return self._send_command(f"DEL {key}")


def run_throughput_test(client, num_ops, workload='mixed'):
    """Run a single-threaded throughput test"""
    successes = 0
    errors = 0
    
    # Warmup phase
    print("Warming up...", end='', flush=True)
    for i in range(min(100, num_ops // 10)):
        if workload == 'set':
            client.set(f"warmup_key_{i}", "warmup_value")
        elif workload == 'get':
            client.get(f"warmup_key_{i}")
        elif workload == 'mixed':
            if i % 2 == 0:
                client.set(f"warmup_key_{i}", "warmup_value")
            else:
                client.get(f"warmup_key_{i}")
    print(" done")
    
    # Pre-populate keys for GET-only tests
    if workload == 'get':
        print("Pre-populating keys...", end='', flush=True)
        for i in range(num_ops):
            client.set(f"test_key_{i}", f"test_value_{i}")
        print(" done")
    
    # Actual test
    print(f"Running {num_ops:,} operations...", end='', flush=True)
    start_time = time.perf_counter()
    
    for i in range(num_ops):
        if workload == 'set':
            result = client.set(f"test_key_{i}", f"test_value_{i}")
            success = result == "OK"
        elif workload == 'get':
            result = client.get(f"test_key_{i % num_ops}")
            success = result != "ERROR" and result != "NOT_FOUND"
        elif workload == 'mixed':
            if i % 2 == 0:
                result = client.set(f"test_key_{i}", f"test_value_{i}")
                success = result == "OK"
            else:
                result = client.get(f"test_key_{i-1}")
                success = result != "ERROR"
        else:  # write-only for other workloads
            result = client.set(f"test_key_{i}", f"test_value_{i}")
            success = result == "OK"
        
        if success:
            successes += 1
        else:
            errors += 1
        
        # Progress indicator
        if (i + 1) % (num_ops // 10) == 0:
            print(".", end='', flush=True)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(" done")
    
    return {
        'total_ops': num_ops,
        'successes': successes,
        'errors': errors,
        'elapsed': elapsed,
        'throughput': num_ops / elapsed
    }


def main():
    parser = argparse.ArgumentParser(
        description='Single-threaded throughput test for KVStore',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with 10,000 operations (default: mixed workload)
  python3 single_thread_throughput.py --ops 10000

  # Test pure write throughput
  python3 single_thread_throughput.py --ops 50000 --workload set

  # Test pure read throughput (will pre-populate keys)
  python3 single_thread_throughput.py --ops 50000 --workload get

  # Use persistent connection for better performance
  python3 single_thread_throughput.py --ops 100000 --reuse-connection
        """
    )
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=8080, help='Server port (default: 8080)')
    parser.add_argument('--ops', type=int, default=10000, help='Number of operations to perform (default: 10000)')
    parser.add_argument('--workload', choices=['set', 'get', 'mixed'], default='mixed',
                       help='Workload type: set (write-only), get (read-only), mixed (50/50) (default: mixed)')
    parser.add_argument('--reuse-connection', action='store_true',
                       help='Reuse a single connection instead of creating new ones (better throughput)')
    
    args = parser.parse_args()
    
    # Check if server is running
    try:
        test_client = KVStoreClient(args.host, args.port)
        result = test_client.set("connection_test", "test")
        if "ERROR" in result and "Connection" in result:
            print(f"Error: Cannot connect to server at {args.host}:{args.port}")
            sys.exit(1)
        test_client.close()
    except Exception as e:
        print(f"Error: Cannot connect to server: {e}")
        sys.exit(1)
    
    print("=" * 60)
    print("KVStore Single-Threaded Throughput Test")
    print("=" * 60)
    print(f"Host: {args.host}:{args.port}")
    print(f"Operations: {args.ops:,}")
    print(f"Workload: {args.workload}")
    print(f"Connection reuse: {args.reuse_connection}")
    print("=" * 60)
    
    client = KVStoreClient(args.host, args.port, args.reuse_connection)
    
    try:
        results = run_throughput_test(client, args.ops, args.workload)
        
        print("\n" + "=" * 60)
        print("Results")
        print("=" * 60)
        print(f"Total operations: {results['total_ops']:,}")
        print(f"Successful: {results['successes']:,}")
        print(f"Errors: {results['errors']:,}")
        print(f"Elapsed time: {results['elapsed']:.3f} seconds")
        print(f"\nThroughput: {results['throughput']:,.0f} ops/sec")
        print(f"           {results['throughput']/1000:.2f} K ops/sec")
        if results['errors'] > 0:
            error_rate = (results['errors'] / results['total_ops']) * 100
            print(f"\nError rate: {error_rate:.2f}%")
        print("=" * 60)
        
    finally:
        client.close()


if __name__ == '__main__':
    main()