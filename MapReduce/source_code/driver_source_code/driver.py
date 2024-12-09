import sys
import os
import json
import socket
import time
from typing import Dict, List, Tuple
import concurrent.futures
from collections import defaultdict

class MapReduceDriver:
    def __init__(self, engine_ports: List[int]):
        self.engine_ports = engine_ports
        self.host = 'localhost'
        self.dataset_path = "sample_dataset/student_scores"

    def read_csv_file(self, file_path: str) -> List[Dict]:
        """Read a CSV file and return list of dictionaries with year and score"""
        data = []
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split(',')
                if len(parts) != 3:
                    continue
                    
                try:
                    student_id, year, score = parts
                    data.append({
                        'year': int(year),
                        'score': float(score)
                    })
                except (ValueError, IndexError):
                    continue
                    
        return data

    def send_to_engine(self, port: int, data: List[Dict]) -> Dict[int, Dict]:
        """Send data to an engine instance and receive results"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(30)
                    sock.connect((self.host, port))
                    
                    # Send data
                    data_bytes = json.dumps(data).encode()
                    size_bytes = f"{len(data_bytes):8d}".encode()
                    sock.send(size_bytes)
                    sock.send(data_bytes)
                    
                    # Receive response
                    size_data = sock.recv(8)
                    if not size_data:
                        raise Exception("No response from engine")
                        
                    data_size = int(size_data.decode().strip())
                    received_data = b""
                    while len(received_data) < data_size:
                        chunk = sock.recv(min(4096, data_size - len(received_data)))
                        if not chunk:
                            break
                        received_data += chunk
                    
                    response = json.loads(received_data.decode())
                    if isinstance(response, dict) and 'error' in response:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        return {}
                    
                    return response
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            
        return {}

    def merge_results(self, results: List[Dict[int, Dict]]) -> Dict[int, Dict]:
        """Merge results from multiple engine instances"""
        merged = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
        
        for result in results:
            if not result:
                continue
                
            for year_str, stats in result.items():
                try:
                    year = int(year_str)
                    merged[year]['min'] = min(merged[year]['min'], stats['min'])
                    merged[year]['max'] = max(merged[year]['max'], stats['max'])
                    merged[year]['sum'] += stats['avg']
                    merged[year]['count'] += 1
                except (ValueError, KeyError):
                    continue
        
        final_results = {}
        for year, stats in merged.items():
            if stats['count'] > 0:
                final_results[year] = {
                    'min': stats['min'],
                    'max': stats['max'],
                    'avg': round(stats['sum'] / stats['count'])
                }
        
        return final_results

    def write_output(self, results: Dict[int, Dict]):
        """Write results to output.txt in the required format"""
        with open('output.txt', 'w') as f:
            for year in sorted(results.keys()):
                stats = results[year]
                line = f"{year},{int(stats['min'])},{int(stats['max'])},{int(stats['avg'])}"
                f.write(line + "\n")
                print(f"Year {year}: min={int(stats['min'])}, max={int(stats['max'])}, avg={int(stats['avg'])}")

    def process_dataset(self):
        """Process the entire dataset using MapReduce across engine instances"""
        print("Starting MapReduce processing...")
        csv_files = [f for f in os.listdir(self.dataset_path) if f.endswith('.csv')]
        if not csv_files:
            raise Exception(f"No CSV files found in {self.dataset_path}")
        print(f"Found {len(csv_files)} files to process")

        all_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.engine_ports)) as executor:
            future_to_file = {}
            
            # Distribute files across engines
            for i, file_name in enumerate(csv_files):
                file_path = os.path.join(self.dataset_path, file_name)
                data = self.read_csv_file(file_path)
                port = self.engine_ports[i % len(self.engine_ports)]
                print(f"Sending {file_name} to engine on port {port}")
                future = executor.submit(self.send_to_engine, port, data)
                future_to_file[future] = file_name
            
            # Collect results
            print("\nCollecting results...")
            for future in concurrent.futures.as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        print(f"Received results for {file_name}")
                        all_results.append(result)
                    else:
                        print(f"No results received for {file_name}")
                except Exception as e:
                    print(f"Error processing {file_name}: {str(e)}")

        print("\nMerging results...")
        final_results = self.merge_results(all_results)
        
        print("\nWriting output...")
        self.write_output(final_results)
        print("Processing complete!")

def main():
    if len(sys.argv) < 2:
        print("Usage: python driver.py <engine_port1> [engine_port2 ...]")
        sys.exit(1)
    
    engine_ports = [int(port) for port in sys.argv[1:]]
    driver = MapReduceDriver(engine_ports)
    driver.process_dataset()

if __name__ == "__main__":
    main()
