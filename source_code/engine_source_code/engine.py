import socket
import sys
import json
import multiprocessing as mp
from typing import Dict, List, Tuple

class MapReduceEngine:
    def __init__(self, port: int):
        self.port = port
        self.host = 'localhost'
        self.num_workers = mp.cpu_count() - 1  # Leave one core for main process

    def mapper(self, data: List[Dict]) -> List[Tuple[int, Dict]]:
        """Maps input data to (year, values) pairs"""
        results = []
        for row in data:
            year = int(row['year'])
            score = float(row['score'])
            results.append((year, {'min': score, 'max': score, 'sum': score, 'count': 1}))
        return results

    def reducer(self, mapped_data: List[Tuple[int, Dict]]) -> Dict[int, Dict]:
        """Reduces mapped data to compute statistics per year"""
        results = {}
        for year, data in mapped_data:
            if year not in results:
                results[year] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}
            
            results[year]['min'] = min(results[year]['min'], data['min'])
            results[year]['max'] = max(results[year]['max'], data['max'])
            results[year]['sum'] += data['sum']
            results[year]['count'] += data['count']
        
        # Calculate averages
        for year in results:
            avg = results[year]['sum'] / results[year]['count']
            results[year] = {
                'min': results[year]['min'],
                'max': results[year]['max'],
                'avg': round(avg)
            }
        return results

    def process_batch(self, data: List[Dict]) -> Dict[int, Dict]:
        """Process a batch of data using MapReduce"""
        try:
            if not data:
                print(f"Engine {self.port}: Received empty batch")
                return {}
                
            print(f"Engine {self.port}: Processing {len(data)} records")
            
            # Create a process pool for this batch
            with mp.Pool(processes=self.num_workers) as pool:
                # Map phase - distribute data across workers
                mapped_data = []
                chunk_size = max(1, len(data) // self.num_workers)
                chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
                
                mapped_results = pool.map(self.mapper, chunks)
                for result in mapped_results:
                    mapped_data.extend(result)
                
                # Reduce phase
                result = self.reducer(mapped_data)
                print(f"Engine {self.port}: Processed {len(result)} year groups")
                return result
                
        except Exception as e:
            print(f"Engine {self.port}: Error processing batch: {str(e)}")
            return {'error': str(e)}

    def start_server(self):
        """Start the socket server to handle client requests"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            print(f"Engine started on port {self.port}")

            while True:
                try:
                    client_socket, addr = server_socket.accept()
                    print(f"Engine {self.port}: Connection from {addr}")
                    
                    try:
                        client_socket.settimeout(30)
                        
                        # Receive data size and data
                        size_data = client_socket.recv(8)
                        if not size_data:
                            continue
                            
                        data_size = int(size_data.decode().strip())
                        print(f"Engine {self.port}: Receiving {data_size} bytes")
                        
                        received_data = b""
                        while len(received_data) < data_size:
                            chunk = client_socket.recv(min(4096, data_size - len(received_data)))
                            if not chunk:
                                break
                            received_data += chunk
                        
                        # Process data and send response
                        data = json.loads(received_data.decode())
                        result = self.process_batch(data)
                        response = json.dumps(result).encode()
                        size_response = f"{len(response):8d}".encode()
                        print(f"Engine {self.port}: Sending response of {len(response)} bytes")
                        client_socket.send(size_response)
                        client_socket.send(response)
                            
                    except Exception as e:
                        print(f"Engine {self.port}: Error processing request: {str(e)}")
                        error_msg = json.dumps({'error': str(e)}).encode()
                        size_error = f"{len(error_msg):8d}".encode()
                        client_socket.send(size_error)
                        client_socket.send(error_msg)
                    finally:
                        client_socket.close()
                        
                except KeyboardInterrupt:
                    print(f"Engine {self.port}: Shutting down...")
                    break
                except Exception as e:
                    print(f"Engine {self.port}: Error accepting connection: {str(e)}")
                    
        finally:
            server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python engine.py <port>")
        sys.exit(1)
        
    port = int(sys.argv[1])
    engine = MapReduceEngine(port)
    engine.start_server()
