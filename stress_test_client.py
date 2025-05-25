# stress_test_client.py
import socket
import json
import base64
import logging
import os
import time
import concurrent.futures
import subprocess
import pandas as pd 
import random
import string

# --- Configuration ---
SERVER_HOST = '127.0.0.1'
# Ports will be set based on server type
# THREAD_SERVER_PORT = 6666 (defined in file_server_threadpool.py)
# PROCESS_SERVER_PORT = 6667 (defined in file_server_processpool.py)

# Client-side directories
CLIENT_UPLOAD_SOURCE_DIR = "files_to_upload_client"
CLIENT_DOWNLOAD_DEST_DIR = "downloads_client"
SERVER_BASE_DIR = "files" # Server's working directory

# Stress Test Parameters
OPERATIONS = ["LIST", "UPLOAD", "DOWNLOAD"]
# VOLUMES_MB = {"10MB": 10, "50MB": 50, "100MB": 100}
VOLUMES_MB_MAP = {"10MB": 10 * 1024 * 1024, "50MB": 50 * 1024 * 1024, "100MB": 100 * 1024 * 1024}
CLIENT_WORKER_POOLS = [1, 5, 10] # Reduced 50 to 10 for quicker initial tests, can be increased
SERVER_WORKER_POOLS = [1, 5, 10] # Reduced 50 to 10

SERVER_TYPES = {
    "ThreadPool": {'script': 'file_server_threadpool.py', 'port': 6666},
    "ProcessPool": {'script': 'file_server_processpool.py', 'port': 6667}
}

# Create dummy files for upload/download
def create_dummy_file(filepath, size_bytes):
    if os.path.exists(filepath) and os.path.getsize(filepath) == size_bytes:
        logging.debug(f"File {filepath} of size {size_bytes} already exists.")
        return
    logging.info(f"Creating dummy file {filepath} of size {size_bytes / (1024*1024):.2f} MB")
    with open(filepath, 'wb') as f:
        chunk_size = 1024 * 1024  # 1MB
        for _ in range(size_bytes // chunk_size):
            f.write(os.urandom(chunk_size))
        if size_bytes % chunk_size > 0:
            f.write(os.urandom(size_bytes % chunk_size))

def setup_test_files():
    os.makedirs(CLIENT_UPLOAD_SOURCE_DIR, exist_ok=True)
    os.makedirs(CLIENT_DOWNLOAD_DEST_DIR, exist_ok=True)
    # Server directory is created by server, but good to have dummy files there for download
    os.makedirs(SERVER_BASE_DIR, exist_ok=True) 

    for vol_label, vol_bytes in VOLUMES_MB_MAP.items():
        # Files for client to upload
        create_dummy_file(os.path.join(CLIENT_UPLOAD_SOURCE_DIR, f"test_{vol_label}.dat"), vol_bytes)
        # Files on server for client to download
        create_dummy_file(os.path.join(SERVER_BASE_DIR, f"server_test_{vol_label}.dat"), vol_bytes)


# --- Client Task Function ---
def client_task(server_ip, server_port, operation, filename=None, file_content_base64=None, download_path=None):
    start_time = time.perf_counter()
    bytes_processed = 0
    status = "FAIL"
    error_detail = ""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60)
        sock.connect((server_ip, server_port))

        command_parts = [operation.upper()]
        if operation.upper() == "UPLOAD":
            if not filename or file_content_base64 is None:
                raise ValueError("UPLOAD requires filename and content")
            command_parts.extend([filename, file_content_base64])
            bytes_processed = len(base64.b64decode(file_content_base64))
        elif operation.upper() == "DOWNLOAD" or operation.upper() == "DELETE": # Added DELETE here
            if not filename:
                raise ValueError(f"{operation.upper()} requires filename")
            command_parts.append(filename)
        
        command_str = " ".join(command_parts) + "\r\n\r\n"

        sock.sendall(command_str.encode('utf-8'))

        # Receive response
        response_buffer = b""
        while True:
            chunk = sock.recv(8192) # Increased chunk size
            if not chunk:
                error_detail = "Connection closed by server prematurely (no full response)."
                break 
            response_buffer += chunk
            if b"\r\n\r\n" in response_buffer:
                break
        
        response_str = response_buffer.decode('utf-8', errors='ignore').strip()
        if not response_str.endswith("}"): # Simple check if JSON might be incomplete
             # Attempt to find the end of JSON
            json_end_index = response_str.rfind("}")
            if json_end_index != -1:
                response_str = response_str[:json_end_index+1]
            else: # Could not find valid JSON ending
                error_detail = f"Incomplete or malformed JSON response: {response_str[:200]}"
                status="FAIL" # Already set, but explicit
                return status, 0, 0, error_detail # time, bytes, error


        response_json = json.loads(response_str)

        if response_json.get("status") == "OK":
            status = "SUCCESS"
            if operation.upper() == "DOWNLOAD":
                file_data_b64 = response_json.get("data_file")
                downloaded_filename = response_json.get("data_namafile")
                if file_data_b64 and downloaded_filename:
                    file_data_bytes = base64.b64decode(file_data_b64)
                    bytes_processed = len(file_data_bytes)
                    if download_path:
                        # Ensure unique name for concurrent downloads to same base name
                        unique_name = f"{os.path.splitext(downloaded_filename)[0]}_{random.randint(1000,9999)}{os.path.splitext(downloaded_filename)[1]}"
                        with open(os.path.join(download_path, unique_name), 'wb') as f:
                            f.write(file_data_bytes)
                else:
                    status = "FAIL"
                    error_detail = "DOWNLOAD OK but missing data_file or data_namafile in response."
            elif operation.upper() == "LIST":
                 bytes_processed = len(json.dumps(response_json.get('data', [])).encode()) # Approx size of list data
        else:
            status = "FAIL"
            error_detail = response_json.get("data", "Unknown error from server.")

    except socket.timeout:
        status = "FAIL"
        error_detail = "Socket timeout"
    except ConnectionRefusedError:
        status = "FAIL"
        error_detail = "Connection refused"
    except ConnectionResetError:
        status = "FAIL"
        error_detail = "Connection reset by server"
    except BrokenPipeError:
        status = "FAIL"
        error_detail = "Broken pipe"
    except json.JSONDecodeError as jde:
        status = "FAIL"
        error_detail = f"JSON decode error: {jde}. Response fragment: {response_buffer.decode('utf-8', errors='ignore')[:200]}"
    except Exception as e:
        status = "FAIL"
        error_detail = str(e)
    finally:
        if 'sock' in locals() and sock:
            sock.close()
    
    end_time = time.perf_counter()
    time_taken = end_time - start_time
    return status, time_taken, bytes_processed, error_detail

# --- Main Stress Test Logic ---
def run_stress_test():
    logging.info("Starting stress test setup...")
    setup_test_files()
    
    results_data = []
    test_run_number = 0

    for server_name, server_config in SERVER_TYPES.items():
        server_script = server_config['script']
        server_port = server_config['port']

        for s_workers in SERVER_WORKER_POOLS:
            logging.info(f"----- Starting Server: {server_name} with {s_workers} workers on port {server_port} -----")
            server_process = None
            try:
                # Start the server
                server_cmd = ['python3', server_script, '--port', str(server_port), '--workers', str(s_workers), '--loglevel', 'WARNING']
                server_process = subprocess.Popen(server_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                time.sleep(3) 

                if server_process.poll() is not None: # Server failed to start
                    logging.error(f"Server {server_script} failed to start. Exit code: {server_process.returncode}")
                    stdout, stderr = server_process.communicate()
                    logging.error(f"Server STDOUT: {stdout.decode(errors='ignore')}")
                    logging.error(f"Server STDERR: {stderr.decode(errors='ignore')}")
                    continue


                for c_workers in CLIENT_WORKER_POOLS:
                    for op in OPERATIONS:
                        # Volume iteration. For LIST, volume is N/A but we iterate to match 81 combinations.
                        for vol_label, vol_bytes in VOLUMES_MB_MAP.items():
                            if op == "LIST" and vol_label != list(VOLUMES_MB_MAP.keys())[0]: # Run LIST only once for "N/A" volume
                                actual_volume_label = "N/A"
                                if vol_label != list(VOLUMES_MB_MAP.keys())[0]: # only iterate list once
                                    # effectively skip redundant list operations for different volumes
                                    # by making this part of an "if" that only runs for the first volume.
                                    # To strictly hit 81 rows, we let it pass. The output table will show N/A.
                                    pass # We'll let it run for all volumes for strict 81, and handle N/A later
                            
                            actual_volume_label = vol_label if op != "LIST" else "N/A"
                            test_run_number += 1
                            logging.info(f"\n{test_run_number}. Testing: Server={server_name}({s_workers}), ClientPool={c_workers}, Op={op}, Vol={actual_volume_label}")

                            tasks = []
                            with concurrent.futures.ThreadPoolExecutor(max_workers=c_workers, thread_name_prefix="StressClient") as executor:
                                for i in range(c_workers): # Number of concurrent client operations
                                    filename_to_use = None
                                    content_b64 = None
                                    
                                    if op == "UPLOAD":
                                        filename_to_use = f"test_{vol_label}.dat" # Client uploads this
                                        source_file_path = os.path.join(CLIENT_UPLOAD_SOURCE_DIR, filename_to_use)
                                        if not os.path.exists(source_file_path):
                                            logging.error(f"Source file for upload not found: {source_file_path}")
                                            tasks.append(executor.submit(lambda: ("FAIL", 0, 0, "Upload source file missing")))
                                            continue
                                        with open(source_file_path, 'rb') as f:
                                            content_b64 = base64.b64encode(f.read()).decode('utf-8')
                                    elif op == "DOWNLOAD":
                                        # Client downloads this file which should exist on server
                                        filename_to_use = f"server_test_{vol_label}.dat" 
                                    elif op == "LIST":
                                        pass # No filename needed
                                    
                                    # For DOWNLOAD, ensure target dir exists
                                    dl_path = CLIENT_DOWNLOAD_DEST_DIR if op == "DOWNLOAD" else None

                                    tasks.append(executor.submit(client_task, SERVER_HOST, server_port, op, filename_to_use, content_b64, dl_path))
                            
                            # Collect results from this batch of client tasks
                            client_task_results = []
                            for future in concurrent.futures.as_completed(tasks):
                                try:
                                    client_task_results.append(future.result())
                                except Exception as exc:
                                    logging.error(f"A client task generated an exception: {exc}")
                                    client_task_results.append(("EXCEPTION", 0, 0, str(exc)))
                            
                            # Process results for this specific combination
                            successful_client_tasks = sum(1 for r in client_task_results if r[0] == "SUCCESS")
                            failed_client_tasks = len(client_task_results) - successful_client_tasks
                            
                            total_time_sum = sum(r[1] for r in client_task_results if r[0] == "SUCCESS" and (op == "UPLOAD" or op == "DOWNLOAD"))
                            total_bytes_sum = sum(r[2] for r in client_task_results if r[0] == "SUCCESS" and (op == "UPLOAD" or op == "DOWNLOAD"))

                            avg_time_per_client = 0
                            avg_throughput_per_client = 0

                            if successful_client_tasks > 0 and (op == "UPLOAD" or op == "DOWNLOAD"):
                                avg_time_per_client = total_time_sum / successful_client_tasks
                                # Calculate throughput only if time is positive to avoid DivisionByZero
                                relevant_times = [r[1] for r in client_task_results if r[0] == "SUCCESS" and (op == "UPLOAD" or op == "DOWNLOAD") and r[1] > 0]
                                relevant_bytes = [r[2] for r in client_task_results if r[0] == "SUCCESS" and (op == "UPLOAD" or op == "DOWNLOAD") and r[1] > 0] # Should match length of relevant_times
                                
                                if relevant_times: # if there are successful ops with time > 0
                                    # Throughput = total successful bytes / total successful time for those bytes
                                    avg_throughput_per_client = sum(relevant_bytes) / sum(relevant_times) if sum(relevant_times) > 0 else 0


                            # Server success/failure interpretation:
                            # If client task failed due to server (e.g. timeout, conn refused after start, server error msg), count as server task failure.
                            # For simplicity, if a client task fails for reasons other than client-side setup, we attribute it to server handling.
                            server_tasks_failed = sum(1 for r in client_task_results if r[0] != "SUCCESS" and "Upload source file missing" not in r[3])
                            server_tasks_successful = successful_client_tasks # Assume server handled these successfully

                            errors_reported = [r[3] for r in client_task_results if r[0] != "SUCCESS" and r[3]]
                            if errors_reported:
                                logging.warning(f"Run {test_run_number} FAILED TASKS REPORTED: {errors_reported[:3]}") # Log first 3 errors


                            results_data.append({
                                "No": test_run_number,
                                "Server Type": server_name,
                                "Operasi": op,
                                "Volume": actual_volume_label,
                                "Jml Client Worker Pool": c_workers,
                                "Jml Server Worker Pool": s_workers,
                                "Waktu Total Avg (s)": f"{avg_time_per_client:.4f}" if (op != "LIST" and successful_client_tasks > 0) else "N/A",
                                "Throughput Avg (Bps)": f"{avg_throughput_per_client:.2f}" if (op != "LIST" and successful_client_tasks > 0) else "N/A",
                                "Client Sukses": successful_client_tasks,
                                "Client Gagal": failed_client_tasks,
                                "Server Tasks Sukses": server_tasks_successful, # How many client reqs it handled
                                "Server Tasks Gagal": server_tasks_failed # How many client reqs it failed to handle
                            })
                            # Optional: Brief cleanup of downloaded files for this run to save space
                            # for item in os.listdir(CLIENT_DOWNLOAD_DEST_DIR):
                            #     if item.startswith("server_test_"): os.remove(os.path.join(CLIENT_DOWNLOAD_DEST_DIR, item))


            except Exception as e_outer:
                logging.error(f"Outer loop error for server {server_script}: {e_outer}", exc_info=True)
            finally:
                if server_process:
                    logging.info(f"----- Stopping Server: {server_name} ({server_script}) -----")
                    server_process.terminate() # Try to terminate gracefully
                    try:
                        server_process.wait(timeout=10) # Wait for termination
                    except subprocess.TimeoutExpired:
                        logging.warning(f"Server {server_script} did not terminate gracefully, killing.")
                        server_process.kill() # Force kill
                    stdout, stderr = server_process.communicate()
                    logging.debug(f"Server {server_script} STDOUT: {stdout.decode(errors='ignore')}")
                    logging.debug(f"Server {server_script} STDERR: {stderr.decode(errors='ignore')}")
                time.sleep(2) # Pause before starting next server configuration

    # Save results to CSV
    df_results = pd.DataFrame(results_data)
    results_filename = "stress_test_results.csv"
    df_results.to_csv(results_filename, index=False)
    logging.info(f"Stress test complete. Results saved to {results_filename}")
    print(f"\nStress test complete. Results saved to {results_filename}")
    print(df_results.head())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    run_stress_test()