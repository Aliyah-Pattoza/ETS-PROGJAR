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
import argparse
import signal 
import sys  
import threading 

# --- Konfigurasi ---
SERVER_HOST = '127.0.0.1'
CLIENT_UPLOAD_SOURCE_DIR = "files_to_upload_client"
CLIENT_DOWNLOAD_DEST_DIR = "downloads_client"
SERVER_FILES_DIR_FOR_TEST_SETUP = "files"
OPERATIONS_DEFAULT = ["LIST", "UPLOAD", "GET"]
VOLUMES_MB_MAP_DEFAULT = {"10MB": 10 * 1024 * 1024, "50MB": 50 * 1024 * 1024, "100MB": 100 * 1024 * 1024}
CLIENT_WORKER_POOLS_DEFAULT = [1, 5, 50]
SERVER_WORKER_POOLS_DEFAULT = [1, 5, 50]
SERVER_TYPES = {
    "ThreadPool": {'script': 'file_server_threadpool.py', 'port': 6666},
    "ProcessPool": {'script': 'file_server_processpool.py', 'port': 6667}
}
CLIENT_SOCKET_TIMEOUT = 300
INTERMEDIATE_RESULTS_FILENAME = "stress_test_results_INTERMEDIATE.csv"

# Variabel global untuk menyimpan data hasil sementara untuk signal handler
_results_data_live = []
_shutdown_event = threading.Event() 

def create_dummy_file(filepath, size_bytes):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if os.path.exists(filepath) and os.path.getsize(filepath) == size_bytes:
        logging.debug(f"File {filepath} dengan ukuran {size_bytes} bytes sudah ada.")
        return
    logging.info(f"Membuat file dummy {filepath} ukuran {size_bytes / (1024*1024):.2f} MB")
    with open(filepath, 'wb') as f:
        chunk_size = 1024 * 1024
        remaining_bytes = size_bytes
        while remaining_bytes > 0:
            current_chunk_size = min(chunk_size, remaining_bytes)
            f.write(os.urandom(current_chunk_size))
            remaining_bytes -= current_chunk_size

def setup_test_files(volumes_map):
    os.makedirs(CLIENT_UPLOAD_SOURCE_DIR, exist_ok=True)
    os.makedirs(CLIENT_DOWNLOAD_DEST_DIR, exist_ok=True)
    os.makedirs(SERVER_FILES_DIR_FOR_TEST_SETUP, exist_ok=True)
    for vol_label, vol_bytes in volumes_map.items():
        create_dummy_file(os.path.join(CLIENT_UPLOAD_SOURCE_DIR, f"upload_test_{vol_label}.dat"), vol_bytes)
        create_dummy_file(os.path.join(SERVER_FILES_DIR_FOR_TEST_SETUP, f"server_download_{vol_label}.dat"), vol_bytes)

def client_task(server_ip, server_port, operation, filename=None, file_content_base64=None, download_path=None, task_id=""):
    start_time = time.perf_counter()
    bytes_processed = 0
    status = "FAIL"
    error_detail = ""
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CLIENT_SOCKET_TIMEOUT)
        logging.debug(f"Task {task_id}: Connecting to {server_ip}:{server_port}")
        sock.connect((server_ip, server_port))
        logging.debug(f"Task {task_id}: Connected.")
        command_parts = [operation.upper()]
        if operation.upper() == "UPLOAD":
            if not filename or file_content_base64 is None:
                error_detail = "UPLOAD memerlukan nama file dan konten"
                raise ValueError(error_detail)
            command_parts.extend([filename, file_content_base64])
            bytes_processed = len(base64.b64decode(file_content_base64))
        elif operation.upper() == "GET":
            if not filename:
                error_detail = "GET (untuk download) memerlukan nama file"
                raise ValueError(error_detail)
            command_parts.append(filename)
        command_str_with_delimiter = " ".join(command_parts) + "\r\n\r\n"
        logging.debug(f"Task {task_id}: Sending command ({len(command_str_with_delimiter)} bytes)...")
        sock.sendall(command_str_with_delimiter.encode('utf-8'))
        logging.debug(f"Task {task_id}: Command sent.")
        response_buffer = b""
        logging.debug(f"Task {task_id}: Waiting for response...")
        while True:
            try:
                chunk = sock.recv(16384)
            except socket.timeout:
                error_detail = "Socket timeout saat menerima data respons"
                logging.warning(f"Task {task_id}: {error_detail}")
                status = "FAIL_TIMEOUT_RECV"
                break
            if not chunk:
                if response_buffer and not response_buffer.strip().endswith(b"\r\n\r\n"):
                    error_detail = "Koneksi ditutup oleh server secara prematur (respons tidak lengkap atau tanpa delimiter)."
                elif not response_buffer:
                    error_detail = "Koneksi ditutup oleh server (tidak ada data respons)."
                break
            response_buffer += chunk
            if response_buffer.endswith(b"\r\n\r\n"):
                break
        if status == "FAIL" and not error_detail and not response_buffer.endswith(b"\r\n\r\n"):
             error_detail = "Respons tidak diakhiri dengan delimiter atau tidak ada respons."
        if response_buffer:
            response_str_full = response_buffer.decode('utf-8', errors='ignore')
            if response_str_full.endswith("\r\n\r\n"):
                response_str = response_str_full[:-4].strip()
            else:
                if not error_detail:
                    error_detail = f"Respons malformed: Delimiter tidak ada. Diterima: {response_str_full[:200]}..."
                response_str = ""
            if response_str:
                try:
                    response_json = json.loads(response_str)
                    if response_json.get("status") == "OK":
                        status = "SUCCESS"
                        if operation.upper() == "GET":
                            file_data_b64 = response_json.get("data_file")
                            downloaded_filename_from_server = response_json.get("data_namafile")
                            if file_data_b64 and downloaded_filename_from_server:
                                file_data_bytes = base64.b64decode(file_data_b64)
                                bytes_processed = len(file_data_bytes)
                                if download_path:
                                    base, ext = os.path.splitext(downloaded_filename_from_server)
                                    unique_download_name = f"{base}_{task_id}_{random.randint(1000,9999)}{ext}"
                                    full_download_path = os.path.join(download_path, unique_download_name)
                                    with open(full_download_path, 'wb') as f:
                                        f.write(file_data_bytes)
                                    logging.debug(f"Task {task_id}: File downloaded to {full_download_path}")
                            else:
                                status = "FAIL"
                                error_detail = "GET (DOWNLOAD) OK tetapi data_file atau data_namafile hilang dalam respons."
                        elif operation.upper() == "LIST":
                            list_data_str = json.dumps(response_json.get('data', []))
                            bytes_processed = len(list_data_str.encode('utf-8'))
                    else:
                        status = "FAIL"
                        error_detail = response_json.get("data", "Server mengembalikan status bukan OK tanpa pesan error.")
                        if "request tidak dikenali" in error_detail:
                            logging.error(f"Task {task_id}: Server merespons 'request tidak dikenali'. Perintah dikirim: {command_str_with_delimiter[:100]}")
                except json.JSONDecodeError as jde:
                    status = "FAIL"
                    error_detail = f"JSON decode error: {jde}. Respons: {response_str[:200]}"
            elif status == "FAIL" and not error_detail:
                 error_detail = "Respons kosong dari server atau delimiter tidak ditemukan."
        elif status == "FAIL" and not error_detail:
            error_detail = "Tidak ada respons dari server (koneksi mungkin langsung ditutup)."
    except socket.timeout:
        status = "FAIL_TIMEOUT_CONNECT_SEND"
        error_detail = "Socket timeout saat koneksi atau mengirim perintah."
        logging.warning(f"Task {task_id}: {error_detail}")
    except ConnectionRefusedError:
        status = "FAIL_CONN_REFUSED"
        error_detail = "Koneksi ditolak oleh server."
    except ConnectionResetError:
        status = "FAIL_CONN_RESET"
        error_detail = "Koneksi direset oleh server."
    except BrokenPipeError:
        status = "FAIL_BROKEN_PIPE"
        error_detail = "Broken pipe (klien atau server menutup secara tidak terduga)."
    except ValueError as ve:
        status = "FAIL_BAD_REQUEST"
        error_detail = str(ve)
    except Exception as e:
        status = "FAIL_CLIENT_EXCEPTION"
        error_detail = f"Exception pada client task: {type(e).__name__} - {str(e)}"
        logging.error(f"Task {task_id}: {error_detail}", exc_info=False)
    finally:
        if sock:
            sock.close()
    end_time = time.perf_counter()
    time_taken = end_time - start_time
    if status != "SUCCESS" and error_detail:
        logging.debug(f"Task {task_id}: Status Akhir={status}, Waktu={time_taken:.2f}s, Bytes={bytes_processed}, Error: {error_detail}")
    elif status == "SUCCESS":
        logging.debug(f"Task {task_id}: Status Akhir={status}, Waktu={time_taken:.2f}s, Bytes={bytes_processed}")
    return status, time_taken, bytes_processed, error_detail

def save_current_results(data_list, filename):
    if not data_list:
        logging.info(f"Tidak ada data hasil untuk disimpan ke {filename}.")
        return
    df = pd.DataFrame(data_list)
    column_order = [
        "Nomor", "Server Type", "Operasi", "Volume",
        "Jml Client Worker Pool", "Jml Server Worker Pool",
        "Waktu Total Avg per Client (s)", "Throughput Avg per Client (Bps)",
        "Client Worker Sukses", "Client Worker Gagal",
        "Server Worker (Est.) Sukses", "Server Worker (Est.) Gagal"
    ]
    for col in column_order:
        if col not in df.columns:
            df[col] = "N/A" 
    df = df.reindex(columns=column_order)

    try:
        df.to_csv(filename, index=False)
        logging.info(f"Hasil disimpan sementara/akhir ke {filename}")
        print(f"\nHasil disimpan ke {filename}")
        if filename != INTERMEDIATE_RESULTS_FILENAME: 
             try:
                print(df.to_string())
             except Exception:
                print(df.head())
    except Exception as e:
        logging.error(f"Gagal menyimpan hasil ke {filename}: {e}")

def signal_interrupt_handler(signum, frame):
    logging.warning(f"Sinyal {signal.Signals(signum).name} diterima. Meminta shutdown halus...")
    _shutdown_event.set()
    if hasattr(signal_interrupt_handler, 'called_once') and signal_interrupt_handler.called_once:
        logging.warning("Sinyal diterima lagi, keluar paksa.")
        save_current_results(_results_data_live, INTERMEDIATE_RESULTS_FILENAME.replace(".csv", "_FORCE_EXIT.csv"))
        sys.exit(1)
    signal_interrupt_handler.called_once = True


def run_stress_test(args):
    global _results_data_live 
    logging.info("Memulai setup stress test...")

    signal.signal(signal.SIGINT, signal_interrupt_handler)
    signal.signal(signal.SIGTERM, signal_interrupt_handler)
    signal_interrupt_handler.called_once = False


    operations_to_run = [args.operation.upper()] if args.operation else OPERATIONS_DEFAULT
    client_pools_to_run = [args.client_workers] if args.client_workers is not None else CLIENT_WORKER_POOLS_DEFAULT
    server_pools_to_run = [args.server_workers] if args.server_workers is not None else SERVER_WORKER_POOLS_DEFAULT
    server_types_to_iterate = SERVER_TYPES
    if args.server_type:
        if args.server_type in SERVER_TYPES:
            server_types_to_iterate = {args.server_type: SERVER_TYPES[args.server_type]}
        else:
            logging.error(f"server_type '{args.server_type}' tidak dikenali. Pilihan: {list(SERVER_TYPES.keys())}")
            return

    volumes_for_file_setup = VOLUMES_MB_MAP_DEFAULT
    if args.volume and args.volume != "N/A":
        is_list_op_explicit = args.operation and args.operation.upper() == "LIST"
        if not is_list_op_explicit:
            if args.volume in VOLUMES_MB_MAP_DEFAULT:
                volumes_for_file_setup = {args.volume: VOLUMES_MB_MAP_DEFAULT[args.volume]}
    if volumes_for_file_setup:
        logging.info(f"Menyiapkan file tes untuk volume: {list(volumes_for_file_setup.keys())}")
        setup_test_files(volumes_for_file_setup)
    else:
        logging.info("Tidak ada volume spesifik untuk disiapkan.")

    _results_data_live.clear() 
    test_run_number = 0

    # Outer loops
    for server_name_key, server_config in server_types_to_iterate.items():
        if _shutdown_event.is_set(): break
        server_script = server_config['script']
        server_port = server_config['port']
        for s_workers in server_pools_to_run:
            if _shutdown_event.is_set(): break
            logging.info(f"----- Memulai Server: {server_name_key} dengan {s_workers} worker di port {server_port} -----")
            server_process = None
            server_log_file = f"server_{server_name_key}_p{server_port}_w{s_workers}.log"
            try:
                with open(server_log_file, 'wb') as slf:
                    server_cmd = ['python3', server_script, '--port', str(server_port), '--workers', str(s_workers), '--loglevel', args.loglevel.upper()]
                    logging.info(f"Menjalankan server dengan perintah: {' '.join(server_cmd)}")
                    server_process = subprocess.Popen(server_cmd, stdout=slf, stderr=subprocess.STDOUT)
                logging.info(f"Menunggu server {server_script} (PID: {server_process.pid if server_process else 'N/A'}) untuk siap...")
                time.sleep(5)
                if server_process.poll() is not None:
                    logging.error(f"Server {server_script} gagal memulai. Kode keluar: {server_process.returncode}. Lihat {server_log_file}")
                    try:
                        with open(server_log_file, 'r', encoding='utf-8', errors='ignore') as f_log_err:
                            last_lines = f_log_err.readlines()[-10:]
                        logging.error(f"Beberapa baris terakhir dari {server_log_file}:\n{''.join(last_lines)}")
                    except Exception as e_log:
                        logging.error(f"Tidak bisa membaca log server {server_log_file}: {e_log}")
                    continue
                logging.info(f"Server {server_script} seharusnya sudah berjalan.")

                for c_workers in client_pools_to_run:
                    if _shutdown_event.is_set(): break
                    for op_actual in operations_to_run:
                        if _shutdown_event.is_set(): break
                        volumes_to_test_for_current_op = {}
                        if op_actual == "LIST":
                            volumes_to_test_for_current_op = {"N/A_placeholder": 0}
                        else:
                            if args.volume and args.volume != "N/A":
                                if args.volume in VOLUMES_MB_MAP_DEFAULT:
                                    volumes_to_test_for_current_op = {args.volume: VOLUMES_MB_MAP_DEFAULT[args.volume]}
                                else:
                                    logging.warning(f"Volume '{args.volume}' tidak terdefinisi untuk operasi '{op_actual}'. Melewati.")
                                    continue
                            elif args.volume == "N/A":
                                 logging.warning(f"Volume 'N/A' tidak berlaku untuk operasi '{op_actual}'. Melewati {op_actual} dengan N/A.")
                                 continue
                            else:
                                volumes_to_test_for_current_op = VOLUMES_MB_MAP_DEFAULT
                        if not volumes_to_test_for_current_op:
                            logging.debug(f"Tidak ada volume yang ditentukan untuk operasi {op_actual}, melewati.")
                            continue

                        for vol_label_internal, vol_bytes in volumes_to_test_for_current_op.items():
                            if _shutdown_event.is_set(): break
                            test_run_number += 1
                            op_for_table = "DOWNLOAD" if op_actual == "GET" else op_actual
                            actual_volume_label_for_table = "N/A" if op_actual == "LIST" else vol_label_internal
                            logging.info(f"\n{test_run_number}. Menguji: Server={server_name_key}(SrvW:{s_workers}), ClientPool(CliW:{c_workers}), Op(Aktual):{op_actual}/(Tabel):{op_for_table}, Vol={actual_volume_label_for_table}")
                            tasks = []
                      
                            content_b64_for_op_shared = None
                            filename_for_op_shared = None
                            if op_actual == "UPLOAD" and vol_label_internal != "N/A_placeholder":
                                upload_filename = f"upload_test_{vol_label_internal}.dat"
                                source_file_path = os.path.join(CLIENT_UPLOAD_SOURCE_DIR, upload_filename)
                                if not os.path.exists(source_file_path):
                                    logging.error(f"GLOBAL: File sumber untuk upload tidak ditemukan: {source_file_path} untuk {vol_label_internal}. Melewati batch ini.")
                                    for i in range(c_workers):
                                        _results_data_live.append({
                                            "Nomor": test_run_number, "Server Type": server_name_key, "Operasi": op_for_table,
                                            "Volume": actual_volume_label_for_table, "Jml Client Worker Pool": c_workers,
                                            "Jml Server Worker Pool": s_workers, "Waktu Total Avg per Client (s)": "N/A",
                                            "Throughput Avg per Client (Bps)": "N/A", "Client Worker Sukses": 0, "Client Worker Gagal": c_workers,
                                            "Server Worker (Est.) Sukses": 0, "Server Worker (Est.) Gagal": c_workers, 
                                            
                                        })
                                    save_current_results(_results_data_live, INTERMEDIATE_RESULTS_FILENAME)
                                    break 
                                with open(source_file_path, 'rb') as f_ul:
                                    content_b64_for_op_shared = base64.b64encode(f_ul.read()).decode('utf-8')
                                filename_for_op_shared = upload_filename
                            
                            with concurrent.futures.ThreadPoolExecutor(max_workers=c_workers, thread_name_prefix="StressClient") as executor:
                                for i in range(c_workers):
                                    task_unique_id = f"{op_actual[:1]}{actual_volume_label_for_table[:3]}_s{s_workers}_c{c_workers}_{i+1}"
                                    current_filename_for_op = filename_for_op_shared
                                    current_content_b64 = content_b64_for_op_shared
                                    download_dest_path_for_op = None

                                    if op_actual == "GET":
                                        if vol_label_internal == "N/A_placeholder": # Seharusnya tidak terjadi karena sudah dicek
                                            tasks.append(executor.submit(lambda: ("FAIL_LOGIC_ERROR", 0, 0, "GET with N/A volume")))
                                            continue
                                        current_filename_for_op = f"server_download_{vol_label_internal}.dat"
                                        download_dest_path_for_op = CLIENT_DOWNLOAD_DEST_DIR
                                    elif op_actual == "UPLOAD" and vol_label_internal == "N/A_placeholder":
                                         tasks.append(executor.submit(lambda: ("FAIL_LOGIC_ERROR", 0, 0, "UPLOAD with N/A volume")))
                                         continue
                                    elif op_actual == "LIST":
                                        pass
                                    tasks.append(executor.submit(client_task, SERVER_HOST, server_port, op_actual,
                                                                current_filename_for_op, current_content_b64, download_dest_path_for_op, task_unique_id))
                            client_task_results = []
                            for future in concurrent.futures.as_completed(tasks):
                                try:
                                    client_task_results.append(future.result())
                                except Exception as exc_future:
                                    logging.error(f"Sebuah future client task menghasilkan exception: {exc_future}")
                                    client_task_results.append(("FAIL_FUTURE_EXC", 0, 0, str(exc_future)))
                            successful_client_tasks = sum(1 for r in client_task_results if r[0] == "SUCCESS")
                            failed_client_tasks = len(client_task_results) - successful_client_tasks
                            successful_op_results = [r for r in client_task_results if r[0] == "SUCCESS"]
                            avg_time_per_client_str = "N/A"
                            avg_throughput_per_client_str = "N/A"
                            if op_actual == "UPLOAD" or op_actual == "GET":
                                successful_op_times = [r[1] for r in successful_op_results]
                                successful_op_bytes = [r[2] for r in successful_op_results]
                                if successful_op_times:
                                    avg_time = sum(successful_op_times) / len(successful_op_times)
                                    avg_time_per_client_str = f"{avg_time:.4f}"
                                    total_bytes_for_successful_ops = sum(successful_op_bytes)
                                    total_time_for_successful_ops = sum(successful_op_times)
                                    if total_time_for_successful_ops > 0:
                                        avg_throughput = total_bytes_for_successful_ops / total_time_for_successful_ops
                                        avg_throughput_per_client_str = f"{avg_throughput:.2f}"
                                    else:
                                        avg_throughput_per_client_str = "Inf" if total_bytes_for_successful_ops > 0 else "0.00"
                            elif op_actual == "LIST":
                                successful_list_times = [r[1] for r in successful_op_results]
                                if successful_list_times:
                                    avg_time = sum(successful_list_times) / len(successful_list_times)
                                    avg_time_per_client_str = f"{avg_time:.4f}"
                            server_tasks_potentially_failed = failed_client_tasks
                            server_tasks_potentially_successful = successful_client_tasks
                            errors_reported_details = [r[3] for r in client_task_results if r[0] != "SUCCESS" and r[3]]
                            if errors_reported_details:
                                unique_errors = list(set(errors_reported_details))
                                logging.warning(f"Run {test_run_number}: {failed_client_tasks} FAILED client tasks. Errors (sampel): {unique_errors[:3]}")
                            _results_data_live.append({
                                "Nomor": test_run_number, "Server Type": server_name_key, "Operasi": op_for_table,
                                "Volume": actual_volume_label_for_table, "Jml Client Worker Pool": c_workers,
                                "Jml Server Worker Pool": s_workers, "Waktu Total Avg per Client (s)": avg_time_per_client_str,
                                "Throughput Avg per Client (Bps)": avg_throughput_per_client_str, "Client Worker Sukses": successful_client_tasks,
                                "Client Worker Gagal": failed_client_tasks, "Server Worker (Est.) Sukses": server_tasks_potentially_successful,
                                "Server Worker (Est.) Gagal": server_tasks_potentially_failed,
                            })
                            save_current_results(_results_data_live, INTERMEDIATE_RESULTS_FILENAME) # Simpan setelah setiap test run
                        if _shutdown_event.is_set(): break 
                    if _shutdown_event.is_set(): break 
                if _shutdown_event.is_set(): break 
            except FileNotFoundError as fnf_err:
                logging.error(f"FileNotFoundError saat mencoba setup server log {server_log_file}: {fnf_err}. Periksa izin tulis atau path.")
                continue
            except Exception as e_outer:
                logging.error(f"Error pada loop luar untuk server {server_script} (s_workers: {s_workers}): {e_outer}", exc_info=True)
            finally:
                if server_process and server_process.poll() is None: # Hanya jika server masih berjalan
                    logging.info(f"----- Menghentikan Server: {server_name_key} (PID: {server_process.pid}) -----")
                    server_process.terminate()
                    try:
                        return_code = server_process.wait(timeout=20)
                        logging.info(f"Server {server_script} berhenti dengan kode: {return_code}")
                    except subprocess.TimeoutExpired:
                        logging.warning(f"Server {server_script} tidak berhenti dengan baik setelah 20 detik, mematikan paksa (kill).")
                        server_process.kill()
                        try:
                           server_process.wait(timeout=5)
                           logging.info(f"Server {server_script} setelah di-kill: {'berhenti' if server_process.poll() is not None else 'masih berjalan?'}")
                        except subprocess.TimeoutExpired:
                           logging.error(f"Server {server_script} tidak berhenti bahkan setelah SIGKILL. Port mungkin masih digunakan.")
                    except Exception as e_sp_wait:
                         logging.error(f"Exception saat menunggu server berhenti: {e_sp_wait}")
                elif server_process and server_process.poll() is not None:
                     logging.info(f"Server {server_name_key} sudah berhenti sebelum dihentikan secara eksplisit (kode: {server_process.returncode}).")
                else:
                    logging.info(f"Tidak ada proses server yang aktif untuk dihentikan untuk {server_name_key} (mungkin gagal start).")
                if os.path.exists(server_log_file):
                    logging.info(f"Log server ada di {server_log_file}")

                time.sleep(5)
        if _shutdown_event.is_set(): break 
    if _shutdown_event.is_set():
        logging.warning("Pengujian dihentikan oleh sinyal.")

    # Simpan hasil akhir
    final_timestamp = time.strftime("%Y%m%d-%H%M%S")
    final_results_filename = f"stress_test_results_{final_timestamp}.csv"
    
    if os.path.exists(INTERMEDIATE_RESULTS_FILENAME):
        try:
            if not _shutdown_event.is_set() and _results_data_live:
                 logging.info("Menyimpan hasil akhir dari memori karena selesai normal.")
                 save_current_results(_results_data_live, final_results_filename)
                 if os.path.exists(INTERMEDIATE_RESULTS_FILENAME): 
                     try: os.remove(INTERMEDIATE_RESULTS_FILENAME)
                     except OSError as e_rem: logging.warning(f"Gagal menghapus file intermediate: {e_rem}")
            else: 
                logging.info(f"Menggunakan hasil dari file intermediate: {INTERMEDIATE_RESULTS_FILENAME}")
                os.rename(INTERMEDIATE_RESULTS_FILENAME, final_results_filename)
                print(f"\nHasil akhir (dari intermediate) disimpan ke {final_results_filename}")
        except Exception as e_rename:
            logging.error(f"Gagal me-rename file intermediate ke final: {e_rename}. Menyimpan _results_data_live jika ada.")
            if _results_data_live: 
                 save_current_results(_results_data_live, final_results_filename)
            else: 
                 logging.warning("Tidak ada data hasil di memori maupun file intermediate untuk disimpan sebagai final.")
                 print("\nStress test selesai. Tidak ada hasil yang dihasilkan.")

    elif _results_data_live:
        logging.info("Menyimpan hasil akhir dari memori (tidak ada file intermediate).")
        save_current_results(_results_data_live, final_results_filename)
    else:
        logging.info("Stress test selesai. Tidak ada hasil yang dihasilkan (tidak ada data di memori maupun file intermediate).")
        print("\nStress test selesai. Tidak ada hasil yang dihasilkan.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stress Test Client untuk File Server")
    parser.add_argument('--server_type', type=str, choices=['ThreadPool', 'ProcessPool'],
                        help='Tipe server spesifik untuk diuji. Jika tidak diset, semua tipe diuji.')
    parser.add_argument('--operation', type=str, choices=['LIST', 'UPLOAD', 'GET'],
                        help='Operasi spesifik untuk diuji. Jika tidak diset, semua operasi diuji.')
    parser.add_argument('--volume', type=str, choices=['10MB', '50MB', '100MB', 'N/A'],
                        help='Volume spesifik untuk diuji. Jika tidak diset, semua volume relevan diuji.')
    parser.add_argument('--client_workers', type=int,
                        help='Jumlah spesifik client worker. Jika tidak diset, semua pool default diuji.')
    parser.add_argument('--server_workers', type=int,
                        help='Jumlah spesifik server worker. Jika tidak diset, semua pool default diuji.')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Level logging untuk stress client')
    args = parser.parse_args()
    log_level_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    logging.basicConfig(level=log_level_map.get(args.loglevel.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    run_stress_test(args)
