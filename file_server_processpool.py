import os
import socket
import logging
import concurrent.futures
import argparse
import multiprocessing
import sys
import signal
import threading
from file_protocol import FileProtocol

SERVER_STORAGE_DIRECTORY = 'files'
fp_instance_for_process = None

def initialize_worker_process():
    global fp_instance_for_process
    fp_instance_for_process = FileProtocol()
    process_name = multiprocessing.current_process().name
    logging.debug(f"Proses {process_name} menginisialisasi instance FileProtocol.")
    
    if not os.path.exists(SERVER_STORAGE_DIRECTORY):
        try:
            os.makedirs(SERVER_STORAGE_DIRECTORY, exist_ok=True)
            logging.debug(f"Proses {process_name} memastikan direktori '{SERVER_STORAGE_DIRECTORY}' ada.")
        except OSError as e:
            logging.error(f"Proses {process_name} gagal membuat direktori '{SERVER_STORAGE_DIRECTORY}': {e}")

def handle_client_connection(connection, address):
    global fp_instance_for_process
    current_process_name = multiprocessing.current_process().name
    logging.info(f"Memproses koneksi dari {address} menggunakan proses {current_process_name}")
    
    temp_byte_buffer = bytearray()
    command_str = None
    client_task_status = "INIT"
    MAX_COMMAND_SIZE = 200 * 1024 * 1024

    try:
        connection.settimeout(300.0)
        try:
            while True:
                if len(temp_byte_buffer) > MAX_COMMAND_SIZE:
                    logging.error(f"Menerima data melebihi MAX_COMMAND_SIZE ({MAX_COMMAND_SIZE} bytes) dari {address} tanpa menemukan delimiter.")
                    client_task_status = "FAIL_CMD_TOO_LARGE"
                    command_str = None
                    break

                data = connection.recv(8192)
                if not data:
                    if len(temp_byte_buffer) > 0:
                        logging.warning(f"Koneksi ditutup oleh {address} setelah menerima sebagian data ({len(temp_byte_buffer)} bytes) tetapi sebelum delimiter ditemukan.")
                        client_task_status = "FAIL_CONN_CLOSED_PARTIAL_CMD"
                    else:
                        logging.info(f"Koneksi ditutup oleh {address} (tidak ada data diterima).")
                        client_task_status = "FAIL_CONN_CLOSED_CMD_READ"
                    command_str = None
                    break
                
                temp_byte_buffer.extend(data)
                
                delimiter_pos = temp_byte_buffer.find(b"\r\n\r\n")
                if delimiter_pos != -1:
                    command_bytes_with_delimiter = temp_byte_buffer[:delimiter_pos + 4]
                    try:
                        command_str_with_delimiter = command_bytes_with_delimiter.decode('utf-8')
                        command_str = command_str_with_delimiter.rstrip("\r\n")
                        client_task_status = "CMD_RECEIVED"
                    except UnicodeDecodeError as ude:
                        logging.error(f"Unicode decode error dari {address} setelah menerima perintah lengkap: {ude}. Data (awal): {command_bytes_with_delimiter[:100]}")
                        client_task_status = "FAIL_UNICODE_CMD_DECODE_FULL"
                        command_str = None
                    break
        except socket.timeout:
            logging.warning(f"Socket timeout (server-side) saat menerima data perintah dari {address}. Buffer diterima: {len(temp_byte_buffer)} bytes.")
            client_task_status = "FAIL_TIMEOUT_SERVER_RECV_LOOP" if len(temp_byte_buffer) > 0 else "FAIL_TIMEOUT_NO_CMD_DATA"
            command_str = None
        except UnicodeDecodeError as ude:
            logging.error(f"Unicode decode error dari {address} saat baca perintah (loop extend): {ude}. Buffer: {temp_byte_buffer[-100:]}")
            client_task_status = "FAIL_UNICODE_CMD_DECODE_CHUNK"
            command_str = None
        except Exception as e_recv:
            logging.error(f"Error menerima perintah dari {address}: {e_recv}", exc_info=True)
            client_task_status = f"FAIL_RECV_CMD_EXCEPTION_{type(e_recv).__name__}"
            command_str = None
        
        if command_str:
            log_command = command_str
            cmd_parts_for_log = command_str.upper().split(' ', 1)
            if cmd_parts_for_log[0] == "UPLOAD":
                parts_for_log = command_str.split(" ", 2)
                if len(parts_for_log) > 1:
                    log_command = f"{parts_for_log[0]} {parts_for_log[1]} [DATA_OMITTED]"
            logging.info(f"Menerima perintah dari {address}: {log_command[:250]}...")
            
            if fp_instance_for_process is None:
                logging.error(f"fp_instance_for_process belum diinisialisasi di {current_process_name} untuk {address}")
                client_task_status = "FAIL_FP_NOT_INITIALIZED"
            else:
                try:
                    hasil = fp_instance_for_process.proses_string(command_str)
                    hasil_dengan_delimiter = hasil + "\r\n\r\n"
                    connection.sendall(hasil_dengan_delimiter.encode('utf-8'))
                    logging.debug(f"Respons dikirim ke {address}: {hasil[:100]}...")
                    client_task_status = "SUCCESS"
                except socket.timeout:
                    logging.error(f"Socket timeout saat sendall ke {address}.")
                    client_task_status = "FAIL_TIMEOUT_SEND_RESPONSE"
                except Exception as e_process:
                    logging.error(f"Error memproses perintah atau mengirim respons untuk {address}: {e_process}", exc_info=True)
                    client_task_status = f"FAIL_PROCESSING_OR_SEND_EXCEPTION_{type(e_process).__name__}"
        
        elif client_task_status == "INIT" or (client_task_status == "CMD_RECEIVED" and not command_str) :
             logging.info(f"Tidak ada string perintah valid untuk diproses untuk {address}. Status akhir: {client_task_status}")

    except ConnectionResetError:
        logging.warning(f"Koneksi direset oleh {address}")
        client_task_status = "FAIL_CONN_RESET"
    except BrokenPipeError:
        logging.warning(f"Broken pipe dengan {address}. Klien mungkin terputus.")
        client_task_status = "FAIL_BROKEN_PIPE"
    except socket.error as se:
        logging.error(f"Error socket umum dengan {address}: {se}", exc_info=True)
        client_task_status = f"FAIL_SOCKET_ERROR_{type(se).__name__}"
    except Exception as e_outer:
        logging.error(f"Error luar menangani klien {address}: {e_outer}", exc_info=True)
        client_task_status = f"FAIL_UNHANDLED_OUTER_EXCEPTION_{type(e_outer).__name__}"
    finally:
        logging.info(f"Tugas worker untuk {address} selesai dengan status: [{client_task_status}] oleh {current_process_name}")
        logging.info(f"Menutup koneksi dari {address}")
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=6667, max_workers=50):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = None
        self.executor = None
        self._shutdown_request = threading.Event()

    def _setup_socket(self):
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(75)
            logging.info(f"Server terikat ke {self.ipinfo} dan sedang mendengarkan.")
            return True
        except socket.error as bind_error:
            logging.critical(f"KRITIS: Gagal mengikat atau mendengarkan pada {self.ipinfo}: {bind_error}")
            if self.my_socket:
                self.my_socket.close()
                self.my_socket = None
            return False

    def _handle_signal(self, signum, frame):
        logging.warning(f"Sinyal {signal.Signals(signum).name} diterima. Memulai proses shutdown...")
        self._shutdown_request.set()
        if self.my_socket:
            try:
                self.my_socket.close()
                logging.info("Socket server ditutup oleh signal handler untuk membuka blokir accept().")
                self.my_socket = None
            except OSError as e:
                logging.debug(f"Error saat menutup socket di signal handler (mungkin sudah ditutup): {e}")

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        original_sigterm_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if not os.path.exists(SERVER_STORAGE_DIRECTORY):
            try:
                os.makedirs(SERVER_STORAGE_DIRECTORY)
                logging.info(f"Proses utama membuat direktori '{SERVER_STORAGE_DIRECTORY}'.")
            except FileExistsError:
                logging.debug(f"Direktori '{SERVER_STORAGE_DIRECTORY}' sudah ada (dicek oleh proses utama).")
            except OSError as e:
                logging.error(f"Proses utama gagal membuat direktori '{SERVER_STORAGE_DIRECTORY}': {e}")

        logging.warning(f"Server (ProcessPool) memulai pada {self.ipinfo} dengan {self.max_workers} pekerja")
        
        if not self._setup_socket():
            return False

        try:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.max_workers,
                initializer=initialize_worker_process
            ) as executor:
                self.executor = executor
                logging.info(f"ProcessPoolExecutor dimulai dengan max_workers={self.max_workers}")

                while not self._shutdown_request.is_set():
                    try:
                        if not self.my_socket:
                            logging.info("Loop accept berhenti karena socket server sudah ditutup.")
                            break
                        
                        self.my_socket.settimeout(1.0) 
                        connection, client_address = self.my_socket.accept()
                        self.my_socket.settimeout(None)

                        if self._shutdown_request.is_set():
                            logging.info("Permintaan shutdown diterima saat/setelah accept, koneksi baru tidak diproses.")
                            connection.close()
                            break
                        
                        logging.info(f"Menerima koneksi dari {client_address}")
                        executor.submit(handle_client_connection, connection, client_address)

                    except socket.timeout:
                        continue 
                    except OSError as e:
                        if self._shutdown_request.is_set() or not self.my_socket:
                            logging.info(f"Socket server error saat accept (kemungkinan karena shutdown): {e}.")
                        else:
                            logging.error(f"Socket server error saat accept: {e}. Menghentikan loop server...")
                        break
                    except Exception as e_accept:
                        if self._shutdown_request.is_set():
                            logging.info(f"Exception saat accept setelah shutdown dimulai: {e_accept}")
                        else:
                            logging.error(f"Error menerima koneksi baru: {e_accept}", exc_info=True)
                        if isinstance(e_accept, (BrokenPipeError, ConnectionResetError)):
                            continue
                        break

                logging.warning("Loop utama penerimaan koneksi telah berhenti.")
        
        except Exception as e_main_executor_scope:
            logging.error(f"Error kritis dalam lingkup utama ProcessPoolExecutor: {e_main_executor_scope}", exc_info=True)
        
        finally:
            logging.warning("Blok 'finally' Server.run() dieksekusi.")
            
            if self.executor:
                 logging.info("Menunggu ProcessPoolExecutor untuk shutdown (jika belum)...")

            if self.my_socket:
                logging.info("Menutup socket server utama di blok finally (jika masih terbuka)...")
                try:
                    self.my_socket.close()
                    self.my_socket = None
                    logging.warning("Socket server utama telah ditutup.")
                except OSError as e:
                    logging.debug(f"Error saat menutup socket server utama di finally (mungkin sudah ditutup): {e}")
            
            signal.signal(signal.SIGINT, original_sigint_handler)
            signal.signal(signal.SIGTERM, original_sigterm_handler)
            
            logging.warning("Metode Server.run() selesai.")

        if self._shutdown_request.is_set():
             logging.info("Server shutdown karena permintaan sinyal.")
        return True

if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    parser = argparse.ArgumentParser(description="File Server dengan ProcessPoolExecutor")
    parser.add_argument('--ip', type=str, default='0.0.0.0', help='Alamat IP untuk bind')
    parser.add_argument('--port', type=int, default=6667, help='Port untuk listen')
    parser.add_argument('--workers', type=int, default=5, help='Jumlah proses pekerja')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Level logging')
    args = parser.parse_args()

    log_level_map = {
        'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING,
        'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL
    }
    logging.basicConfig(level=log_level_map.get(args.loglevel.upper(), logging.INFO),
                        format='%(asctime)s - %(levelname)s - %(processName)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                        handlers=[logging.StreamHandler()])

    svr = Server(ipaddress=args.ip, port=args.port, max_workers=args.workers)
    if not svr.run():
        logging.critical(f"Server gagal menginisialisasi dengan benar (misalnya, error bind pada port {args.port}). Keluar dengan kode error 1.")
        sys.exit(1)
    else:
        logging.info("Server telah shutdown.")
        sys.exit(0)
