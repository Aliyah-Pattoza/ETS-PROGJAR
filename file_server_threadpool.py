# file_server_threadpool.py
import os
import socket
import logging
import concurrent.futures
import argparse
from file_protocol import FileProtocol 

fp_instance = FileProtocol()

def handle_client_connection(connection, address):
    global fp_instance # Menggunakan instance global fp
    logging.info(f"Processing connection from {address} using thread {threading.current_thread().name}")
    buffer = ""
    try:
        connection.settimeout(30.0) 

        while True:
            try:
                data = connection.recv(4096) 
                if not data:
                    logging.info(f"Connection closed by {address} (no data received).")
                    break 
                buffer += data.decode('utf-8', errors='ignore') # Tambahkan error handling untuk decoding
                
                if "\r\n\r\n" in buffer:
                    command_str, _, rest_buffer = buffer.partition("\r\n\r\n")
                    buffer = rest_buffer 
                    break 
            except socket.timeout:
                logging.warning(f"Socket timeout while receiving data from {address}. Buffer so far: '{buffer[:100]}...'")
                if not buffer.strip(): 
                    logging.info(f"No data received from {address} before timeout.")
                else: 
                    logging.warning(f"Partial data from {address} before timeout, command might be incomplete.")
                return 

            except UnicodeDecodeError as ude:
                logging.error(f"Unicode decode error from {address}: {ude}. Buffer: {buffer}")
                
                return
        
        if command_str: 

            log_command = command_str
            if "UPLOAD" in command_str.upper().split(' ', 1)[0]: # Cek apakah perintahnya UPLOAD
                parts = command_str.split(" ", 2)
                if len(parts) > 1: 
                    log_command = f"{parts[0]} {parts[1]} [DATA_OMITTED]"
            logging.info(f"Received command from {address}: {log_command[:200]}...") # Log sebagian kecil saja
            
            hasil = fp_instance.proses_string(command_str)
            hasil_dengan_delimiter = hasil + "\r\n\r\n"
            connection.sendall(hasil_dengan_delimiter.encode('utf-8'))
            logging.debug(f"Response sent to {address}: {hasil[:100]}...")
        else:
            logging.info(f"No complete command received from {address} or empty buffer.")

    except ConnectionResetError:
        logging.warning(f"Connection reset by {address}")
    except BrokenPipeError:
        logging.warning(f"Broken pipe with {address}. Client may have disconnected.")
    except socket.error as se:
        logging.error(f"Socket error with {address}: {se}")
    except Exception as e:
        if not isinstance(e, socket.timeout):
             logging.error(f"Error handling client {address}: {e}", exc_info=True)
    finally:
        logging.info(f"Closing connection from {address}")
        connection.close()

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=6666, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # fp_instance sudah diinisialisasi global atau bisa di-pass jika perlu

    def run(self):
        logging.warning(f"Server (ThreadPool) running on {self.ipinfo} with {self.max_workers} workers")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(75) 

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ClientHandlerThread") as executor:
            try:
                while True:
                    try:
                        connection, client_address = self.my_socket.accept()
                        logging.info(f"Accepted connection from {client_address}")
                       
                        executor.submit(handle_client_connection, connection, client_address)
                    except OSError as e:
                        logging.info(f"Server socket closed or error during accept: {e}. Shutting down...")
                        break 
                    except Exception as e_accept:
                        logging.error(f"Error accepting new connection: {e_accept}", exc_info=True)
            except KeyboardInterrupt:
                logging.warning("Server shutdown initiated by KeyboardInterrupt.")
            finally:
                logging.warning("Shutting down ThreadPoolExecutor...")
                executor.shutdown(wait=True) 
                self.my_socket.close()
                logging.warning("Server socket closed. Server shutdown complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Server with ThreadPoolExecutor")
    parser.add_argument('--port', type=int, default=6666, help='Port to listen on')
    parser.add_argument('--workers', type=int, default=5, help='Number of worker threads in the pool')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    args = parser.parse_args()

    import threading 

    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    logging.basicConfig(level=log_level_map.get(args.loglevel.upper(), logging.INFO), 
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    SERVER_STORAGE_DIRECTORY = 'files' 
    if not os.path.exists(SERVER_STORAGE_DIRECTORY):
        os.makedirs(SERVER_STORAGE_DIRECTORY)
        logging.info(f"Direktori '{SERVER_STORAGE_DIRECTORY}' telah dibuat oleh server.")

    svr = Server(port=args.port, max_workers=args.workers)
    svr.run()