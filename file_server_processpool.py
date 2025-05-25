# file_server_processpool.py
import os
import socket
import logging
import concurrent.futures
import argparse
import multiprocessing 
from file_protocol import FileProtocol

fp_instance = FileProtocol()
SERVER_STORAGE_DIRECTORY = 'files' 

def handle_client_connection(connection, address):
    global fp_instance, SERVER_STORAGE_DIRECTORY
    
    if not os.path.exists(SERVER_STORAGE_DIRECTORY):
        try:
            os.makedirs(SERVER_STORAGE_DIRECTORY)
            logging.info(f"Process {multiprocessing.current_process().name} created dir '{SERVER_STORAGE_DIRECTORY}'.")
        except FileExistsError:
            pass 

    logging.info(f"Processing connection from {address} using process {multiprocessing.current_process().name}")
    buffer = ""
    try:
        connection.settimeout(60.0) 

        while True: 
            try:
                data = connection.recv(4096)
                if not data:
                    logging.info(f"Connection closed by {address} (no data received).")
                    break
                buffer += data.decode('utf-8', errors='ignore')
                
                if "\r\n\r\n" in buffer:
                    command_str, _, rest_buffer = buffer.partition("\r\n\r\n")
                    buffer = rest_buffer 
                    break 
            except socket.timeout:
                logging.warning(f"Socket timeout while receiving command from {address}. Buffer so far: '{buffer[:100]}...'")
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
            if "UPLOAD" in command_str.upper().split(' ', 1)[0]:
                parts = command_str.split(" ", 2) 
                if len(parts) > 1:
                    log_command = f"{parts[0]} {parts[1]} [DATA_OMITTED]"
            logging.info(f"Received command from {address}: {log_command[:200]}...") 

            hasil = fp_instance.proses_string(command_str)
            hasil_dengan_delimiter = hasil + "\r\n\r\n" 
            connection.sendall(hasil_dengan_delimiter.encode('utf-8'))
            logging.debug(f"Response sent to {address}: {hasil[:100]}...")
        else:
            logging.info(f"No complete command received from {address} or empty buffer after loop.")

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
    def __init__(self, ipaddress='0.0.0.0', port=6667, max_workers=5): 
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def run(self):
        global SERVER_STORAGE_DIRECTORY
        if not os.path.exists(SERVER_STORAGE_DIRECTORY):
            os.makedirs(SERVER_STORAGE_DIRECTORY)
            logging.info(f"Main process created directory '{SERVER_STORAGE_DIRECTORY}'.")

        logging.warning(f"Server (ProcessPool) running on {self.ipinfo} with {self.max_workers} workers")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(75)

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
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
                logging.warning("Shutting down ProcessPoolExecutor...")
                executor.shutdown(wait=True, cancel_futures=False) 
                self.my_socket.close()
                logging.warning("Server socket closed. Server shutdown complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Server with ProcessPoolExecutor")
    parser.add_argument('--ip', type=str, default='0.0.0.0', help='IP address to bind to')
    parser.add_argument('--port', type=int, default=6667, help='Port to listen on (default 6667 for ProcessPool)')
    parser.add_argument('--workers', type=int, default=5, help='Number of worker processes in the pool')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    args = parser.parse_args()

    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    logging.basicConfig(level=log_level_map.get(args.loglevel.upper(), logging.INFO), 
                        format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s')


    svr = Server(ipaddress=args.ip, port=args.port, max_workers=args.workers)
    svr.run()