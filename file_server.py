import socket
import threading
import logging

from file_protocol import FileProtocol
fp = FileProtocol()

class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        threading.Thread.__init__(self)
        self.connection = connection
        self.address = address

    def run(self):
        buffer = ""
        while True:
            try:
                data = self.connection.recv(1024)
                if not data:
                    break
                buffer += data.decode()
                if "\r\n\r\n" in buffer:
                    break
            except Exception as e:
                logging.warning(f"Error menerima data: {e}")
                break

        if buffer:
            command_str = buffer.strip()
            logging.warning(f"String diproses: {command_str[:100]}...")  
            hasil = fp.proses_string(command_str)
            hasil += "\r\n\r\n"
            try:
                self.connection.sendall(hasil.encode())
            except BrokenPipeError:
                logging.warning("Broken pipe saat mengirim balasan ke client")

        self.connection.close()

class Server(threading.Thread):
    def __init__(self, ipaddress='0.0.0.0', port=6666):
        threading.Thread.__init__(self)
        self.ipinfo = (ipaddress, port)
        self.the_clients = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def run(self):
        logging.warning(f"Server berjalan di {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        while True:
            connection, client_address = self.my_socket.accept()
            logging.warning(f"Koneksi dari {client_address}")
            clt = ProcessTheClient(connection, client_address)
            clt.start()
            self.the_clients.append(clt)

def main():
    logging.basicConfig(level=logging.WARNING)
    svr = Server(ipaddress='0.0.0.0', port=6666)
    svr.start()

if __name__ == "__main__":
    main()
