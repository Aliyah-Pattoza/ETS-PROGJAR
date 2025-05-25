# file_client.py
import socket
import json
import base64
import logging
import os

server_address = ('127.0.0.1', 6666)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    logging.warning(f"Connecting to {server_address}")
    try:
        # Tambahkan delimiter
        if not command_str.endswith("\r\n\r\n"):
            command_str += "\r\n\r\n"

        sock.sendall(command_str.encode())
        data_received = ""
        while True:
            data = sock.recv(1024)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                break
        hasil = json.loads(data_received.strip())
        return hasil
    except Exception as e:
        logging.warning(f"Error saat menerima: {str(e)}")
        return False
    finally:
        sock.close()

def remote_list():
    hasil = send_command("LIST")
    if hasil and hasil.get('status') == 'OK':
        print("Daftar file:")
        for fname in hasil.get('data', []): 
            print(f"- {fname}")
    else:
        print("Gagal mengambil daftar file atau daftar kosong.")
        if hasil and hasil.get('data'):
             print(f"Pesan server: {hasil['data']}")


def remote_get(filename=""):
    hasil = send_command(f"GET {filename}")
    if hasil and hasil.get('status') == 'OK':
        namafile = hasil['data_namafile']
        isifile = base64.b64decode(hasil['data_file'])
       
        download_dir = "downloads_client"
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        file_path_to_save = os.path.join(download_dir, namafile)

        with open(file_path_to_save, 'wb') as fp:
            fp.write(isifile)
        print(f"File {namafile} berhasil diunduh ke folder '{download_dir}'.")
    else:
        print("Gagal mengunduh file.")
        if hasil and hasil.get('data'):
             print(f"Pesan server: {hasil['data']}")

def remote_upload(filepath=""):
    try:
        if not os.path.exists(filepath):
            print(f"File {filepath} tidak ditemukan.")
            return False
        
        filename = os.path.basename(filepath)
        with open(filepath, 'rb') as f:
            content = base64.b64encode(f.read()).decode() 
        
        command_str = f"UPLOAD {filename} {content}"
        hasil = send_command(command_str)
        
        if hasil and hasil.get('status') == 'OK':
            print(f"Server: {hasil.get('data', f'File {filename} berhasil diupload.')}")
            return True
        else:
            print(f"Upload gagal: {hasil.get('data', 'Terjadi kesalahan') if hasil else 'Tidak ada respons server'}")
            return False
            
    except FileNotFoundError:
        print(f"Upload gagal: File {filepath} tidak ditemukan.")
        return False
    except Exception as e:
        print(f"Upload gagal: {str(e)}")
        return False

def remote_delete(filename=""):
    command_str = f"DELETE {filename}"
    hasil = send_command(command_str)
    if hasil and hasil.get('status') == 'OK':
        print(f"Server: {hasil.get('data', f'File {filename} berhasil dihapus.')}")
        return True
    else:
        print(f"Hapus gagal: {hasil.get('data', 'Terjadi kesalahan') if hasil else 'Tidak ada respons server'}")
        return False

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING) 
   
    upload_source_dir = "files_to_upload_client"
    if not os.path.exists(upload_source_dir):
        os.makedirs(upload_source_dir)
        print(f"Direktori '{upload_source_dir}' telah dibuat. Tempatkan file yang ingin diupload di sini.")

    download_dir = "downloads_client"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        print(f"Direktori '{download_dir}' telah dibuat untuk menyimpan file yang diunduh.")

    while True:
        print("\nMenu:")
        print("1. Upload file (dari direktori 'files_to_upload_client')")
        print("2. List file di server")
        print("3. Download file (ke direktori 'downloads_client')")
        print("4. Delete file di server")
        print("5. Exit")
        choice = input("Pilih menu: ")

        if choice == '1':
            fname_to_upload = input(f"Masukkan nama file (dari direktori '{upload_source_dir}') untuk diupload: ")
            path_to_upload = os.path.join(upload_source_dir, fname_to_upload)
            remote_upload(path_to_upload)
        elif choice == '2':
            remote_list()
        elif choice == '3':
            fname_to_download = input("Nama file di server yang ingin di-download: ")
            remote_get(fname_to_download)
        elif choice == '4':
            fname_to_delete = input("Nama file di server yang ingin dihapus: ")
            remote_delete(fname_to_delete)
        elif choice == '5':
            break
        else:
            print("Pilihan tidak valid.")