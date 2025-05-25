# file_interface.py
import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):

        self.default_folder = 'files/' 
        if not os.path.exists(self.default_folder):
            os.makedirs(self.default_folder)

    def list(self,params=[]):
        try:
            # Menggunakan self.default_folder untuk path yang lebih eksplisit
            filelist = [f for f in os.listdir(self.default_folder) if os.path.isfile(os.path.join(self.default_folder, f))]
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return dict(status='ERROR', data='Nama file tidak boleh kosong') 
            
            filepath = os.path.join(self.default_folder, filename)
            if not os.path.exists(filepath):
                return dict(status='ERROR', data=f'File {filename} tidak ditemukan')

            fp = open(filepath,'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except IndexError: # Jika params kosong atau kurang
            return dict(status='ERROR', data='Parameter nama file kurang')
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def upload(self, params=[]):
        try:
            if len(params) < 2:
                return dict(status='ERROR', data='Parameter tidak lengkap untuk upload (membutuhkan nama file dan konten)')
            
            filename = params[0]
            content_base64 = params[1]

            if not filename or "/" in filename or "\\" in filename:
                 return dict(status='ERROR', data='Nama file tidak valid')

            filepath = os.path.join(self.default_folder, filename)
            
            if not os.path.abspath(filepath).startswith(os.path.abspath(self.default_folder)):
                return dict(status='ERROR', data='Path file tidak valid')

            with open(filepath, 'wb') as f:
                f.write(base64.b64decode(content_base64.encode()))
            return dict(status='OK', data=f'{filename} berhasil diunggah')
        except IndexError:
             return dict(status='ERROR', data='Parameter nama file atau konten kurang')
        except base64.binascii.Error:
            return dict(status='ERROR', data='Konten base64 tidak valid')
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def delete(self, params=[]):
        try:
            if not params:
                return dict(status='ERROR', data='Parameter nama file kurang')

            filename = params[0]
            filepath = os.path.join(self.default_folder, filename)

            if not filename or "/" in filename or "\\" in filename:
                 return dict(status='ERROR', data='Nama file tidak valid')

            if not os.path.abspath(filepath).startswith(os.path.abspath(self.default_folder)):
                return dict(status='ERROR', data='Path file tidak valid')

            if os.path.exists(filepath) and os.path.isfile(filepath): 
                os.remove(filepath)
                return dict(status='OK', data=f'{filename} berhasil dihapus')
            else:
                return dict(status='ERROR', data=f'File {filename} tidak ditemukan atau bukan file')
        except IndexError:
            return dict(status='ERROR', data='Parameter nama file kurang')
        except Exception as e:
            return dict(status='ERROR', data=str(e))