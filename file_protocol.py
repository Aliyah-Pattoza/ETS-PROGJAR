# file_protocol.py
import json
import logging
from file_interface import FileInterface

class FileProtocol:
    def __init__(self):
        self.file = FileInterface() 
    
    def proses_string(self, string_datamasuk=''):

        logging.debug(f"Protokol: String masuk (awal): {string_datamasuk[:150]}...") 
        
        try:
            cleaned_string_datamasuk = string_datamasuk.strip()

            parts = cleaned_string_datamasuk.split(' ', 2)
            
            if not parts: 
                logging.error("Protokol: String masuk kosong setelah strip.")
                return json.dumps(dict(status='ERROR', data='Perintah kosong diterima'))

            c_request = parts[0].strip().lower()
            logging.info(f"Protokol: Request diproses: '{c_request}'")

            params = []
            if c_request == "upload":
                if len(parts) == 3: 
                    params.append(parts[1])  
                    params.append(parts[2])  
                else:
                    logging.error(f"Protokol: Perintah UPLOAD tidak lengkap. Diterima: '{cleaned_string_datamasuk[:100]}...'")
                    return json.dumps(dict(status='ERROR', data='Perintah UPLOAD tidak lengkap (membutuhkan nama file dan data)'))
            
            elif c_request == "get" or c_request == "delete": 
                if len(parts) >= 2:
                    params.append(parts[1])  
                else:
                    logging.error(f"Protokol: Perintah {c_request.upper()} kekurangan nama file. Diterima: '{cleaned_string_datamasuk[:100]}...'")
                    return json.dumps(dict(status='ERROR', data=f'Perintah {c_request.upper()} kekurangan nama file'))
            
            elif c_request == "list": 
                params = [] 
            
            else:

                logging.warning(f"Protokol: Request '{c_request}' tidak memiliki logika parsing khusus, akan dicoba getattr.")

            cl = getattr(self.file, c_request)(params)
            return json.dumps(cl)

        except AttributeError:
            logging.error(f"Protokol: AttributeError - request '{c_request}' tidak dikenali (method tidak ditemukan di FileInterface).")
            return json.dumps(dict(status='ERROR', data='request tidak dikenali (method tidak ditemukan)'))
        except IndexError: 
            logging.error(f"Protokol: IndexError saat parsing perintah. Perintah mungkin terlalu pendek. String awal: '{string_datamasuk[:100]}...'")
            return json.dumps(dict(status='ERROR', data='format request salah (argumen kurang)'))
        except Exception as e:
            logging.error(f"Protokol: Kesalahan umum saat memproses string '{string_datamasuk[:100]}...': {str(e)}", exc_info=True)
            return json.dumps(dict(status='ERROR', data='kesalahan server internal saat memproses permintaan'))
