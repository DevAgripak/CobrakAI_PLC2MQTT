import logging

import snap7.error

# Ottieni un logger specifico per QUESTO modulo.
# Il nome del logger sarà 'Util' (basato sul nome del file/modulo).
# Erediterà automaticamente gli handler (file e console) configurati in Run.py sul root logger.
logger = logging.getLogger(__name__)

try:
    import os
    import platform
    import snap7
    import snap7.common
    import snap7.error
    from snap7.util import (set_int, set_bool, set_real, set_dint, set_string, set_byte, set_word,
                            get_bool, get_byte, get_int, get_dint, get_word, get_real, get_string, get_lreal)
    from Util.DataFormat import FormatValue

except ImportError as e:
    logging.error(f"Errore nell'importazione delle librerie Siemens il programma si chiude: {e}")
    exit(1)


def estrae_adr_db(dato):
    if dato:
        db = ""
        numero = 0
        byte_bit = ""
        passo = 0
        for c in dato:
            if c != "%" and passo == 0:
                if c == ".":
                    passo = 1
                    continue
                if c.isdigit():
                    numero = numero + int(c)
                db = db + c
            if passo == 1:
                if c.isdigit() or c == ".":
                    byte_bit = byte_bit + c
        # print(db)
        # print(numero)
        # print(byte_bit)
        if byte_bit.find(".") == -1:
            byte_bit = byte_bit + ".0"

        return db, numero, byte_bit


class Js7Client:
    """
    A client for connecting to and interacting with a Siemens S7 PLC using the snap7 library.

    Attributes:
        ip_address (str): The IP address of the PLC. (default plc 10.0.0.1)
        port (int): The port number of the PLC (default is 102).
        rack (int): The rack number of the PLC (default is 0).
        slot (int): The slot number of the PLC (default is 1).
        connection (snap7.client.Client): The snap7 client connection object.

    Methods:
        connect(): Establishes a connection to the PLC.
        disconnect(): Closes the connection to the PLC.
        read_by_address(address, data_type, count=1): Reads data from the PLC at the specified address.
        write_by_address(address, value, data_type): Writes data to the PLC at the specified address.
    """

    def __init__(self, dati_plc=dict(), strutturaDb=dict()):
        """
        Initializes a new JS7Client object.
        and load dll

        Args:
            ip_address (str): The IP address of the PLC.
            port (int): The port number of the PLC (default is 102).
            rack (int): The rack number of the PLC (default is 0).
            slot (int): The slot number of the PLC (default is 1).
        """
        self._s7Cla = snap7.client.Client()
        self._ip_address = dati_plc["ip"]
        self._rack = dati_plc["rack"]
        self._slot = dati_plc["slot"]
        self._oS = platform.system()
        self._architettura = None
        self._connection = None
        self.strdb = strutturaDb

        # Carica la dll adeguata per la comunicazione
        self.loadDll()

    def loadDll(self):
        logging.info("Caricamento DLL Siemens ")
        self._architettura = platform.architecture()
        try:
            if os.name in ("nt", "dos", "ce"):
                if self._architettura[0] in "64bit":
                    self._phat_dll = "".join(os.getcwd() + "/Siemens_S7/DllWin64/snap7.dll")
                elif self._architettura[0] in "32bit":                    
                    self._phat_dll = "".join(os.getcwd() + "/Siemens_S7/DllWin32/snap7.dll")
                else:
                    raise "Non riesco a caricare la libreria snap7 corretta "
            elif  os.name in ("posix"):      
                self._phat_dll = "".join(os.getcwd() + "/Siemens_S7/DllLinux/libsnap7.so")
            if self._phat_dll:
                snap7.common.load_library(self._phat_dll)
        except Exception as e:
            logger.error(f"Impossibile caricare le Dll Siemens: {e}")
            exit(1)

    def checkIp(self) -> bool:
        """
        Verifica se il cavo di rete è connesso
        Returns: true = Ok
        """
        if self._ip_address:
            logger.info("controllo cavo di rete collegato al pc ")
            try:
                self.result = os.system(
                    "ping "
                    + ("-n 1 " if self._oS == "Windows" else "-c 1 ")
                    + self._ip_address
                )
            finally:
                if self.result == 0:
                    logger.info("cavo di rete collegato correnttamente ")
                    return True
                else:
                    logger.error("nessun cavo di rete è collegato al pc o indirizzo errarto ")
                    return False

    def s7connect(self):
        """
        Creo la connessione con il PLC .
        """
        try:
            if not self._s7Cla.get_connected():
                if self.checkIp():
                    logger.info("S7: connect start ip=%s rack=%s slot=%s", self._ip_address, self._rack, self._slot)
                    p = self._s7Cla.connect(self._ip_address, self._rack, self._slot)
                    logger.info("S7: connect ok ip=%s rack=%s slot=%s result=%s", self._ip_address, self._rack, self._slot, p)
                    return True
        except RuntimeError as err:
            logger.error(f"Errore nel tentativo di connessione al plc {err}")
            return False
        except Exception as e:
            logger.error(f"Errore nel tentativo di connessione al plc {e}")
            return False

    def isconnect(self) -> bool:
        return self._s7Cla.get_connected

    def disconnect(self):
        """
        Chiudo la connessione con il PLC.
        """
        logger.info("S7: disconnect ip=%s", self._ip_address)
        if self._s7Cla.get_connected():
            try:
                self._s7Cla.disconnect()
                print("Disconnesso dal PLC.")

            finally:
                self.connection = None

    def read_aree_plc(self) -> dict:
        self._dict_row_values = {}
        if self._s7Cla.get_connected():
            ok = 0
            fail = 0
            for _adr in self.strdb:
                try:
                    self._dict_row_values[_adr["nome"]] = self._s7Cla.db_read(_adr["nr"], _adr["start"], _adr["end"])
                    ok += 1
                except Exception as e:
                    logging.error(f"Errore nella lettura delle {_adr} del plc: {e}")
                    fail += 1
            logger.info("S7: db_read done ip=%s ok=%s fail=%s", self._ip_address, ok, fail)
            return self._dict_row_values

    @staticmethod
    def get_valorerx(_byte_array: dict, var: dict, nrdec=3) -> str:

        v = None
        db, numero, bayt_bit = estrae_adr_db(var["adr"])
        bayt_bit = bayt_bit.split(".")
        strlength = var["length"]
        if var['tipo'].upper() == "BOOL":
            try:
                v = FormatValue.bool_to_str(get_bool(_byte_array[db], int(bayt_bit[0]), int(bayt_bit[1])))
            except Exception as e:
                logging.error(f"errore conversione {var} : {e}")
            finally:
                return v
        elif var['tipo'].upper() == "BYTE":
            try:
                v = FormatValue.byte_to_str(get_byte(_byte_array[db], int(bayt_bit[0])))
            finally:
                return v
        elif var['tipo'].upper() == "INT":
            try:
                v = FormatValue.int_to_str(get_int(_byte_array[db], int(bayt_bit[0])), 0)
            finally:
                return v
        elif var['tipo'].upper() == "WORD":
            try:
                v = FormatValue.word_to_str(get_word(_byte_array[db], int(bayt_bit[0])), 0)
            finally:
                return v
        elif var['tipo'].upper() == "DINT":
            try:
                v = FormatValue.int_to_str(get_dint(_byte_array[db], int(bayt_bit[0])), 0)
            finally:
                return v
        elif var['tipo'].upper() == "REAL":
            try:
                v = FormatValue.float_to_str(get_real(bytearray_=_byte_array[db],
                                                      byte_index=int(bayt_bit[0])), nrdec)
            finally:
                return v
        elif var['tipo'].upper() == "LREAL":
            try:
                v = FormatValue.float_to_str(get_lreal(bytearray_=_byte_array[db],
                                                       byte_index=int(bayt_bit[0])), nrdec)
                #logging.info(f"Valore di {var} e: {v}")
            except Exception as e:
                logging.error(f"errore conversione {var} : {e}")
            finally:
                return v
        # inserita funzione per leggere le stringe  11-11-25 joe
        elif var['tipo'].upper() == "STRING":
            try:
                # Calculate the slice length: header (2 bytes) + max string length
                slice_len = int(strlength) + 2
                bytearrayAppStr = _byte_array[db][int(bayt_bit[0]):int(bayt_bit[0]) + slice_len]
                #v = get_string(bytearray_=bytearrayAppStr,
                #                byte_index=int(strlength))
                v = bytearrayAppStr[2:].split(b"\x00")[0].decode("utf-8")
                #logging.info(f"Valore di {var} e: {v}")
            except Exception as e:
                logging.error(f"errore conversione {var} : {e}")
            finally:
                return v        

            # elif var.tipo.upper() == "STRING" and var.plc.upper() == "S":
            #     try:
            #         var.valorerow = get_string(bytearray_=_byte_array, byte_index=var.jbyte)
            #     finally:
            #         return var.valorerow
            pass
