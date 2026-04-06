from decimal import Decimal


class FormatValue:
    """
    Classe contenente le funzioni di conversione dei dati
    """
    _divisore = [10 ** n for n in range(10)]
    _decimali = [Decimal("0.1") ** n for n in range(16)]
    _punto = "."
    _dato = None
    _controllo = None
    _s: str = ""
    _ndiz = {}
    _lista = []

    @classmethod
    def str_to_str_dec(cls, dato: str = None, numerodecimali=0, inout: bool = False) -> str:
        """[summary]
        Fromatta una stringa aggiungendo la virgola dei decimali
        Se inout = false prende la stringa e aggiunge i decimali,
        Se inout = True aggiunge 2 deciali alla stringa che estrae
        Args:
            :param dato: (str, optional): [Valore da convertire]. Defaults to None.
            :param numerodecimali: (int, optional): [Numero di decimali da visualizzare]. Defaults to 0.
            :param inout: 0 = Out 1 = Ingresso
        Returns:
            str: [stringa formattata]

        """
        try:
            cls._controllo = float(dato)
            cls._dato = dato
            if cls._dato.find(cls._punto) == -1:
                # if nrDecimali != 0:
                if inout:
                    numm = cls._controllo * cls._divisore[numerodecimali]
                else:
                    numm = cls._controllo
                num = Decimal(
                    str(numm / cls._divisore[numerodecimali]))
                num.quantize(cls._decimali[numerodecimali])
                num = format(num, f".{numerodecimali}f")
                return str(num)
            elif cls._dato.find(cls._punto) != -1:
                # if nrDecimali != 0:
                num = Decimal(cls._dato)
                num = format(num, f".{numerodecimali}f")
                return str(num)
        except TypeError:
            # jlog.wlog(titlolo="Error", messaggio=f"Mask {err}")
            return dato
        except ValueError:
            # jlog.wlog(titlolo="Error", messaggio=f"Mask {err}")
            return dato

    @classmethod
    def str_to_dec_str(cls, dato: str) -> str:
        """[summary]
        Questa funzione prende una stringa con decilami e la restituisce senza decimali
        Args:
        :param: dato (str): [stringa da trasformare]

        Returns:
            str: [ritorna una stringa senza decimali]
        """
        for c in dato:
            if c != ".":
                cls._s = cls._s + c
        return cls._s

    @classmethod
    def bool_to_str(cls, dato: bool) -> str:
        """[summary]
        Questa funzione trasforma un valore boleano in una stringa 0-1
        Args:
         :param: dato (bool): [numero boleano ]

        Returns:
            str: [un 1 per True e 0 per False]
        """
        result: str = ""
        if type(dato) == bool:
            if dato:
                result = "1"
            else:
                result = "0"
        return result

    @classmethod
    def str_to_bool(cls, dato: str) -> bool:
        """[summary]
        Converte una stringa 0-1 oppure True False in un numero Booleano
        Args:
         :param: dato (str): [Valore da convertire ]

        Returns:
            bool: [ritorna un bool]
        """
        if type(dato) == str:
            if dato == "1" or dato.strip().lower() == "true":
                return True
            elif dato == "0" or dato.strip().lower() == "false":
                return False

    @classmethod
    def byte_to_str(cls, dato: bytes, numerodecimali: int = 0) -> str:
        """[summary]
        Converte un byte in una stringa
        Args:
         :param: dato (int): [Valore ]
         :param: numerodecimali (int, optional): [numero di decimali da visualizzare]. Defaults to 0.

        Returns:
            str: [riotrna una stringa formattata]
        """
        result: str = ""
        if type(dato) == int:
            result = str(dato)
            result = cls.str_to_str_dec(result, numerodecimali)
            pass
        return result

    @classmethod
    def int_to_str(cls, dato: int, numerodecimali: int = 0) -> str:
        """[summary]
        Converte un intero in una stringa con decimali
        Args:
         :param: dato (int): [Valore ]
         :param:numerodecimali (int, optional): [numero di decimali da visualizzare]. Defaults to 0.

        Returns:
            str: [riotrna una stringa formattata]
        """
        result: str = ""
        if type(dato) == int:
            result = str(dato)
            result = cls.str_to_str_dec(result, numerodecimali)
        return result

    @classmethod
    def word_to_str(cls, dato: bytearray, numerodecimali: int = 0) -> str:
        """[summary]
        Converte un intero in una stringa con decimali
        Args:
         :param: dato (int): [Valore ]
         :param:numerodecimali (int, optional): [numero di decimali da visualizzare]. Defaults to 0.

        Returns:
            str: [riotrna una stringa formattata]
        """
        valore = int().from_bytes(dato, byteorder='big')
        result: str = ""
        if type(valore) == int:
            result = str(dato)
            result = cls.str_to_str_dec(result, numerodecimali)
        return result

    @classmethod
    def str_to_int(cls, dato: str, numerodecimali: int = 0) -> int:

        """
        Converte una scringa in un intero
        :param: dato
        :param: numerodecimali =0
        :return: numero intero oppure None in caso di errore
        """
        result: int = 0
        app: str = ""
        if type(dato) == str:
            try:
                if dato.find(".") != -1:
                    for c in dato:
                        if c != ".":
                            app = app + c
                else:
                    app = dato
                    for c in range(numerodecimali):
                        app = app + "0"
                result = int(app.strip())
            except TypeError as errore:
                # jlog.wlog(titlolo="Errore", messaggio=f" str_to_int  {errore}")
                pass
            except ValueError as errore:
                # jlog.wlog(titlolo="Errore", messaggio=f" str_to_int  {errore}")
                pass
        return result

    @classmethod
    def float_to_str(cls, dato: float, numerodecimali: int) -> str:
        """
         Converte un float in una stringa
        :param dato:
        :param numerodecimali:
        :return: Stringa con il valore
        """
        if type(dato) == float:
            result = str(dato)
            result = cls.str_to_str_dec(result, numerodecimali)
            return result

    @classmethod
    def str_to_float(cls, dato: str, decimali: int) -> float:
        """
        Converte una stringa in un float con deciami
        :param dato:
        :param decimali:
        :return: Float
        """
        if type(dato) == str:
            try:
                return float(cls.str_to_str_dec(dato.strip(), decimali))
            except TypeError:
                pass
            except ValueError:
                pass

    @classmethod
    def int_to_chars(cls, intero: int = 0) -> str:
        """
        Prende un intero ed estrae i 2 byte traformati in stringa
        Weintec scrive i carattieri su 2 byte di un intero Siemens
        :param : intero:
        :return: Stringa
        """
        numero = intero
        c1 = numero & 0xFF
        numero = numero >> 8
        c2 = numero & 0xFF
        lettere = str(chr(c1) + chr(c2))
        lettere = lettere.rstrip("\x00")
        return lettere

    @classmethod
    def mette_caratteri_in_array(cls, carattere: str = ""):
        pass

    @classmethod
    def diz_int_to_byte(cls, value: dict) -> dict:
        """
        Converte un dizionario di byte in un dizionario interi
        :param : value:
        :return: dict
        """
        cls._ndiz = {}
        for dato in value.keys():
            cls._ndiz[dato] = bytearray(value[dato])
        return cls._ndiz

    @classmethod
    def diz_byte_to_int(cls, value: dict) -> dict:
        """
        riporta i byte in interi
        :param : value:
        :return:
        """
        cls._ndiz = {}
        for f in value.keys():
            cls._lista = []
            for v in value[f]:
                cls._lista.append(int(v))
            cls._ndiz[f] = cls._lista
        return cls._ndiz


if __name__ == '__main__':
    _decimali = [Decimal("0.1") ** n for n in range(16)]
    print(_decimali)
    s = "234"
    fn = Decimal(s).quantize(_decimali[2])
    print(fn)
    converte = FormatValue.int_to_str(123, 2)
    print(converte)
