# import sqlite3
import pandas as pd
import configparser
import os
from datetime import datetime
#import mariadb
import logging

# Ottieni un logger specifico per QUESTO modulo.
# Il nome del logger sarà 'Util' (basato sul nome del file/modulo).
# Erediterà automaticamente gli handler (file e console) configurati in Run.py sul root logger.
logger = logging.getLogger(__name__)


# def get_db_connection(parametri_db={}):
#     # server = "127.0.0.1\SQLEXPRESS"
#     # database = "CobrakAi"
#     # username = "sa"
#     # password = "1234"
#
#     host = parametri_db["host"]
#     database = parametri_db["database"]
#     username = parametri_db["user"]
#     password = parametri_db["password"]
#     port = int(parametri_db["port"])
#
#     try:
#         conn = mariadb.connect(
#             user=username,
#             password=password,
#             host=host,
#             port=port,
#             database=database,
#         )
#
#     except mariadb.Error as e:
#         logging.critical(
#             f"Errore critico: IMMPOSSIBILE CONNETTERSI AL DATABASE : {e}",
#             exc_info=True,
#         )
#         return None
#
#     return conn


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


def creaTimeStamp() -> str:
    # current_timestamp_spazi = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    return current_timestamp
    # print(current_timestamp_spazi)


def prendeDati(row):
    _rowNomeVar = 0
    _rowTipoVar = 4
    _rowAddressVar = 8
    dati = {"nomevar": "", "alias": "", "nrdb": 1, "nrbyte": 0, "nrbit": 0, "tipo": ""}
    if row.iloc[_rowNomeVar]:
        _nomeVar = row.iloc[_rowNomeVar]
        _nomeVar = _nomeVar.replace(".", "_")
        _nomeVar = _nomeVar.replace("[", "_")
        _nomeVar = _nomeVar.replace("]", "_")
        _nomeVar = _nomeVar.replace('"', "_")
        _nomeVar = _nomeVar.replace(":", "_")
        _nomeVar = _nomeVar.replace("(", "_")
        _nomeVar = _nomeVar.replace(")", "_")
        dati["nomevar"] = _nomeVar
        dati["alias"] = _nomeVar
        dati["tipo"] = row.iloc[_rowTipoVar]
        db, numerodb, byte_bit = estrae_adr_db(row.iloc[_rowAddressVar])
        byte_bit = byte_bit.split(".")
        dati["nrdb"] = numerodb
        dati["nrbyte"] = int(byte_bit[0])
        dati["nrbit"] = int(byte_bit[1])
        return dati
        pass


def estrai_variabili_file_exel(NomeFile):
    _rowNomeVar = 0
    _rowTipoVar = 4
    _rowAddressVar = 8
    _nomeVar = ""
    _alias = ""
    _tipo = ""
    _adr = ""
    connessione = get_db_connection()
    cursore = connessione.cursor()
    for filename in os.listdir(NomeFile):
        file_path = os.path.join(NomeFile, filename)
        try:
            df = pd.read_excel(file_path, skiprows=1)
            for index, row in df.iterrows():
                _nomeVar = row.iloc[_rowNomeVar]
                if _nomeVar == "" or _nomeVar == None:
                    break
                _adr = row.iloc[_rowAddressVar]
                _alias = _nomeVar
                _tipo = row.iloc[_rowTipoVar]
                _nomeVar = _nomeVar.replace(".", "_")
                _nomeVar = _nomeVar.replace("[", "_")
                _nomeVar = _nomeVar.replace("]", "_")
                _nomeVar = _nomeVar.replace('"', "_")
                _nomeVar = _nomeVar.replace(":", "_")
                _nomeVar = _nomeVar.replace("(", "_")
                _nomeVar = _nomeVar.replace(")", "_")
                db, numerodb, byte_bit = estrae_adr_db(_adr)
                byte_bit = byte_bit.split(".")
                # TODO togliere la conversione ep provare
                cursore.execute(
                    "INSERT INTO mappatura (varname, alias, nrbd, nrbyte, nrbit, type) VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        _nomeVar,
                        _alias,
                        numerodb,
                        int(byte_bit[0]),
                        int(byte_bit[1]),
                        _tipo,
                    ),
                )

        except KeyError:
            return "Errore: Il file Excel non contiene le colonne richieste (Name, Datatype, address)."
    connessione.commit()
    connessione.close()
    return "Variabili generate e salvate nel database."


def sincronizza_mappatura_da_excel(
    percorso_excel, params_db={}, nome_tabella="mappatura"
):
    """
    TODO !!! Per il momento non uso le 3 colonne numero db,byte,dit, in quanto li estraggo dalla stringa varaddres
    devo decidere se quei campi vanno poi tolti

    ---------------------------------------------------------------------------------------------------------------------------
    Sincronizza la tabella 'mappatura' del DB con i dati di un file Excel.
    Paraticamente alline il db con il file exel cancellando modificando e inserendo le variabili

    Args:
        percorso_excel (str or Path): Percorso del file .xlsx.
        params_db (dict): Dizionario con i parametri di connessione al DB MariaDB.
        nome_tabella (str): Nome della tabella di mappatura nel DB.

    Returns:
        bool: True se la sincronizzazione è completata (o non necessaria),
              False se si è verificato un errore significativo.
    """
    logger.info(
        f"Avvio sincronizzazione tabella '{nome_tabella}' da file '{percorso_excel}'"
    )

    # --- 1. Leggi Dati da Excel ---
    try:
        df_excel = pd.read_excel(percorso_excel, sheet_name=0)  # Legge il primo foglio
        # Assicurati che le colonne necessarie esistano
        colonne_necessarie = ["Name", "DataType", "Address","Length"]
        if not all(col in df_excel.columns for col in colonne_necessarie):
            logger.error(
                f"Il file Excel '{percorso_excel}' non contiene le colonne necessarie: {colonne_necessarie}"
            )
            return False,{}

        # Pulisci eventuali righe con nomeVar mancante e rimuovi duplicati basati su nomeVar
        df_excel = df_excel.dropna(subset=["Name"])
        df_excel = df_excel.drop_duplicates(subset=["Name"], keep="first")

        # Converti in dizionario per lookup veloce: {nomeVar: {'tipo': valore, 'adr': valore}}
        # Gestisci potenziali tipi non stringa convertendoli se necessario
        excel_data_map = {
            row["Name"]: {
                "tipo": str(row["DataType"]) if pd.notna(row["DataType"]) else None,
                # Converti 'adr' in int se possibile, altrimenti stringa o None
                "adr": str(row["Address"]) if pd.notna(row["Address"]) else None,
                "length": int(row["Length"]) if pd.notna(row["Length"]) else None,

            }
            for index, row in df_excel.iterrows()
        }
        excel_nomevars = set(excel_data_map.keys())
        logger.info(f"Letti {len(excel_nomevars)} record unici da Excel.")

    except FileNotFoundError:
        logger.error(f"File Excel non trovato: '{percorso_excel}'")
        return False ,{}
    except Exception as e:
        logger.exception(
            f"Errore durante la lettura del file Excel '{percorso_excel}': {e}"
        )
        return False ,{}
    finally:
        return True ,excel_data_map

    # # --- 2. Leggi Dati dal Database ---
    # db_data_map = {}
    # db_nomevars = set()
    # conn = None
    # try:
    #     conn = get_db_connection(params_db)
    #     cursor = conn.cursor()
    #     # Leggi TUTTI i record attuali dalla tabella (assumi colonne varname, type, adrvar)
    #     # Assicurati che nomeVar sia una chiave primaria o unica per efficienza
    #     cursor.execute(f"SELECT varname, type, adrvar FROM {nome_tabella}")
    #     dizvar={}
    #     for row in cursor.fetchall():
    #         db_data_map[row[0]] = {"tipo": row[1], "adr": row[2]}
    #     dizvar = db_data_map    
    #     db_nomevars = set(db_data_map.keys())
    #     logger.info(
    #         f"Letti {len(db_nomevars)} record dalla tabella DB '{nome_tabella}'."
    #     )

    # except mariadb.Error as err:
    #     logger.exception(
    #         f"Errore database durante la lettura dalla tabella '{nome_tabella}': {err}"
    #     )
    #     if conn:
    #         conn.close()
    #     return False,{}

    # --- 3. Confronta e Determina Azioni ---
    # try:
    #     vars_to_delete = list(db_nomevars - excel_nomevars)
    #     vars_to_insert = list(excel_nomevars - db_nomevars)
    #     vars_to_check = excel_nomevars.intersection(db_nomevars)
    #     # print(vars_to_check)

    #     updates_to_perform = []  # Lista di tuple: (nuovo_tipo, nuovo_adr, nomeVar)
    #     inserts_data = []  # Lista di tuple: (nomeVar, tipo, adr)

    #     # Identifica gli aggiornamenti necessari
    #     for nomeVar in vars_to_check:
    #         excel_row = excel_data_map[nomeVar]
    #         db_row = db_data_map[nomeVar]
    #         # Confronta: attenzione a potenziali None e tipi diversi (es. int vs str)
    #         if str(excel_row["tipo"]) != str(db_row["tipo"]) or str(
    #             excel_row["adr"]
    #         ) != str(db_row["adr"]):
    #             updates_to_perform.append(
    #                 (excel_row["tipo"], excel_row["adr"], nomeVar)
    #             )

    #     # Prepara i dati per gli inserimenti
    #     for nomeVar in vars_to_insert:
    #         excel_row = excel_data_map[nomeVar]
    #         inserts_data.append((nomeVar, excel_row["tipo"], excel_row["adr"]))

    #     logger.info(
    #         f"Confronto completato: Da eliminare={len(vars_to_delete)}, Da inserire={len(inserts_data)}, Da aggiornare={len(updates_to_perform)}"
    #     )

    #     # Se non ci sono modifiche, esco subito
    #     if not vars_to_delete and not inserts_data and not updates_to_perform:
    #         logger.info("Nessuna modifica necessaria. Database già sincronizzato.")
    #         return True,dizvar  # Ritorna True perché la sincronia è confermata

    #     # --- 4. Esegui Operazioni Bulk sul DB (in una transazione) ---

    #     cursor = conn.cursor()  # Ottieni un nuovo cursore se necessario
    #     # DELETE

    #     if vars_to_delete:
    #         placeholders = ", ".join(["%s"] * len(vars_to_delete))
    #         delete_sql = f"DELETE FROM {nome_tabella} WHERE varname IN ({placeholders})"
    #         cursor.execute(delete_sql, tuple(vars_to_delete))
    #         logger.info(f"Eliminati {cursor.rowcount} record dal DB.")

    #     # UPDATE
    #     if updates_to_perform:
    #         update_sql = (
    #             f"UPDATE {nome_tabella} SET type = %s, adrvar = %s WHERE varname = %s"
    #         )
    #         # executemany è molto efficiente per operazioni bulk
    #         cursor.executemany(update_sql, updates_to_perform)
    #         logger.info(
    #             f"Aggiornati {cursor.rowcount} record nel DB."
    #         )  # rowcount potrebbe non essere accurato per executemany su tutti i DB/connector

    #     # INSERT
    #     if inserts_data:
    #         insert_sql = f"INSERT INTO {nome_tabella} (varname, type, adrvar) VALUES (%s, %s, %s)"
    #         cursor.executemany(insert_sql, inserts_data)
    #         logger.info(
    #             f"Inseriti {cursor.rowcount} nuovi record nel DB."
    #         )  # rowcount potrebbe non essere accurato

    #     # --- 5. Commit Transazione ---
    #     conn.commit()
    #     logger.info("Transazione completata con successo. DB sincronizzato.")
    #     return True, dizvar
    
    # except mariadb.Error as err:
    #     logger.exception(
    #         f"Errore database durante l'applicazione delle modifiche: {err}"
    #     )
    #     try:
    #         conn.rollback()  # Annulla tutte le modifiche della transazione in caso di errore
    #         logger.warning("Rollback della transazione eseguito.")
    #     except mariadb.Error as r_err:
    #         logger.error(f"Errore durante il rollback: {r_err}")
    #     return False,{}
    # except Exception as e:
    #     logger.exception(f"Errore generico durante la sincronizzazione: {e}")
    #     try:
    #         if conn:
    #             conn.rollback()
    #     except:
    #         pass  # Ignora errori durante il rollback in caso di errore generico
    #     return False,{}
    # finally:
    #     if conn:
    #         conn.close()
    #         logger.debug("Connessione al database chiusa.")

