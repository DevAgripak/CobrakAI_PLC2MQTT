import logging.handlers
from pathlib import Path

# ---------------------------------------------------------------------------------------------------------------------------------------
#                                               GESTIONE PARTE DI LOG CON CONFIGURAZIONE
# ---------------------------------------------------------------------------------------------------------------------------------------

# 1. Determina il percorso della directory che CONTIENE questo script (Run.py)
#    Questo è un punto di riferimento più affidabile di os.getcwd()
script_dir = Path(__file__).resolve().parent

# 2. Definisci il percorso della directory dei log RELATIVO alla directory dello script
log_dir = script_dir / "log"  # Usa l'operatore / per unire percorsi con pathlib
db_buffer = script_dir / "db_buffer"  # Usa l'operatore / per unire percorsi con pathlib
# 3. Crea la directory dei log se non esiste già
try:
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # parents=True: crea anche le directory genitore se necessario (utile per log/subdir/...)
    # exist_ok=True: non solleva un errore se la directory esiste già
except OSError as e:
    # Gestisci il caso in cui non si possa creare la directory (es. permessi)
    print(f"ATTENZIONE: Impossibile creare la directory di log '{log_dir}': {e}")
    # Potresti decidere di uscire o di loggare nel CWD come fallback
    log_dir = Path.cwd()  # Fallback alla directory corrente in caso di errore
try:
    db_buffer.mkdir(parents=True, exist_ok=True)
except OSError as e:
    # Gestisci il caso in cui non si possa creare la directory (es. permessi)
    print(f"ATTENZIONE: Impossibile creare la directory di db_buffer '{db_buffer}': {e}")
    # Potresti decidere di uscire o di loggare nel CWD come fallback
    db_buffer = Path.cwd()  # Fallback alla directory corrente in caso di errore



# 4. Definisci il percorso completo del file di log DENTRO la directory log
nome_file_log_errori = "CobrakAi.log"
log_file_path = (
        log_dir / nome_file_log_errori
)  # Unisci la directory log con il nome file

base_name = log_file_path.stem
log_ext = log_file_path.suffix
max_backup_files = 3
try:
    oldest = log_dir / f"{base_name}_{max_backup_files}{log_ext}"
    if oldest.exists():
        oldest.unlink()
    for i in range(max_backup_files - 1, 0, -1):
        src = log_dir / f"{base_name}_{i}{log_ext}"
        dst = log_dir / f"{base_name}_{i + 1}{log_ext}"
        if src.exists():
            src.replace(dst)
    if log_file_path.exists():
        first_backup = log_dir / f"{base_name}_1{log_ext}"
        log_file_path.replace(first_backup)
except Exception as e:
    print(f"ATTENZIONE: rotazione log fallita: {e}")

# 5. Ottieni il logger RADICE (root logger)
logger_principale = logging.getLogger()
logger_principale.setLevel(logging.DEBUG)  # Cattura tutto, gli handler filtrano

# --- Verifica se gli handler sono già stati aggiunti (per evitare duplicati) ---
# Questo è utile se chiami la funzione di setup più volte accidentalmente
if any(
        isinstance(h, logging.FileHandler) and h.baseFilename == str(log_file_path)
        for h in logger_principale.handlers
):
    print("Configurazione logging già presente per questo file, salto.")
else:
    # 6. Definisci un Formatter comune
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # 7. Crea l'Handler per il file di ERRORE usando il percorso completo calcolato
    try:
        file_handler_errori = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
        file_handler_errori.setLevel(logging.INFO)
        file_handler_errori.setFormatter(formatter)
        logger_principale.addHandler(file_handler_errori)
    except Exception as e:
        print(
            f"ATTENZIONE: Impossibile configurare il file handler per gli errori in '{log_file_path}': {e}"
        )
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Fallita configurazione file handler errori: {e}", exc_info=True)

    # 8. (Opzionale) Crea l'Handler per la Console
    # La configurazione del console handler non cambia
    console_handler = logging.StreamHandler()
    # Evita di aggiungere handler console duplicati
    if not any(
            isinstance(h, logging.StreamHandler) for h in logger_principale.handlers
    ):
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter("%(name)s - %(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)
        logger_principale.addHandler(console_handler)

    logging.info(f"Configurazione del logging completata. File errori: {log_file_path}")

logging.getLogger("opcua").setLevel(logging.WARNING)

# --- FINE CONFIGURAZIONE LOGGING ---


# --- INIZIO IMPORT MODULI PROGRAMMA ---
try:
    import pandas as pd
    import configparser
    # import paho.mqtt.client as mqtt
    from paho.mqtt import client as mqtt
    from Siemens_S7.ClientSiemens import Js7Client
    from Util.Util import *
    # modifica Joe del 26-02-26
    from parquetdb.JsonToParquetHelper import JsonToParquetHelper
    import json
    import time
    try:
        from OpcUa.ClientOpcUa import JOpcUaClient
    except Exception:
        JOpcUaClient = None
except ImportError as err:
    logging.critical(
        f"Errore critico: impossibile importare i moduli necessari: {err}",
        exc_info=True,
    )
    print(err)
    exit(1)  # Esce se i moduli fondamentali non si trovano

# 1. Crea un'istanza del parser
config = configparser.ConfigParser()

CONFIG_DIR = script_dir / "config/config.ini"

if __name__ == "__main__":
    # ---------------------------------------------------------------------------------------------------------------------------------------
    #                                               SINCRONIZZAZIONE VARIABILI CON DB
    # ---------------------------------------------------------------------------------------------------------------------------------------

    # 1. Crea un'istanza del parser
    config = configparser.ConfigParser()
    # Verifico la presenza del file ini con al suo interno i parametri

    try:
        files_letti = config.read(
            CONFIG_DIR, encoding="utf-8"
        )  # encoding è opzionale ma consigliato
        if not files_letti:
            print(f"Attenzione: File INI '{CONFIG_DIR}' non trovato o vuoto.")
            # Puoi decidere se uscire o continuare con valori di default
            logging.error(
                "file ini con parametri mancante o danneggiato il programma si chiude"
            )
            exit(1)
    except configparser.Error as e:
        logging.error(
            f"Errore durante la lettura del file INI il programma si chiude: {e}"
        )
        exit(1)

    modelettura = "S7"
    try:
        if "Reader" in config and "MODE" in config["Reader"]:
            modelettura = str(config["Reader"]["MODE"]).upper()
    except Exception:
        modelettura = "S7"
    logging.info("START mode=%s config=%s", modelettura, str(CONFIG_DIR))

    if modelettura == "OPCUA":
        logging.info("OPCUA: start")
        try:
            endpoint = config["OpcUa"]["ENDPOINT"]
            username = config["OpcUa"].get("USERNAME", "").strip()
            password = config["OpcUa"].get("PWD", "").strip()
            security_mode = config["OpcUa"].get("SECURITY_MODE", "None").strip()
            security_policy = config["OpcUa"].get("SECURITY_POLICY", "None").strip()
            client_cert_path = config["OpcUa"].get("CLIENT_CERT_PATH", "").strip()
            client_key_path = config["OpcUa"].get("CLIENT_KEY_PATH", "").strip()
            server_cert_path = config["OpcUa"].get("SERVER_CERT_PATH", "").strip()
            auto_generate_cert_raw = config["OpcUa"].get("AUTO_GENERATE_CERT", "1").strip()
        except KeyError as er:
            logging.error(f"Errore nei parametri OPC-UA: {er}")
            exit(1)

        if JOpcUaClient is None:
            logging.error("Libreria OPC-UA non disponibile")
            exit(1)

        auto_generate_cert = auto_generate_cert_raw not in ("0", "FALSE", "NO", "OFF")
        logging.info(
            "OPCUA: config endpoint=%s security_policy=%s security_mode=%s user=%s auto_cert=%s",
            endpoint,
            security_policy,
            security_mode,
            "yes" if username else "no",
            "yes" if auto_generate_cert else "no",
        )
        driverOpcUa = JOpcUaClient(
            endpoint,
            username if username else None,
            password if password else None,
            security_mode=security_mode,
            security_policy=security_policy,
            client_cert_path=client_cert_path if client_cert_path else None,
            client_key_path=client_key_path if client_key_path else None,
            server_cert_path=server_cert_path if server_cert_path else None,
            auto_generate_cert=auto_generate_cert,
            timeout_s=60,
        )
        try:
            if not driverOpcUa.connect():
                logging.error("OPCUA: connect failed endpoint=%s", endpoint)
                exit(1)
            logging.info("OPCUA: connect ok endpoint=%s security_policy=%s security_mode=%s", endpoint, security_policy, security_mode)
            variabili_dir = script_dir / "Variabili"
            logging.info("OPCUA: sync variables dir=%s", str(variabili_dir))
            file_out, varriabili = driverOpcUa.sync_opcua_variables_file(variabili_dir=variabili_dir, prefix="opcua_")
            logging.info("OPCUA: nodes_visited=%s variables_written=%s", driverOpcUa._last_browse_stats.get("nodes_visited"), len(varriabili))
            if not varriabili:
                logging.error("OPCUA: no variables found")
                exit(1)
            logging.info("OPCUA: done file=%s", str(file_out))
        finally:
            try:
                driverOpcUa.disconnect()
            except Exception:
                logging.error("OPCUA: disconnect failed", exc_info=True)
        exit(0)

    try:
        parametri_db = {
            "user": config["Database"]["user"],
            "password": config["Database"]["password"],
            "host": config["Database"]["host"],
            "port": config["Database"]["port"],
            "database": config["Database"]["database"],
        }
        mqtt_params = {
            "broker": config["Mqtt"]["BROKER"],
            "port": config["Mqtt"]["PORT"],
            "topic": config["Mqtt"]["TOPIC"],
            "username": config["Mqtt"]["USERNAME"],
            "pwd": config["Mqtt"]["PWD"]}
    except KeyError as er:
        logging.error(
            f"Errore nei parametri chiave mancante nei parametri Db/Mqtt il programma si chiude: {er}"
        )
        exit(1)


    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connessione con il server MQTT avvenuta")
        else:
            logging.error(f"Errore di connessione con il server MQTT : {rc} ")


    variabili_dir = script_dir / config["NomiFile"]["fileNameVar"]
    sinkOk, varriabili = sincronizza_mappatura_da_excel(percorso_excel=variabili_dir, params_db=parametri_db)
    logging.info("S7: variabili loaded file=%s variables=%s", str(variabili_dir), len(varriabili))

    # ---------------------------------------------------------------------------------------------------------------------------------------
    #                                               INIZIALIZZAZIONE CLASSE DRIVER STEP 7
    # ---------------------------------------------------------------------------------------------------------------------------------------
    if sinkOk:
        data_var = {"client_id": "CI2306-P01",
                    "timestamp": 20250712150210123,
                    "data": {}}
        # crea il client MQTT

        lista_struttura_db = []
        dati_caricati = None
        variabili_dir = script_dir / config["NomiFile"]["fileNameDbPlc"]
        try:
            with open(variabili_dir, "r", encoding="utf-8") as file_json:
                dati_caricati = json.load(file_json)
                if dati_caricati:
                    lista_struttura_db = dati_caricati.get("strutturadb", [])
            logging.info("S7: struttura db loaded file=%s db_count=%s", str(variabili_dir), len(lista_struttura_db))
        except FileNotFoundError:
            logging.error(
                f"Non trovo il file struttura delle DB il programma si chiude!! "
            )
            exit(1)
        except json.JSONDecodeError as e:
            logging.error(
                f"Errore: Il file '{variabili_dir}' non contiene JSON valido."
            )
            logging.error(f"  Dettagli errore: {e}")
            exit(1)
        except Exception as e:
            logging.error(f"Errore imprevisto!! {e}")
        try:
            datiplc = {
                "ip": config["Plc"]["ip"],
                "rack": int(config["Plc"]["rack"]),
                "slot": int(config["Plc"]["slot"]),
                "timeupdate": float(config["Plc"]["timeupdate"])
            }
            tempoAggiornamento = float(config["Plc"]["timeupdate"])
        except KeyError as er:
            logging.error(
                f"Errore nei parametri chiave mancante nei parametri Db il programma si chiude: {er}"
            )
            exit(1)
        driverS7 = Js7Client(datiplc, lista_struttura_db)
        # client = mqtt.Client(client_id="CI2306-PLC01")  # Creare un nuovo
        # usa API versione 2 di mqtt

        
    # --------------------------------------------------------------------------------------------------------------------------------------
    #                                               ISTANZIO LA COMUNICAZIONE MQTT
    # --------------------------------------------------------------------------------------------------------------------------------------  
     
        timestampum = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="CI2306-PLC01_TEST")
        client.username_pw_set(username=mqtt_params["username"], password=mqtt_params["pwd"])
        # client.subscribe(mqtt_params["topic"])
        # in caso di caduda dovrebbe mettere il valore non connesso
        client.will_set("Agripak/plc_log-TEST", f"[{timestampum}] INFO: I'm going offline {timestampum} record inviati 0", qos=2, retain=False)
        connected = False
       
        try:
            client.connect_async(host=mqtt_params["broker"], port=int(mqtt_params["port"]))
            connected = True
            logging.info("MQTT: connect_async host=%s port=%s topic=%s", mqtt_params["broker"], mqtt_params["port"], mqtt_params["topic"])
        except (ConnectionRefusedError, OSError) as e:
            logging.error(f"Errore di connessione MQTT {e}")
            connected = False

        client.loop_start()    
        
      


        def add_variable_to_dict(name_variabile, valore_variabile):
            rigajson = {name_variabile: str(valore_variabile)}
            data_var["data"].update(rigajson)

        #logging.info(f"La lista variabile è la seguente {varriabili}")
        # Inizializzazione Helper per Parquet
        helper = JsonToParquetHelper()
        buffer_enabled = True
        try:
            if "Test" in config and "BUFFER_ENABLED" in config["Test"]:
                buffer_enabled = str(config["Test"]["BUFFER_ENABLED"]).strip().upper() not in ("0", "FALSE", "NO", "OFF")
        except Exception:
            buffer_enabled = True
        incremento: int = 0
        
    # ---------------------------------------------------------------------------------------------------------------------------------------
    #                                              LOOP DI COMUNICAZIONE E QUERY SU DB
    # ---------------------------------------------------------------------------------------------------------------------------------------       
        mqtt_ok =False         
        # Variabile per gestire il tentativo di riconnessione iniziale senza bloccare
        last_connection_attempt = 0
        reconnect_interval = 10 # secondi tra tentativi di connessione

        while True:
            timestamp = creaTimeStamp()
            timestamphuman = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            valori_letti = None
            driverS7.s7connect()
            dati = driverS7.read_aree_plc()
            if dati:
                valori_letti = {}
                for var in varriabili:
                    value = driverS7.get_valorerx(dati, varriabili[var])
                    valori_letti[var] = value
            if valori_letti:
                for k, v in valori_letti.items():
                    add_variable_to_dict(k, v)
                data_var["timestamp"] = timestamp
                if client.is_connected():
                    if buffer_enabled:
                        buffer_stats = {"count": 0}
                        def mqtt_publish_callback(buffered_data):
                            try:
                                info = client.publish(topic=mqtt_params["topic"], payload=json.dumps(buffered_data), qos=2, retain=False)
                                info.wait_for_publish(timeout=5.0) #TODO Tempo di attesa per la conferma di pubblicazione
                                if info.is_published():
                                    logging.info("Dato bufferizzato inviato con successo MQTT.")
                                    buffer_stats["count"] += 1
                                    return True
                                else:
                                    logging.warning("Timeout invio dato bufferizzato MQTT.")
                                    return False
                            except Exception as e:
                                logging.error(f"Errore callback invio buffer: {e}")
                                return False
                        helper.read_and_process_buffer(mqtt_publish_callback)
                        if buffer_stats["count"] > 0:
                             client.publish("Agripak/plc_log-TEST", # TODO: Togliere la parte TEST
                                       f"[{timestamphuman}] INFO: System online and buffer sent to MQTT broker {timestamphuman} record bufferizzati inviati {buffer_stats['count']}",
                                       qos=2, retain=False)
                    incremento = incremento + 1
                    client.publish(topic=mqtt_params["topic"], payload=json.dumps(data_var), qos=2, retain=False)
                    client.publish("Agripak/plc_log-TEST", #TODO Togliere la parte TEST
                                   f"[{timestamphuman}] INFO: System online and data sent to MQTT broker {timestamphuman} record inviati {incremento}",
                                   qos=2, retain=False)

                    logging.info("Dati pubblicati correttamente")
                else:
                    if buffer_enabled:
                        logging.error("MQTT non connesso - Salvataggio dati su buffer locale Parquet")
                        helper.write_to_db(data_var)
                    else:
                        logging.error("MQTT non connesso - Buffer disabilitato (fase test), dato scartato")

            else:
                if client.is_connected():
                    client.publish("Agripak/plc_log-TEST",
                                   f"[{timestamphuman}] INFO: System online and not data read to plc  sent to MQTT broker {timestamphuman} record inviati {incremento}",
                                   qos=2, retain=False)

            time.sleep(datiplc["timeupdate"])
