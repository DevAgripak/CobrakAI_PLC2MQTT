import os
import glob
import logging
import pandas as pd
from datetime import datetime

class JsonToParquetHelper:
    """
    Classe Helper per il buffering locale di dati JSON in formato Parquet.
    Gestisce la scrittura giornaliera e la lettura sequenziale (FIFO).
    """

    def __init__(self, buffer_dir_name="db_buffer"):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.buffer_dir = os.path.join(self.base_dir, buffer_dir_name)

        if not os.path.exists(self.buffer_dir):
            try:
                os.makedirs(self.buffer_dir)
                logging.info(f"Directory di buffer creata: {self.buffer_dir}")
            except OSError as e:
                logging.error(f"Errore nella creazione della directory {self.buffer_dir}: {e}")

    def _get_daily_filename(self):
        """Genera il nome del file per il giorno corrente."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.buffer_dir, f"buffer_{date_str}.parquet")

    def write_to_db(self, data: dict) -> bool:
        """
        Appende il dato al file Parquet giornaliero.
        Se il file esiste, lo legge, aggiunge la riga e lo sovrascrive.
        """
        try:
            if not data:
                return False

            df_new = pd.DataFrame([data])
            file_path = self._get_daily_filename()

            if os.path.exists(file_path):
                try:
                    # Leggi esistente e appendi
                    df_existing = pd.read_parquet(file_path, engine='pyarrow')
                    df_final = pd.concat([df_existing, df_new], ignore_index=True)
                except Exception as e:
                    logging.error(f"File corrotto {file_path}, creo nuovo backup per non perdere dati: {e}")
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_path = os.path.join(self.buffer_dir, f"recovery_{timestamp}.parquet")
                    df_final = df_new
            else:
                df_final = df_new

            df_final.to_parquet(file_path, engine='pyarrow', index=False)
            logging.info(f"Dato bufferizzato su file: {os.path.basename(file_path)}")
            return True

        except Exception as e:
            logging.error(f"Errore scrittura buffer Parquet: {e}")
            return False

    def get_all_buffered_files(self) -> list:
        """Restituisce lista file ordinati per nome (data)."""
        search_pattern = os.path.join(self.buffer_dir, "*.parquet")
        files = glob.glob(search_pattern)
        files.sort()
        return files

    def read_and_process_buffer(self, callback_function):
        """
        Legge i file, invia record per record.
        Se l'invio fallisce a metà, salva i record rimanenti e si ferma.
        Se tutto ok, cancella il file e passa al successivo.
        """
        files = self.get_all_buffered_files()
        if not files:
            return

        logging.info(f"Processo buffer: trovati {len(files)} file.")

        for file_path in files:
            try:
                df = pd.read_parquet(file_path, engine='pyarrow')
                records = df.to_dict(orient='records')
                
                records_to_keep = []
                file_fully_processed = True

                for i, record in enumerate(records):
                    # Esegui callback (invio MQTT)
                    if callback_function(record):
                        continue # Successo, passa al prossimo record
                    else:
                        # Fallimento
                        logging.warning(f"Invio fallito al record {i+1}/{len(records)} del file {os.path.basename(file_path)}. Interrompo.")
                        records_to_keep = records[i:]
                        file_fully_processed = False
                        break
                
                if file_fully_processed:
                    # Rimuovi file completato
                    try:
                        os.remove(file_path)
                        logging.info(f"File completato e rimosso: {os.path.basename(file_path)}")
                    except OSError as e:
                        logging.error(f"Errore rimozione file {file_path}: {e}")
                else:
                    # Sovrascrivi file con record rimanenti e STOP
                    if records_to_keep:
                        df_remaining = pd.DataFrame(records_to_keep)
                        df_remaining.to_parquet(file_path, engine='pyarrow', index=False)
                        logging.info(f"File aggiornato con {len(records_to_keep)} record rimanenti.")
                    
                    # Interrompiamo il ciclo sui file per mantenere ordine cronologico
                    break

            except Exception as e:
                logging.error(f"Errore processamento file {file_path}: {e}")
                break
