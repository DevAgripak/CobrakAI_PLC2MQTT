import argparse
import logging
import os
import re
from pathlib import Path
import sys

import yaml
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label, ListItem, ListView, RadioButton, RadioSet


class ConfigLoader:
    def __init__(self, config_file: str = "config.yaml"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(current_dir, "config", config_file)
        self._config = self._load_config()

    def _default_config(self) -> dict:
        return {
            "database": {"host": "", "port": 3306, "user": "", "password": "", "database": ""},
            "plc": {"ip": "", "rack": 0, "slot": 0, "timeupdate": 30.0},
            "mqtt": {"broker": "", "port": 1883, "topic": "", "status_topic": "", "username": "", "pwd": ""},
            "nomifile": {"fileNameDbPlc": "StrutturaDb.json", "fileNameVar": "HMITags.xlsx"},
            "reader": {"mode": "S7"},
            "opcua": {
                "server": {"ip": "", "port": 4840},
                "client": {
                    "url": "",
                    "anonymous": True,
                    "username": "",
                    "password": "",
                    "security_mode": "None",
                    "security_policy": "None",
                    "commessa": "",
                    "client_cert_path": "OpcUa/pki/client_cert.pem",
                    "client_key_path": "OpcUa/pki/client_key.pem",
                    "server_cert_path": "",
                    "auto_generate_cert": 1,
                },
            },
            "test": {"mode": "run"},
        }

    def _load_config(self) -> dict:
        config_path = Path(self.config_path)
        if not config_path.exists():
            default_config = self._default_config()
            config_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                config_path.write_text(yaml.safe_dump(default_config, sort_keys=False, allow_unicode=True), encoding="utf-8")
            except Exception:
                pass
            return default_config
        try:
            data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def save_config(self, config_data: dict) -> bool:
        try:
            config_path = Path(self.config_path)
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(yaml.safe_dump(config_data, sort_keys=False, allow_unicode=True), encoding="utf-8")
            self._config = config_data
            return True
        except Exception:
            return False

    def get_config(self) -> dict:
        return self._config

    def get_opcua_server_config(self) -> dict:
        return self._config.get("opcua", {}).get("server", {})

    def get_opcua_client_config(self) -> dict:
        return self._config.get("opcua", {}).get("client", {})


class ConfigScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Indietro")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Modifica Configurazione OPC UA", classes="title"),
            Label("IP Server:"),
            Input(placeholder="127.0.0.1", id="ip_input"),
            Label("Porta Server:"),
            Input(placeholder="4840", id="port_input"),
            Label("URL Client (opc.tcp://...):"),
            Input(placeholder="opc.tcp://...", id="url_input"),
            Label("Commessa/Cliente:"),
            Input(placeholder="MDI", id="commessa_input"),
            Label("Tipo Autenticazione:", id="security_title"),
            Horizontal(
                Vertical(
                    Label("Security Mode:"),
                    RadioSet(
                        RadioButton("None", id="mode_none"),
                        RadioButton("Sign", id="mode_sign"),
                        RadioButton("Sign & Encrypt", id="mode_sign_encrypt"),
                        id="mode_set",
                    ),
                    classes="security-col",
                ),
                Vertical(
                    Label("Security Policy:"),
                    RadioSet(
                        RadioButton("None", id="policy_none"),
                        RadioButton("Basic128RSA15", id="policy_b128"),
                        RadioButton("Basic256", id="policy_b256"),
                        RadioButton("Basic256SHA256", id="policy_b256sha256"),
                        id="policy_set",
                    ),
                    classes="security-col",
                ),
                id="security-container",
            ),
            Label("Username (opzionale):", id="user_label"),
            Input(placeholder="Username", id="user_input"),
            Label("Password (opzionale):", id="pass_label"),
            Input(placeholder="Password", password=True, id="pass_input"),
            Button("Salva Configurazione", variant="success", id="save_btn"),
            classes="config-container",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.config_loader = ConfigLoader()
        config = self.config_loader.get_config()
        server = config.get("opcua", {}).get("server", {})
        client = config.get("opcua", {}).get("client", {})

        self.query_one("#ip_input", Input).value = str(server.get("ip", ""))
        self.query_one("#port_input", Input).value = str(server.get("port", ""))
        self.query_one("#url_input", Input).value = str(client.get("url", ""))
        self.query_one("#commessa_input", Input).value = str(client.get("commessa", ""))

        self.query_one("#user_input", Input).value = str(client.get("username", ""))
        self.query_one("#pass_input", Input).value = str(client.get("password", ""))

        mode = client.get("security_mode", "None")
        if mode == "None":
            self.query_one("#mode_none", RadioButton).value = True
        elif mode == "Sign":
            self.query_one("#mode_sign", RadioButton).value = True
        elif mode == "Sign & Encrypt":
            self.query_one("#mode_sign_encrypt", RadioButton).value = True

        policy = client.get("security_policy", "None")
        if policy == "None":
            self.query_one("#policy_none", RadioButton).value = True
        elif policy == "Basic128RSA15":
            self.query_one("#policy_b128", RadioButton).value = True
        elif policy == "Basic256":
            self.query_one("#policy_b256", RadioButton).value = True
        elif policy == "Basic256SHA256":
            self.query_one("#policy_b256sha256", RadioButton).value = True

        self.update_auth_fields(mode == "None")

    def update_auth_fields(self, anonymous: bool) -> None:
        self.query_one("#user_input", Input).disabled = anonymous
        self.query_one("#pass_input", Input).disabled = anonymous
        self.query_one("#policy_set", RadioSet).disabled = anonymous

    @on(RadioSet.Changed, "#mode_set")
    def on_mode_changed(self, event: RadioSet.Changed) -> None:
        is_anonymous = event.pressed.id == "mode_none"
        if is_anonymous:
            self.query_one("#policy_none", RadioButton).value = True
        self.update_auth_fields(is_anonymous)

    @on(Input.Changed, "#ip_input")
    def on_ip_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url_input = self.query_one("#url_input", Input)
            port = self.query_one("#port_input", Input).value
            ip = event.value
            url_input.value = f"opc.tcp://{ip}:{port}/"

    @on(Input.Changed, "#port_input")
    def on_port_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url_input = self.query_one("#url_input", Input)
            ip = self.query_one("#ip_input", Input).value
            port = event.value
            url_input.value = f"opc.tcp://{ip}:{port}/"

    @on(Input.Changed, "#url_input")
    def on_url_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url = event.value
            match = re.match(r"opc\\.tcp://([^:/]+)(?::(\\d+))?", url)
            if match:
                new_ip = match.group(1)
                new_port = match.group(2)
                ip_input = self.query_one("#ip_input", Input)
                if ip_input.value != new_ip:
                    ip_input.value = new_ip
                if new_port:
                    port_input = self.query_one("#port_input", Input)
                    if port_input.value != new_port:
                        port_input.value = new_port

    @on(Button.Pressed, "#save_btn")
    def save_config(self):
        ip = self.query_one("#ip_input", Input).value
        port = self.query_one("#port_input", Input).value
        url = self.query_one("#url_input", Input).value
        commessa = self.query_one("#commessa_input", Input).value

        mode_set = self.query_one("#mode_set", RadioSet)
        security_mode = "None"
        if mode_set.pressed_button:
            security_mode = str(mode_set.pressed_button.label)

        anonymous = security_mode == "None"

        username = self.query_one("#user_input", Input).value
        password = self.query_one("#pass_input", Input).value

        security_policy = "None"
        if not anonymous:
            policy_set = self.query_one("#policy_set", RadioSet)
            if policy_set.pressed_button:
                security_policy = str(policy_set.pressed_button.label)

        config = self.config_loader.get_config()
        config.setdefault("opcua", {}).setdefault("server", {})
        config.setdefault("opcua", {}).setdefault("client", {})
        config["opcua"]["server"]["ip"] = ip
        try:
            config["opcua"]["server"]["port"] = int(port)
        except ValueError:
            pass

        config["opcua"]["client"]["url"] = url
        config["opcua"]["client"]["commessa"] = commessa
        config["opcua"]["client"]["anonymous"] = anonymous
        config["opcua"]["client"]["username"] = username
        config["opcua"]["client"]["password"] = password
        config["opcua"]["client"]["security_mode"] = security_mode
        config["opcua"]["client"]["security_policy"] = security_policy

        if self.config_loader.save_config(config):
            self.notify("Configurazione salvata!", severity="information")
        else:
            self.notify("Errore durante il salvataggio!", severity="error")


class GeneralConfigScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Indietro")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Modifica Configurazione App", id="general_title_box"),
            Label("Modalità Lettura", id="reader_mode_title"),
            Container(
                RadioSet(
                    RadioButton("S7", id="reader_mode_s7"),
                    RadioButton("OPCUA", id="reader_mode_opcua"),
                    id="reader_mode_set",
                ),
                id="reader_mode_container",
            ),
            Label("PLC / File", id="plc_title"),
            Container(
                Label("IP (S7):"),
                Input(placeholder="10.0.0.1", id="plc_ip"),
                Label("Rack:"),
                Input(placeholder="0", id="plc_rack"),
                Label("Slot:"),
                Input(placeholder="0", id="plc_slot"),
                Label("Timeupdate (sec):"),
                Input(placeholder="30.0", id="plc_timeupdate"),
                Label("fileNameDbPlc:"),
                Input(placeholder="StrutturaDb.json", id="file_dbplc"),
                Label("fileNameVar:"),
                Input(placeholder="HMITags.xlsx", id="file_var"),
                id="plc-container",
            ),
            Label("DATABASE", id="db_title"),
            Container(
                Label("Host:"),
                Input(placeholder="127.0.0.1", id="db_host"),
                Label("Port:"),
                Input(placeholder="3306", id="db_port"),
                Label("User:"),
                Input(placeholder="agripak", id="db_user"),
                Label("Password:"),
                Input(placeholder="Agr1pak", password=True, id="db_password"),
                Label("Database:"),
                Input(placeholder="cobrakai", id="db_database"),
                id="db-container",
            ),
            Label("MQTT", id="mqtt_title"),
            Container(
                Label("Broker:"),
                Input(placeholder="217.112.93.9", id="mqtt_broker"),
                Label("Port:"),
                Input(placeholder="1883", id="mqtt_port"),
                Label("Topic:"),
                Input(placeholder="PLC-DATA-OPCUA-TEST", id="mqtt_topic"),
                Label("Status Topic:"),
                Input(placeholder="...", id="mqtt_status_topic"),
                Label("Username:"),
                Input(placeholder="cobrakai", id="mqtt_user"),
                Label("Password:"),
                Input(placeholder="password", password=True, id="mqtt_pwd"),
                id="mqtt-container",
            ),
            Label("Configurazione OPC UA", id="opcua_title"),
            Container(
                Label("IP Server:"),
                Input(placeholder="127.0.0.1", id="ip_input"),
                Label("Porta Server:"),
                Input(placeholder="4840", id="port_input"),
                Label("URL Client (opc.tcp://...):"),
                Input(placeholder="opc.tcp://...", id="url_input"),
                Label("Commessa/Cliente:"),
                Input(placeholder="MDI", id="commessa_input"),
                Label("Tipo Autenticazione:", id="security_title"),
                Horizontal(
                    Vertical(
                        Label("Security Mode:"),
                        RadioSet(
                            RadioButton("None", id="mode_none"),
                            RadioButton("Sign", id="mode_sign"),
                            RadioButton("Sign & Encrypt", id="mode_sign_encrypt"),
                            id="mode_set",
                        ),
                        classes="security-col",
                    ),
                    Vertical(
                        Label("Security Policy:"),
                        RadioSet(
                            RadioButton("None", id="policy_none"),
                            RadioButton("Basic128RSA15", id="policy_b128"),
                            RadioButton("Basic256", id="policy_b256"),
                            RadioButton("Basic256SHA256", id="policy_b256sha256"),
                            id="policy_set",
                        ),
                        classes="security-col",
                    ),
                    id="security-container",
                ),
                Label("Username:", id="user_label"),
                Input(placeholder="Username", id="user_input"),
                Label("Password:", id="pass_label"),
                Input(placeholder="Password", password=True, id="pass_input"),
                id="opcua-container",
            ),
            Label("ESECUZIONE", id="runmode_title"),
            Container(
                RadioSet(
                    RadioButton("Debug (solo log)", id="runmode_debug"),
                    RadioButton("Test Run (Log + file test)", id="runmode_test"),
                    RadioButton("Run (Log + MQTT)", id="runmode_run"),
                    id="runmode_set",
                ),
                id="runmode_container",
            ),
            Button("Salva Configurazione", variant="success", id="save_general_btn"),
            classes="config-container",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.config_loader = ConfigLoader()
        cfg = self.config_loader.get_config()
        mode = str(cfg.get("reader", {}).get("mode", "S7")).strip().upper()
        self.query_one("#reader_mode_s7", RadioButton).value = mode == "S7"
        self.query_one("#reader_mode_opcua", RadioButton).value = mode == "OPCUA"

        db_cfg = cfg.get("database", {})
        self.query_one("#db_host", Input).value = str(db_cfg.get("host", ""))
        self.query_one("#db_port", Input).value = str(db_cfg.get("port", "3306"))
        self.query_one("#db_user", Input).value = str(db_cfg.get("user", ""))
        self.query_one("#db_password", Input).value = str(db_cfg.get("password", ""))
        self.query_one("#db_database", Input).value = str(db_cfg.get("database", ""))

        mqtt_cfg = cfg.get("mqtt", {})
        self.query_one("#mqtt_broker", Input).value = str(mqtt_cfg.get("broker", ""))
        self.query_one("#mqtt_port", Input).value = str(mqtt_cfg.get("port", "1883"))
        self.query_one("#mqtt_topic", Input).value = str(mqtt_cfg.get("topic", ""))
        self.query_one("#mqtt_status_topic", Input).value = str(mqtt_cfg.get("status_topic", ""))
        self.query_one("#mqtt_user", Input).value = str(mqtt_cfg.get("username", ""))
        self.query_one("#mqtt_pwd", Input).value = str(mqtt_cfg.get("pwd", ""))

        plc_cfg = cfg.get("plc", {})
        self.query_one("#plc_ip", Input).value = str(plc_cfg.get("ip", ""))
        self.query_one("#plc_rack", Input).value = str(plc_cfg.get("rack", "0"))
        self.query_one("#plc_slot", Input).value = str(plc_cfg.get("slot", "0"))
        self.query_one("#plc_timeupdate", Input).value = str(plc_cfg.get("timeupdate", "30.0"))

        nf = cfg.get("nomifile", {})
        file_dbplc = str(nf.get("fileNameDbPlc", "")).replace("\\", "/")
        if file_dbplc.lower().startswith("variabili/"):
            file_dbplc = file_dbplc.split("/", 1)[1]
        file_var = str(nf.get("fileNameVar", "")).replace("\\", "/")
        if file_var.lower().startswith("variabili/"):
            file_var = file_var.split("/", 1)[1]
        self.query_one("#file_dbplc", Input).value = file_dbplc
        self.query_one("#file_var", Input).value = file_var

        mode = str(cfg.get("test", {}).get("mode", "run")).strip().lower()
        self.query_one("#runmode_debug", RadioButton).value = mode == "debug"
        self.query_one("#runmode_test", RadioButton).value = mode in ("test", "test_run", "testrun")
        self.query_one("#runmode_run", RadioButton).value = mode not in ("debug", "test", "test_run", "testrun")

        server = cfg.get("opcua", {}).get("server", {})
        client = cfg.get("opcua", {}).get("client", {})
        self.query_one("#ip_input", Input).value = str(server.get("ip", ""))
        self.query_one("#port_input", Input).value = str(server.get("port", ""))
        self.query_one("#url_input", Input).value = str(client.get("url", ""))
        self.query_one("#commessa_input", Input).value = str(client.get("commessa", ""))
        self.query_one("#user_input", Input).value = str(client.get("username", ""))
        self.query_one("#pass_input", Input).value = str(client.get("password", ""))

        sec_mode = client.get("security_mode", "None")
        if sec_mode == "None":
            self.query_one("#mode_none", RadioButton).value = True
        elif sec_mode == "Sign":
            self.query_one("#mode_sign", RadioButton).value = True
        elif sec_mode == "Sign & Encrypt":
            self.query_one("#mode_sign_encrypt", RadioButton).value = True

        sec_policy = client.get("security_policy", "None")
        if sec_policy == "None":
            self.query_one("#policy_none", RadioButton).value = True
        elif sec_policy == "Basic128RSA15":
            self.query_one("#policy_b128", RadioButton).value = True
        elif sec_policy == "Basic256":
            self.query_one("#policy_b256", RadioButton).value = True
        elif sec_policy == "Basic256SHA256":
            self.query_one("#policy_b256sha256", RadioButton).value = True

        self.update_auth_fields(sec_mode == "None")

    def update_auth_fields(self, anonymous: bool) -> None:
        self.query_one("#user_input", Input).disabled = anonymous
        self.query_one("#pass_input", Input).disabled = anonymous
        self.query_one("#policy_set", RadioSet).disabled = anonymous

    @on(RadioSet.Changed, "#mode_set")
    def on_mode_changed(self, event: RadioSet.Changed) -> None:
        is_anonymous = event.pressed.id == "mode_none"
        if is_anonymous:
            self.query_one("#policy_none", RadioButton).value = True
        self.update_auth_fields(is_anonymous)

    @on(Input.Changed, "#ip_input")
    def on_ip_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url_input = self.query_one("#url_input", Input)
            port = self.query_one("#port_input", Input).value
            ip = event.value
            url_input.value = f"opc.tcp://{ip}:{port}/"

    @on(Input.Changed, "#port_input")
    def on_port_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url_input = self.query_one("#url_input", Input)
            ip = self.query_one("#ip_input", Input).value
            port = event.value
            url_input.value = f"opc.tcp://{ip}:{port}/"

    @on(Input.Changed, "#url_input")
    def on_url_changed(self, event: Input.Changed) -> None:
        if event.input.has_focus:
            url = event.value
            match = re.match(r"opc\\.tcp://([^:/]+)(?::(\\d+))?", url)
            if match:
                new_ip = match.group(1)
                new_port = match.group(2)
                ip_input = self.query_one("#ip_input", Input)
                if ip_input.value != new_ip:
                    ip_input.value = new_ip
                if new_port:
                    port_input = self.query_one("#port_input", Input)
                    if port_input.value != new_port:
                        port_input.value = new_port

    @on(Button.Pressed, "#save_general_btn")
    def save_general(self):
        cfg = self.config_loader.get_config()
        cfg.setdefault("reader", {})
        cfg.setdefault("database", {})
        cfg.setdefault("mqtt", {})
        cfg.setdefault("plc", {})
        cfg.setdefault("nomifile", {})
        cfg.setdefault("test", {})
        cfg.setdefault("opcua", {})
        cfg["opcua"].setdefault("server", {})
        cfg["opcua"].setdefault("client", {})

        cfg["reader"]["mode"] = "OPCUA" if self.query_one("#reader_mode_opcua", RadioButton).value else "S7"
        cfg["database"]["host"] = self.query_one("#db_host", Input).value.strip()
        try:
            cfg["database"]["port"] = int(self.query_one("#db_port", Input).value.strip())
        except Exception:
            cfg["database"]["port"] = self.query_one("#db_port", Input).value.strip()
        cfg["database"]["user"] = self.query_one("#db_user", Input).value.strip()
        cfg["database"]["password"] = self.query_one("#db_password", Input).value
        cfg["database"]["database"] = self.query_one("#db_database", Input).value.strip()
        cfg["mqtt"]["broker"] = self.query_one("#mqtt_broker", Input).value.strip()
        try:
            cfg["mqtt"]["port"] = int(self.query_one("#mqtt_port", Input).value.strip())
        except Exception:
            cfg["mqtt"]["port"] = self.query_one("#mqtt_port", Input).value.strip()
        cfg["mqtt"]["topic"] = self.query_one("#mqtt_topic", Input).value.strip()
        cfg["mqtt"]["status_topic"] = self.query_one("#mqtt_status_topic", Input).value.strip()
        cfg["mqtt"]["username"] = self.query_one("#mqtt_user", Input).value.strip()
        cfg["mqtt"]["pwd"] = self.query_one("#mqtt_pwd", Input).value

        cfg["plc"]["ip"] = self.query_one("#plc_ip", Input).value.strip()
        try:
            cfg["plc"]["rack"] = int(self.query_one("#plc_rack", Input).value.strip())
        except Exception:
            cfg["plc"]["rack"] = self.query_one("#plc_rack", Input).value.strip()
        try:
            cfg["plc"]["slot"] = int(self.query_one("#plc_slot", Input).value.strip())
        except Exception:
            cfg["plc"]["slot"] = self.query_one("#plc_slot", Input).value.strip()
        try:
            cfg["plc"]["timeupdate"] = float(self.query_one("#plc_timeupdate", Input).value.strip())
        except Exception:
            cfg["plc"]["timeupdate"] = self.query_one("#plc_timeupdate", Input).value.strip()

        file_dbplc = self.query_one("#file_dbplc", Input).value.strip().replace("\\", "/")
        if file_dbplc.lower().startswith("variabili/"):
            file_dbplc = file_dbplc.split("/", 1)[1]
        file_var = self.query_one("#file_var", Input).value.strip().replace("\\", "/")
        if file_var.lower().startswith("variabili/"):
            file_var = file_var.split("/", 1)[1]
        cfg["nomifile"]["fileNameDbPlc"] = file_dbplc
        cfg["nomifile"]["fileNameVar"] = file_var
        if self.query_one("#runmode_debug", RadioButton).value:
            cfg["test"]["mode"] = "debug"
        elif self.query_one("#runmode_test", RadioButton).value:
            cfg["test"]["mode"] = "test"
        else:
            cfg["test"]["mode"] = "run"

        ip = self.query_one("#ip_input", Input).value.strip()
        port_raw = self.query_one("#port_input", Input).value.strip()
        url = self.query_one("#url_input", Input).value.strip()
        commessa = self.query_one("#commessa_input", Input).value.strip()

        mode_set = self.query_one("#mode_set", RadioSet)
        security_mode = "None"
        if mode_set.pressed_button:
            security_mode = str(mode_set.pressed_button.label)
        anonymous = security_mode == "None"

        security_policy = "None"
        if not anonymous:
            policy_set = self.query_one("#policy_set", RadioSet)
            if policy_set.pressed_button:
                security_policy = str(policy_set.pressed_button.label)

        cfg["opcua"]["server"]["ip"] = ip
        try:
            cfg["opcua"]["server"]["port"] = int(port_raw)
        except Exception:
            cfg["opcua"]["server"]["port"] = port_raw

        cfg["opcua"]["client"]["url"] = url
        cfg["opcua"]["client"]["commessa"] = commessa
        cfg["opcua"]["client"]["anonymous"] = anonymous
        cfg["opcua"]["client"]["username"] = self.query_one("#user_input", Input).value.strip()
        cfg["opcua"]["client"]["password"] = self.query_one("#pass_input", Input).value
        cfg["opcua"]["client"]["security_mode"] = security_mode
        cfg["opcua"]["client"]["security_policy"] = security_policy

        if self.config_loader.save_config(cfg):
            self.notify("Configurazione salvata!", severity="information")
        else:
            self.notify("Errore durante il salvataggio!", severity="error")


class MainMenu(Screen):
    BINDINGS = [("c", "open_config", "Configura App")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Container(
            Label("CobrakAI PLC2MQTT", id="main-title"),
            ListView(
                ListItem(Label("🧩  Configura App"), id="opt-config-app"),
                ListItem(Label("🚪 Quit"), id="opt-exit"),
            ),
            id="menu-container",
        )

    @on(ListView.Selected)
    def on_list_view_selected(self, event: ListView.Selected):
        if event.item.id == "opt-config-app":
            self.app.push_screen(GeneralConfigScreen())
        elif event.item.id == "opt-exit":
            self.app.exit()

    def action_open_config(self) -> None:
        self.app.push_screen(GeneralConfigScreen())


class ConfigEditorApp(App):
    CSS = """
    #main-title {
        text-align: center;
        text-style: bold;
        color: yellow;
        margin: 2;
    }
    #menu-container {
        align: center middle;
        border: solid yellow;
        padding: 1;
    }
    ListView {
        width: 50%;
        height: auto;
        border: none;
    }
    .title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }
    .config-container {
        layout: vertical;
        align: center middle;
        width: 60%;
        height: auto;
        border: solid magenta;
        padding: 2;
    }
    #general_title_box {
        width: 100%;
        height: 1;
        padding: 0 1;
        margin-bottom: 1;
        text-style: bold;
        color: yellow;
        content-align: center middle;
    }
    #mqtt_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #db_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #db-container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    #runmode_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #runmode_container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    #reader_mode_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #reader_mode_container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    #mqtt-container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    #opcua_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #opcua-container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    #opcua-container Label {
        color: white;
    }
    #opcua_title, #security_title {
        color: yellow;
    }
    #plc_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #plc-container {
        border: solid yellow;
        layout: vertical;
        width: 100%;
        height: auto;
        padding: 1;
        margin-bottom: 1;
    }
    Input {
        margin-bottom: 1;
    }
    #security_title {
        color: yellow;
        text-style: bold;
        margin-top: 1;
    }
    #security-container {
        height: auto;
        border: solid blue;
        padding: 1;
        margin-bottom: 1;
    }
    .security-col {
        width: 50%;
        height: auto;
    }
    RadioSet {
        border: none;
        height: auto;
    }
    RadioButton {
        height: 1;
    }
    """

    BINDINGS = [("q", "quit", "Esci")]

    def on_mount(self) -> None:
        self.push_screen(MainMenu())


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="config_editor.py")
    parser.parse_args(argv)
    logging.getLogger("textual").setLevel(logging.WARNING)
    app = ConfigEditorApp()
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
