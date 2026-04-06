from pathlib import Path

import pandas as pd
from opcua import Client, ua

import logging
import re
import time

from Util.DataFormat import FormatValue


class JOpcUaClient:
    def __init__(
        self,
        endpoint: str,
        username: str | None = None,
        password: str | None = None,
        security_mode: str = "None",
        security_policy: str = "None",
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        server_cert_path: str | None = None,
        auto_generate_cert: bool = True,
        timeout_s: int = 60,
    ):
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self._security_mode = security_mode
        self._security_policy = security_policy
        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._server_cert_path = server_cert_path
        self._auto_generate_cert = auto_generate_cert
        self._timeout_s = timeout_s
        self._base_dir = Path(__file__).resolve().parents[1]
        self._client = None
        self._connected = False
        self._last_browse_stats = {"nodes_visited": 0, "variables_found": 0, "duration_s": 0.0}
        self._logger = logging.getLogger(__name__)

    def _resolve_path(self, value: str | None) -> Path | None:
        if not value:
            return None
        p = Path(value)
        if p.is_absolute():
            return p
        return (self._base_dir / p).resolve()

    @staticmethod
    def _normalize_security_mode(value: str) -> str:
        v = (value or "").strip().upper()
        if v in ("", "NONE", "NO", "DISABLED"):
            return "None"
        if v in ("SIGN",):
            return "Sign"
        if v in ("SIGNANDENCRYPT", "SIGN_ENCRYPT", "SIGN&ENCRYPT", "SIGN & ENCRYPT", "SIGNANDENCRYPTION"):
            return "SignAndEncrypt"
        return "None"

    @staticmethod
    def _normalize_security_policy(value: str) -> str:
        v = (value or "").strip().upper()
        if v in ("", "NONE", "NO", "DISABLED"):
            return "None"
        if v in ("BASIC128RSA15", "BASIC128RSA15 "):
            return "Basic128Rsa15"
        if v == "BASIC256":
            return "Basic256"
        if v in ("BASIC256SHA256", "BASIC256SHA256 "):
            return "Basic256Sha256"
        return "None"

    def _ensure_client_certificate(self, cert_der_path: Path, key_pem_path: Path):
        if cert_der_path.exists() and key_pem_path.exists():
            return
        cert_der_path.parent.mkdir(parents=True, exist_ok=True)
        key_pem_path.parent.mkdir(parents=True, exist_ok=True)

        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        from datetime import datetime, timedelta, timezone

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
        key_pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        key_pem_path.write_bytes(key_pem)

        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "IT"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "CobrakAi"),
                x509.NameAttribute(NameOID.COMMON_NAME, "CobrakAi OPCUA Client"),
            ]
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=365))
            .add_extension(
                x509.SubjectAlternativeName([x509.UniformResourceIdentifier("urn:CobrakAi:OPCUA:Client")]),
                critical=False,
            )
            .sign(key, hashes.SHA256(), default_backend())
        )
        cert_der_path.write_bytes(cert.public_bytes(serialization.Encoding.DER))

    def _apply_security(self):
        if self._client is None:
            return
        policy = self._normalize_security_policy(self._security_policy)
        mode = self._normalize_security_mode(self._security_mode)
        if policy == "None" or mode == "None":
            return

        mode_enum = ua.MessageSecurityMode.SignAndEncrypt
        if mode == "Sign":
            mode_enum = ua.MessageSecurityMode.Sign

        cert_path = self._resolve_path(self._client_cert_path)
        key_path = self._resolve_path(self._client_key_path)
        server_cert_path = self._resolve_path(self._server_cert_path)

        if cert_path is None or key_path is None:
            raise ValueError("Percorsi certificato/chiave OPC-UA mancanti")

        if self._auto_generate_cert and (not cert_path.exists() or not key_path.exists()):
            self._ensure_client_certificate(cert_path, key_path)

        if not cert_path.exists() or not key_path.exists():
            raise FileNotFoundError("Certificato/chiave OPC-UA non trovati")

        if server_cert_path and server_cert_path.exists():
            self._client.set_security(policy, str(cert_path), str(key_path), str(server_cert_path), mode=mode_enum)
        else:
            self._client.set_security(policy, str(cert_path), str(key_path), mode=mode_enum)

    def connect(self) -> bool:
        try:
            if not self._connected:
                self._client = Client(self._endpoint, timeout=self._timeout_s)
                self._logger.info(
                    "OPCUA: connect start endpoint=%s security_policy=%s security_mode=%s user=%s cert=%s key=%s server_cert=%s",
                    self._endpoint,
                    self._normalize_security_policy(self._security_policy),
                    self._normalize_security_mode(self._security_mode),
                    "yes" if self._username else "no",
                    str(self._resolve_path(self._client_cert_path)) if self._client_cert_path else "",
                    str(self._resolve_path(self._client_key_path)) if self._client_key_path else "",
                    str(self._resolve_path(self._server_cert_path)) if self._server_cert_path else "",
                )
                self._apply_security()
                if self._username:
                    self._client.set_user(self._username)
                if self._password:
                    self._client.set_password(self._password)
                self._client.connect()
                self._connected = True
                self._logger.info("OPCUA: connect ok endpoint=%s", self._endpoint)
            return True
        except Exception as e:
            self._logger.error("OPCUA: connect failed endpoint=%s err=%s", self._endpoint, e, exc_info=True)
            self._connected = False
            return False

    def is_connected(self) -> bool:
        return self._connected

    def disconnect(self):
        try:
            if self._client:
                self._client.disconnect()
        finally:
            self._connected = False
            self._logger.info("OPCUA: disconnect endpoint=%s", self._endpoint)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        if not name:
            return ""
        for ch in (".", "[", "]", '"', ":", "(", ")", " "):
            name = name.replace(ch, "_")
        while "__" in name:
            name = name.replace("__", "_")
        return name.strip("_")

    @staticmethod
    def _variant_type_to_datatype(vt) -> str:
        if vt is None:
            return "UNKNOWN"
        if vt == ua.VariantType.Boolean:
            return "BOOL"
        if vt in (ua.VariantType.SByte, ua.VariantType.Byte):
            return "BYTE"
        if vt in (ua.VariantType.Int16, ua.VariantType.UInt16):
            return "INT"
        if vt in (ua.VariantType.Int32, ua.VariantType.UInt32):
            return "DINT"
        if vt in (ua.VariantType.Int64, ua.VariantType.UInt64):
            return "LINT"
        if vt == ua.VariantType.Float:
            return "REAL"
        if vt == ua.VariantType.Double:
            return "LREAL"
        if vt == ua.VariantType.String:
            return "STRING"
        return "UNKNOWN"

    @staticmethod
    def _convert_value(value, tipo: str, nrdec: int = 3) -> str:
        if tipo is None:
            return str(value)
        t = tipo.upper()
        if t == "BOOL":
            try:
                return FormatValue.bool_to_str(bool(value))
            except Exception:
                return "0"
        if t in ("BYTE", "WORD", "INT", "DINT"):
            try:
                return FormatValue.int_to_str(int(value), 0)
            except Exception:
                return str(value)
        if t in ("REAL", "LREAL", "FLOAT", "DOUBLE"):
            try:
                return FormatValue.float_to_str(float(value), nrdec)
            except Exception:
                return str(value)
        if t == "STRING":
            try:
                return str(value)
            except Exception:
                return ""
        return str(value)

    def _find_node_in_subtree(self, start_node, target_browse_name: str, max_depth: int = 20, max_nodes: int = 20000):
        queue = [(start_node, 0)]
        visited = set()
        target_upper = (target_browse_name or "").strip().upper()
        while queue and len(visited) < max_nodes:
            node, depth = queue.pop(0)
            try:
                nodeid = node.nodeid.to_string()
            except Exception:
                continue
            if nodeid in visited:
                continue
            visited.add(nodeid)
            try:
                browse_name = node.get_browse_name()
                name = getattr(browse_name, "Name", "") or ""
            except Exception:
                name = ""
            if name.strip().upper() == target_upper:
                return node
            if depth >= max_depth:
                continue
            try:
                children = node.get_children()
            except Exception:
                children = []
            for child in children:
                queue.append((child, depth + 1))
        return None

    def _get_datablocks_global_node(self):
        try:
            root = self._client.get_root_node()
            objects = root.get_child(["0:Objects"])
        except Exception:
            return None
        node = self._find_node_in_subtree(objects, "DataBlocksGlobal", max_depth=20, max_nodes=20000)
        return node

    def _get_datablocks_global_hmi_node(self):
        datablocks = self._get_datablocks_global_node()
        if datablocks is None:
            return None
        node = self._find_node_in_subtree(datablocks, "Hmi", max_depth=10, max_nodes=20000)
        return node

    def _browse_variable_nodes(self, max_depth: int = 25, max_nodes: int = 50000, scope: str | None = None) -> list[dict]:
        if not self._connected:
            if not self.connect():
                return []

        t0 = time.perf_counter()
        start_node = None
        start_path = []
        scope_used = scope
        max_depth_used = max_depth
        max_nodes_used = max_nodes
        log_enabled = False
        if scope:
            scope_clean = scope.strip()
            if scope_clean.lower().startswith("ns="):
                try:
                    start_node = self._client.get_node(scope_clean)
                except Exception:
                    start_node = None
                if start_node is None:
                    self._logger.error("OPCUA: scope node not found scope=%s endpoint=%s", scope_clean, self._endpoint)
                    return []
                start_path = [self._name_from_nodeid_string(scope_clean)]
                if "datablocksglobal" in scope_clean.lower():
                    log_enabled = True
                    self._logger.info("OPCUA: scope node found scope=%s endpoint=%s", scope_clean, self._endpoint)
                    max_depth_used = max(max_depth_used, 100)
                    max_nodes_used = max(max_nodes_used, 250000)
            elif scope_clean.upper() == "DATABLOCKSGLOBAL":
                start_node = self._get_datablocks_global_node()
                if start_node is None:
                    self._logger.error("OPCUA: scope node not found scope=%s endpoint=%s", scope_clean, self._endpoint)
                    return []
                start_path = ["DataBlocksGlobal"]
                log_enabled = True
                self._logger.info("OPCUA: scope node found scope=%s endpoint=%s", scope_clean, self._endpoint)
                max_depth_used = max(max_depth_used, 100)
                max_nodes_used = max(max_nodes_used, 250000)
        if start_node is None:
            start_node = self._client.get_root_node()
        queue = [(start_node, start_path, 0)]
        visited = set()
        found = []
        skipped_string = 0

        while queue and len(visited) < max_nodes_used:
            node, path_parts, depth = queue.pop(0)
            try:
                nodeid = node.nodeid.to_string()
            except Exception:
                continue
            if nodeid in visited:
                continue
            visited.add(nodeid)

            try:
                browse_name = node.get_browse_name()
                current_name = getattr(browse_name, "Name", "") or str(browse_name)
            except Exception:
                current_name = ""

            try:
                node_class = node.get_node_class()
            except Exception:
                node_class = None

            next_path_parts = path_parts
            if current_name:
                next_path_parts = path_parts + [current_name]

            if node_class == ua.NodeClass.Variable:
                try:
                    vt = node.get_data_type_as_variant_type()
                except Exception:
                    vt = None
                datatype = self._variant_type_to_datatype(vt)
                if vt == ua.VariantType.String:
                    skipped_string += 1
                    continue
                full_name = self._sanitize_name("_".join(next_path_parts) or current_name or nodeid)
                found.append(
                    {
                        "Name": full_name,
                        "DataType": datatype,
                        "Address": nodeid,
                        "Length": 0,
                    }
                )

            if depth >= max_depth_used:
                continue

            try:
                children = node.get_children()
            except Exception:
                children = []
            for child in children:
                queue.append((child, next_path_parts, depth + 1))

        duration_s = time.perf_counter() - t0
        limit_reached = bool(queue)
        unique = {}
        for row in found:
            key = row["Name"]
            if key not in unique:
                unique[key] = row
                continue
            i = 2
            while f"{key}_{i}" in unique:
                i += 1
            row2 = dict(row)
            row2["Name"] = f"{key}_{i}"
            unique[row2["Name"]] = row2

        self._last_browse_stats = {
            "nodes_visited": len(visited),
            "variables_found": len(unique),
            "duration_s": duration_s,
        }
        if log_enabled:
            self._logger.info(
                "OPCUA: browse done endpoint=%s scope=%s nodes_visited=%s variables_found=%s skipped_string=%s max_depth=%s max_nodes=%s limit_reached=%s duration_s=%.3f",
                self._endpoint,
                scope_used or "",
                len(visited),
                len(unique),
                skipped_string,
                max_depth_used,
                max_nodes_used,
                "yes" if limit_reached else "no",
                duration_s,
            )
        return list(unique.values())

    @staticmethod
    def _load_nodeid_list(file_path: Path) -> list[str]:
        if not file_path.exists():
            return []
        items = []
        for raw in file_path.read_text(encoding="utf-8").splitlines():
            s = raw.strip()
            if not s:
                continue
            if s.startswith("#") or s.startswith(";"):
                continue
            items.append(s)
        seen = set()
        unique = []
        for x in items:
            if x in seen:
                continue
            seen.add(x)
            unique.append(x)
        return unique

    @classmethod
    def _name_from_nodeid_string(cls, nodeid_str: str) -> str:
        s = (nodeid_str or "").strip()
        m = re.search(r";s=(.+)$", s, flags=re.IGNORECASE)
        if m:
            symbolic = m.group(1).strip()
            symbolic = symbolic.replace('"', "")
            symbolic = symbolic.replace(".", "_")
            symbolic = symbolic.replace("[", "_").replace("]", "_")
            symbolic = symbolic.replace(" ", "_")
            while "__" in symbolic:
                symbolic = symbolic.replace("__", "_")
            symbolic = symbolic.strip("_")
            if symbolic:
                return cls._sanitize_name(symbolic)
        return cls._sanitize_name(s.replace(":", "_").replace(";", "_").replace("=", "_"))

    def _rows_from_nodeid_list(self, nodeid_list_path: Path) -> list[dict]:
        nodeids = self._load_nodeid_list(nodeid_list_path)
        if not nodeids:
            return []
        ok = 0
        fail = 0
        rows = []
        t0 = time.perf_counter()
        for nodeid in nodeids:
            try:
                node = self._client.get_node(nodeid)
                try:
                    node_class = node.get_node_class()
                except Exception:
                    node_class = None
                if node_class is not None and node_class != ua.NodeClass.Variable:
                    fail += 1
                    continue
                try:
                    vt = node.get_data_type_as_variant_type()
                except Exception:
                    vt = None
                datatype = self._variant_type_to_datatype(vt)
                name = self._name_from_nodeid_string(nodeid)
                rows.append(
                    {
                        "Name": name,
                        "DataType": datatype,
                        "Address": nodeid,
                        "Length": 0,
                    }
                )
                ok += 1
            except Exception:
                fail += 1
        duration_s = time.perf_counter() - t0
        self._last_browse_stats = {"nodes_visited": 0, "variables_found": ok, "duration_s": duration_s}
        self._logger.info(
            "OPCUA: list read done file=%s requested=%s ok=%s fail=%s duration_s=%.3f",
            str(nodeid_list_path),
            len(nodeids),
            ok,
            fail,
            duration_s,
        )
        return rows

    def sync_opcua_variables_file(self, variabili_dir: Path, prefix: str = "opcua_") -> tuple[Path, dict]:
        variabili_dir.mkdir(parents=True, exist_ok=True)
        out_path = variabili_dir / f"{prefix}HMITags.xlsx"

        # list_path = variabili_dir / f"{prefix}variable_list.txt"
        # rows = self._rows_from_nodeid_list(list_path)
        # if rows:
        #     self._logger.info("OPCUA: using variable list file=%s", str(list_path))
        # else:
        #     rows = self._browse_variable_nodes(scope="DataBlocksGlobal")
        rows = self._browse_variable_nodes(scope="ns=3;s=DataBlocksGlobal", max_depth=100, max_nodes=250000)
        if not rows:
            self._logger.error("OPCUA: sync variables file failed (no rows) out_path=%s", str(out_path))
            return out_path, {}

        df_new = pd.DataFrame(rows, columns=["Name", "DataType", "Address", "Length"])
        df_new = df_new.dropna(subset=["Name", "Address"])

        new_by_addr = {
            str(r["Address"]): (str(r["Name"]), str(r["DataType"]))
            for r in df_new[["Name", "DataType", "Address"]].to_dict(orient="records")
        }
        self._logger.info("OPCUA: variables read total=%s out_path=%s", len(new_by_addr), str(out_path))

        must_write = not out_path.exists()
        if out_path.exists():
            self._logger.info("OPCUA: variables file exists out_path=%s", str(out_path))
            try:
                df_old = pd.read_excel(out_path, sheet_name=0)
                df_old = df_old[["Name", "DataType", "Address", "Length"]].copy()
                df_old = df_old.dropna(subset=["Name", "Address"])

                old_by_addr = {
                    str(r["Address"]): (str(r["Name"]), str(r["DataType"]))
                    for r in df_old[["Name", "DataType", "Address"]].to_dict(orient="records")
                }
                old_addrs = set(old_by_addr.keys())
                new_addrs = set(new_by_addr.keys())
                added = sorted(new_addrs - old_addrs)
                removed = sorted(old_addrs - new_addrs)
                common = new_addrs.intersection(old_addrs)
                changed = sorted(a for a in common if old_by_addr.get(a) != new_by_addr.get(a))

                must_write = bool(added or removed or changed)
                self._logger.info(
                    "OPCUA: variables diff old=%s new=%s added=%s removed=%s changed=%s",
                    len(old_by_addr),
                    len(new_by_addr),
                    len(added),
                    len(removed),
                    len(changed),
                )

                if added:
                    added_names = [new_by_addr[a][0] for a in added[:20] if a in new_by_addr]
                    self._logger.info("OPCUA: variables added sample_count=%s names=%s", len(added_names), ";".join(added_names))
                if removed:
                    removed_names = [old_by_addr[a][0] for a in removed[:20] if a in old_by_addr]
                    self._logger.info("OPCUA: variables removed sample_count=%s names=%s", len(removed_names), ";".join(removed_names))
                if changed:
                    changed_names = [new_by_addr[a][0] for a in changed[:20] if a in new_by_addr]
                    self._logger.info("OPCUA: variables changed sample_count=%s names=%s", len(changed_names), ";".join(changed_names))
            except Exception as e:
                self._logger.error("OPCUA: variables file read/compare failed out_path=%s err=%s", str(out_path), e, exc_info=True)
                must_write = True

        if must_write:
            df_new.to_excel(out_path, index=False)
            self._logger.info("OPCUA: variables file written out_path=%s rows=%s", str(out_path), len(df_new))
        else:
            self._logger.info("OPCUA: variables file unchanged out_path=%s rows=%s", str(out_path), len(df_new))

        var_map = {
            str(row["Name"]): {
                "tipo": str(row["DataType"]) if pd.notna(row["DataType"]) else None,
                "adr": str(row["Address"]) if pd.notna(row["Address"]) else None,
                "length": int(row["Length"]) if pd.notna(row["Length"]) else 0,
            }
            for row in df_new.to_dict(orient="records")
        }
        self._logger.info("OPCUA: variables map ready variables=%s", len(var_map))
        return out_path, var_map

    def read_values(self, var_map: dict) -> dict:
        result = {}
        if not self._connected:
            if not self.connect():
                return result
        for name, meta in var_map.items():
            try:
                nodeid = meta.get("adr")
                tipo = meta.get("tipo")
                node = self._client.get_node(nodeid)
                val = node.get_value()
                sval = self._convert_value(val, tipo, 3)
                result[name] = sval
            except Exception:
                result[name] = ""
        return result

