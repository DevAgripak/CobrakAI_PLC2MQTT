import asyncio
import logging
import re
import time
from pathlib import Path

import pandas as pd
from asyncua import Client, ua
from asyncua.client.ua_client import UASocketProtocol
from asyncua.common.utils import wait_for
from asyncua.crypto.security_policies import SecurityPolicyBasic128Rsa15, SecurityPolicyBasic256, SecurityPolicyBasic256Sha256
from asyncua.ua.uaerrors._base import UaError

from Util.DataFormat import FormatValue

logger = logging.getLogger(__name__)


async def _patched_open_secure_channel(self, params):
    request = ua.OpenSecureChannelRequest()
    request.Parameters = params
    if self._open_secure_channel_exchange is not None:
        raise UaError(
            "Two Open Secure Channel requests can not happen too close to each other. "
            "The response must be processed and returned before the next request can be sent."
        )
    self._open_secure_channel_exchange = params
    timeout_to_use = self.timeout if self.timeout and self.timeout > 10 else 60
    await wait_for(self._send_request(request, timeout=timeout_to_use, message_type=ua.MessageType.SecureOpen), timeout_to_use)
    _return = self._open_secure_channel_exchange.Parameters
    self._open_secure_channel_exchange = None
    return _return


UASocketProtocol.open_secure_channel = _patched_open_secure_channel


class RobustClient(Client):
    def server_policy(self, token_type: ua.UserTokenType) -> ua.UserTokenPolicy:
        current_policy_uri = self.security_policy.URI
        for policy in self._policy_ids:
            if policy.TokenType == token_type and policy.SecurityPolicyUri == current_policy_uri:
                logger.info("OPCUA: selected UserTokenPolicy id=%s uri=%s", policy.PolicyId, policy.SecurityPolicyUri)
                return policy
        preferred_policies = [
            "http://opcfoundation.org/UA/SecurityPolicy#Basic256",
            "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256",
            "http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss",
        ]
        for pref_uri in preferred_policies:
            for policy in self._policy_ids:
                if policy.TokenType == token_type and policy.SecurityPolicyUri == pref_uri:
                    logger.info("OPCUA: selected preferred UserTokenPolicy id=%s uri=%s", policy.PolicyId, policy.SecurityPolicyUri)
                    return policy
        logger.warning("OPCUA: no optimized UserTokenPolicy found, fallback")
        return super().server_policy(token_type)

    async def connect(self) -> None:
        await self.connect_socket()
        try:
            await self.send_hello()
            await self.open_secure_channel()
            try:
                await self.create_session()
                try:
                    if self._username:
                        await self.activate_session(username=self._username, password=self._password, certificate=None)
                    else:
                        await self.activate_session(username=self._username, password=self._password, certificate=self.user_certificate)
                except Exception:
                    await self.close_session()
                    raise
            except Exception:
                await self.close_secure_channel()
                raise
        except Exception:
            self.disconnect_socket()
            raise


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
        self._client: RobustClient | None = None
        self._connected = False
        self._last_browse_stats = {"nodes_visited": 0, "variables_found": 0, "duration_s": 0.0}
        self._loop = asyncio.new_event_loop()

    def _run(self, coro):
        try:
            asyncio.set_event_loop(self._loop)
            return self._loop.run_until_complete(coro)
        finally:
            asyncio.set_event_loop(None)

    def _resolve_path(self, value: str | None) -> Path | None:
        if not value:
            return None
        p = Path(value)
        if p.is_absolute():
            return p
        return (self._base_dir / p).resolve()

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
    def _normalize_security_mode(value: str) -> str:
        v = (value or "").strip().upper()
        if v in ("", "NONE", "NO", "DISABLED"):
            return "None"
        if v in ("SIGN",):
            return "Sign"
        if v in ("SIGNANDENCRYPT", "SIGN_ENCRYPT", "SIGN&ENCRYPT", "SIGN & ENCRYPT", "SIGNANDENCRYPTION", "SIGNANDENCRYPT", "SIGNANDENCRYPTION"):
            return "SignAndEncrypt"
        if v in ("SIGN & ENCRYPT",):
            return "SignAndEncrypt"
        return "None"

    @staticmethod
    def _normalize_security_policy(value: str) -> str:
        v = (value or "").strip().upper()
        if v in ("", "NONE", "NO", "DISABLED"):
            return "None"
        if v in ("BASIC128RSA15", "BASIC128RSA15 "):
            return "Basic128RSA15"
        if v == "BASIC256":
            return "Basic256"
        if v in ("BASIC256SHA256", "BASIC256SHA256 ", "BASIC256SHA256"):
            return "Basic256SHA256"
        return "None"

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
        if t in ("BYTE", "WORD", "INT", "DINT", "LINT"):
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

    def _ensure_client_certificate_pem(self, cert_path: Path, key_path: Path):
        cert_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        if cert_path.exists() and key_path.exists():
            return

        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from datetime import datetime, timedelta, timezone

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
        key_path.write_bytes(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

        subject = issuer = x509.Name(
            [
                x509.NameAttribute(x509.NameOID.COUNTRY_NAME, "IT"),
                x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, "Italy"),
                x509.NameAttribute(x509.NameOID.LOCALITY_NAME, "Locality"),
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, "CobrakAi"),
                x509.NameAttribute(x509.NameOID.COMMON_NAME, "CobrakAi OPCUA Client"),
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
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    async def _discover_endpoint_url(self, policy: str, mode: ua.MessageSecurityMode) -> str | None:
        if policy == "None":
            return None
        discovery = Client(url=self._endpoint)
        discovery.application_uri = "urn:CobrakAi:OPCUA:Client"
        try:
            endpoints = await discovery.connect_and_get_server_endpoints()
        except Exception:
            try:
                async with discovery:
                    endpoints = await discovery.get_endpoints()
            except Exception:
                return None

        policy_token = None
        if policy == "Basic256SHA256":
            policy_token = "Basic256Sha256"
        elif policy == "Basic256":
            policy_token = "Basic256"
        elif policy == "Basic128RSA15":
            policy_token = "Basic128Rsa15"

        selected = None
        for ep in endpoints:
            try:
                ep_policy = getattr(ep, "SecurityPolicyUri", "")
                ep_mode = getattr(ep, "SecurityMode", None)
                ep_url = getattr(ep, "EndpointUrl", None)
            except Exception:
                continue
            if not ep_url:
                continue
            if policy_token and policy_token in ep_policy:
                if ep_mode == mode:
                    selected = ep_url
                    break
                if selected is None:
                    selected = ep_url
        return selected

    async def _connect_async(self) -> bool:
        policy = self._normalize_security_policy(self._security_policy)
        mode_str = self._normalize_security_mode(self._security_mode)
        mode = ua.MessageSecurityMode.None_
        if mode_str == "Sign":
            mode = ua.MessageSecurityMode.Sign
        elif mode_str == "SignAndEncrypt":
            mode = ua.MessageSecurityMode.SignAndEncrypt

        endpoint_url = self._endpoint
        selected = await self._discover_endpoint_url(policy, mode)
        if selected:
            endpoint_url = selected

        self._client = RobustClient(url=endpoint_url, watchdog_intervall=3600, timeout=float(self._timeout_s))
        self._client.application_uri = "urn:CobrakAi:OPCUA:Client"
        self._client.product_uri = "urn:CobrakAi:OPCUA:Client"
        self._client.name = "CobrakAi OPCUA Client"
        self._client.secure_channel_timeout = 60000
        self._client.session_timeout = 60000
        if hasattr(self._client, "uaclient"):
            self._client.uaclient._timeout = float(self._timeout_s)
            self._client.uaclient.secure_channel_timeout = 60000
            self._client.uaclient.session_timeout = 60000

        if self._username:
            self._client.set_user(self._username)
        if self._password:
            self._client.set_password(self._password)

        if policy != "None" and mode != ua.MessageSecurityMode.None_:
            policy_map = {
                "Basic128RSA15": SecurityPolicyBasic128Rsa15,
                "Basic256": SecurityPolicyBasic256,
                "Basic256SHA256": SecurityPolicyBasic256Sha256,
            }
            policy_class = policy_map.get(policy)
            if policy_class:
                cert_path_raw = self._resolve_path(self._client_cert_path) if self._client_cert_path else (self._base_dir / "OpcUa" / "pki" / "client_cert.pem")
                key_path_raw = self._resolve_path(self._client_key_path) if self._client_key_path else (self._base_dir / "OpcUa" / "pki" / "client_key.pem")
                server_cert_path = self._resolve_path(self._server_cert_path) if self._server_cert_path else None

                cert_path = cert_path_raw if cert_path_raw.suffix.lower() == ".pem" else cert_path_raw.with_suffix(".pem")
                key_path = key_path_raw if key_path_raw.suffix.lower() == ".pem" else key_path_raw.with_suffix(".pem")

                if self._auto_generate_cert:
                    self._ensure_client_certificate_pem(cert_path, key_path)

                self._client.set_security(
                    policy=policy_class,
                    certificate=str(cert_path),
                    private_key=str(key_path),
                    server_certificate=str(server_cert_path) if server_cert_path and server_cert_path.exists() else None,
                    mode=mode,
                )

        logger.info(
            "OPCUA: connect start endpoint=%s policy=%s mode=%s user=%s",
            endpoint_url,
            policy,
            mode_str,
            "yes" if self._username else "no",
        )
        try:
            await self._client.connect()
            self._endpoint = endpoint_url
            self._connected = True
            logger.info("OPCUA: connect ok endpoint=%s", endpoint_url)
            return True
        except Exception as e:
            logger.error("OPCUA: connect failed endpoint=%s err=%s", endpoint_url, e, exc_info=True)
            self._connected = False
            return False

    async def _disconnect_async(self):
        if self._client is None:
            return
        try:
            await self._client.disconnect()
        finally:
            self._connected = False
            logger.info("OPCUA: disconnect endpoint=%s", self._endpoint)

    def connect(self) -> bool:
        if self._connected:
            return True
        return bool(self._run(self._connect_async()))

    def is_connected(self) -> bool:
        return self._connected

    def disconnect(self):
        try:
            self._run(self._disconnect_async())
        finally:
            try:
                self._loop.stop()
            except Exception:
                pass
            try:
                self._loop.close()
            except Exception:
                pass

    async def _browse_variable_nodes_async(self, scope: str, max_depth: int, max_nodes: int) -> list[dict]:
        if not self._connected or self._client is None:
            return []

        t0 = time.perf_counter()
        start_node = self._client.get_node(scope)
        start_path = [self._name_from_nodeid_string(scope)]
        queue = [(start_node, start_path, 0)]
        visited = set()
        found = []
        skipped_string = 0

        while queue and len(visited) < max_nodes:
            node, path_parts, depth = queue.pop(0)
            try:
                nodeid_str = node.nodeid.to_string()
            except Exception:
                nodeid_str = str(getattr(node, "nodeid", ""))
            if not nodeid_str:
                continue
            if nodeid_str in visited:
                continue
            visited.add(nodeid_str)

            try:
                browse_name = await node.read_browse_name()
                current_name = getattr(browse_name, "Name", "") or ""
            except Exception:
                current_name = ""

            next_path_parts = path_parts + [current_name] if current_name else path_parts

            try:
                node_class = await node.read_node_class()
            except Exception:
                node_class = None

            if node_class == ua.NodeClass.Variable:
                vt = None
                is_string = False
                try:
                    read_dt = getattr(node, "read_data_type_as_variant_type", None)
                    if read_dt:
                        vt = await read_dt()
                except Exception:
                    vt = None
                if vt == ua.VariantType.String:
                    is_string = True
                if not is_string:
                    try:
                        val = await node.read_value()
                        if isinstance(val, str):
                            is_string = True
                    except Exception:
                        pass
                if is_string:
                    skipped_string += 1
                else:
                    datatype = self._variant_type_to_datatype(vt)
                    full_name = self._sanitize_name("_".join(next_path_parts) or current_name or nodeid_str)
                    found.append({"Name": full_name, "DataType": datatype, "Address": nodeid_str, "Length": 0})

            if depth >= max_depth:
                continue
            try:
                children = await node.get_children()
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

        self._last_browse_stats = {"nodes_visited": len(visited), "variables_found": len(unique), "duration_s": duration_s}
        logger.info(
            "OPCUA: browse done endpoint=%s scope=%s nodes_visited=%s variables_found=%s skipped_string=%s max_depth=%s max_nodes=%s limit_reached=%s duration_s=%.3f",
            self._endpoint,
            scope,
            len(visited),
            len(unique),
            skipped_string,
            max_depth,
            max_nodes,
            "yes" if limit_reached else "no",
            duration_s,
        )
        return list(unique.values())

    def sync_opcua_variables_file(self, variabili_dir: Path, prefix: str = "opcua_") -> tuple[Path, dict]:
        variabili_dir.mkdir(parents=True, exist_ok=True)
        out_path = variabili_dir / f"{prefix}HMITags.xlsx"

        rows = self._run(self._browse_variable_nodes_async(scope="ns=3;s=DataBlocksGlobal", max_depth=100, max_nodes=250000))
        if not rows:
            logger.error("OPCUA: sync variables file failed (no rows) out_path=%s", str(out_path))
            return out_path, {}

        df_new = pd.DataFrame(rows, columns=["Name", "DataType", "Address", "Length"])
        df_new = df_new.dropna(subset=["Name", "Address"])

        new_by_addr = {
            str(r["Address"]): (str(r["Name"]), str(r["DataType"]))
            for r in df_new[["Name", "DataType", "Address"]].to_dict(orient="records")
        }
        logger.info("OPCUA: variables read total=%s out_path=%s", len(new_by_addr), str(out_path))

        must_write = not out_path.exists()
        if out_path.exists():
            logger.info("OPCUA: variables file exists out_path=%s", str(out_path))
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
                logger.info(
                    "OPCUA: variables diff old=%s new=%s added=%s removed=%s changed=%s",
                    len(old_by_addr),
                    len(new_by_addr),
                    len(added),
                    len(removed),
                    len(changed),
                )

                if added:
                    added_names = [new_by_addr[a][0] for a in added[:20] if a in new_by_addr]
                    logger.info("OPCUA: variables added sample_count=%s names=%s", len(added_names), ";".join(added_names))
                if removed:
                    removed_names = [old_by_addr[a][0] for a in removed[:20] if a in old_by_addr]
                    logger.info("OPCUA: variables removed sample_count=%s names=%s", len(removed_names), ";".join(removed_names))
                if changed:
                    changed_names = [new_by_addr[a][0] for a in changed[:20] if a in new_by_addr]
                    logger.info("OPCUA: variables changed sample_count=%s names=%s", len(changed_names), ";".join(changed_names))
            except Exception as e:
                logger.error("OPCUA: variables file read/compare failed out_path=%s err=%s", str(out_path), e, exc_info=True)
                must_write = True

        if must_write:
            df_new.to_excel(out_path, index=False)
            logger.info("OPCUA: variables file written out_path=%s rows=%s", str(out_path), len(df_new))
        else:
            logger.info("OPCUA: variables file unchanged out_path=%s rows=%s", str(out_path), len(df_new))

        var_map = {
            str(row["Name"]): {
                "tipo": str(row["DataType"]) if pd.notna(row["DataType"]) else None,
                "adr": str(row["Address"]) if pd.notna(row["Address"]) else None,
                "length": int(row["Length"]) if pd.notna(row["Length"]) else 0,
            }
            for row in df_new.to_dict(orient="records")
        }
        logger.info("OPCUA: variables map ready variables=%s", len(var_map))
        return out_path, var_map

    async def _read_values_async(self, var_map: dict) -> dict:
        result = {}
        if not self._connected or self._client is None:
            return result
        items = [(name, meta) for name, meta in var_map.items() if meta and meta.get("adr")]
        batch_size = 200
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            nodes = [self._client.get_node(meta["adr"]) for _, meta in batch]
            try:
                values = await self._client.read_values(nodes)
                for (name, meta), val in zip(batch, values):
                    tipo = meta.get("tipo")
                    result[name] = self._convert_value(val, tipo, 3)
            except Exception:
                for name, meta in batch:
                    try:
                        node = self._client.get_node(meta["adr"])
                        val = await node.read_value()
                        result[name] = self._convert_value(val, meta.get("tipo"), 3)
                    except Exception:
                        result[name] = ""
        return result

    def read_values(self, var_map: dict) -> dict:
        if not self._connected:
            if not self.connect():
                return {}
        return self._run(self._read_values_async(var_map))

