import logging
import sys

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Dict, List, Optional, TypeVar
from urllib.parse import urlparse

import grpc

from patroni.dcs.etcd import StaleEtcdNodeGuard
from patroni.exceptions import DCSError

# Generated from etcd v3.5.17 proto definitions.
_STUBS_PATH = str(Path(__file__).resolve().parent / 'etcd3_grpc_stubs')
if _STUBS_PATH not in sys.path:
    sys.path.insert(0, _STUBS_PATH)

from etcd_proto.api.etcdserverpb import rpc_pb2, rpc_pb2_grpc  # noqa: E402
from google.protobuf.message import Message  # noqa: E402

logger = logging.getLogger(__name__)

RespT = TypeVar("RespT")


class Etcd3GrpcError(DCSError):
    pass


class Etcd3GrpcClient(StaleEtcdNodeGuard):
    """Low-level etcd3 gRPC client with mTLS authentication."""

    def __init__(self, config: Dict[str, Any]) -> None:
        StaleEtcdNodeGuard.__init__(self)
        self._endpoints = self._parse_endpoints(config)
        self._current_endpoint_idx = 0
        self._channel: Optional[grpc.Channel] = None
        self._use_tls = bool(config.get("cacert") or config.get("cert"))
        self._credentials = self._build_credentials(config) if self._use_tls else None
        self._ssl_target_name: Optional[str] = config.get("ssl_target_name")
        self._retry_timeout: float = config.get("retry_timeout", 10.0)
        self._kv_stub: Any = None
        self._lease_stub: Any = None
        self._watch_stub: Any = None
        self._cluster_stub: Any = None

    @staticmethod
    def _parse_endpoints(config: Dict[str, Any]) -> List[str]:
        default_port = config.get("port", 2379)
        if "hosts" in config:
            hosts = config["hosts"]
            if isinstance(hosts, str):
                hosts = hosts.split(",")
            endpoints = []
            for h in hosts:
                h = h.strip()
                if not h:
                    continue
                parsed = urlparse("//{}".format(h))
                host = parsed.hostname or h
                port = parsed.port or default_port
                if ":" in host:
                    endpoints.append(f"[{host}]:{port}")
                else:
                    endpoints.append(f"{host}:{port}")
            if endpoints:
                return endpoints
        host = config.get("host", "127.0.0.1")
        return [f"{host}:{default_port}"]

    @staticmethod
    def _build_credentials(config: Dict[str, Any]) -> grpc.ChannelCredentials:
        ca_cert = Path(config["cacert"]).read_bytes() if config.get("cacert") else None
        client_key = Path(config["key"]).read_bytes() if config.get("key") else None
        client_cert = Path(config["cert"]).read_bytes() if config.get("cert") else None
        return grpc.ssl_channel_credentials(
            root_certificates=ca_cert,
            private_key=client_key,
            certificate_chain=client_cert,
        )

    def _create_channel(self) -> grpc.Channel:
        endpoint = self._endpoints[self._current_endpoint_idx]
        logger.info("Connecting to etcd at %s via gRPC", endpoint)
        options: List[tuple[str, Any]] = [
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        if self._use_tls:
            if self._ssl_target_name:
                options.append(("grpc.ssl_target_name_override", self._ssl_target_name))
            return grpc.secure_channel(endpoint, self._credentials, options=options)
        return grpc.insecure_channel(endpoint, options=options)

    def connect(self) -> None:
        self._channel = self._create_channel()
        self._kv_stub = rpc_pb2_grpc.KVStub(self._channel)
        self._lease_stub = rpc_pb2_grpc.LeaseStub(self._channel)
        self._cluster_stub = rpc_pb2_grpc.ClusterStub(self._channel)

    def close(self) -> None:
        if self._channel:
            self._channel.close()
            self._channel = None

    def _rotate_endpoint(self) -> None:
        self._current_endpoint_idx = (self._current_endpoint_idx + 1) % len(self._endpoints)
        self.close()
        self.connect()

    def _call(
        self,
        stub_method: Callable[..., RespT],
        request: Message,
        timeout: Optional[float] = None,
    ) -> RespT:
        try:
            return stub_method(request, timeout=timeout or self._default_timeout)
        except grpc.RpcError as e:
            code = e.code()
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                self._rotate_endpoint()
            raise Etcd3GrpcError(f"gRPC error: {code.name} {e.details()}") from e

    # -- KV operations --

    def put(self, key: str, value: str, lease: int = 0) -> rpc_pb2.PutResponse:
        request = rpc_pb2.PutRequest(key=key.encode(), value=value.encode(), lease=lease)
        return self._call(self._kv_stub.Put, request)

    def get(self, key: str) -> rpc_pb2.RangeResponse:
        request = rpc_pb2.RangeRequest(key=key.encode())
        return self._call(self._kv_stub.Range, request)

    def get_prefix(self, prefix: str, timeout: Optional[float] = None) -> rpc_pb2.RangeResponse:
        range_end = _prefix_range_end(prefix)
        request = rpc_pb2.RangeRequest(key=prefix.encode(), range_end=range_end)
        return self._call(self._kv_stub.Range, request, timeout=timeout)

    def delete(self, key: str) -> rpc_pb2.DeleteRangeResponse:
        request = rpc_pb2.DeleteRangeRequest(key=key.encode())
        return self._call(self._kv_stub.DeleteRange, request)

    def delete_prefix(self, prefix: str) -> rpc_pb2.DeleteRangeResponse:
        range_end = _prefix_range_end(prefix)
        request = rpc_pb2.DeleteRangeRequest(key=prefix.encode(), range_end=range_end)
        return self._call(self._kv_stub.DeleteRange, request)

    def txn(self, compare: List[Any], success: List[Any], failure: Optional[List[Any]] = None) -> rpc_pb2.TxnResponse:
        request = rpc_pb2.TxnRequest(compare=compare, success=success, failure=failure or [])
        return self._call(self._kv_stub.Txn, request)

    # -- Lease operations --

    def lease_grant(self, ttl: int) -> int:
        request = rpc_pb2.LeaseGrantRequest(TTL=ttl)
        resp = self._call(self._lease_stub.LeaseGrant, request)
        return int(resp.ID)

    def lease_keepalive(self, lease_id: int) -> bool:
        def request_iter() -> Iterator[rpc_pb2.LeaseKeepAliveRequest]:
            yield rpc_pb2.LeaseKeepAliveRequest(ID=lease_id)

        try:
            responses = self._lease_stub.LeaseKeepAlive(request_iter())
            resp = next(responses)
            return bool(resp.TTL > 0)
        except grpc.RpcError as e:
            raise Etcd3GrpcError(f"lease_keepalive failed: {e.code().name}") from e

    # -- Cluster --

    def member_list(self) -> List[str]:
        request = rpc_pb2.MemberListRequest()
        resp = self._call(self._cluster_stub.MemberList, request)
        return [url for member in resp.members for url in member.clientURLs]

    def get_cluster(self, path: str) -> List[Dict[str, str]]:
        """Get all keys under path, returning them in Patroni's expected node format."""
        resp = self.get_prefix(path)
        nodes = []
        for kv in resp.kvs:
            nodes.append(
                {
                    "key": kv.key.decode("utf-8"),
                    "value": kv.value.decode("utf-8"),
                    "mod_revision": str(kv.mod_revision),
                    "lease": str(kv.lease) if kv.lease else None,
                }
            )
        return nodes


def _prefix_range_end(prefix: str) -> bytes:
    """Compute the range_end for a prefix scan (same logic as etcdctl)."""
    encoded = prefix.encode()
    end = bytearray(encoded)
    end[-1] = end[-1] + 1
    return bytes(end)
