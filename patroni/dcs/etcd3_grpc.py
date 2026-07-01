import time
import logging
import sys

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypeVar
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
        self._machines_cache_ttl: int = config.get("machines_cache_ttl", 300)
        self._machines_cache_updated: float = 0
        self._update_machines_cache: bool = False

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
        options: List[Tuple[str, Any]] = [
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

    @staticmethod
    def _calculate_timeouts(etcd_nodes: int, timeout: float) -> Tuple[int, float, int]:
        """Calculate per-node timeout and retries, splitting the budget across available nodes.

        For clusters with 1 node: up to 2 retries per node.
        For clusters with 2 nodes: 1 retry per node.
        For clusters with 3+ nodes: no retries, just try next node.
        If per-node timeout would be < 1s, reduce the number of nodes to try.
        """
        per_node_timeout = timeout
        max_retries = 4 - min(etcd_nodes, 3)
        per_node_retries = 1
        min_timeout = 1.0

        while etcd_nodes > 0:
            per_node_timeout = timeout / etcd_nodes
            if per_node_timeout >= min_timeout:
                while per_node_retries < max_retries and per_node_timeout / (per_node_retries + 1) >= min_timeout:
                    per_node_retries += 1
                per_node_timeout /= per_node_retries
                break
            etcd_nodes -= 1
            max_retries = 1

        return etcd_nodes, per_node_timeout, per_node_retries - 1

    def _refresh_machines_cache(self) -> None:
        """Refresh the endpoint list by calling member_list() on the current node."""
        try:
            request = rpc_pb2.MemberListRequest()
            resp = self._cluster_stub.MemberList(request, timeout=2.0)
            new_endpoints = []
            for member in resp.members:
                for url in member.clientURLs:
                    parsed = urlparse(url)
                    host = parsed.hostname
                    port = parsed.port or 2379
                    if not host:
                        continue
                    endpoint = f"[{host}:{port}]" if ":" in host else f"{host}:{port}"
                    if endpoint not in new_endpoints:
                        new_endpoints.append(endpoint)
            if new_endpoints:
                logger.info("Updated etcd endpoints from member_list: %s", new_endpoints)
                self._endpoints = new_endpoints
                self._current_endpoint_idx = self._current_endpoint_idx % len(self._endpoints)
            self._machines_cache_updated = time.time()
            self._update_machines_cache = False
        except Exception as e:
            logger.warning("Failed to refresh machines cache: %r", e)
            self._update_machines_cache = True

    def _call(
        self,
        stub_method: Callable[..., RespT],
        request: Message,
        timeout: Optional[float] = None,
    ) -> RespT:
        if self._update_machines_cache:
            self._refresh_machines_cache()
        elif time.time() - self._machines_cache_updated > self._machines_cache_ttl:
            self._refresh_machines_cache()

        deadline = time.time() + (timeout or self._retry_timeout)
        some_request_failed = False

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise Etcd3GrpcError("Exceeded retry deadline")

            etcd_nodes = len(self._endpoints)
            nodes_to_try, per_node_timeout, retries = self._calculate_timeouts(etcd_nodes, remaining)

            if nodes_to_try == 0:
                raise Etcd3GrpcError("No time left to try any etcd node")

            for _ in range(nodes_to_try):
                for attempt in range(retries + 1):
                    try:
                        result = stub_method(request, timeout=per_node_timeout)
                        if some_request_failed:
                            self._refresh_machines_cache()
                        return result
                    except grpc.RpcError as e:
                        code = e.code()
                        if code not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                            raise Etcd3GrpcError(f"gRPC error: {code.name} {e.details()}") from e
                        some_request_failed = True
                        if attempt == retries:
                            break
                self._rotate_endpoint()

            remaining = deadline - time.time()
            if remaining <= 0:
                raise Etcd3GrpcError("Exceeded retry deadline after exhausting all nodes")

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
        self._refresh_machines_cache()
        return list(self._endpoints)

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
