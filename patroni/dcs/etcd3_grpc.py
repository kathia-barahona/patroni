import json
import logging
import os
import sys
import time

from collections import defaultdict
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, TypeVar, Union
from urllib.parse import urlparse

import grpc

from patroni.dcs import AbstractDCS, Cluster, ClusterConfig, \
    Failover, Leader, Member, Status, SyncState, TimelineHistory
from patroni.dcs.etcd import StaleEtcdNodeGuard
from patroni.exceptions import DCSError
from patroni.utils import deep_compare, Retry, RetryFailedError

if TYPE_CHECKING:
    from ..postgresql.mpp import AbstractMPP

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
            responses = self._lease_stub.LeaseKeepAlive(request_iter(), timeout=self._retry_timeout)
            resp = next(responses)
            return bool(resp.TTL > 0)
        except grpc.RpcError as e:
            code = e.code()
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                self._rotate_endpoint()
            raise Etcd3GrpcError(f"lease_keepalive failed: {code.name}") from e

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


def catch_grpc_errors(func: Callable[..., Any]) -> Any:
    def wrapper(self: "Etcd3_grpc", *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except (Etcd3GrpcError, RetryFailedError) as e:
            logger.error("%s failed: %r", func.__name__, e)
            return False

    return wrapper


class Etcd3_grpc(AbstractDCS):
    def __init__(self, config: Dict[str, Any], mpp: 'AbstractMPP') -> None:
        super().__init__(config, mpp)
        self._ttl = int(config.get("ttl") or 30)
        self._retry = Retry(
            deadline=config["retry_timeout"], max_delay=1, max_tries=-1, retry_exceptions=Etcd3GrpcError
        )
        self._lease: Optional[int] = None
        self._last_lease_refresh: float = 0

        client_config = config.get(self.__class__.__name__.lower(), config)
        client_config.setdefault("retry_timeout", config["retry_timeout"])
        self._client = Etcd3GrpcClient(client_config)
        self._connect_with_retry()

        self._do_not_watch = False

        if not self._ctl:
            self._create_lease()

    def retry(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        retry = self._retry.copy()
        return retry(method, *args, **kwargs)

    def _connect_with_retry(self) -> None:
        while True:
            try:
                self._client.connect()
                return
            except Exception:
                logger.info("waiting on etcd (gRPC)")
                time.sleep(5)

    def _create_lease(self) -> None:
        while not self._lease:
            try:
                self._lease = self._client.lease_grant(self._ttl)
            except Etcd3GrpcError:
                logger.info("waiting on etcd to grant lease")
                time.sleep(5)

    def _refresh_lease(self) -> bool:
        if self._lease and self._last_lease_refresh + self._loop_wait > time.time():
            return False

        if self._lease and not self._client.lease_keepalive(self._lease):
            self._lease = None

        if not self._lease:
            self._lease = self._client.lease_grant(self._ttl)

        self._last_lease_refresh = time.time()
        return True

    # -- AbstractDCS required properties/methods --

    def set_ttl(self, ttl: int) -> Optional[bool]:
        ttl = int(ttl)
        self._do_not_watch = self._ttl != ttl
        self._ttl = ttl
        if self._do_not_watch:
            self._lease = None
        return None

    @property
    def ttl(self) -> int:
        return self._ttl

    def set_retry_timeout(self, retry_timeout: int) -> None:
        self._retry.deadline = retry_timeout
        self._client._retry_timeout = float(retry_timeout)

    # -- Cluster loading --

    @staticmethod
    def member(node: Dict[str, str]) -> Member:
        return Member.from_node(node["mod_revision"], os.path.basename(node["key"]), node["lease"], node["value"])

    def _cluster_from_nodes(self, nodes: Dict[str, Any]) -> Cluster:
        initialize = nodes.get(self._INITIALIZE)
        initialize = initialize and initialize["value"]

        config = nodes.get(self._CONFIG)
        config = config and ClusterConfig.from_node(config["mod_revision"], config["value"])

        history = nodes.get(self._HISTORY)
        history = history and TimelineHistory.from_node(history["mod_revision"], history["value"])

        status = nodes.get(self._STATUS) or nodes.get(self._LEADER_OPTIME)
        status = Status.from_node(status and status["value"])

        members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count("/") == 1]

        leader = nodes.get(self._LEADER)
        if (
            not self._ctl
            and leader
            and leader["value"] == self._name
            and self._lease
            and str(self._lease) != leader.get("lease")
        ):
            logger.warning("I am the leader but not owner of the lease")

        if leader:
            member = Member(-1, leader["value"], None, {})
            member = ([m for m in members if m.name == leader["value"]] or [member])[0]
            leader = Leader(leader["mod_revision"], leader["lease"], member)

        failover = nodes.get(self._FAILOVER)
        if failover:
            failover = Failover.from_node(failover["mod_revision"], failover["value"])

        sync = nodes.get(self._SYNC)
        sync = SyncState.from_node(sync and sync["mod_revision"], sync and sync["value"])

        failsafe = nodes.get(self._FAILSAFE)
        try:
            failsafe = json.loads(failsafe["value"]) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)

    def _get_cluster_nodes(self, path: str) -> List[Dict[str, Any]]:
        return self._client.get_cluster(path)

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        nodes = {
            node["key"][len(path):]: node for node in self._get_cluster_nodes(path) if node["key"].startswith(path)
        }
        return self._cluster_from_nodes(nodes)

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        clusters: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        path = self._base_path + "/"
        for node in self._get_cluster_nodes(path):
            key = node["key"][len(path):].split("/", 1)
            if len(key) == 2 and self._mpp.group_re.match(key[0]):
                clusters[int(key[0])][key[1]] = node
        return {group: self._cluster_from_nodes(nodes) for group, nodes in clusters.items()}

    def _load_cluster(
        self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]],
    ) -> Union[Cluster, Dict[int, Cluster]]:
        try:
            cluster = loader(path)
        except Exception as e:
            logger.error("Failed to load cluster from etcd: %r", e)
            raise Etcd3GrpcError("Etcd is not responding properly") from e
        return cluster

    # -- Member operations --

    @catch_grpc_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
        self.retry(self._refresh_lease)

        if not self._lease:
            return False

        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)

        if member and member.session == str(self._lease) and deep_compare(data, member.data):
            return True

        value = json.dumps(data, separators=(",", ":"))
        self.retry(self._client.put, self.member_path, value, lease=self._lease)
        return True

    # -- Leader operations --

    @catch_grpc_errors
    def take_leader(self) -> bool:
        self.retry(self._client.put, self.leader_path, self._name, lease=self._lease)
        return True

    def attempt_to_acquire_leader(self) -> bool:
        try:
            self.retry(self._refresh_lease)
        except (Etcd3GrpcError, RetryFailedError):
            return False

        if not self._lease:
            return False

        compare = [
            rpc_pb2.Compare(
                result=rpc_pb2.Compare.EQUAL,
                target=rpc_pb2.Compare.CREATE,
                key=self.leader_path.encode(),
                create_revision=0,
            )
        ]
        success = [
            rpc_pb2.RequestOp(
                request_put=rpc_pb2.PutRequest(
                    key=self.leader_path.encode(),
                    value=self._name.encode(),
                    lease=self._lease,
                )
            )
        ]
        try:
            resp = self.retry(self._client.txn, compare, success)
            if not resp.succeeded:
                logger.info("Could not take out TTL lock")
            return bool(resp.succeeded)
        except (Etcd3GrpcError, RetryFailedError):
            raise Etcd3GrpcError("attempt_to_acquire_leader")

    def _update_leader(self, leader: Leader) -> bool:
        try:
            self.retry(self._refresh_lease)
        except (Etcd3GrpcError, RetryFailedError):
            raise Etcd3GrpcError("update_leader lease refresh failed")

        if not self._lease:
            return False

        if str(self._lease) == leader.session:
            return True

        compare = [
            rpc_pb2.Compare(
                result=rpc_pb2.Compare.EQUAL,
                target=rpc_pb2.Compare.VALUE,
                key=self.leader_path.encode(),
                value=self._name.encode(),
            )
        ]
        success = [
            rpc_pb2.RequestOp(
                request_put=rpc_pb2.PutRequest(
                    key=self.leader_path.encode(),
                    value=self._name.encode(),
                    lease=self._lease,
                )
            )
        ]
        try:
            resp = self.retry(self._client.txn, compare, success)
            return bool(resp.succeeded)
        except (Etcd3GrpcError, RetryFailedError):
            raise Etcd3GrpcError("update_leader failed")

    @catch_grpc_errors
    def _delete_leader(self, leader: Leader) -> bool:
        compare = [
            rpc_pb2.Compare(
                result=rpc_pb2.Compare.EQUAL,
                target=rpc_pb2.Compare.VALUE,
                key=self.leader_path.encode(),
                value=self._name.encode(),
            )
        ]
        success = [
            rpc_pb2.RequestOp(
                request_delete_range=rpc_pb2.DeleteRangeRequest(
                    key=self.leader_path.encode(),
                )
            )
        ]
        return bool(self._client.txn(compare, success).succeeded)

    # -- Config/state operations --

    @catch_grpc_errors
    def set_failover_value(self, value: str, version: Optional[str] = None) -> bool:
        return bool(self._client.put(self.failover_path, value))

    @catch_grpc_errors
    def set_config_value(self, value: str, version: Optional[str] = None) -> bool:
        return bool(self._client.put(self.config_path, value))

    @catch_grpc_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        return bool(self._client.put(self.leader_optime_path, last_lsn))

    @catch_grpc_errors
    def _write_status(self, value: str) -> bool:
        return bool(self._client.put(self.status_path, value))

    @catch_grpc_errors
    def _write_failsafe(self, value: str) -> bool:
        return bool(self._client.put(self.failsafe_path, value))

    @catch_grpc_errors
    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        if not create_new:
            return bool(self._client.put(self.initialize_path, sysid))

        compare = [
            rpc_pb2.Compare(
                result=rpc_pb2.Compare.EQUAL,
                target=rpc_pb2.Compare.CREATE,
                key=self.initialize_path.encode(),
                create_revision=0,
            )
        ]
        success = [
            rpc_pb2.RequestOp(
                request_put=rpc_pb2.PutRequest(
                    key=self.initialize_path.encode(),
                    value=sysid.encode(),
                )
            )
        ]
        return bool(self.retry(self._client.txn, compare, success).succeeded)

    @catch_grpc_errors
    def cancel_initialization(self) -> bool:
        return bool(self.retry(self._client.delete, self.initialize_path))

    @catch_grpc_errors
    def delete_cluster(self) -> bool:
        return bool(self.retry(self._client.delete_prefix, self.client_path("")))

    @catch_grpc_errors
    def set_history_value(self, value: str) -> bool:
        return bool(self._client.put(self.history_path, value))

    @catch_grpc_errors
    def set_sync_state_value(self, value: str, version: Optional[str] = None) -> Union[bool, str]:
        resp = self.retry(self._client.put, self.sync_path, value)
        return str(resp.header.revision)

    @catch_grpc_errors
    def delete_sync_state(self, version: Optional[str] = None) -> bool:
        return bool(self.retry(self._client.delete, self.sync_path))

    def _cluster_prefix(self) -> str:
        return str(self._base_path + "/" if self.is_mpp_coordinator() else self.client_path(""))

    def watch(self, leader_version: Optional[str], timeout: float) -> bool:
        self._last_lease_refresh = 0
        if self._do_not_watch:
            self._do_not_watch = False
            return True
        if leader_version:
            timeout += 0.5
        try:
            return bool(super().watch(None, timeout))
        except Exception:
            return True
        finally:
            self.event.clear()
