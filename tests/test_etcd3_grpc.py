from pathlib import Path
from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest

from patroni.dcs import Cluster
from patroni.dcs.etcd3_grpc import _prefix_range_end, catch_grpc_errors, \
    Etcd3_grpc, Etcd3GrpcClient, Etcd3GrpcError, GrpcKVCache
from patroni.postgresql.mpp import get_mpp
from patroni.utils import RetryFailedError


class SleepException(Exception):
    pass


def mock_grpc_channel() -> MagicMock:
    channel = MagicMock()
    return channel


def mock_kv(key: str, value: str, mod_revision: int = 1, lease: int = 123) -> Mock:
    kv = Mock()
    kv.key = key.encode("utf-8")
    kv.value = value.encode("utf-8")
    kv.mod_revision = mod_revision
    kv.lease = lease
    return kv


def make_range_response(kvs: List[Mock]) -> Mock:
    resp = Mock()
    resp.kvs = [mock_kv(**kv) if isinstance(kv, dict) else kv for kv in kvs]
    return resp


def make_cluster_kvs() -> List[Mock]:
    return [
        mock_kv("/patroni/test/initialize", "12345", mod_revision=1, lease=0),
        mock_kv("/patroni/test/leader", "foo", mod_revision=1, lease=123),
        mock_kv("/patroni/test/members/foo", "{}", mod_revision=1, lease=123),
        mock_kv("/patroni/test/members/bar", '{"version":"1.6.5"}', mod_revision=2, lease=456),
        mock_kv("/patroni/test/failover", "{}", mod_revision=1, lease=0),
        mock_kv("/patroni/test/failsafe", "{", mod_revision=1, lease=0),
    ]


class TestPrefixRangeEnd:
    def test_basic(self) -> None:
        assert _prefix_range_end("/patroni/test/") == b"/patroni/test0"

    def test_single_char(self) -> None:
        assert _prefix_range_end("a") == b"b"


class TestEtcd3GrpcClient:
    def setup_method(self) -> None:
        self.config = {
            "host": "127.0.0.1",
            "port": 2379,
            "cacert": None,
            "key": None,
            "cert": None,
        }

    def test_parse_endpoints_single(self) -> None:
        endpoints = Etcd3GrpcClient._parse_endpoints({"host": "10.0.0.1", "port": 2379})
        assert endpoints == ["10.0.0.1:2379"]

    def test_parse_endpoints_hosts_string(self) -> None:
        endpoints = Etcd3GrpcClient._parse_endpoints({"hosts": "10.0.0.1,10.0.0.2", "port": 2379})
        assert endpoints == ["10.0.0.1:2379", "10.0.0.2:2379"]

    def test_parse_endpoints_hosts_with_port(self) -> None:
        endpoints = Etcd3GrpcClient._parse_endpoints({"hosts": "10.0.0.1:2380,10.0.0.2:2380"})
        assert endpoints == ["10.0.0.1:2380", "10.0.0.2:2380"]

    def test_parse_endpoints_hosts_list(self) -> None:
        endpoints = Etcd3GrpcClient._parse_endpoints({"hosts": ["10.0.0.1", "10.0.0.2"], "port": 2379})
        assert endpoints == ["10.0.0.1:2379", "10.0.0.2:2379"]

    @patch("grpc.ssl_channel_credentials")
    def test_build_credentials_no_certs(self, mock_ssl: Mock) -> None:
        mock_ssl.return_value = "creds"
        result = Etcd3GrpcClient._build_credentials({"cacert": None, "key": None, "cert": None})
        mock_ssl.assert_called_once_with(root_certificates=None, private_key=None, certificate_chain=None)
        assert result == "creds"

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_connect(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        expected_options = [
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        mock_channel.assert_called_once_with("127.0.0.1:2379", options=expected_options)
        assert client._kv_stub is not None
        assert client._lease_stub is not None

    @patch("grpc.secure_channel", return_value=mock_grpc_channel())
    @patch("grpc.ssl_channel_credentials", return_value="creds")
    def test_connect_tls(self, mock_ssl: Mock, mock_secure_channel: Mock) -> None:
        config = dict(self.config, cacert="/tmp/ca.crt")
        with patch.object(Path, "read_bytes", return_value=MagicMock(read=Mock(return_value=b"cert-data"))):
            client = Etcd3GrpcClient(config)
        client.connect()
        expected_options = [
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        mock_secure_channel.assert_called_once_with("127.0.0.1:2379", "creds", options=expected_options)

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_close(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client.close()
        assert client._channel is None

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_rotate_endpoint(self, mock_channel: Mock) -> None:
        config = {"hosts": "10.0.0.1,10.0.0.2", "port": 2379}
        client = Etcd3GrpcClient(config)
        client.connect()
        assert client._current_endpoint_idx == 0
        client._rotate_endpoint()
        assert client._current_endpoint_idx == 1
        client._rotate_endpoint()
        assert client._current_endpoint_idx == 0

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_get_cluster(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client._kv_stub.Range = Mock(
            return_value=make_range_response(
                [
                    mock_kv("/patroni/test/leader", "foo", mod_revision=5, lease=123),
                ]
            )
        )
        nodes = client.get_cluster("/patroni/test/")
        assert len(nodes) == 1
        assert nodes[0]["key"] == "/patroni/test/leader"
        assert nodes[0]["value"] == "foo"
        assert nodes[0]["mod_revision"] == "5"
        assert nodes[0]["lease"] == "123"

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_get_cluster_no_lease(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client._kv_stub.Range = Mock(
            return_value=make_range_response(
                [
                    mock_kv("/patroni/test/config", "{}", mod_revision=1, lease=0),
                ]
            )
        )
        nodes = client.get_cluster("/patroni/test/")
        assert nodes[0]["lease"] is None

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_lease_grant(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.ID = 99999
        client._lease_stub.LeaseGrant = Mock(return_value=resp)
        lease_id = client.lease_grant(30)
        assert lease_id == 99999

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_lease_keepalive(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.TTL = 30
        client._lease_stub.LeaseKeepAlive = Mock(return_value=iter([resp]))
        assert client.lease_keepalive(99999) is True

    @patch("grpc.insecure_channel", return_value=mock_grpc_channel())
    def test_lease_keepalive_expired(self, mock_channel: Mock) -> None:
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.TTL = 0
        client._lease_stub.LeaseKeepAlive = Mock(return_value=iter([resp]))
        assert client.lease_keepalive(99999) is False


class TestCatchGrpcErrors:
    def test_catches_etcd3_grpc_error(self) -> None:
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self) -> None:
                raise Etcd3GrpcError("test error")

        assert FakeDCS().do_something() is False

    def test_catches_retry_failed_error(self) -> None:
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self) -> None:
                raise RetryFailedError("")

        assert FakeDCS().do_something() is False

    def test_passes_through_on_success(self) -> None:
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self) -> bool:
                return True

        assert FakeDCS().do_something() is True


@patch("grpc.insecure_channel", return_value=mock_grpc_channel())
class TestEtcd3Grpc:
    def _make_dcs(self) -> Etcd3_grpc:
        with patch.object(Etcd3GrpcClient, "lease_grant", return_value=123):
            with patch.object(Etcd3GrpcClient, "get_prefix",
                              return_value=make_range_response(make_cluster_kvs())):
                with patch.object(GrpcKVCache, "start"):
                    dcs = Etcd3_grpc(
                        {
                            "namespace": "/patroni/",
                            "ttl": 30,
                            "retry_timeout": 10,
                            "name": "foo",
                            "scope": "test",
                            "etcd3_grpc": {"host": "127.0.0.1", "port": 2379},
                        },
                        get_mpp({}),
                    )
        return dcs

    def test_init(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        assert dcs._ttl == 30
        assert dcs._lease == 123
        assert dcs._client is not None

    def test_get_cluster(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        assert isinstance(cluster, Cluster)
        assert cluster.leader.name == "foo"
        assert len(cluster.members) == 2

    def test_get_cluster_error(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", side_effect=Exception("connection refused")):
            with pytest.raises(Etcd3GrpcError):
                dcs.get_cluster()

    def test_touch_member(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()) as mock_put:
            with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
                dcs._last_lease_refresh = 0
                assert dcs.touch_member({"conn_url": "http://localhost:5432"}) is True
                mock_put.assert_called_once()

    def test_touch_member_no_lease(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        dcs._lease = None
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, "lease_grant", side_effect=Etcd3GrpcError("no lease")):
            assert dcs.touch_member({}) is False

    def test_touch_member_data_unchanged(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, "put") as mock_put:
            with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
                dcs._last_lease_refresh = 0
                assert dcs.touch_member({}) is True
                mock_put.assert_not_called()

    def test_take_leader(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()):
            assert dcs.take_leader() is True

    def test_attempt_to_acquire_leader_success(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, "txn", return_value=txn_resp):
            with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
                dcs._last_lease_refresh = 0
                assert dcs.attempt_to_acquire_leader() is True

    def test_attempt_to_acquire_leader_fail(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = False
        with patch.object(Etcd3GrpcClient, "txn", return_value=txn_resp):
            with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
                dcs._last_lease_refresh = 0
                assert dcs.attempt_to_acquire_leader() is False

    def test_attempt_to_acquire_leader_no_lease(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        dcs._lease = None
        with patch.object(Etcd3GrpcClient, "lease_grant", side_effect=Etcd3GrpcError("err")):
            assert dcs.attempt_to_acquire_leader() is False

    def test_update_leader_same_session(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
            dcs._last_lease_refresh = 0
            assert dcs._update_leader(cluster.leader) is True

    def test_update_leader_different_session(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        dcs._lease = 999
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, "txn", return_value=txn_resp):
            with patch.object(Etcd3GrpcClient, "lease_keepalive", return_value=True):
                dcs._last_lease_refresh = 0
                assert dcs._update_leader(cluster.leader) is True

    def test_delete_leader(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, "txn", return_value=txn_resp):
            assert dcs._delete_leader(cluster.leader) is True

    def test_set_failover_value(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()):
            assert dcs.set_failover_value("{}") is True

    def test_set_config_value(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()):
            assert dcs.set_config_value("{}") is True

    def test_initialize(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, "txn", return_value=txn_resp):
            assert dcs.initialize() is True

    def test_initialize_not_new(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()):
            assert dcs.initialize(create_new=False, sysid="12345") is True

    def test_cancel_initialization(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "delete", return_value=Mock()):
            assert dcs.cancel_initialization() is True

    def test_delete_cluster(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "delete_prefix", return_value=Mock()):
            assert dcs.delete_cluster() is True

    def test_set_history_value(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "put", return_value=Mock()):
            assert dcs.set_history_value("[]") is True

    def test_set_sync_state_value(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        resp = Mock()
        resp.header.revision = 42
        with patch.object(Etcd3GrpcClient, "put", return_value=resp):
            result = dcs.set_sync_state_value("{}")
            assert result == "42"

    def test_delete_sync_state(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "delete", return_value=Mock()):
            assert dcs.delete_sync_state() is True

    def test_set_ttl(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        assert dcs._ttl == 30
        dcs.set_ttl(20)
        assert dcs._ttl == 20
        assert dcs._lease is None

    def test_set_ttl_same_value(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        dcs.set_ttl(30)
        assert dcs._lease == 123

    def test_set_retry_timeout(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        dcs.set_retry_timeout(20)
        assert dcs._retry.deadline == 20

    @patch("time.sleep", Mock(side_effect=SleepException))
    def test_create_lease_retry(self, mock_channel: Mock) -> None:
        with patch.object(Etcd3GrpcClient, "lease_grant", side_effect=Etcd3GrpcError("err")):
            with pytest.raises(SleepException):
                Etcd3_grpc(
                    {
                        "namespace": "/patroni/",
                        "ttl": 30,
                        "retry_timeout": 10,
                        "name": "foo",
                        "scope": "test",
                        "etcd3_grpc": {
                            "host": "127.0.0.1",
                            "port": 2379,
                            "cacert": None,
                            "key": None,
                            "cert": None,
                        },
                    },
                    get_mpp({}),
                )

    def test_watch(self, mock_channel: Mock) -> None:
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, "get_prefix", return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        result = dcs.watch(None, 0)
        assert result in (True, False)
