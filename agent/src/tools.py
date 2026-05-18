"""Agent tools wrapping the gRPC clusterd server."""
import json

import grpc
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from .stub import rediscluster_pb2, rediscluster_pb2_grpc

GRPC_SERVER = "localhost:50051"

_channel = None
_stub = None

def _get_stub():
    global _channel, _stub
    if _stub is None:
        _channel = grpc.insecure_channel(GRPC_SERVER)
        _stub = rediscluster_pb2_grpc.RedisClusterServiceStub(_channel)
    return _stub


def _proto_to_dict(msg):
    """Convert a protobuf message to a JSON-safe dict."""
    from google.protobuf.json_format import MessageToDict
    return MessageToDict(msg, preserving_proto_field_name=True)


class CreateClusterInput(BaseModel):
    name: str = Field(description="Cluster name")
    shards: int = Field(description="Number of shards (masters)", default=3)
    nodes_per_shard: int = Field(description="Nodes per shard", default=2)

class GetStatusInput(BaseModel):
    name: str = Field(description="Cluster name to query")

class ScaleClusterInput(BaseModel):
    name: str = Field(description="Cluster name")
    shards: int = Field(description="New shard count")

class DiagnoseInput(BaseModel):
    name: str = Field(description="Cluster name to diagnose")


@tool(args_schema=CreateClusterInput)
def create_cluster(name: str, shards: int = 3, nodes_per_shard: int = 2) -> str:
    """Create a new Redis cluster with specified topology."""
    req = rediscluster_pb2.CreateClusterRequest(
        name=name, shards=shards, nodes_per_shard=nodes_per_shard
    )
    resp = _get_stub().CreateCluster(req)
    return json.dumps(_proto_to_dict(resp))


@tool(args_schema=GetStatusInput)
def get_cluster_status(name: str) -> str:
    """Get Redis cluster status including topology and health."""
    req = rediscluster_pb2.GetStatusRequest(name=name)
    resp = _get_stub().GetStatus(req)
    return json.dumps(_proto_to_dict(resp))


@tool(args_schema=ScaleClusterInput)
def scale_cluster(name: str, shards: int) -> str:
    """Scale a Redis cluster by changing the number of shards."""
    req = rediscluster_pb2.ScaleClusterRequest(name=name, shards=shards)
    resp = _get_stub().ScaleCluster(req)
    return json.dumps(_proto_to_dict(resp))


@tool
def list_clusters() -> str:
    """List all Redis clusters."""
    req = rediscluster_pb2.ListClustersRequest()
    resp = _get_stub().ListClusters(req)
    return json.dumps(_proto_to_dict(resp))


@tool(args_schema=DiagnoseInput)
def diagnose_cluster(name: str) -> str:
    """Run diagnostics on a Redis cluster and return findings."""
    req = rediscluster_pb2.DiagnoseRequest(name=name)
    resp = _get_stub().Diagnose(req)
    return json.dumps(_proto_to_dict(resp))


@tool
def get_events(name: str) -> str:
    """Get recent K8s events for a Redis cluster."""
    req = rediscluster_pb2.GetEventsRequest(name=name)
    resp = _get_stub().GetEvents(req)
    return json.dumps(_proto_to_dict(resp))


ALL_TOOLS = [create_cluster, get_cluster_status, scale_cluster, list_clusters, diagnose_cluster, get_events]
