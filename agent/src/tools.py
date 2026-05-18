"""Agent tools wrapping the Go Tool Server HTTP API."""
import httpx
from langchain_core.tools import tool
from pydantic import BaseModel, Field

GO_TOOL_SERVER = "http://localhost:8080"

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
    resp = httpx.post(f"{GO_TOOL_SERVER}/tools/create_cluster",
        json={"name": name, "shards": shards, "nodes_per_shard": nodes_per_shard},
        timeout=120)
    return resp.text

@tool(args_schema=GetStatusInput)
def get_cluster_status(name: str) -> str:
    """Get Redis cluster status including topology and health."""
    resp = httpx.post(f"{GO_TOOL_SERVER}/tools/get_status",
        json={"name": name}, timeout=30)
    return resp.text

@tool(args_schema=ScaleClusterInput)
def scale_cluster(name: str, shards: int) -> str:
    """Scale a Redis cluster by changing the number of shards."""
    resp = httpx.post(f"{GO_TOOL_SERVER}/tools/scale_cluster",
        json={"name": name, "shards": shards}, timeout=120)
    return resp.text

@tool
def list_clusters() -> str:
    """List all Redis clusters."""
    resp = httpx.post(f"{GO_TOOL_SERVER}/tools/list_clusters", timeout=30)
    return resp.text

@tool(args_schema=DiagnoseInput)
def diagnose_cluster(name: str) -> str:
    """Run diagnostics on a Redis cluster and return findings."""
    resp = httpx.post(f"{GO_TOOL_SERVER}/tools/diagnose",
        json={"name": name}, timeout=60)
    return resp.text

@tool
def get_events(name: str) -> str:
    """Get recent K8s events for a Redis cluster."""
    resp = httpx.get(f"{GO_TOOL_SERVER}/tools/events", params={"name": name}, timeout=30)
    return resp.text

ALL_TOOLS = [create_cluster, get_cluster_status, scale_cluster, list_clusters, diagnose_cluster, get_events]
