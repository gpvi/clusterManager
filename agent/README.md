# Cluster Manager Agent

Python LangGraph agent for managing Redis clusters via the Go Tool Server.

## Setup

```bash
pip install -e .
```

## Prerequisites

- Set the `OPENAI_API_KEY` environment variable.
- The Go Tool Server must be running at `localhost:8080`.

## Usage

### Interactive script
```python
from src.workflows import create_agent
from langchain_core.messages import HumanMessage

agent = create_agent()
config = {"configurable": {"thread_id": "my-cluster"}}
result = agent.invoke(
    {"messages": [HumanMessage(content="create a cluster named dev with 3 shards")], "cluster_name": "dev"},
    config,
)
print(result["messages"][-1].content)
```

### HTTP server
```bash
pip install fastapi uvicorn
uvicorn src.server:app --host 0.0.0.0 --port 8000
```

Then POST to `/chat`:
```json
{"message": "create a cluster named dev with 3 shards", "cluster_name": "dev"}
```

## Available Tools

| Tool               | Description                            |
|--------------------|----------------------------------------|
| `create_cluster`   | Create a new Redis cluster             |
| `get_cluster_status` | Get cluster topology and health      |
| `scale_cluster`    | Scale cluster shard count              |
| `list_clusters`    | List all clusters                      |
| `diagnose_cluster` | Run diagnostics on a cluster           |
| `get_events`       | Get recent Kubernetes events           |
