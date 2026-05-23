"""Minimal FastAPI server exposing the LangGraph agent via HTTP."""
from fastapi import FastAPI
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from .workflows import create_agent

app = FastAPI(title="Cluster Manager Agent", version="0.1.0")
agent_executor = create_agent()


class ChatRequest(BaseModel):
    message: str
    cluster_name: str = ""


class ChatResponse(BaseModel):
    response: str


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    """Send a natural-language message to the agent and get a response."""
    messages = [HumanMessage(content=req.message)]
    config = {"configurable": {"thread_id": req.cluster_name or "default"}}
    result = await agent_executor.ainvoke({"messages": messages, "cluster_name": req.cluster_name}, config)
    last = result["messages"][-1]
    return ChatResponse(response=last.content if hasattr(last, "content") else str(last))


@app.get("/health")
async def health():
    return {"ok": True}
