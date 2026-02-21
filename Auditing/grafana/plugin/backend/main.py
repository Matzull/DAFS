"""
DAFS Grafana Assistant Backend — LangChain agentic rewrite.

The agent has 5 read-only Elasticsearch tools and a minimal system prompt.
It self-discovers indices, mappings, field values, and chains queries to
answer questions without any hardcoded domain knowledge.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Pydantic models (FastAPI layer)
# ---------------------------------------------------------------------------

class QueryContext(BaseModel):
    dashboardTitle: Optional[str] = None
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None


class HistoryMessage(BaseModel):
    role: str  # 'user' | 'assistant'
    content: str


class QueryRequest(BaseModel):
    query: str
    context: QueryContext
    systemPrompt: Optional[str] = None
    history: list[HistoryMessage] = Field(default_factory=list)


class QueryResponse(BaseModel):
    answer: str
    tool_calls_made: int = 0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4.1-mini")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ES config — populated at startup
ES_BASE_URL: str = ""
ES_USER: str = ""
ES_PASSWORD: str = ""
ES_VERIFY_TLS: bool = False
ES_CA_CERT_PATH: str = ""


def _init_es_config() -> None:
    global ES_BASE_URL, ES_USER, ES_PASSWORD, ES_VERIFY_TLS, ES_CA_CERT_PATH
    base = os.getenv("ES_URL", "https://es01").rstrip("/")
    if not base.rsplit(":", 1)[-1].isdigit():
        base = f"{base}:{os.getenv('ES_PORT', '9200')}"
    ES_BASE_URL = base
    ES_USER = os.getenv("ELASTIC_USER", "elastic")
    ES_PASSWORD = os.getenv("ELASTIC_PASSWORD", "")
    ES_VERIFY_TLS = os.getenv("ES_VERIFY_TLS", "false").lower() == "true"
    ES_CA_CERT_PATH = os.getenv("ES_CA_CERT_PATH", "")


def _configure_logger() -> None:
    valid = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
    level = LOG_LEVEL if LOG_LEVEL in valid else "INFO"
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        enqueue=True,
        backtrace=False,
        diagnose=False,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}",
    )
    logger.info("Logger configured LOG_LEVEL={}", level)


def _es_verify() -> bool | str:
    if ES_VERIFY_TLS and ES_CA_CERT_PATH:
        return ES_CA_CERT_PATH
    return ES_VERIFY_TLS


_init_es_config()
_configure_logger()


# ---------------------------------------------------------------------------
# ES HTTP helpers — used by all tools
# ---------------------------------------------------------------------------

def _es_get(path: str, timeout: float = 15.0) -> dict[str, Any] | str:
    url = f"{ES_BASE_URL}{path}"
    logger.debug("ES REQUEST  GET {}", url)
    try:
        with httpx.Client(timeout=timeout, verify=_es_verify()) as client:
            r = client.get(url, auth=(ES_USER, ES_PASSWORD))
        logger.debug("ES RESPONSE GET {} → status={} took={}ms",
                     url, r.status_code, r.elapsed.microseconds // 1000)
        if r.status_code >= 400:
            logger.debug("ES RESPONSE body: {}", r.text[:500])
            return f"ERROR {r.status_code}: {r.text[:300]}"
        return r.json()
    except Exception as exc:
        logger.warning("ES GET {} failed: {}", url, exc)
        return f"ERROR: {exc}"


def _es_post(path: str, body: dict[str, Any], timeout: float = 30.0) -> dict[str, Any] | str:
    url = f"{ES_BASE_URL}{path}"
    logger.debug("ES REQUEST  POST {}\n  body: {}", url, json.dumps(body, ensure_ascii=False))
    try:
        with httpx.Client(timeout=timeout, verify=_es_verify()) as client:
            r = client.post(
                url,
                auth=(ES_USER, ES_PASSWORD),
                headers={"Content-Type": "application/json"},
                json=body,
            )
        logger.debug("ES RESPONSE POST {} → status={} took={}ms",
                     url, r.status_code, r.elapsed.microseconds // 1000)
        if r.status_code >= 400:
            logger.debug("ES RESPONSE body: {}", r.text[:1000])
            return f"ERROR {r.status_code}: {r.text[:500]}"
        return r.json()
    except Exception as exc:
        logger.warning("ES POST {} failed: {}", url, exc)
        return f"ERROR: {exc}"


def _fmt(obj: Any, max_len: int = 4000) -> str:
    s = json.dumps(obj, ensure_ascii=False, default=str)
    if len(s) > max_len:
        s = s[:max_len] + f"\n... (truncated, {len(s)} chars total)"
    return s


# ---------------------------------------------------------------------------
# LangChain tools
# ---------------------------------------------------------------------------

@tool
def list_indices() -> str:
    """
    List all Elasticsearch indices with their document counts and sizes.
    Use this first to discover what data is available before making any queries.
    Returns index names, document count, and storage size.
    """
    result = _es_get("/_cat/indices?h=index,docs.count,store.size,status&s=index&format=json")
    if isinstance(result, str):
        return result
    filtered = [r for r in result if not r.get("index", "").startswith(".")]
    if not filtered:
        return "No user indices found."
    lines = [
        f"  {r['index']}: {r.get('docs.count', '?')} docs, {r.get('store.size', '?')}"
        for r in filtered
    ]
    out = "Available indices:\n" + "\n".join(lines)
    logger.debug("ES RESULT   list_indices → {} indices", len(filtered))
    return out


@tool
def get_mapping(index: str) -> str:
    """
    Get the complete field mapping (schema) for an Elasticsearch index.
    Shows all field names and their data types.
    IMPORTANT: For text fields that have a .keyword sub-field, you MUST use
    fieldname.keyword for exact filtering and aggregations.
    Always call this before querying an index you haven't inspected yet.

    Args:
        index: The index name (e.g. 'dfs-audit').
    """
    result = _es_get(f"/{index}/_mapping")
    if isinstance(result, str):
        return result

    def collect(props: dict, prefix: str = "", depth: int = 0) -> list[str]:
        if depth > 4:
            return []
        lines: list[str] = []
        for fname, meta in sorted(props.items()):
            full = f"{prefix}{fname}" if prefix else fname
            ftype = meta.get("type", "object")
            hint = ""
            if ftype == "text" and "keyword" in meta.get("fields", {}):
                hint = f"  → use {full}.keyword for filters/aggs"
            lines.append(f"  {full}: {ftype}{hint}")
            if "properties" in meta:
                lines.extend(collect(meta["properties"], f"{full}.", depth + 1))
        return lines

    sections: list[str] = []
    for idx, data in result.items():
        if idx.startswith("."):
            continue
        props = data.get("mappings", {}).get("properties", {})
        sections.append(f"{idx} fields:\n" + "\n".join(collect(props)))

    out = "\n\n".join(sections) or "No mapping found."
    logger.debug("ES RESULT   get_mapping index={} fields={}", index, out.count("\n"))
    return out


@tool
def get_field_values(index: str, field: str, size: int = 20, filter_json: str = "") -> str:
    """
    Get the distinct values actually present in a field using a terms aggregation.
    Use this to discover what values exist before filtering — never guess or invent
    field values. Always use the .keyword sub-field for text fields.

    Args:
        index: Index name (e.g. 'dfs-audit').
        field: Field to aggregate, use .keyword for text (e.g. 'action.keyword').
        size: Max distinct values to return (default 20, max 100).
        filter_json: Optional ES query clause (JSON string) to pre-filter documents
                     before aggregating. Example: '{"term":{"user.keyword":"admin"}}'
    """
    size = max(1, min(int(size), 100))
    body: dict[str, Any] = {
        "size": 0,
        "aggs": {"values": {"terms": {"field": field, "size": size}}},
    }
    if filter_json:
        try:
            body["query"] = json.loads(filter_json)
        except json.JSONDecodeError as exc:
            return f"ERROR: filter_json is not valid JSON: {exc}"

    result = _es_post(f"/{index}/_search", body)
    if isinstance(result, str):
        return result

    buckets = result.get("aggregations", {}).get("values", {}).get("buckets", [])
    total = result.get("hits", {}).get("total", {})
    lines = [f"  {b['key']!r}: {b['doc_count']} docs" for b in buckets]
    out = (
        f"Field '{field}' in '{index}' (total docs: {total}):\n"
        + ("\n".join(lines) if lines else "  (no values found)")
    )
    logger.debug("ES RESULT   get_field_values field={} values={}", field,
                 [b["key"] for b in buckets])
    return out


@tool
def get_sample_documents(index: str, size: int = 3,
                          query_json: str = "", sort_json: str = "") -> str:
    """
    Retrieve sample documents from an index to understand its structure and content.
    Use this to explore what data looks like — see actual field values, understand
    the data model, and discover what questions can be answered.

    Args:
        index: Index name.
        size: Number of documents to retrieve (1–10, default 3).
        query_json: Optional ES query clause (JSON string) to filter documents.
                    Example: '{"term":{"action.keyword":"USER_REGISTER"}}'
        sort_json: Optional sort array (JSON string).
                   Example: '[{"timestamp":{"order":"desc"}}]'
    """
    size = max(1, min(int(size), 10))
    body: dict[str, Any] = {"size": size}
    if query_json:
        try:
            body["query"] = json.loads(query_json)
        except json.JSONDecodeError as exc:
            return f"ERROR: query_json is not valid JSON: {exc}"
    if sort_json:
        try:
            body["sort"] = json.loads(sort_json)
        except json.JSONDecodeError as exc:
            return f"ERROR: sort_json is not valid JSON: {exc}"

    result = _es_post(f"/{index}/_search", body)
    if isinstance(result, str):
        return result

    hits = result.get("hits", {}).get("hits", [])
    total = result.get("hits", {}).get("total", {})
    if not hits:
        return f"No documents found in '{index}'. Total: {total}"

    docs = [h.get("_source", {}) for h in hits]
    out = f"Sample documents from '{index}' (total matching: {total}):\n" + _fmt(docs)
    logger.debug("ES RESULT   get_sample_documents index={} returned={}", index, len(docs))
    return out


@tool
def search(index: str, body_json: str) -> str:
    """
    Execute a full Elasticsearch DSL search query.
    Use this for any query: filters, aggregations, ranges, bool combinations, etc.
    Use size=0 for pure aggregations (counts, stats, terms breakdowns).
    Use size>0 to retrieve matching documents.

    IMPORTANT: Only use field names that you have confirmed exist in the mapping
    via get_mapping. A must_not or filter on a non-existent field will silently
    match all documents — the total will equal the full index size, which is wrong.

    Args:
        index: Index name or pattern (e.g. 'dfs-audit', 'logs-*').
               Use empty string '' to search all indices.
        body_json: Complete ES _search body as a JSON string.
          Examples:
            Count by field:
              '{"size":0,"aggs":{"by_action":{"terms":{"field":"action.keyword","size":20}}}}'
            Filter by time range + get docs:
              '{"size":5,"query":{"range":{"timestamp":{"gte":"now-1h"}}},"sort":[{"timestamp":{"order":"desc"}}]}'
            Bool filter with aggregation:
              '{"size":0,"query":{"bool":{"filter":[{"term":{"action.keyword":"USER_REGISTER"}},{"range":{"timestamp":{"gte":"now-24h"}}}]}}}'
            Stats over a numeric field:
              '{"size":0,"aggs":{"cpu_stats":{"stats":{"field":"details.cpu_usage"}}}}'
            Failed operations (use actual field names from get_mapping):
              '{"size":0,"query":{"term":{"success":false}}}'
    """
    try:
        body = json.loads(body_json)
    except json.JSONDecodeError as exc:
        return f"ERROR: body_json is not valid JSON: {exc}"
    if not isinstance(body, dict):
        return "ERROR: body_json must be a JSON object, not an array or scalar."

    # Safety: reject scripts
    body_str_lower = json.dumps(body).lower()
    for forbidden in ("\"script\"", "'script'"):
        if forbidden in body_str_lower:
            return "ERROR: 'script' queries are not allowed in read-only mode."

    path = f"/{index}/_search" if index else "/_search"
    result = _es_post(path, body)
    if isinstance(result, str):
        return result

    hits_data = result.get("hits", {})
    hits = hits_data.get("hits", [])
    total = hits_data.get("total")
    took = result.get("took")
    aggs = result.get("aggregations")

    summary: dict[str, Any] = {
        "took_ms": took,
        "total": total,
        "returned_hits": len(hits),
    }
    if hits:
        summary["hits"] = [h.get("_source", {}) for h in hits[:20]]
    if aggs:
        summary["aggregations"] = aggs

    out = _fmt(summary)
    logger.debug("ES RESULT   search total={} returned={} has_aggs={}",
                 total, len(hits), bool(aggs))
    return out


# All tools exposed to the agent
ES_TOOLS = [list_indices, get_mapping, get_field_values, get_sample_documents, search]

# ---------------------------------------------------------------------------
# System prompt — minimal, no hardcoded domain knowledge
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an observability assistant with read-only access to an Elasticsearch cluster.
You have tools to explore and query it. Use them proactively to answer questions.

## Mandatory workflow — follow this every time

**RULE: You MUST call `get_mapping` for an index before using ANY field name in a \
`search` query, even if previous messages contain field names. Mappings must be \
verified in the current session.**

1. `list_indices` — discover what indices exist and their doc counts.
2. `get_mapping` — verify the EXACT field names and their types.
   - Never assume or invent a field name (e.g. `result`, `outcome`, `severity`).
   - The mapping is the only source of truth for field names.
3. `get_field_values` — check what values actually exist in a field before filtering.
   - Never guess values (e.g. `"FAILED"`, `"ERROR"`) — fetch them first.
4. `get_sample_documents` — look at real documents when you need to understand structure.
5. `search` — only after steps 2–3 confirm the field names and values.

## Sanity-checking results

After every `search`, verify the result makes sense:
- If `total` equals the full document count of the index, your filter probably
  matched everything — a non-existent field in `must_not` or `filter` matches all docs.
  → Re-check field names with `get_mapping` and retry.
- If `total` is 0 and you expected results, the field name or value is likely wrong.
  → Re-check with `get_mapping` and `get_field_values`, then retry.
- Do not report a result to the user until it passes the sanity check.

## Chaining queries

Use results from one query to drive the next:
- Found a suspicious total? Investigate with `get_field_values` or `get_sample_documents`.
- Got aggregation buckets? Drill into specific buckets with another `search`.
- Refine until you have a confident, correct answer.

## Rules

- Read-only: never use scripts or write operations.
- Time filter: use the `timestamp` field with range queries when questions are
  about recent activity. The dashboard time range is provided in the context.
- For text fields, always use `fieldname.keyword` for exact filtering and aggregations.
- Answer in the same language as the user's question.
- Format the final answer clearly — use markdown tables or lists when helpful.
- Do not expose raw query internals in your final answer unless the user asks.
"""

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="DAFS Grafana Assistant Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    logger.debug("Health check")
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(payload: QueryRequest) -> QueryResponse:
    logger.info(
        "Query received dashboard={} has_time_range={} history_turns={}",
        payload.context.dashboardTitle,
        bool(payload.context.from_ and payload.context.to),
        len(payload.history),
    )

    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENROUTER_API_KEY not set.")
    if not ES_PASSWORD:
        raise HTTPException(status_code=500, detail="ELASTIC_PASSWORD not set.")

    # Build context string injected into system message
    ctx_parts: list[str] = []
    if payload.context.dashboardTitle:
        ctx_parts.append(f"dashboard='{payload.context.dashboardTitle}'")
    if payload.context.from_ and payload.context.to:
        ctx_parts.append(f"time_range={payload.context.from_}..{payload.context.to}")
    context_text = ", ".join(ctx_parts) if ctx_parts else "no dashboard context"

    # Build LLM — points to OpenRouter via OpenAI-compatible API
    llm = ChatOpenAI(
        model=OPENROUTER_MODEL,
        openai_api_key=api_key,
        openai_api_base=OPENROUTER_BASE_URL,
        temperature=0.2,
        max_retries=2,
    )

    # Prompt template
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT + "\n\nDashboard context: {context}"),
        MessagesPlaceholder("chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])

    # Agent + executor
    agent = create_tool_calling_agent(llm, ES_TOOLS, prompt)
    executor = AgentExecutor(
        agent=agent,
        tools=ES_TOOLS,
        verbose=False,
        max_iterations=12,
        max_execution_time=120,
        handle_parsing_errors=True,
        return_intermediate_steps=True,
    )

    # Convert history to LangChain messages
    lc_history: list[HumanMessage | AIMessage] = []
    for h in payload.history:
        if h.role == "user":
            lc_history.append(HumanMessage(content=h.content))
        elif h.role == "assistant":
            lc_history.append(AIMessage(content=h.content))

    try:
        result = executor.invoke({
            "input": payload.query,
            "context": context_text,
            "chat_history": lc_history,
        })
    except Exception as exc:
        logger.exception("Agent execution failed")
        raise HTTPException(status_code=500, detail=f"Agent error: {exc}") from exc

    answer = result.get("output", "").strip() or "No response from agent."
    steps = result.get("intermediate_steps", [])
    tool_calls_made = len(steps)

    logger.info("Query answered tool_calls={} answer_len={}", tool_calls_made, len(answer))
    for i, (action, observation) in enumerate(steps):
        logger.debug("  step[{}] tool={} input={!r} → obs={!r}",
                     i + 1, action.tool,
                     str(action.tool_input)[:120],
                     str(observation)[:200])

    return QueryResponse(answer=answer, tool_calls_made=tool_calls_made)
