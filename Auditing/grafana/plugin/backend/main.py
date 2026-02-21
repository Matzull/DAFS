import json
import os
import sys
from typing import Any, Optional

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
from loguru import logger
from pydantic import BaseModel, Field


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


OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4.1-mini")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def _configure_logger() -> None:
    valid_levels = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
    level = LOG_LEVEL if LOG_LEVEL in valid_levels else "INFO"
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        enqueue=True,
        backtrace=False,
        diagnose=False,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}",
    )
    logger.info("Logger configurado con LOG_LEVEL={}", level)


def _build_es_base_url() -> str:
    es_url = os.getenv("ES_URL", "https://es01").rstrip("/")
    if es_url.endswith(":9200"):
        return es_url

    es_port = os.getenv("ES_PORT", "9200")
    if es_url.rsplit(":", 1)[-1].isdigit():
        return es_url

    return f"{es_url}:{es_port}"


ES_BASE_URL = _build_es_base_url()
ES_USER = os.getenv("ELASTIC_USER", "elastic")
ES_PASSWORD = os.getenv("ELASTIC_PASSWORD", "")
ES_VERIFY_TLS = os.getenv("ES_VERIFY_TLS", "false").lower() == "true"
ES_CA_CERT_PATH = os.getenv("ES_CA_CERT_PATH", "")


_configure_logger()


READONLY_ES_SEARCH_TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "es_search",
        "description": (
            "Ejecuta una búsqueda de solo lectura en Elasticsearch. "
            "Puedes incluir query DSL, aggregations, sort y size. "
            "Para contar documentos o hacer agregaciones usa size=0."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "index": {
                    "type": "string",
                    "description": "Índice o patrón de índice (por ejemplo logs-*). Opcional.",
                },
                "query": {
                    "type": "object",
                    "description": (
                        "Query clause de Elasticsearch (e.g. {\"match_all\":{}}, {\"range\":{...}}, "
                        "{\"bool\":{...}}). NO incluyas la clave 'query' exterior; pon directamente "
                        "el clause. Omite si quieres match_all."
                    ),
                },
                "aggs": {
                    "type": "object",
                    "description": "Aggregations de Elasticsearch (mismo formato que el campo 'aggs' del body _search).",
                },
                "sort": {
                    "description": "Campo sort del body _search (array u objeto).",
                },
                "size": {
                    "type": "integer",
                    "description": "Número máximo de hits a devolver (0-100). Usa 0 para agregaciones puras.",
                    "minimum": 0,
                    "maximum": 100,
                },
            },
            "required": [],
        },
    },
}


def _es_verify() -> bool | str:
    if ES_VERIFY_TLS and ES_CA_CERT_PATH:
        return ES_CA_CERT_PATH
    return ES_VERIFY_TLS


def _ensure_no_forbidden_query_keys(value: Any) -> None:
    forbidden = {"script", "stored_script"}
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in forbidden:
                raise HTTPException(status_code=400, detail="El query DSL incluye campos no permitidos para modo readonly.")
            _ensure_no_forbidden_query_keys(nested)
    elif isinstance(value, list):
        for nested in value:
            _ensure_no_forbidden_query_keys(nested)


def _run_es_search(args: dict[str, Any]) -> dict[str, Any]:
    index = str(args.get("index", "")).strip()
    query = args.get("query")
    aggs = args.get("aggs") or args.get("aggregations")
    sort = args.get("sort")
    size = int(args.get("size", 20))
    size = max(0, min(size, 100))

    logger.debug("es_search args recibidos index={!r} query={!r} aggs={} sort={} size={}",
                 index, query, bool(aggs), bool(sort), size)

    # Coerce query string → dict
    if isinstance(query, str):
        stripped = query.strip()
        if stripped:
            try:
                query = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"El campo 'query' no es un JSON válido: {exc}",
                ) from exc
        else:
            query = None

    if query is None:
        query_clause: dict[str, Any] = {"match_all": {}}
    elif not isinstance(query, dict):
        raise HTTPException(
            status_code=400,
            detail=f"'query' debe ser un objeto JSON, recibido tipo {type(query).__name__}.",
        )
    else:
        query_clause = query

    # If query_clause already looks like a full ES body, use it directly;
    # otherwise wrap it under the 'query' key.
    _ES_BODY_KEYS = {"query", "aggs", "aggregations", "sort", "from", "_source",
                     "highlight", "post_filter", "knn", "track_total_hits",
                     "fields", "collapse", "rescore", "explain", "timeout"}
    if query_clause.keys() & _ES_BODY_KEYS:
        body: dict[str, Any] = dict(query_clause)
    else:
        logger.debug("es_search: envolviéndolo en {{query: ...}}")
        body = {"query": query_clause}

    # Merge aggs/sort provided as sibling args (model often puts them at top level)
    if aggs and "aggs" not in body and "aggregations" not in body:
        body["aggs"] = aggs
    if sort and "sort" not in body:
        body["sort"] = sort

    body["size"] = size

    _ensure_no_forbidden_query_keys(body)

    if not ES_PASSWORD:
        raise HTTPException(status_code=500, detail="Falta ELASTIC_PASSWORD en entorno del backend.")

    path = f"/{index}/_search" if index else "/_search"
    logger.debug("Ejecutando es_search readonly path={} size={} has_aggs={} verify_tls={}",
                 path, size, "aggs" in body or "aggregations" in body, ES_VERIFY_TLS)

    with httpx.Client(timeout=30.0, verify=_es_verify()) as client:
        response = client.post(
            f"{ES_BASE_URL}{path}",
            auth=(ES_USER, ES_PASSWORD),
            headers={"Content-Type": "application/json"},
            json=body,
        )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail=f"Elasticsearch respondió {response.status_code}: {response.text[:500]}",
            )
        data = response.json()

    hits = data.get("hits", {}).get("hits", [])
    summary: dict[str, Any] = {
        "took": data.get("took"),
        "total": data.get("hits", {}).get("total"),
        "returned_hits": len(hits),
        "hits": hits[:20],
    }
    # Include aggregations in the summary if present
    if "aggregations" in data:
        summary["aggregations"] = data["aggregations"]
    logger.debug("es_search completado took={} total={} returned_hits={} has_aggs={}",
                 summary.get("took"), summary.get("total"),
                 summary.get("returned_hits"), "aggregations" in summary)
    return summary


def _openrouter_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _fetch_es_indices() -> str:
    """Return a compact string listing all non-system ES indices with their doc counts."""
    try:
        with httpx.Client(timeout=10.0, verify=_es_verify()) as client:
            response = client.get(
                f"{ES_BASE_URL}/_cat/indices?h=index,docs.count&s=index",
                auth=(ES_USER, ES_PASSWORD),
            )
            if response.status_code >= 400:
                logger.warning("No se pudo obtener índices ES status={}", response.status_code)
                return ""
            lines = []
            for line in response.text.strip().splitlines():
                parts = line.split()
                if not parts:
                    continue
                idx = parts[0]
                if idx.startswith("."):
                    continue
                docs = parts[1] if len(parts) > 1 else "?"
                lines.append(f"  - {idx} ({docs} docs)")
            result = "\n".join(lines)
            logger.debug("Índices ES descubiertos:\n{}", result)
            return result
    except Exception as exc:
        logger.warning("Error al listar índices ES: {}", exc)
        return ""


def _fetch_es_schema() -> str:
    """Return field names and types for each non-system index, to help the LLM build correct queries."""
    try:
        with httpx.Client(timeout=10.0, verify=_es_verify()) as client:
            resp = client.get(
                f"{ES_BASE_URL}/*/_mapping",
                auth=(ES_USER, ES_PASSWORD),
            )
            if resp.status_code >= 400:
                return ""
            mappings = resp.json()

        sections: list[str] = []
        for idx, idx_data in sorted(mappings.items()):
            if idx.startswith("."):
                continue
            props: dict[str, Any] = idx_data.get("mappings", {}).get("properties", {})
            if not props:
                continue
            field_lines: list[str] = []
            _collect_fields(props, prefix="", out=field_lines, max_depth=3)
            sections.append(f"  {idx}:\n" + "\n".join(f"    {f}" for f in field_lines))

        result = "\n".join(sections)
        logger.debug("Schema ES descubierto:\n{}", result)
        return result
    except Exception as exc:
        logger.warning("Error al obtener schema ES: {}", exc)
        return ""


def _collect_fields(
    props: dict[str, Any],
    prefix: str,
    out: list[str],
    max_depth: int,
    depth: int = 0,
) -> None:
    if depth >= max_depth:
        return
    for field, meta in props.items():
        full = f"{prefix}{field}" if prefix else field
        ftype = meta.get("type", "object")
        # include .keyword sub-field hint for text fields
        keyword_hint = ""
        if ftype == "text" and "keyword" in meta.get("fields", {}):
            keyword_hint = f"  (use {full}.keyword for aggregations/sorting)"
        out.append(f"{full}: {ftype}{keyword_hint}")
        nested = meta.get("properties")
        if isinstance(nested, dict):
            _collect_fields(nested, f"{full}.", out, max_depth, depth + 1)


def _extract_text_from_message(message: dict[str, Any]) -> str:
    content = message.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text", "")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return ""


app = FastAPI(title="DAFS Grafana Assistant Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    logger.debug("Health check solicitado")
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(payload: QueryRequest) -> QueryResponse:
    logger.info(
        "Nueva query recibida dashboard={} has_time_range={} history_turns={}",
        payload.context.dashboardTitle,
        bool(payload.context.from_ and payload.context.to),
        len(payload.history),
    )
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        logger.error("OPENROUTER_API_KEY no está definido en entorno")
        raise HTTPException(status_code=500, detail="Falta OPENROUTER_API_KEY en entorno del backend.")

    context_parts = []
    if payload.context.dashboardTitle:
        context_parts.append(f"dashboard='{payload.context.dashboardTitle}'")
    if payload.context.from_ and payload.context.to:
        context_parts.append(f"range={payload.context.from_}..{payload.context.to}")

    context_text = ", ".join(context_parts) if context_parts else "sin contexto"
    system_prompt = (
        payload.systemPrompt
        or "Eres un asistente de observabilidad. Cuando necesites datos, usa es_search en modo readonly."
    )

    es_indices = _fetch_es_indices()
    es_schema = _fetch_es_schema()

    indices_section = (
        f"\nÍndices disponibles en Elasticsearch (usa exactamente estos nombres):\n{es_indices}"
        if es_indices else ""
    )
    schema_section = (
        f"\nEsquema de campos por índice (usa EXACTAMENTE estos nombres de campo):\n{es_schema}"
        if es_schema else ""
    )

    # Build message list: system → history turns → current user query
    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": (
                f"{system_prompt}\n"
                "Reglas: solo consultas de lectura en Elasticsearch; no generes acciones de escritura. "
                f"Contexto dashboard: {context_text}."
                f"{indices_section}"
                f"{schema_section}"
            ),
        },
    ]
    for h in payload.history:
        if h.role in ("user", "assistant"):
            messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": payload.query})

    MAX_ITERATIONS = 4
    total_tool_calls = 0

    with httpx.Client(timeout=60.0) as client:
        for iteration in range(MAX_ITERATIONS):
            logger.debug("Agentic loop iteración={}", iteration + 1)

            response = client.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers=_openrouter_headers(api_key),
                json={
                    "model": OPENROUTER_MODEL,
                    "messages": messages,
                    "tools": [READONLY_ES_SEARCH_TOOL],
                    "tool_choice": "auto",
                    "temperature": 0.2,
                },
            )

            if response.status_code >= 400:
                logger.error("Error OpenRouter it={} status={} body={}", iteration + 1, response.status_code, response.text[:500])
                raise HTTPException(
                    status_code=502,
                    detail=f"OpenRouter respondió {response.status_code}: {response.text[:500]}",
                )

            data = response.json()
            choice = data.get("choices", [{}])[0]
            message = choice.get("message", {})
            tool_calls = message.get("tool_calls") or []
            finish_reason = choice.get("finish_reason", "")

            logger.debug("it={} finish_reason={} tool_calls={}", iteration + 1, finish_reason, len(tool_calls))

            if not tool_calls:
                # Model is done — extract final answer
                answer = _extract_text_from_message(message).strip()
                if not answer:
                    answer = "No se recibió contenido de respuesta del modelo."
                logger.info(
                    "Respuesta generada it={} tool_calls_total={}",
                    iteration + 1,
                    total_tool_calls,
                )
                return QueryResponse(answer=answer, tool_calls_made=total_tool_calls)

            # Execute all tool calls and append results
            total_tool_calls += len(tool_calls)
            tool_outputs: list[dict[str, Any]] = []

            for tool_call in tool_calls:
                function_data = tool_call.get("function", {})
                function_name = function_data.get("name")
                arguments_raw = function_data.get("arguments", "{}")
                logger.debug("tool_call it={} name={} arguments_raw={!r}", iteration + 1, function_name, arguments_raw)

                try:
                    arguments = json.loads(arguments_raw)
                except json.JSONDecodeError:
                    logger.warning("json.JSONDecodeError al parsear arguments_raw={!r}", arguments_raw)
                    arguments = {}

                if function_name != "es_search":
                    tool_result: dict[str, Any] = {"error": f"Tool no permitido: {function_name}"}
                else:
                    try:
                        tool_result = _run_es_search(arguments)
                    except HTTPException as exc:
                        logger.warning(
                            "Error controlado en es_search it={} status={} detail={}",
                            iteration + 1, exc.status_code, exc.detail,
                        )
                        tool_result = {
                            "error": exc.detail,
                            "status_code": exc.status_code,
                            "hint": "Revisa el índice y la estructura del query DSL e inténtalo de nuevo.",
                        }
                    except Exception as exc:
                        logger.exception("Error inesperado ejecutando es_search it={}", iteration + 1)
                        tool_result = {
                            "error": f"{type(exc).__name__}: {exc}",
                            "hint": "Inténtalo con una query más simple.",
                        }

                tool_outputs.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id"),
                    "content": json.dumps(tool_result, ensure_ascii=False),
                })

            # Append the assistant's tool-calling message and tool results to the conversation
            messages.append({
                "role": "assistant",
                "content": message.get("content"),
                "tool_calls": tool_calls,
            })
            messages.extend(tool_outputs)

        # Exhausted iterations — ask model to summarize with what it has
        logger.warning("Agentic loop agotó {} iteraciones, solicitando resumen final", MAX_ITERATIONS)
        fallback = client.post(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            headers=_openrouter_headers(api_key),
            json={
                "model": OPENROUTER_MODEL,
                "messages": messages + [{"role": "user", "content": "Resume lo que has encontrado hasta ahora."}],
                "temperature": 0.2,
            },
        )
        if fallback.status_code < 400:
            fb_message = fallback.json().get("choices", [{}])[0].get("message", {})
            answer = _extract_text_from_message(fb_message).strip()
        else:
            answer = "Se superó el límite de iteraciones y no se pudo obtener una respuesta final."

    return QueryResponse(answer=answer, tool_calls_made=total_tool_calls)
