# DAFS Grafana AI Assistant Panel

Panel plugin para Grafana self-hosted con UI tipo asistente y backend en Python.

## Estructura

- `src/`: frontend del panel (React + TypeScript)
- `plugin.json`: manifiesto del plugin
- `backend/`: API FastAPI (`POST /query`)

## Build del plugin frontend

Desde `Auditing/grafana/plugin`:

```bash
npm install
npm run build
```

Esto genera `module.js` en la raíz del plugin.

## Ejecutar backend Python

```bash
docker build -t dafs-ai-backend ./backend
docker run --rm -p 8008:8008 dafs-ai-backend
```

Healthcheck:

```bash
curl http://localhost:8008/health
```

## Variables requeridas en .env (Auditing/.env)

Añade estas variables para usar OpenRouter + Elasticsearch readonly:

```env
OPENROUTER_API_KEY=tu_api_key
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_MODEL=gpt-4.1-mini

# opcional para TLS a ES (si quieres verificar certificado)
ES_VERIFY_TLS=false
# ES_CA_CERT_PATH=/app/certs/ca/ca.crt
```

El backend usa `ES_URL`, `ES_PORT`, `ELASTIC_USER` y `ELASTIC_PASSWORD` del mismo `.env` para consultar Elasticsearch con `_search` en modo solo lectura.

## Integración con Grafana self-hosted

1. Montar `Auditing/grafana/plugin` en `/var/lib/grafana/plugins/dafs-ai-assistant-panel`.
2. Permitir unsigned plugin:
   - `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=dafs-ai-assistant-panel`
3. Reiniciar Grafana.

## Uso

1. En un dashboard, agrega el panel **DAFS AI Assistant Panel**.
2. Ubícalo en la columna derecha del dashboard.
3. En opciones del panel, confirma `Backend URL` (`http://localhost:8008`).
4. Escribe una consulta y pulsa **Preguntar**.
