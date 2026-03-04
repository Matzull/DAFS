# Plan de implementación de OpenRaft en DAFS

## 1) Estado actual del proyecto (baseline)

Este plan parte del estado real de la rama `master`:

- `services/master` es **single-node** y concentra todo el metadata state.
- El estado de metadatos vive en memoria (`MasterState`) y se persiste vía WAL propio JSON (`WalRecord`/`WalOperation`) en `services/master/master.wal`.
- El master recupera estado en arranque con `replay_wal(...)`.
- Las operaciones mutables principales son: `CreateFile`, `DeleteFile`, `RenameFile`, `SetFileStatus`, `RegisterDatanode`.
- `services/datanode` envía heartbeat a **una sola** dirección `metadata_node_address`.
- `client/fscore` ya tiene `master_endpoints: Vec<String>`, pero hoy usa `master_endpoints[0]` en todas las llamadas.
- `services/master/Cargo.toml` incluye `raft = "0.7.0"`, pero no se usa en código.

Conclusión: la base ya tiene semántica de log y replay, lo que facilita migrar a un state machine de Raft, pero todavía no hay consenso distribuido ni redirección a líder.

---

## 2) Objetivo funcional

Implementar un clúster de metadata con OpenRaft donde:

- Solo el líder acepta escrituras de metadata.
- Seguidores replican log y aplican el mismo state machine.
- El cliente y datanodes toleran failover de líder.
- Se elimina el WAL propio como fuente de verdad.

Fuera de alcance inicial (fase 1):

- Repartir bloques de datos entre masters (Raft será solo para metadata).
- Migrar `users.db` (auth SQLite) a consenso distribuido desde el día 1.

---

## 3) Diseño objetivo de alto nivel

### 3.1 Componentes nuevos en `services/master`

1. **Raft Node Runtime**
   - Inicializa `openraft::Raft<...>`.
   - Administra election, replication y membership.

2. **Raft Network**
   - Transporte RPC entre masters (AppendEntries/Vote/InstallSnapshot).
   - Implementación sobre tonic (nuevo servicio interno de clúster).

3. **Raft Storage**
   - `RaftLogStorage` + `RaftStateMachine` + `RaftSnapshotBuilder`.
   - Persistencia durable (recomendado RocksDB o Sled).

4. **State Machine de Metadata**
   - Aplica comandos de dominio (create/delete/rename/status/datanode register).
   - Reemplaza `replay_wal` como mecanismo de verdad.

5. **Metadata gRPC Facade**
   - `MetadataService` queda como API pública.
   - En escrituras: `client_write(...)` hacia Raft.
   - En lecturas: linearizable (`ensure_linearizable`) o read-index según costo.

### 3.2 Scope de consistencia

- Consensuar en Raft:
  - Namespace de archivos (`files` trie + block metadata).
  - Registro de datanodes activos (al menos identidad/dirección).
  - `next_node_id`.
- Mantener fuera de Raft en fase inicial:
  - Cola interna de auditoría y workers (GC/replicator).
  - Estado transitorio de heartbeat de alta frecuencia (timestamp puede quedarse local o agregarse por expiración no replicada).

Regla práctica: todo lo que cambia el resultado de `CreateFile/GetFileMetadata/ListDirectory` debe estar en state machine replicada.

---

## 4) Refactor del modelo de dominio

Crear una capa explícita de comandos/eventos para evitar acoplar RPC a OpenRaft:

- `enum MetadataCommand` (input de Raft log):
  - `CreateFile { ... }`
  - `DeleteFile { ... }`
  - `RenameFile { ... }`
  - `SetFileStatus { ... }`
  - `RegisterDatanode { ... }`
- `enum MetadataResponse` para resultados de aplicación.

Mapeo desde código actual:

- `WalOperation` → `MetadataCommand` (mismo dominio, distinta mecánica).
- `MasterState` seguirá existiendo, pero su mutación quedará encapsulada dentro de `apply(command)` del state machine.

---

## 5) Cambios por crate/archivo

## 5.1 `services/master`

### A. Dependencias

- Reemplazar `raft = "0.7.0"` por OpenRaft:
  - `openraft`
  - backend persistente (`rocksdb` o `sled`, elegir uno y estandarizar).
- Mantener `tonic`, `tokio`, `serde`, etc.

### B. Estructura sugerida

Agregar módulos nuevos:

- `services/master/src/raft/types.rs`
- `services/master/src/raft/network.rs`
- `services/master/src/raft/storage.rs`
- `services/master/src/raft/state_machine.rs`
- `services/master/src/raft/admin.rs`

Reducir `main.rs` (hoy monolítico) separando:

- Arranque/config
- API pública `MetadataService`
- Workers auxiliares

### C. Config nueva

Extender `config.toml` del master con:

- `node_id`
- `raft_listen_address` (tráfico Raft interno)
- `metadata_service_address` (RPC pública actual)
- `raft_data_dir`
- `initial_members` (lista de nodos bootstrap)
- timeouts de election/heartbeat Raft

### D. Flujo de escritura

Cada RPC mutable del `MetadataService`:

1. Validaciones y auth actuales.
2. Construye `MetadataCommand`.
3. Ejecuta `raft.client_write(command)`.
4. Devuelve respuesta de dominio.
5. Si no es líder, responde `failed_precondition` o `unavailable` con hint de líder.

### E. Flujo de lectura

RPC de lectura (`GetFileMetadata`, `ListDirectory`):

- opción segura inicial: exigir líder + `ensure_linearizable`.
- optimización posterior: follower reads con lease/read-index.

### F. WAL legado

- Fase de transición: mantener lectura WAL para migrar estado inicial.
- Objetivo final: desactivar `wal_writer` y `replay_wal`.

## 5.2 `common` + `proto/dfs.proto`

Agregar API mínima de control y descubrimiento:

- `GetMetadataLeader` o `GetClusterInfo` (para cliente y datanode).
- (Opcional) `NotLeader` payload explícito en respuestas para redirección limpia.

Agregar servicio interno de clúster (si se usa tonic para red Raft):

- `RaftService` con RPCs requeridas por OpenRaft network adapter.

## 5.3 `client/fscore` + `client/cli`

Hoy usa siempre `master_endpoints[0]`. Cambiar a estrategia:

1. intentar endpoint conocido líder,
2. en error `not leader`, seguir hint,
3. fallback round-robin sobre `master_endpoints`.

Aplicar a: register/login/create/get/list/rename/delete/set_status.

## 5.4 `services/datanode`

Sustituir `metadata_node_address: String` por lista (`metadata_node_endpoints: Vec<String>`) o discovery vía `GetMetadataLeader`.

Heartbeat loop:

- si endpoint falla o no es líder, reintentar con hint/lista.
- mantener backoff exponencial ya existente.

---

## 6) Plan de ejecución por fases

## Fase 0 — Preparación y limpieza (1-2 días)

- Quitar dependencia `raft` no usada.
- Introducir módulos y tipos de dominio (`MetadataCommand`, `MetadataResponse`) sin cambiar comportamiento.
- Añadir tests unitarios de aplicación de comandos en memoria.

**Done when:** compila igual y pasa test suite actual.

## Fase 1 — Storage y state machine OpenRaft (3-5 días)

- Implementar storage persistente de OpenRaft.
- Implementar apply de comandos sobre `MasterState`.
- Implementar snapshot builder.

**Done when:** tests de storage/state machine pasan (append/apply/snapshot/restart).

## Fase 2 — Red Raft entre masters (3-5 días)

- Definir `RaftNetworkFactory` + `RaftNetwork`.
- Exponer servicio interno de clúster.
- Arrancar nodo Raft con config mínima.

**Done when:** 3 masters en local eligen líder y replican no-op.

## Fase 3 — Integrar MetadataService con Raft (3-5 días)

- Reescribir handlers mutables para usar `client_write`.
- Lecturas linealizables básicas.
- Propagar `leader_hint` en errores.

**Done when:** pruebas CRUD con failover mantienen consistencia.

## Fase 4 — Migración del WAL legado (2-3 días)

- Crear migrador one-shot: `replay_wal` → snapshot/state machine inicial.
- Flag de compatibilidad para rollback (`--legacy-wal-mode`).
- Desactivar escritor WAL por defecto.

**Done when:** nodo arranca desde estado previo WAL y continúa bajo OpenRaft.

## Fase 5 — Cliente y datanode HA-aware (2-4 días)

- Cliente: selección de líder + retry entre endpoints.
- Datanode: heartbeats al líder con failover.

**Done when:** caída del líder no rompe upload/download/heartbeat tras reconvergencia.

## Fase 6 — Membership y operaciones de clúster (2-4 días)

- Endpoints/admin para add learner, promote, remove.
- Documentar playbooks operativos.

**Done when:** escala de 1→3 nodos y 3→1 controlada y testeada.

## Fase 7 — Hardening y observabilidad (continuo)

- Métricas: término, commit index, applied index, lag por follower.
- Alertas: election storms, snapshot failure, quorum loss.
- Chaos tests básicos.

---

## 7) Estrategia de migración recomendada

1. **Milestone A (shadow mode local):** Raft activo en 1 nodo, sin cambiar cliente.
2. **Milestone B (cluster mode):** 3 masters, cliente/datanode todavía apuntando a uno + redirección.
3. **Milestone C (HA real):** cliente y datanode multi-endpoint con leader discovery.
4. **Milestone D (cleanup):** eliminar WAL legacy y código muerto.

Esta secuencia reduce riesgo porque separa consenso interno de cambios de edge (cliente/datanode).

---

## 8) Riesgos específicos de este repositorio y mitigaciones

- **`main.rs` muy grande en master**: riesgo alto de regresión.
  - Mitigar con extracción incremental de módulos antes de lógica Raft.

- **Heartbeats muy frecuentes**: no deben inundar log Raft.
  - Replicar solo cambios semánticos (registro/cambio de endpoint), no cada timestamp.

- **Auth local en SQLite (`users.db`)**: posible divergencia entre masters.
  - Fase inicial: login/register solo en líder y replicar eventos de usuario o mover auth después a state machine.

- **Cliente actualmente pegado al primer endpoint**.
  - Añadir wrapper único de llamada metadata con retry + redirección para no duplicar lógica.

---

## 9) Plan de testing

## 9.1 Unit tests

- Apply de cada `MetadataCommand`.
- Serialización/deserialización de entries/snapshot.
- Compatibilidad de migración WAL → snapshot.

## 9.2 Integration tests

- Cluster 3 nodos:
  - elección inicial,
  - write/read consistente,
  - failover por caída del líder,
  - rejoin de nodo rezagado.

## 9.3 End-to-end actuales

Reutilizar scripts en `testing/` y tests en `tests/` para validar:

- upload/download/list/rename/delete,
- heartbeats datanode,
- replicación de bloques tras operaciones metadata.

---

## 10) Checklist de implementación (orden sugerido)

1. `services/master`: introducir `MetadataCommand` + `apply()` en módulo separado.
2. `services/master`: implementar storage OpenRaft persistente.
3. `services/master`: implementar network adapter y arranque de Raft.
4. `services/master`: mover RPC mutables a `client_write`.
5. `common/proto`: agregar discovery (`GetMetadataLeader`/`GetClusterInfo`).
6. `client/fscore`: reemplazar `master_endpoints[0]` por selector de líder.
7. `services/datanode`: heartbeat HA-aware con múltiples endpoints.
8. Migrador WAL legacy y bandera de rollback.
9. Suite de pruebas de failover.
10. Eliminar WAL writer legacy por defecto.

---

## 11) Resultado esperado al finalizar

- Metadata altamente disponible (quorum) con OpenRaft.
- Consistencia fuerte en operaciones de namespace.
- Recuperación automática ante caída del master líder.
- Cliente y datanodes tolerantes a failover.
- Menos riesgo de pérdida de metadata frente al modelo single-master actual.
