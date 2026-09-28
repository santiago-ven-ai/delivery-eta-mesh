# Study Guide — delivery-eta-mesh

Complementa [`RUNBOOK.md`](RUNBOOK.md) (cómo correrlo) y
[`LEARNING_BUILD.md`](LEARNING_BUILD.md) (cómo se construyó). Esta guía es
para entender cada componente, cómo interactúan, qué pasa cuando algo falla
y cómo defender cada número del README en una entrevista técnica.

Todas las rutas son relativas a la raíz del repo. Toda métrica citada está
en la tabla "Measured in this repo" del `README.md`.

---

## 1. Resumen en 3 niveles

**Una frase:** Malla de recálculo de ETA para dispatch de delivery, que
sobrevive a GPS tardío/desordenado y a restaurantes que concentran la
mayoría del tráfico.

**30 segundos (EN):** *"An event-driven ETA mesh for food delivery. A
Spring Boot worker on Fargate scores ETAs in real time off an SQS queue,
writing only the current value — never history. Late or reordered GPS
pings don't corrupt that live state; a nightly PySpark replay applies a
10-minute watermark to decide what counts as a correction versus a live
update, and salts the restaurant_id shuffle key because 5% of restaurants
generate 60% of orders."*

**2 minutos (EN):** *"Stale ETAs from late GPS data cause bad courier
assignments and refund costs. I built a two-speed system: a stateless
Spring Boot worker on Fargate that's the hot path — polls SQS, applies a
fixed scoring heuristic (prep time + distance/speed), and overwrites the
current ETA per order_id, so redelivery is safe by construction, not by
accident. Late GPS pings (11.4% of the synthetic run) never touch that
live state — they're watermarked out and handled only in a nightly PySpark
replay, which also has to deal with the real skew problem: 5% of
restaurants generate 60% of orders, which would otherwise dump most of the
shuffle onto one Spark partition. I salted the key, verified per-restaurant
totals still match exactly between the naive and salted paths, and measured
partition imbalance drop from 11.5x to 1.7x — not a wall-clock number,
because on a 2-core local Spark session a timing number at that scale would
be noise, not signal. I also computed the real Fargate-vs-Lambda cost
crossover instead of asserting one was cheaper by default."*

---

## 2. Mapa de componentes

| Componente | Archivo | Responsabilidad | Input → Output | Por qué existe / por qué esa tecnología |
|---|---|---|---|---|
| Generador de dispatch | `src/ingestion/data_gen.py` | Sintetiza un día de `order_placed` + `courier_gps`, con 5% de restaurantes concentrando 60% de órdenes y 12% de GPS "tardío" | `--hours --restaurants --orders-per-hour` → JSONL | Sin skew ni tardanza inyectados no hay nada que probar — son los dos problemas reales que este repo resuelve |
| Publisher | `src/ingestion/publisher.py` | Publica solo `order_placed` a SNS (los `courier_gps` van directo al replay batch, no al hot path) | JSONL → mensajes SNS | El hot path solo necesita saber que existe una orden para empezar a estimar; el GPS es para el replay nocturno |
| Worker de scoring | `src/worker/src/main/java/com/portfolio/etaworker/EtaScoringWorker.java` | Consume SQS cada 1s, calcula ETA con una heurística fija (prep 12 min + distancia/22 km/h), sobreescribe `eta-current` por `order_id` | Mensaje SQS → fila en DynamoDB | **Por qué Java/Spring Boot:** consumer SQS de alto throughput y larga duración — el JVM con connection pooling maduro rinde mejor que arrancar procesos Python fríos para polling continuo |
| Replay nocturno | `src/transformation/replay.py` | Aplica watermark a GPS tardío, calcula conteos naive vs. salted por restaurante, verifica que coincidan, escribe agregados a Parquet | Eventos S3 → agregados Parquet + reporte de balance | El batch es donde se puede pagar el costo de reconciliar todo el día; el hot path no puede esperar a Spark |
| Despliegue ECS | `src/orchestration/deploy_ecs.py` | Construye la imagen del worker, la sube a ECR real, plantillea la task definition y la registra/corre en ECS | Dockerfile + template → task ECS corriendo | Demuestra el ciclo real build→push→pull→run, no solo un contenedor local con el mismo nombre de imagen |
| Warehouse | `src/utils/warehouse.py` | DuckDB leyendo los agregados Parquet directo de S3, stand-in de Redshift | Consulta SQL → filas | Mismo patrón de acceso que Redshift Spectrum sin correr un cluster MPP |
| API de serving | `src/serving/api.py` | FastAPI: ETA actual de una orden (lee `eta-current` en vivo), MAE diario (calculado en vivo, no del agregado persistido) | HTTP GET → JSON | `/accuracy/daily` deliberadamente no lee el agregado nocturno — calcula MAE en vivo desde `eta-current` + eventos, para que el número siempre refleje el estado actual |
| Comparador de costo | `scripts/cost_compare.py` | Calcula el costo real Fargate vs. Lambda al volumen dado, y el punto de cruce | `--events-per-month` → tabla de costo + crossover | Un número de costo memorizado se vuelve mentira en cuanto cambian los precios de AWS; esto se recalcula en vivo |

---

## 3. Recorrido de un evento

```
data_gen.py → JSONL: order_placed (60% concentrado en 5% de restaurantes)
                    + courier_gps (12% marcado "tardío")
      |
      v
publisher.py --SOLO order_placed--> SNS "dispatch-events"
      |
      v
SQS "eta-scoring-queue" (+ DLQ)
      |
      v
EtaScoringWorker.java (Spring Boot, Fargate) -- poll cada 1s --
      |
      v
  ETA = 12 min prep + (distance_km / 22 km/h) * 60
      |
      v
  PutItem en DynamoDB "eta-current"  (mismo order_id -> overwrite, nunca fila nueva)

(todos los eventos crudos también caen a S3, particionados)
      |
      v
replay.py (PySpark, nocturno)
      |
      +-- apply_watermark(gps): arrival_delay > 10 min -> "late" (solo corrige historial)
      |                          arrival_delay <= 10 min -> "on_time" (ya vivo en eta-current)
      |
      +-- naive_order_counts(): groupBy directo por restaurant_id -> partición caliente
      +-- salted_order_counts(): salt aleatorio -> agregado parcial -> re-agregado
      |         |
      |         v
      |   verificación: totales naive == totales salted (si no, RuntimeError)
      |
      v
S3 dispatch-agg/order_counts (Parquet)

api.py :: GET /eta/{order_id}        lee eta-current en vivo (DynamoDB)
api.py :: GET /accuracy/daily        calcula MAE en vivo (eta-current + events), no el agregado
```

**El punto que hay que poder explicar sin dudar:** el worker **nunca** toca
historial — solo escribe el ETA actual. La corrección de eventos tardíos
ocurre exclusivamente en el replay nocturno. Esa separación es intencional:
mezclar "vivo" y "corrección histórica" en la misma escritura haría que el
ETA que ya vio un cliente cambiara silenciosamente después.

---

## 4. Interacciones y contratos entre componentes

| Frontera | Garantía | Por qué |
|---|---|---|
| SQS → worker → `eta-current` | Redelivery-safe (idempotente por sobreescritura) | Un `PutItem` con el mismo `order_id` sobreescribe, nunca duplica — probado con reentrega real de SQS |
| GPS on-time vs. late | On-time actualiza estado vivo; late solo corrige en el replay nocturno | ADR 0001: sobreescribir siempre haría oscilar el ETA con paquetes rezagados; dropear tardíos perdería la corrección para siempre |
| `naive_order_counts` vs. `salted_order_counts` | Deben coincidir exactamente por restaurante | El salting es una optimización de *distribución*, no puede cambiar el resultado de negocio — `replay.py` lanza `RuntimeError` si difieren |
| `/accuracy/daily` | Calcula MAE en vivo, no lee el agregado persistido | El agregado nocturno puede estar desactualizado; el endpoint de serving siempre refleja el estado actual de `eta-current` + eventos |
| Worker (Fargate) ↔ ECR | Imagen realmente empujada y jalada, no un tag local reusado | `deploy_ecs.py` hace `docker push` real y compara el digest de `ecr describe-images` contra `docker inspect` local para confirmar que no es una imagen vieja con el mismo nombre |

---

## 5. Modos de falla — "¿qué pasa si...?"

| Si esto pasa... | ...entonces | Dónde se ve / se prueba |
|---|---|---|
| El worker recibe el mismo `order_id` dos veces (SQS at-least-once) | `PutItem` sobreescribe, queda 1 fila, no 2 | `tests/integration/test_worker_idempotency.py` |
| Un GPS ping llega con `arrival_delay_min > 10` | No actualiza `eta-current`; se marca "late" y solo se usa en el replay nocturno | `apply_watermark()` en `replay.py`; 545/4800 pings (11.4%) en la corrida medida |
| `salted_order_counts` y `naive_order_counts` no coinciden para algún restaurante | `replay.py` lanza `RuntimeError` explícito antes de escribir Parquet — nunca escribe un agregado silenciosamente incorrecto | Bloque de verificación en `main()` de `replay.py` |
| El procesamiento de un mensaje SQS lanza una excepción en el worker | El mensaje **no** se borra de la cola; SQS lo reentrega tras el visibility timeout, y como el `PutItem` es un overwrite, la reentrega es segura | `catch (Exception e)` en `pollAndScore()`, comentario explícito en el código |
| Se necesita escalar el worker por carga | Hoy es un conteo fijo de workers, no autoscaling; la mejora identificada (no implementada) es atar el autoscaling a `ApproximateNumberOfMessages` de la cola | `docs/INTERVIEW_PREP.md` §"What would you change for production?" |
| Se intenta correr como ECS Service en vez de un RunTask puntual | Solo parcialmente validado contra MiniStack — limitación identificada y documentada, no oculta | README, sección "Emulated vs. real" |
| Cambia `WATERMARK_MINUTES` o `SALT_BUCKETS` sin actualizar specs/tests | Invalida los % de tardíos y el ratio de imbalance publicados en el README hasta volver a medir | `CLAUDE.md` §5 |

---

## 6. Conceptos senior — qué son, cómo aparecen aquí, cuándo NO usarlos

### Watermarking de eventos tardíos
**Qué es:** una ventana explícita de tolerancia después de la cual un
evento fuera de orden deja de tratarse como "actualización en vivo" y pasa
a ser "corrección histórica".
**Aquí:** `WATERMARK_MINUTES = 10` en `replay.py::apply_watermark`.
**Trade-off:** un ETA vivo más estable (no oscila con paquetes rezagados) a
cambio de que las correcciones reales solo aparezcan hasta el batch
nocturno.
**Cuándo NO usarlo:** si el dominio exige que *toda* señal, por tardía que
sea, se refleje inmediatamente en el estado vivo — ahí el watermark es
exactamente lo que no quieres.
**Cómo escalaría:** el mismo concepto es la base de Spark Structured
Streaming (`withWatermark`) o de Flink — aquí está implementado a mano en
un batch nocturno porque no hay streaming real en el emulador.

### Salting de una clave con skew
**Qué es:** repartir artificialmente las filas de una clave "caliente" en
varias sub-claves antes del shuffle, y re-agregar después.
**Aquí:** `salted_order_counts()` — `_salt = rand() * 8`, agregado parcial
por `(restaurant_id, _salt)`, luego re-agregado por `restaurant_id`.
**Trade-off:** un shuffle extra (dos pasadas de agregación) a cambio de que
ninguna partición concentre el trabajo de un solo restaurante caliente.
**Cuándo NO usarlo:** si la clave no tiene skew real, el salting solo
añade una pasada de shuffle innecesaria.
**El detalle honesto que hay que decir sin que lo pregunten:** en un
`local[2]` de una sola máquina no hay señal confiable de mejora de
wall-clock — lo que se mide y reporta es el balance de particiones
(11.52x → 1.7x), que es el mecanismo real que el salting arregla, no un
número de tiempo que sería ruido a esa escala.
**Verificación de corrección, no solo de rendimiento:** los totales por
restaurante deben coincidir exactamente entre el camino naive y el salted
— si no coinciden, el salting está perdiendo o duplicando filas, lo cual es
peor que el problema que pretende resolver.

### Idempotencia por sobreescritura (vs. idempotencia por rechazo)
**Qué es:** en vez de rechazar un duplicado (como el gate de fintech con
409), aquí la idempotencia es que escribir dos veces el mismo dato produce
el mismo resultado — un simple overwrite.
**Aquí:** `PutItem` sobre `eta-current` con `order_id` como key; la
reentrega de SQS es inofensiva porque el segundo `PutItem` no crea una fila
nueva.
**Cuándo NO usarlo:** cuando el efecto de escribir dos veces no es
idempotente por diseño (ej. un contador que se incrementa) — ahí hace
falta una condición explícita, no un overwrite simple.
**Contraste útil en entrevista:** es un patrón distinto pero relacionado al
`PutItem` condicional de fintech — aquí no se necesita rechazar el
duplicado porque el dato final es el mismo sin importar cuántas veces se
escriba.

### Comparación de costo real vs. cifra de memoria
**Qué es:** calcular el costo y el punto de cruce entre dos arquitecturas
con una fórmula ejecutable contra los precios publicados, en vez de citar
un número aprendido de memoria.
**Aquí:** `scripts/cost_compare.py --events-per-month N` — a 10M
eventos/mes, Fargate cuesta $8.89/mes ($0.89/M eventos) vs. Lambda $6.17/mes
($0.62/M eventos); el cruce real está en ~14.4M eventos/mes.
**Por qué importa en entrevista:** decir "lo correría en vivo si me
preguntan, en vez de citar una cifra vieja de memoria" es una señal de
rigor mucho más fuerte que memorizar el número.
**Cuándo NO usarlo:** decisiones de arquitectura no deberían basarse solo
en costo — el ADR 0003 elige Fargate por las propiedades del JVM para un
consumer de alto throughput, y el análisis de costo es un dato adicional,
no el criterio único.

### Separación de responsabilidades por velocidad (hot path vs. batch)
**Qué es:** dos sistemas con distintos SLA de latencia para el mismo
dominio de datos: uno que responde en milisegundos con una vista
simplificada, y otro que reconcilia todo el día con una vista completa.
**Aquí:** el worker Java nunca corrige historial; solo `replay.py` (batch)
tiene la vista completa con watermark y salting.
**Cuándo NO usarlo:** si el dominio no tolera ninguna divergencia temporal
entre "lo que ve el cliente ahora" y "la verdad reconciliada" — ahí hace
falta un sistema de una sola vista, más caro de construir.

---

## 7. ADRs en 3 líneas

- **ADR 0001 — Watermark de 10 min vs. overwrite-always vs. drop
  tardío:** overwrite-always hace oscilar el ETA con paquetes rezagados;
  dropear tardíos pierde la corrección para siempre. El watermark es el
  punto medio: vivo estable + corrección nocturna real.
- **ADR 0002 — Salting vs. subir `shuffle.partitions` vs. repartition por
  otra key vs. broadcast:** subir particiones no ayuda porque la misma key
  caliente sigue en una sola task; repartir por otra key rompe el groupBy
  de negocio; broadcast no aplica a agregaciones grandes. Restricción
  real: los totales naive y salted deben coincidir exactamente.
- **ADR 0003 — Fargate/Spring Boot vs. Lambda:** un consumer SQS de alto
  throughput y larga duración se beneficia del connection pooling maduro
  de la JVM; Lambda tendría cold-start por batch. El cruce de costo real
  está documentado, no asumido.
- **ADR 0004 — Trivy filesystem vs. OWASP dependency-check vs. Snyk vs.
  dependency-review-action para el árbol Maven:** dependency-check exige
  gestión de caché NVD y API key; Snyk exige cuenta; dependency-review-action
  solo ve diffs de PR, no un push directo a main. Trivy filesystem corre en
  cada push del job `security`, aislado de `test`/`e2e`, sin bloquear el demo.

---

## 8. Métricas y cómo defenderlas

| Métrica publicada | Cómo se midió | Límite honesto que hay que decir sin que lo pregunten |
|---|---|---|
| Top 5% de restaurantes = 59.5% de órdenes | `data_gen.py` sobre una corrida sintética de 24h×100 restaurantes×200 órdenes/h | Es la distribución que el propio generador produce a propósito, no un dato de mercado real |
| Imbalance naive 11.52x → salted 1.7x | `replay.py::partition_balance` sobre `local[2]` | No es una mejora de wall-clock — es balance de particiones, la métrica que el salting sí puede probar de forma confiable a esta escala |
| 545/4800 pings GPS tardíos (11.4%) | `apply_watermark()` sobre la corrida sintética | Es la tasa que `data_gen.py` inyecta a propósito (`LATE_PING_FRACTION=0.12`), no una tasa real de campo |
| 5/5 órdenes anotadas end-to-end, sin duplicado en reentrega | Verificación manual contra SQS + worker + DynamoDB reales | Es una prueba manual puntual documentada en el BUILD_GUIDE, no un load test |
| Crossover Fargate/Lambda ≈ 14.4M eventos/mes | `scripts/cost_compare.py` contra precios publicados de AWS | Basado en precios de lista, no en una factura real medida |

---

## 9. Preguntas de entrevista (EN)

1. **"Walk me through what happens when courier GPS data arrives late."**
   The nightly replay computes `arrival_delay_min` (ingestion `ts` minus
   `event_ts`). Anything under the 10-minute watermark is treated as
   on-time and was already reflected live by the worker. Anything over it
   is a correction-only record — it never touches `eta-current`, only the
   nightly reconciliation.

2. **"Why does the worker only ever overwrite, never append history?"**
   Because the worker's job is a single hot-path responsibility — the
   current best estimate — and mixing "live" with "historical correction"
   in the same write would make an ETA a customer already saw change
   silently later. History belongs to the batch layer.

3. **"How do you know your skew fix didn't change any business numbers,
   just the distribution?"**
   `replay.py` computes per-restaurant order counts both the naive way and
   the salted way and raises a `RuntimeError` before writing anything if
   they disagree for any restaurant — that invariant is checked on every
   run, not asserted once and forgotten.

4. **"Why report partition balance instead of a speedup number for the
   skew fix?"** On a single-machine `local[2]` Spark session there's no
   real multi-node cluster for a hot partition to actually bottleneck — a
   wall-clock number at that scale would be noise. Partition balance is
   the actual mechanism salting fixes, and it's reliably measurable
   locally.

5. **"Why Java/Spring Boot for this one worker and not Python, when the
   rest of the portfolio is Python?"** It's the one hot-path piece that
   benefits from the JVM specifically — a long-running, high-throughput
   SQS consumer, where mature connection pooling and threading matter more
   than avoiding a second language.

6. **"Why Fargate instead of Lambda for this worker?"**
   A continuous SQS-polling consumer doesn't map well to Lambda's
   per-invocation model — Lambda would cold-start per batch. I computed
   the real cost crossover (~14.4M events/month) rather than asserting
   Fargate is "just better."

7. **"What happens if the worker crashes mid-message?"**
   The message isn't deleted from the queue, so SQS redelivers it after
   the visibility timeout. Because scoring is a plain overwrite by
   `order_id`, redelivery is safe — no duplicate row, no special
   dedup logic needed.

8. **"Is your ECS deployment real or does it just call a mocked API?"**
   The image is really pushed to ECR (`docker push`) and the task
   definition is templated to reference the compose-network hostname, not
   `localhost`, because the ECS task runs in its own network namespace. I
   verified the deployed digest matches what I pushed by comparing
   `ecr describe-images` against `docker inspect` locally — not just that
   the task didn't error.

9. **"What's not fully validated here?"**
   Running this as an ECS Service (vs. a one-shot RunTask) is only
   partially validated against MiniStack — I found and documented that
   limitation instead of claiming it works.

10. **"Why doesn't this repo use Lambda or Step Functions like the other
    four?"** Deliberate: the worker is a long-running process doing
    continuous polling, which is architecturally the opposite of a
    per-event Lambda invocation. It's a different tool for a different
    shape of problem, not an omission.

11. **"What would you change for a real production deployment?"**
    ECS Service with real autoscaling tied to
    `ApproximateNumberOfMessages` on the queue instead of a fixed worker
    count, and I'd re-run the cost crossover against actual measured
    throughput instead of the synthetic default.

12. **"How do you compute ETA accuracy, and why compute it live instead of
    reading a stored aggregate?"** `/accuracy/daily` computes MAE directly
    from current `eta-current` rows plus raw events, not from the
    persisted nightly aggregate — so the number always reflects the
    current state instead of going stale between replay runs.

---

## 10. Flashcards

<!-- card -->
Q: ¿Qué hace el worker cuando SQS reentrega el mismo `order_id`?
A: Sobreescribe la fila existente en `eta-current` — nunca crea una fila duplicada, porque `PutItem` usa `order_id` como key.

<!-- card -->
Q: ¿Cuál es el umbral de watermark para GPS tardío, y qué pasa con un ping que lo supera?
A: 10 minutos (`WATERMARK_MINUTES`). Un ping que lo supera nunca actualiza `eta-current` en vivo — solo se usa como corrección en el replay nocturno.

<!-- card -->
Q: ¿Por qué `replay.py` calcula los conteos naive y salted por separado?
A: Para verificar que coincidan exactamente por restaurante — si difieren, `replay.py` lanza `RuntimeError` antes de escribir cualquier Parquet, porque un desacuerdo significa que el salting está perdiendo o duplicando filas.

<!-- card -->
Q: ¿Qué métrica reporta el repo para el fix de skew, y por qué no un speedup de wall-clock?
A: Balance de particiones (11.52x → 1.7x). En `local[2]` de una sola máquina no hay cluster real donde una partición caliente pueda hacer cuello de botella, así que un número de tiempo sería ruido, no señal.

<!-- card -->
Q: ¿Por qué el worker está escrito en Java/Spring Boot y no en Python?
A: Es un consumer SQS de alto throughput y larga duración; el connection pooling y threading maduro de la JVM rinden mejor ahí que arrancar procesos Python en frío para polling continuo.

<!-- card -->
Q: ¿Por qué se eligió Fargate en vez de Lambda para el worker?
A: Un consumer de polling continuo no encaja con el modelo por-invocación de Lambda (cold-start por batch); además el cruce de costo real (~14.4M eventos/mes) está calculado, no asumido.

<!-- card -->
Q: ¿Qué pasa si el worker lanza una excepción procesando un mensaje?
A: El mensaje no se borra de la cola; SQS lo reentrega tras el visibility timeout, y como el `PutItem` es un overwrite, la reentrega es segura.

<!-- card -->
Q: ¿Por qué `/accuracy/daily` calcula el MAE en vivo en vez de leer el agregado persistido por el replay nocturno?
A: Porque el agregado nocturno puede estar desactualizado — el endpoint de serving siempre debe reflejar el estado actual de `eta-current` + eventos, no un snapshot de la última corrida batch.

<!-- card -->
Q: ¿Qué demuestra el push real a ECR en `deploy_ecs.py`, y cómo se verificó que no fuera solo una imagen local reusada?
A: El ciclo real build→push→pull→run; se verificó comparando el digest de `ecr describe-images` contra `docker inspect` local, y confirmando que la task alcanzó `RUNNING` con esa imagen específica.

<!-- card -->
Q: ¿Por qué este repo no usa Lambda ni Step Functions, a diferencia de los otros 4 del portafolio?
A: Es deliberado — el worker es un proceso de larga duración haciendo polling continuo, arquitectónicamente opuesto a una invocación de Lambda por evento; es una herramienta distinta para un problema distinto, no un olvido.

<!-- card -->
Q: ¿Qué alternativa a salting se descartó por no resolver el problema real?
A: Subir `spark.sql.shuffle.partitions` — más particiones no ayuda porque la misma key caliente sigue concentrada en una sola task.

<!-- card -->
Q: ¿Qué limitación honesta declara el repo sobre el despliegue en ECS?
A: Correrlo como ECS Service (en vez de un RunTask puntual) está solo parcialmente validado contra MiniStack — se documentó la limitación en vez de ocultarla.

<!-- card -->
Q: ¿Qué porcentaje de pings GPS llega "tarde" en la corrida sintética medida, y qué constante del generador lo controla?
A: 11.4% (545 de 4,800), controlado por `LATE_PING_FRACTION = 0.12` en `data_gen.py`.

<!-- card -->
Q: ¿Qué escaneo de seguridad se añadió específicamente para el árbol de dependencias Maven del worker, y por qué no bastaba con Dependabot?
A: Trivy en modo filesystem (`trivy fs src/worker`), en el job `security` de CI. Dependabot es asíncrono (PRs semanales); Trivy falla el build en el mismo push que introduce un CVE HIGH/CRITICAL.

<!-- card -->
Q: ¿Qué heurística de scoring usa el worker para calcular el ETA?
A: 12 minutos de preparación + (distancia_km / 22 km/h) * 60 — una heurística fija, no un modelo aprendido.

<!-- card -->
Q: ¿Qué tan grande es realmente la superficie de dependencias del worker Java, y por qué eso cambió la decisión de escanear?
A: ~170 líneas de código propio, pero `mvn dependency:tree` resuelve ~85 artefactos (~52 en compile/runtime) — AWS SDK v2 y Netty arrastran muchos módulos transitivos. "El código propio es chico" era el criterio equivocado para decidir si escanear.

<!-- card -->
Q: ¿Qué endpoint expone el estado de salud del worker, y qué información da?
A: `GET /health` en `EtaScoringWorker.java` — status, mensajes procesados y latencia del último poll.

<!-- card -->
Q: ¿Qué invariante de negocio no debe romperse nunca al salt-ear la clave de agregación?
A: El total de órdenes por restaurante debe ser idéntico entre el camino naive y el salted — el salting cambia la distribución del trabajo, nunca el resultado.

<!-- card -->
Q: ¿Por qué `publisher.py` solo publica eventos `order_placed` y no `courier_gps`?
A: El hot path (worker) solo necesita saber que existe una orden para estimar el ETA; el GPS alimenta directamente el replay batch desde S3, no el camino en tiempo real.

<!-- card -->
Q: ¿Qué comparte el puerto `:8080` con este worker, y por qué nunca deben correr juntos sin gestión?
A: El gate Go de `fintech-txn-integrity-pipeline` — ambos repos usan ese puerto por defecto, así que hay que correr uno a la vez o usar `scripts/run_with_bg.sh`.

---

## 11. Quiz

<!-- quiz -->
Q: Un GPS ping llega con `arrival_delay_min = 14`. ¿Qué pasa con `eta-current`?
- [ ] Se actualiza inmediatamente con el nuevo dato
- [x] No se actualiza en vivo — se marca "late" y solo se usa en el replay nocturno
- [ ] Se descarta permanentemente
- [ ] Dispara un reintento automático del worker

Why: Supera el watermark de 10 minutos (`WATERMARK_MINUTES`), así que `apply_watermark()` lo clasifica como corrección histórica, no actualización en vivo.

<!-- quiz -->
Q: ¿Por qué `replay.py` calcula tanto `naive_order_counts` como `salted_order_counts` en cada corrida?
- [ ] Por redundancia en caso de que Spark falle
- [x] Para verificar que ambos coincidan exactamente por restaurante antes de confiar en el resultado salted
- [ ] Porque el naive es más rápido y se usa como caché
- [ ] Es un remanente de una versión anterior, sin propósito actual

Why: El salting es una optimización de distribución, no puede cambiar el resultado de negocio — el script lanza `RuntimeError` si los totales no coinciden.

<!-- quiz -->
Q: SQS reentrega un mensaje ya procesado por el worker. ¿Qué pasa en DynamoDB?
- [ ] Se crea una segunda fila con el mismo order_id
- [x] La fila existente se sobreescribe con el mismo resultado (o uno más nuevo si el ETA cambió)
- [ ] El worker lanza una excepción de clave duplicada
- [ ] La reentrega se ignora sin ningún efecto

Why: `PutItem` con `order_id` como key es un overwrite plano — la idempotencia aquí viene de que escribir dos veces produce el mismo resultado, no de rechazar el duplicado.

<!-- quiz -->
Q: ¿Por qué el README no reporta un número de "speedup" en segundos para el fix de skew por salting?
- [x] Porque en un Spark `local[2]` de una sola máquina no hay cluster real donde una partición caliente cause un cuello de botella medible con señal confiable
- [ ] Porque el salting no mejora el rendimiento en ningún escenario
- [ ] Porque Spark no permite medir tiempo de ejecución por partición
- [ ] Porque el equipo no tuvo tiempo de correr el benchmark

Why: A esa escala, un número de wall-clock sería ruido; el balance de particiones (11.52x → 1.7x) es la métrica que el salting sí puede probar de forma confiable localmente.

<!-- quiz -->
Q: ¿Qué pasa si el worker lanza una excepción al procesar un mensaje SQS?
- [x] El mensaje no se borra de la cola y SQS lo reentrega tras el visibility timeout
- [ ] El mensaje se mueve inmediatamente a la DLQ
- [ ] El worker se reinicia automáticamente
- [ ] El mensaje se pierde silenciosamente

Why: El código deja el mensaje sin `deleteMessage` en el bloque `catch`; como el `PutItem` es un overwrite, la reentrega posterior es segura sin lógica especial de dedup.

<!-- quiz -->
Q: ¿Por qué se eligió Java/Spring Boot para el worker de scoring en vez de Python, cuando el resto del portafolio es Python?
- [ ] Porque Java es más rápido de escribir para este equipo
- [x] Porque es un consumer SQS de alto throughput y larga duración, donde el connection pooling y threading maduro de la JVM rinden mejor que procesos Python arrancados en frío
- [ ] Porque AWS SDK solo tiene soporte completo en Java
- [ ] Por consistencia con otro repo del portafolio que también usa Java

Why: Es la única pieza del portafolio donde el runtime JVM se justifica por las propiedades reales del problema (polling continuo, alto throughput), no una preferencia de lenguaje.

<!-- quiz -->
Q: Según `docs/cost-comparison.md`, ¿qué pasa por encima de ~14.4M eventos/mes?
- [ ] Lambda siempre es más barato sin importar el volumen
- [x] Fargate se vuelve más barato porque su costo fijo deja de escalar con el número de invocaciones
- [ ] Ambos cuestan exactamente lo mismo indefinidamente
- [ ] El costo de Lambda crece de forma no lineal por encima de ese volumen

Why: Por debajo del cruce, Lambda es más barato (se paga solo por invocación real); por encima, el costo siempre-encendido de Fargate deja de crecer con el volumen y termina ganando.

<!-- quiz -->
Q: ¿Cuál es la limitación honesta que el propio repo documenta sobre el despliegue en ECS?
- [x] Correr como ECS Service (en vez de un RunTask puntual) está solo parcialmente validado contra MiniStack
- [ ] ECS no puede lanzar contenedores reales en este entorno
- [ ] ECR no soporta push real, solo simula la respuesta
- [ ] El worker no puede desplegarse en Fargate, solo en EC2

Why: El README y el ADR 0003 declaran explícitamente esa limitación en vez de afirmar que el despliegue está completamente validado.

<!-- quiz -->
Q: ¿Por qué `/accuracy/daily` en `api.py` no simplemente lee el agregado que escribió el último `replay.py`?
- [x] Porque ese agregado puede estar desactualizado; el endpoint calcula el MAE en vivo desde `eta-current` + eventos para reflejar el estado actual
- [ ] Porque el agregado nocturno no incluye el campo MAE
- [ ] Por un bug pendiente de corregir
- [ ] Porque DuckDB no puede leer los Parquet del agregado

Why: El replay nocturno es una vista de un punto en el tiempo; el serving layer necesita reflejar lo que está pasando ahora, no la última corrida batch.

<!-- quiz -->
Q: ¿Por qué el generador de eventos concentra deliberadamente el 60% de las órdenes en el 5% de los restaurantes?
- [x] Para poder probar y medir el fix real de skew en el shuffle de Spark — sin esa concentración no habría nada que arreglar
- [ ] Es un efecto secundario no intencional del generador aleatorio
- [ ] Para simular un error de datos que el pipeline debe filtrar
- [ ] Para reducir el volumen total de datos generados

Why: El skew inyectado es exactamente el escenario que `replay.py` tiene que resolver con salting — sin él, el fix no tendría nada real que demostrar.
