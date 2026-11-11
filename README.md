# ms-report — BarrioDigital EV5 / EV6

Microservicio de reportería basado en eventos Kafka y PostgreSQL. Se mantiene el
proyecto generado por Initializr: **Java 21, Spring Boot 4.1.2**, paquete base
`cl.barriodigital.ms_report`. No reemplaza RabbitMQ ni modifica otros servicios.

## Arquitectura

```text
bd.requests.events.v1 -- requestId --> @KafkaListener (bd-report-v1)
    JSON + Bean Validation + clave
      -> transacción PostgreSQL bd_report
         processed_event + request_projection + analytics_outbox
      -> commit DB -> acknowledgment Kafka

analytics_outbox -> publicación confirmada -> bd.requests.analytics.v1
fallo definitivo -> failed_event persistido -> confirmación del offset recuperado
GET /api/report/* -> agregaciones SQL de request_projection
```

Paquetes: `config`, `controller`, `dto`, `entity`, `repository`, `service`, `kafka`
y `exception`. JPA gestiona entidades/proyecciones/outbox; JDBC se usa dentro de
las mismas transacciones para inserciones `ON CONFLICT`, locks y agregaciones SQL.
No hay estadísticas precargadas, seeds de producción ni mocks en `src/main`.

Spring Boot administra las versiones: Spring Kafka 4.1.2, cliente Kafka 4.2.2,
Jackson 3.1.7 y Flyway 12.4.0 en la resolución verificada. La versión **del cliente**
no cambia los brokers existentes Kafka 3.9.1 ni introduce KRaft. Se fuerza el
protocolo de grupo `classic`. No hay beans `NewTopic`, administración de tópicos,
infraestructura Kafka adicional ni suscripción al tópico de analítica.

## Contrato de entrada v1

| Campo | Representación / regla |
|---|---|
| `eventId` | UUID; identifica de manera estable un evento, incluso en reenvíos |
| `eventType` | `REQUEST_CREATED` o `REQUEST_STATUS_CHANGED` |
| `schemaVersion` | Entero `1` |
| `requestId` | Entero positivo; debe coincidir con la clave Kafka en texto decimal |
| `requestVersion` | Entero positivo, inicia en 1 e incrementa exactamente en uno por solicitud |
| `procedureId` | Texto no vacío, máximo 100 caracteres |
| `procedureName` | Texto no vacío, máximo 250 caracteres |
| `previousStatus` | `null` al crear; estado anterior obligatorio en cambios |
| `status` | Uno de los seis estados descritos abajo |
| `occurredAt` | Instante ISO-8601 con zona; se almacena y consulta en UTC |

Estados JSON, exactamente como el enum revisado en `ms-requests`:
`INGRESADO`, `ADMITIDO`, `EN_GESTION`, `EN_TERRENO`, `RESUELTO`, `RECHAZADO`.

La creación requiere versión 1, estado `INGRESADO` y `previousStatus=null`.
Un cambio requiere versión siguiente, estado anterior coincidente, estado nuevo
diferente, mismo `procedureId` y tiempo no anterior al último evento aplicado.
El nombre del procedimiento puede actualizarse. El productor sigue siendo el
responsable de autorizar las transiciones del dominio; report no agrega reglas
de negocio que compitan con `ms-requests`.

`requestVersion` es el único campo adicional de secuencia: permite distinguir
duplicados, faltantes y desorden aun cuando dos eventos compartan timestamp.
Los productores deben publicar **todos** los cambios relevantes en orden, usando
`requestId.toString()` como clave y una versión incrementada de forma transaccional.
No reutilizar un `eventId` con otro contenido. No enviar encabezados Java de tipos;
el contrato es JSON explícito, sin deserialización polimórfica de clases arbitrarias.

Ejemplos válidos, solo para publicación manual en un entorno local de pruebas:

- [REQUEST_CREATED](docs/events/request-created.json).
- [REQUEST_STATUS_CHANGED](docs/events/request-status-changed.json).

Los dos ejemplos corresponden a la misma solicitud `900001`, versiones 1 y 2.
No se publican ni se cargan en la base automáticamente. El DTO RabbitMQ existente
no es este contrato Kafka: no se ha modificado su productor.

## Entrega, idempotencia y fallos

El listener recibe strings y deserializa JSON explícitamente con Jackson 3. Esto
permite conservar también JSON roto y tombstones en cuarentena. Se valida el DTO
antes de procesar; no se aceptan campos desconocidos o tipos/estados no soportados.

Auto-commit está desactivado y el modo es `MANUAL_IMMEDIATE`. El listener confirma
después del retorno del servicio transaccional, es decir, después del commit DB.
Un reinicio entre el commit y el acknowledgment puede repetir el evento: la PK
`event_id`, el hash del DTO normalizado y la restricción `(request_id,request_version)`
evitan aplicar de nuevo la proyección o producir otro outbox. Un lock transaccional
PostgreSQL por solicitud serializa incluso creaciones concurrentes. Los locks usan
el namespace lógico `bd_report.request` para evitar compartir IDs con otros usos.

**La entrega es al menos una vez**, con efectos idempotentes en la proyección.
No se afirma exactamente una vez entre Kafka y PostgreSQL.

Un error recuperable recibe por defecto dos reintentos, con un segundo entre ellos.
Un contrato inválido no consume reintentos inútiles. Al agotar los intentos se
persiste la entrada en `bd_report.failed_event`, en una transacción independiente:
payload original, clave, eventId/requestId cuando son legibles, tópico, partición,
offset, tipo, razón y fecha. Solo entonces se confirma ese offset. La clave única
de coordenadas Kafka evita duplicar la cuarentena si se repite la recuperación.
Si falla también la escritura/commit de cuarentena, el recuperador lanza error,
el offset no se confirma y la partición vuelve al registro pendiente.

Un evento posterior de la misma solicitud no puede ocultar un hueco de versión:
también quedará en cuarentena hasta reparar la secuencia. Otras solicitudes válidas
pueden continuar. La cuarentena no es un tópico nuevo ni una DLQ RabbitMQ.

Los logs JSON incluyen `eventId`, `requestId`, partición/offset o intento cuando
corresponde. No se registra el payload completo en los mensajes de aplicación.
La cuarentena sí lo conserva: limitar acceso a estas tablas como datos operativos.

## Outbox y eventos de analítica

Cada evento aplicado crea un outbox en la misma transacción que la proyección.
El scheduler selecciona filas con `FOR UPDATE SKIP LOCKED`, sin adelantar versiones
pendientes de una misma solicitud. Publica con clave `requestId`, `acks=all` e
idempotencia del productor y marca `published_at` solo tras recibir confirmación.

Si Kafka no está disponible, se conserva el registro con intentos y `last_error`;
se reintenta con demora exponencial acotada a 60 segundos, sin descartarlo por
alcanzar un máximo. Una caída después del envío y antes del commit puede duplicar
el mensaje: los futuros consumidores deben deduplicar por `eventId` de analítica,
estable y derivado del `sourceEventId`. El lock DB se mantiene durante un envío
acotado por timeout; cada publicación usa su propia transacción para no bloquear
todo un batch. Esto se eligió por sencillez para la carga del laboratorio.

Salida: `bd.requests.analytics.v1`, tipo `REQUEST_ANALYTICS_UPDATED`, esquema 1.
`AnalyticsEventV1` contiene `eventId`, `sourceEventId`, `sourceEventType`, solicitud,
versión, procedimiento, estados anterior/actual, `createdAt`, `occurredAt` y
`resolutionSeconds` (null si no está resuelta). Es una actualización de proyección
para consumidores futuros; no inventa un total global en cada mensaje.
El servicio rechaza configurar entrada y salida con el mismo nombre.

## PostgreSQL y migraciones

La única migración, `V1__report_schema.sql`, crea cuatro tablas en **`bd_report`**:

- `processed_event`: historial inmutable de eventos aplicados y deduplicación.
- `request_projection`: una fila por solicitud con su estado actual y tiempos.
- `analytics_outbox`: publicaciones pendientes/confirmadas, intentos y orden.
- `failed_event`: fallos definitivos recuperables por el operador.

Flyway mantiene su historial también en `bd_report`. Se usa `ddl-auto=validate`,
`clean-disabled=true` y sin baseline automático. No hay DDL, lecturas o escrituras
sobre las tablas de requests/catalog u otros esquemas de aplicación.
El usuario DB necesita permiso para crear `bd_report` y sus objetos la primera
vez; para despliegues posteriores se pueden separar migraciones y usuario runtime.
No se automatiza el borrado de historial ni outbox: monitorizar crecimiento y
acordar una política de archivado antes de eliminar claves de deduplicación.

Para Supabase, obtener host/puerto/usuario reales del panel Connect y proporcionar
la contraseña por entorno/gestor de secretos. Usar conexión directa para migraciones
cuando haya conectividad IPv6/IPv4 adecuada, o evaluar el Session pooler en 5432
para este servicio persistente. No usar el Transaction pooler 6543 como configuración
predeterminada de Flyway/JPA. Configurar TLS, como mínimo `sslmode=require`, o
`verify-full` con la CA correspondiente. No se conectó a Supabase durante el desarrollo.

## API y semántica temporal

Todas las operaciones son GET, bajo `/api/report`; no hay endpoints de modificación.

| Endpoint | Rango por defecto | Respuesta |
|---|---|---|
| `/api/report/kpis` | `last24h` | Total, seis estados, resueltas y promedio en segundos |
| `/api/report/top-procedures` | `last7d` | Ranking por procedimiento; `limit=10` (1..50) |
| `/api/report/requests-by-hour` | `last24h` | Cantidad creada por hora UTC |

Rangos: `last24h`, `last7d`, `last30d`; alternativamente ambos `from` y `to` en
ISO-8601 con zona. No combinar ambas modalidades. Máximo 90 días por rango explícito.
Todos los intervalos son **[from, to)**, inclusivo al inicio y exclusivo al final.

La población de cada consulta son solicitudes cuyo **createdAt** cae dentro del
rango; el estado es su **estado actual procesado**, no una reconstrucción as-of.
`resolvedRequests` cuenta las que actualmente están RESUELTO dentro de esa cohorte.
El promedio usa `occurredAt de resolución - createdAt`, en segundos completos,
solo para esas solicitudes; devuelve `null` si no hay ninguna. Si una solicitud
se reabre, deja de contar como resuelta hasta una nueva resolución; el historial
permanece en `processed_event`. Los resultados tienen consistencia eventual respecto
a Kafka. Consultas KPI compuestas usan una transacción read-only repeatable-read.

El ranking agrupa por `procedureId` (no por nombres que puedan cambiar) y muestra
el nombre del evento más reciente en la cohorte. Empates: procedureId ascendente.
La serie horaria devuelve únicamente horas con registros; una lista vacía significa
que no hubo creaciones en el rango, no un error ni datos simulados.

Forma de KPI para una base sin registros en el intervalo consultado:

```json
{
  "range": {"from": "2026-11-08T00:00:00Z", "to": "2026-11-09T00:00:00Z"},
  "totalRequests": 0,
  "requestsByStatus": {"INGRESADO": 0, "ADMITIDO": 0, "EN_GESTION": 0, "EN_TERRENO": 0, "RESUELTO": 0, "RECHAZADO": 0},
  "resolvedRequests": 0,
  "averageResolutionSeconds": null
}
```

Es un ejemplo de formato vacío, no una respuesta precargada. Rangos/limit inválidos:
HTTP 400 ProblemDetail; DB no disponible: 503 sin SQL/credenciales; métodos de
escritura sobre estas rutas: 405. Actuator: `/actuator/health`, probes de liveness
y readiness, `/actuator/info`, `/actuator/metrics`. Health DB no garantiza Kafka al
día: comprobar también consumer lag, cuarentena y outbox.

## Variables de entorno

`.env.example` es solo una plantilla y **Spring Boot no la carga automáticamente**.
Establecer variables en la terminal, IDE, contenedor o gestor de secretos.

| Variable | Obligatoria / valor por defecto |
|---|---|
| `DB_URL` | Sí; JDBC PostgreSQL, sin contraseña embebida |
| `DB_USERNAME`, `DB_PASSWORD` | Sí; valores reales proporcionados fuera del repositorio |
| `DB_POOL_SIZE` | 5 |
| `SERVER_PORT`, `SERVER_ADDRESS` | 8083 / 127.0.0.1 |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:19092,localhost:29092,localhost:39092 |
| `KAFKA_CONSUMER_GROUP` | bd-report-v1 |
| `KAFKA_EVENTS_TOPIC`, `KAFKA_ANALYTICS_TOPIC` | bd.requests.events.v1 / bd.requests.analytics.v1 |
| `KAFKA_CONCURRENCY` | 3 |
| `KAFKA_LISTENER_ENABLED` | true |
| `KAFKA_SECURITY_PROTOCOL` | PLAINTEXT (local) |
| `KAFKA_CONSUMER_RETRIES`, `KAFKA_RETRY_DELAY_MS` | 2 / 1000; rangos 0..10 y 100..10000 |
| `OUTBOX_ENABLED`, `OUTBOX_DELAY_MS` | true / 1000 |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_SEND_TIMEOUT_MS` | 10 / 20000; rangos 1..100 y 1000..60000 |

Para SASL/TLS futuro se pueden suministrar las propiedades estándar `spring.kafka`
mediante configuración externa de Spring Boot. No incluir material secreto en
este repositorio. No se ha modificado la autenticación Entra ni la integración BFF.

## Compilar y probar

Desde `ms-report`, con JDK 21:

```powershell
.\mvnw.cmd -B -ntp test
.\mvnw.cmd -B -ntp package
```

Las pruebas unitarias JUnit/Mockito no requieren Docker, Kafka, PostgreSQL ni variables
DB. El test vacío generado por Initializr se reemplazó por pruebas de comportamiento.

Integración optativa con PostgreSQL **aislado**:

```powershell
.\mvnw.cmd -B -ntp -Ppostgres-it verify
```

Requiere Docker Linux. Testcontainers usa `postgres:17.6-alpine`, limitado a 256 MiB,
puerto aleatorio y credenciales de prueba administradas por la biblioteca. Nunca
usa DB_URL del entorno: la prueba sustituye las propiedades con las del contenedor.
Las tablas se reinician solo en esa base descartable entre casos; al terminar se
retiran sus contenedores temporales. Se prueban migraciones, validación Hibernate,
concurrencia, rollback, SQL agregado, outbox, cuarentena y HTTP real. Kafka está
simulado en esta suite, no se publica en tópicos de negocio.

Compatibilidad de red, **solo lectura**, con el Kafka existente:

```powershell
.\mvnw.cmd -B -ntp -Pkafka-check verify
```

Verifica Admin/consumer/producer del cliente administrado, metadatos, ISR y offsets.
No crea tópicos, envía registros, se une a grupos ni confirma offsets. Ejecutar los
dos perfiles por separado. Resultados reales en [docs/VALIDACION.md](docs/VALIDACION.md).

Si el helper de Docker no está en PATH al usar Testcontainers, añadir la carpeta
`resources\bin` de Docker Desktop al PATH de esa terminal. No se requieren cambios
a credenciales Docker. El Wrapper puede necesitar acceso a Internet/caché Maven.

## Prueba local del flujo

1. Mantener en ejecución el clúster existente de `infra-barriodigital` sin modificarlo.
2. Preparar una base PostgreSQL de desarrollo o asignar acceso al esquema `bd_report`.
   Cargar `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` en el proceso desde valores propios.
3. Iniciar el servicio (Flyway crea únicamente su esquema/tablas):

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092,localhost:29092,localhost:39092'
$env:SERVER_PORT = '8083'
.\mvnw.cmd spring-boot:run
```

4. En otra terminal, comprobar salud y consultar:

```powershell
Invoke-RestMethod 'http://localhost:8083/actuator/health'
Invoke-RestMethod 'http://localhost:8083/api/report/kpis?range=last24h'
Invoke-RestMethod 'http://localhost:8083/api/report/top-procedures?range=last7d&limit=10'
Invoke-RestMethod 'http://localhost:8083/api/report/requests-by-hour?from=2026-11-08T00:00:00Z&to=2026-11-09T00:00:00Z'
```

5. Para una prueba manual controlada, publicar primero creación y después cambio,
   clave string `900001`, valor JSON de los archivos de ejemplo. En Kafbat UI los
   campos serían Key=String y Value=JSON/String, sin headers de tipos Java.
   **La UI actual está en modo read-only**: no se habilitó escritura ni se cambió
   infra. Como alternativa sin alterar configuración, usar el CLI del broker:

```powershell
foreach ($file in @('request-created.json', 'request-status-changed.json')) {
    $payload = Get-Content "docs/events/$file" -Raw | ConvertFrom-Json | ConvertTo-Json -Compress
    "900001|$payload" | docker exec -i barriodigital-kafka-kafka-1-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic bd.requests.events.v1 --property parse.key=true --property 'key.separator=|' --producer-property acks=all --producer-property enable.idempotence=true
}
```

Este comando escribe eventos de prueba en el tópico local de negocio: usar solo
en un entorno de desarrollo y elegir un requestId nuevo si ya hay datos con ese ID.
No se ejecutó durante esta implementación. Los archivos están fechados para un
intervalo explícito; actualizar fechas/eventIds/requestId antes de una demostración
posterior, conservando la secuencia. Para probar duplicados, reenviar exactamente
el mismo JSON, sin cambiar eventId ni contenido.

6. Consultar con rango explícito que cubra las fechas publicadas. En una DB vacía,
   los dos ejemplos producen una sola solicitud ADMITIDO, dos eventos procesados
   y dos publicaciones analíticas; promedio de resolución null. Para observar
   duración, continuar con eventos de versiones 3, 4 y 5 por EN_GESTION, EN_TERRENO
   y RESUELTO, cada uno con UUID nuevo, previousStatus correcto y timestamp posterior.
7. Revisar Kafbat: grupo `bd-report-v1`, offsets/lag, particiones y mensajes de
   `bd.requests.analytics.v1`. La UI puede refrescar metadatos con demora.

## Operación y recuperación

Consultar sin modificar datos:

```sql
SELECT event_id, request_id, request_version, source_partition, source_offset
FROM bd_report.processed_event ORDER BY processed_at DESC LIMIT 20;
SELECT request_id, request_version, status, created_at, resolved_at, resolution_seconds
FROM bd_report.request_projection ORDER BY request_id;
SELECT id, request_id, request_version, attempts, available_at, last_error
FROM bd_report.analytics_outbox WHERE published_at IS NULL ORDER BY created_at;
SELECT id, event_id, request_id, source_topic, source_partition, source_offset, failure_type, failure_reason
FROM bd_report.failed_event ORDER BY failed_at DESC LIMIT 20;
```

Después de reiniciar, el mismo grupo retoma offsets confirmados y el scheduler
retoma outbox no publicado. Si el último commit Kafka faltó, la deduplicación evita
reaplicar datos. No cambiar de grupo para ocultar un fallo ni borrar tablas/offsets.

Para reparar cuarentena, identificar la causa y verificar el payload guardado con
acceso restringido. Corregir el contrato o recuperar los eventos faltantes desde
la fuente y republicar en **orden de requestVersion**, con la clave original.
Un evento válido que antes falló mantiene su eventId. Una entrada malformada debe
corregirse según el contrato, sin reutilizar IDs ya procesados con distinto contenido.
Los eventos posteriores también en cuarentena deben reproducirse después de sus
predecesores. Confirmar presencia en processed_event y avance de la proyección.
La fila original de failed_event se conserva como evidencia; no hay endpoint de
replay, borrado o modificación del dominio. Este procedimiento requiere operación
explícita; no se simula recuperación automática de una secuencia incompleta.

La retención Kafka existente puede eliminar el evento inicial antes del primer
consumo del servicio. Una proyección completa requiere historia desde la creación
o un futuro procedimiento de bootstrap acordado con requests. No se inventan
fechas de creación para aceptar cambios huérfanos.

## Docker y preparación AWS

El Dockerfile empaqueta el JAR previamente compilado, usa JRE 21 y usuario no root:

```powershell
.\mvnw.cmd -B -ntp package
docker build -t barriodigital/ms-report:local .
```

Para conectarlo al Kafka existente usar `--network barriodigital-kafka-net`,
`KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092`, y DB por entorno.
El Dockerfile define `SERVER_ADDRESS=0.0.0.0`; limitar la publicación local a
`127.0.0.1:8083:8083`. Comenzar con límite de 768 MiB y medir con la carga real.
`.dockerignore` excluye todo salvo Dockerfile y JAR, incluidos secretos y `.env`.
No se construyó ni desplegó esta imagen durante la validación.

Para AWS: inyectar configuración/secrets, endpoints privados de brokers/DB y TLS
según el laboratorio, ejecutar migraciones controladamente y mantener la API en
red interna detrás del BFF. Este servicio no valida JWT en esta fase y no debe
publicarse directamente como API pública. No se ha desplegado ni modificado AWS.

## Referencias de compatibilidad

- [Spring Boot 4 y Jackson 3](https://spring.io/blog/2025/10/07/introducing-jackson-3-support-in-spring/).
- [Módulos y starter Flyway de Spring Boot 4](https://spring.io/blog/2025/10/28/modularizing-spring-boot/).
- [Compatibilidad cliente/broker Kafka](https://github.com/apache/kafka/blob/trunk/docs/getting-started/compatibility.md).
- [Conexiones PostgreSQL en Supabase](https://supabase.com/docs/guides/database/connecting-to-postgres).
- [Testcontainers PostgreSQL](https://java.testcontainers.org/modules/databases/postgres/).
