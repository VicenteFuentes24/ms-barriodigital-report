# Validación de ms-report

Fecha: 8 de noviembre de 2026 (America/Santiago).

## Base conservada

Se inspeccionó el proyecto de Spring Initializr antes de editar. Se mantuvieron
Spring Boot 4.1.2, Java 21, el artifact `ms-report`, el paquete `cl.barriodigital.ms_report`,
los starters existentes y Maven Wrapper 3.10.0. La carpeta no contenía `.git`;
no se inicializó repositorio ni se realizaron commits/pushes.

Se añadieron los starters/módulos Flyway y Testcontainers PostgreSQL de prueba
sin fijar versiones ajenas al BOM. La resolución real fue:

| Dependencia | Versión |
|---|---|
| Spring Boot | 4.1.2 |
| Spring Kafka | 4.1.2 |
| kafka-clients | 4.2.2 |
| Jackson databind/core | 3.1.7 |
| Flyway core/PostgreSQL | 12.4.0 |
| JUnit Jupiter | 6.0.3 |
| Mockito | 5.23.0 |
| Testcontainers | 2.0.5 |

La versión Kafka 4.2.2 corresponde al cliente administrado; los brokers existentes
permanecieron en 3.9.1 con ZooKeeper. No se introdujo ni ejecutó Kafka embebido.

## Comandos y resultados ejecutados

```powershell
.\mvnw.cmd -B -ntp -DskipTests compile
.\mvnw.cmd -B -ntp -Ppostgres-it verify
.\mvnw.cmd -B -ntp -Pkafka-check test-compile failsafe:integration-test failsafe:verify
.\mvnw.cmd -B -ntp package
```

| Suite final | Ejecutadas | Fallos | Errores | Omitidas |
|---|---:|---:|---:|---:|
| Unitarias, `package` final | 39 | 0 | 0 | 0 |
| PostgreSQL/HTTP, `PostgresIT` | 7 | 0 | 0 | 0 |
| Kafka real de solo lectura, `KafkaReadOnlyIT` | 1 | 0 | 0 | 0 |

Las pruebas PostgreSQL se ejecutaron después de los cambios de persistencia y
mensajería. Posteriormente se añadió manejo HTTP 503 para un fallo al abrir la
transacción y pruebas del contrato de los archivos de ejemplo; se volvió a
ejecutar `package`, con las 39 pruebas unitarias y generación del JAR final.

Artefacto generado: `target/ms-report-0.0.1-SNAPSHOT.jar`, Spring Boot ejecutable.
Los informes están en `target/surefire-reports` y `target/failsafe-reports`.
Logs resumidos de las ejecuciones: `target/verification.log`, `target/kafka-check.log`
y `target/final-unit-build.log`; `target/` está excluido de Git.

## Cobertura demostrada

Pruebas unitarias:

- JSON de creación y cambio de estado, archivos de ejemplos, payload vacío/null/roto,
  clave Kafka incorrecta, versión incompatible, campos desconocidos y validaciones.
- Creación de proyección y outbox; actualización de resolución y cálculo de duración.
- Evento duplicado sin efectos adicionales y eventId reutilizado con contenido distinto.
- Versiones faltantes, evento huérfano, previousStatus incorrecto y tiempo retroactivo.
- Acknowledgment después del retorno del servicio; sin ack ante fallo de persistencia.
- Dos reintentos antes de cuarentena; commit del offset recuperado después del store.
- Si falla la cuarentena: seek al registro original y ningún commit de offset.
- Outbox confirmado y reintento cuando Kafka falla; prohibición de entrada=salida.
- KPIs con agregados persistidos simulados solo en tests, promedio null sin resultados,
  rangos relativos/explícitos, límites, errores 400/405 y DB no disponible 503.

Integración con **PostgreSQL 17.6 real, temporal y aislado**:

- Flyway crea `bd_report`; Hibernate valida mappings frente al esquema real.
- Evento duplicado no genera otro journal/outbox; SQL KPI, ranking y agrupación horaria
  devuelven datos reales persistidos por el test; límite superior del rango excluyente.
- Dos transacciones concurrentes del mismo evento producen una aplicación y un duplicado.
- Hueco de versión deja intacta la proyección y el historial.
- Error de integridad al insertar outbox revierte también journal y proyección.
- Fallo de publicación mantiene outbox pendiente; la versión siguiente de la misma
  solicitud espera a su predecesora; después de disponibilidad ambas se publican.
- Cuarentena durable e idempotente por tópico/partición/offset.
- Servidor HTTP real en puerto aleatorio: KPI de datos persistidos y Actuator HTTP 200.

El KafkaTemplate se simuló en esta suite para controlar éxitos/fallos de envío.
No se afirma que esos tests hayan publicado en un broker. Testcontainers creó su
propia base, con límite de 256 MiB, y retiró los contenedores temporales al finalizar.
Sus resets afectaron exclusivamente esa base de prueba, no Supabase.

Kafka real existente, comprobado sin escrituras:

- El AdminClient administrado identificó tres brokers y los dos tópicos requeridos.
- Ambos tenían 3 particiones, RF=3, ISR de 3 y `min.insync.replicas=2`.
- KafkaConsumer leyó offsets y realizó poll con asignación directa, sin grupo ni commits.
- KafkaProducer idempotente obtuvo metadata del tópico de analítica sin enviar registros.

## Revisión de configuración y alcance

- Sin mocks, seeds ni inicializadores de datos en `src/main` según inspección/búsqueda.
- JDBC y Flyway limitados al esquema `bd_report`; no hay DDL sobre tablas de otros servicios.
- Configuración DB por variables obligatorias; `.env.example` contiene placeholders
  y campos de credenciales vacíos. No se abrieron `.env` reales.
- Puerto HTTP 8083 sobrescribible; bind local por defecto; Docker usa 0.0.0.0.
- Auto-commit Kafka desactivado; `MANUAL_IMMEDIATE`; protocolo `classic`.
- Sin `NewTopic`, creación automática de tópicos ni modificaciones de infraestructura.
- Cambios restringidos a la carpeta `ms-report`. Sin cambios RabbitMQ, requests,
  BFF, Notify, React, Supabase, AWS ni repositorio de infraestructura.

## Límites de esta validación

No se ejecutó el circuito completo con consumo de un mensaje nuevo desde Kafka,
persistencia de ese mensaje y publicación real posterior hacia analítica. Se
verificaron separadamente transacciones/listeners/outbox y compatibilidad de red.
No se publicaron los ejemplos en los tópicos existentes para evitar introducir
solicitudes de prueba en ellos durante el desarrollo.

No se conectó a Supabase, no se ejecutaron migraciones en esa cuenta y no se dejó
el microservicio conectado a una DB permanente. Para ejecución local persistente
faltan las variables DB proporcionadas por el usuario y el arranque indicado en README.
No se construyó la imagen Docker ni se desplegó AWS. No se probaron caídas reales
de broker, reinicios completos, carga sostenida ni recuperación tras retención Kafka.

Maven Wrapper necesitó ejecutarse fuera del sandbox para acceder a su caché y red.
Hubo avisos 401 al consultar índices opcionales de repositorios transitivos de
Flyway; no impidieron resolver dependencias desde Maven Central ni compilar/probar.
Una advertencia de API obsoleta de JsonNode no impide compilación. No se ocultaron
errores de compilación o pruebas: los fallos encontrados durante desarrollo se
corrigieron antes de los resultados finales anteriores.

## Inventario de cambios

Modificados: `pom.xml`, `.gitignore`, `MsReportApplication.java`,
`src/main/resources/application.properties`.

Reemplazado el test vacío `MsReportApplicationTests.java` por pruebas específicas.

Creados:

- 28 clases Java adicionales bajo los ocho paquetes de arquitectura.
- Migración `src/main/resources/db/migration/V1__report_schema.sql`.
- 12 archivos de soporte/tests Java (9 clases unitarias, 2 de integración y TestEvents).
- `.env.example`, `Dockerfile`, `.dockerignore`.
- `README.md`, este informe y dos ejemplos JSON bajo `docs/events`.

Consultar [README](../README.md) para variables, semántica de KPIs, publicación
manual de ejemplos, SQL operativo, recuperación y preparación de despliegue.
