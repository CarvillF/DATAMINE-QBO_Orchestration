
# Documentación del Proyecto: Ingesta de Datos QuickBooks Online

Este documento detalla la arquitectura, configuración, operación y mantenimiento de los pipelines de extracción de datos desde QuickBooks Online (QBO) hacia el almacén de datos PostgreSQL, orquestados mediante Mage AI.

## 1. Descripción y Arquitectura

El sistema implementa un proceso ETL (Extract, Transform, Load) diseñado para la carga histórica (backfill) desde la API de QuickBooks Online. Su arquitectura se basa en **bloques dinámicos**, permitiendo el chunking de datos al permitir los datos en tramos diarios. Se evita de esta forma la carga excesiva de datos a memoria, además que permite la tolerancia a fallos.

### Diagrama de Flujo de Datos

```mermaid
graph LR
    A[Mage Trigger] --> B[Generador de Tramos (Dynamic Block)];
    B --> C{Iterador por Fechas};
    C -->|Día 1| D[Extractor QBO (Loader)];
    C -->|Día N| D;
    D -->|JSON| E[Cargador Postgres (Exporter)];
    E -->|Upsert| F[(Base de Datos Raw)];
    
    subgraph Observabilidad
    D -.-> G[Logs & Métricas];
    E -.-> G;
    end
```

**Componentes Principales:**
*   **Fuente:** API v3 de QuickBooks Online (REST/JSON).
*   **Orquestador:** Mage AI (Python).
*   **Destino:** PostgreSQL (Tablas en esquema `raw` con tipo de dato `JSONB`).
*   **UI de la base de datos:** PGADmin

## 2. Despliegue y Configuración

Para levantar el entorno se utiliza Docker.

1.  **Clonar el repositorio:**
    Asegúrese de tener acceso al repositorio de código fuente.    
2.  **Ejecución de Contenedores:**
    Ejecute el comando de inicio:
    ```bash
    docker-compose up -d
    ```
    Los contenedores se alzarán automáticamente
4.  **Acceso:**
    La interfaz de Mage AI está disponible en `http://localhost:6789`. Las credenciales usadas son las default.
    En caso de ser necesario PGAdmin también se encuentra accesible en `http://localhost:8085`. Las credenciales son "Carlosvfloresv@gmail.com" y por contraseña la utilizada en clase

## 3. Gestión de Secretos

Los credenciales y parámetros sensibles no se almacenan en el código. Se gestionan a través del gestor de secretos de Mage AI o variables de entorno del sistema operativo.

| Nombre de Variable | Propósito | Responsable | Política de Rotación |
| :--- | :--- | :--- | :--- |
| `QBO_CLIENT_ID` | Identificador de la aplicación en Intuit Developer. | Para autenticación de la app con el servicio de QBO | Ante brecha de seguridad. |
| `QBO_CLIENT_SECRET` | Llave secreta para autenticación OAuth 2.0. | Para autenticación de la app | Ante brecha de seguridad. |
| `QBO_REFRESH_TOKEN` | Token persistente para generar Access Tokens. | Data Engineer | Cada 100 días (política de Intuit) o en caso de renovación por UI. |
| `QBO_REALM_ID` | Identificador de la compañía (Company ID). | Nombre de la compañia sandbox | Estático. |
| `QBO_ENTORNO` | Define el entorno: `sandbox` o `production`. | Data Engineer | Estático |
| `POSTGRES_HOST` | Host de la base de datos destino. | Nombre docker del contenedor | Estático |
| `POSTGRES_USER` | Usuario con permisos de escritura en esquema `raw`. | DBA | Semestral. |
| `POSTGRES_PASSWORD` | Contraseña de base de datos. | DBA | Semestral. |

Todos estos secretos se encuentran cargados dentro de Mage Secrets. 

Para inicializar y proteger los valores sensibles de los contenedores QBO y PGAdmin se realizó el siguiente procedimiento:
1. Se creo un archivo docker-compose.yaml con credenciales únicas.
2. Se realizo docker compose up sobre este .yaml para inicializar los contenedores y volumenes con estas credenciales.
3. Una vez inicializados los volumenes, se remplazaron los valores del docker-compose.yaml por credenciales mockup.
4. Se subió a github esta version del docker-compose.yaml, junto a los volumenes inicializados.


## 4. Detalle de Pipelines: `qb_<entidad>_backfill`

Estos pipelines están diseñados para la carga masiva e incremental. Operan bajo una lógica de "Divide y Vencerás" mediante bloques dinámicos.

### Configuración y Parámetros
El pipeline requiere dos variables de ejecución (Runtime Variables) obligatorias especificadas dentro de los triggers:
*   `fecha_inicio`: Formato ISO 8601 (YYYY-MM-DD).
*   `fecha_fin`: Formato ISO 8601 (YYYY-MM-DD).

### Estrategia de Segmentación (Chunking)
El sistema divide el rango de fechas ingresado en **intervalos diarios**.
*   **Ventaja:** Si el proceso falla en un día específico, no es necesario reiniciar toda la carga, solo el tramo afectado.
*   **Control de Memoria:** Se procesa y libera la memoria día a día, evitando desbordamientos (OOM) en rangos extensos.

### Límites y Reintentos
*   **Rate Limiting:** Se maneja el error `429 Too Many Requests` mediante una espera exponencial (Backoff: 2s, 4s, 8s, etc.).
*   **Circuit Breaker:** Si se excede el número máximo de reintentos (configurado en 6), el bloque falla controladamente para evitar bloqueos de IP.
*   **Paginación:** Las peticiones a la API se realizan en páginas de 1000 registros (máximo permitido por QBO).

### Runbook de Operación
1.  **Ejecución Normal:** Configurar las fechas y lanzar el trigger "run once".
2.  **Fallo Parcial:** Identificar en los logs qué bloque de fecha falló (ej. `invoice_backfill_2025-10-15`). Reintentar únicamente ese bloque desde la interfaz de Mage.
3.  **Reanudación:** Si el pipeline se detuvo a la mitad, iniciar una nueva ejecución ajustando `fecha_inicio` al día siguiente del último bloque exitoso.


## 5. Trigger One-Time

Para cargas planificadas o iniciales, se debe configurar un trigger de tipo único.

### Especificaciones de Tiempo
La orquestación se basa estrictamente en tiempo UTC para garantizar consistencia.

*   **Fecha de Configuración (Ejemplo):** 2026-02-02 04:07:00 UTC.
*   **Equivalencia Local (Ecuador):** 2026-02-01 23:07:00 America/Guayaquil.

### Política de Deshabilitación
Al finalizar una ejecución exitosa de tipo One-Time:
1.  Verificar el estado "Completed" en el dashboard.
2.  **Acción Obligatoria:** Cambiar el estado del trigger a `Inactive` (Deshabilitado).
3.  **Justificación:** Esto previene re-ejecuciones accidentales que consuman cuota de API innecesariamente, aunque el sistema es idempotente.

## 6. Esquema Raw

Los datos se almacenan tal cual se reciben de la fuente para garantizar trazabilidad y permitir reprocesos.

**Tabla:** `raw.qb_invoices` (Ejemplo para entidad Invoice)

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `id` | `VARCHAR` (PK) | Identificador único de la transacción en QBO. |
| `payload` | `JSONB` | Respuesta completa de la API. |
| `ingested_at_utc` | `TIMESTAMP` | Fecha/hora de inserción en el Data Warehouse. |
| `extract_window_start_utc`| `TIMESTAMP` | Inicio del rango de extracción del bloque. |
| `extract_window_end_utc` | `TIMESTAMP` | Fin del rango de extracción del bloque. |
| `page_number` | `INTEGER` | Número de página de origen (auditoría). |

**Idempotencia:**
Se utiliza la instrucción `ON CONFLICT (id) DO UPDATE`. Si un registro ya existe, se actualizan sus campos y metadatos. Esto permite re-ejecutar tramos sin duplicar información.

## 7. Validaciones y Volumetría

El sistema implementa controles automáticos de calidad (Quality Gates) en cada bloque.

### Ejecución de Validaciones
Las validaciones son intrínsecas al código. Se deben revisar los logs de Mage buscando las etiquetas:
*   `Validation: Integrity Check Passed`
*   `Validation: [ALERT]`

### Interpretación de Resultados
1.  **Integridad Entrada/Salida:** El sistema compara `rows_fetched` (leídos de API) vs `rows_upserted` (escritos en BD). Si hay discrepancia no justificada (0 escritos con >0 leídos), el bloque fallará.
2.  **Detección de Regresión:** Si un día reporta 0 registros, se genera una alerta `WARNING` en los logs. Esto debe ser revisado manualmente por un analista para confirmar si es un día sin ventas real o un error de extracción.
3.  **Verificación Manual (SQL):**
    ```sql
    SELECT 
        DATE(extract_window_start_utc) as fecha_proceso, 
        COUNT(*) as total_registros
    FROM raw.qb_invoices
    GROUP BY 1 ORDER BY 1;
    ```

## 8. Troubleshooting (Solución de Problemas)

### Autenticación (Auth)
*   **Síntoma:** Error `401 Unauthorized` persistente.
*   **Causa:** El `REFRESH_TOKEN` ha expirado o ha sido revocado.
*   **Solución:** Generar un nuevo token mediante OAuth Playground de Intuit y actualizar el secreto en Mage.

### Paginación y Límites
*   **Síntoma:** El proceso es lento o se detiene.
*   **Revisión:** Verificar logs por mensajes `API Limit: 429`. Si ocurre frecuentemente, aumentar los tiempos de espera en la función de backoff.

### Timezones
*   **Problema:** Discrepancia de fechas entre QBO y Base de Datos.
*   **Causa:** Consultas realizadas en hora local en lugar de UTC.
*   **Política:** Todos los campos `MetaData.LastUpdatedTime` se consultan y almacenan asumiendo UTC. No realizar conversiones manuales dentro del código Python.

### Almacenamiento y Permisos
*   **Error:** `Transaction failed` o `Permission denied`.
*   **Solución:** Verificar que el usuario configurado en `POSTGRES_USER` tenga permisos `USAGE` sobre el esquema `raw` y permisos de `INSERT/UPDATE` sobre la tabla destino.



