
-- ==========================================
-- DATA ENGINEERING - ARESEP
-- Inicializacion de PostgreSQL 17
-- ==========================================

BEGIN;

-- ==========================================
-- 1. ESQUEMAS
-- ==========================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS monitoring;


-- ==========================================
-- 2. RAW ORIGINAL
-- Compatibilidad con scripts anteriores
-- ==========================================

CREATE TABLE IF NOT EXISTS raw.generacion_planta (
    id BIGSERIAL PRIMARY KEY,
    anho SMALLINT,
    id_mes SMALLINT,
    empresa_genera TEXT,
    planta TEXT,
    datos JSONB NOT NULL,
    fecha_ingesta TIMESTAMPTZ DEFAULT NOW()
);


-- ==========================================
-- 3. RAW V2
-- Almacenamiento incremental por hash
-- ==========================================

CREATE TABLE IF NOT EXISTS raw.generacion_planta_v2 (
    id BIGSERIAL PRIMARY KEY,

    anho SMALLINT NOT NULL,
    id_mes SMALLINT NOT NULL,
    empresa_genera TEXT NOT NULL,
    planta TEXT NOT NULL,

    datos JSONB NOT NULL,
    contenido_hash TEXT NOT NULL UNIQUE,

    fecha_primera_ingesta TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    fecha_ultima_vista TIMESTAMPTZ
        NOT NULL DEFAULT NOW()
);


-- ==========================================
-- 4. STAGING
-- Datos transformados
-- ==========================================

CREATE TABLE IF NOT EXISTS staging.generacion_planta (
    raw_id BIGINT,
    anho SMALLINT,
    id_mes SMALLINT,
    empresa_genera TEXT,
    planta TEXT,

    energia_punta NUMERIC,
    energia_valle NUMERIC,
    energia_noche NUMERIC,
    factor_carga NUMERIC,

    contenido_hash TEXT
);


-- ==========================================
-- 5. MONITORING - CALIDAD
-- ==========================================

CREATE TABLE IF NOT EXISTS monitoring.calidad_datos (
    id BIGSERIAL PRIMARY KEY,

    dag_run_id TEXT NOT NULL,
    anho SMALLINT NOT NULL,

    registros_revisados INTEGER NOT NULL,
    errores_detectados INTEGER NOT NULL,

    fecha_validacion TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    UNIQUE (dag_run_id, anho)
);


-- ==========================================
-- 6. MONITORING - CARGAS
-- ==========================================

CREATE TABLE IF NOT EXISTS monitoring.carga_datos (
    id BIGSERIAL PRIMARY KEY,

    dag_run_id TEXT NOT NULL,
    anho SMALLINT NOT NULL,

    registros_recibidos INTEGER NOT NULL,
    registros_unicos_lote INTEGER NOT NULL,
    registros_nuevos INTEGER NOT NULL,
    registros_existentes INTEGER NOT NULL,

    duracion_segundos NUMERIC(12,3),

    fecha_carga TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    UNIQUE (dag_run_id, anho)
);


-- ==========================================
-- 7. MONITORING - EJECUCIONES
-- ==========================================

CREATE TABLE IF NOT EXISTS monitoring.ejecuciones_pipeline (
    dag_run_id TEXT PRIMARY KEY,

    estado TEXT NOT NULL
        CHECK (estado IN (
            'INICIADO',
            'EXITOSO',
            'FALLIDO'
        )),

    tarea_fallida TEXT,
    detalle_error TEXT,

    fecha_inicio TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    fecha_fin TIMESTAMPTZ
);

COMMIT;
