
BEGIN;

-- ==========================================
-- 1. CREAR ESQUEMA STAGING
-- ==========================================

CREATE SCHEMA IF NOT EXISTS staging;

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

-- Compatibilidad con la tabla existente
ALTER TABLE staging.generacion_planta
ADD COLUMN IF NOT EXISTS contenido_hash TEXT;


-- ==========================================
-- 2. ACTUALIZAR STAGING DESDE RAW V2
-- ==========================================

TRUNCATE TABLE staging.generacion_planta;

INSERT INTO staging.generacion_planta (
    raw_id,
    anho,
    id_mes,
    empresa_genera,
    planta,
    energia_punta,
    energia_valle,
    energia_noche,
    factor_carga,
    contenido_hash
)
SELECT
    id,
    anho,
    id_mes,
    TRIM(empresa_genera),
    TRIM(planta),

    NULLIF(datos->>'energiaPunta', '')::NUMERIC,
    NULLIF(datos->>'energiaValle', '')::NUMERIC,
    NULLIF(datos->>'energiaNoche', '')::NUMERIC,
    NULLIF(datos->>'factorCarga', '')::NUMERIC,

    contenido_hash

FROM raw.generacion_planta_v2;


-- ==========================================
-- 3. CREAR ESQUEMA MART
-- ==========================================

CREATE SCHEMA IF NOT EXISTS mart;


-- ==========================================
-- 4. ACTUALIZAR VISTA ANALITICA
-- ==========================================

CREATE OR REPLACE VIEW mart.generacion_por_planta AS

WITH registros AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY
                empresa_genera,
                planta,
                anho,
                id_mes
        ) AS registros_periodo
    FROM staging.generacion_planta
)

SELECT
    raw_id,
    anho,
    id_mes,
    empresa_genera,
    planta,
    energia_punta,
    energia_valle,
    energia_noche,

    -- No sumar automaticamente registros
    -- ambiguos de una misma planta y mes
    CASE
        WHEN registros_periodo = 1
        THEN energia_punta
           + energia_valle
           + energia_noche
        ELSE NULL
    END AS energia_total,

    factor_carga,
    contenido_hash,
    registros_periodo,

    CASE
        WHEN registros_periodo = 1
        THEN 'UNICO'
        ELSE 'MULTIPLES_REGISTROS'
    END AS estado_periodo

FROM registros;

COMMIT;

