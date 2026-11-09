select
    id as raw_id,
    anho,
    id_mes,
    trim(empresa_genera) as empresa_genera,
    trim(planta) as planta,
    nullif(datos ->> 'energiaPunta', '')::numeric as energia_punta,
    nullif(datos ->> 'energiaValle', '')::numeric as energia_valle,
    nullif(datos ->> 'energiaNoche', '')::numeric as energia_noche,
    nullif(datos ->> 'factorCarga', '')::numeric as factor_carga,
    contenido_hash
from {{ source('aresep_raw', 'generacion_planta_v2') }}
