select 1 as fuente_vacia
where not exists (
    select 1 from {{ source('aresep_raw', 'generacion_planta_v2') }}
)
