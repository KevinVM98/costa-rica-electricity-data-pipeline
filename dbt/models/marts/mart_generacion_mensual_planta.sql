with periodos as (
    select
        make_date(anho, id_mes, 1) as mes,
        empresa_genera,
        planta,
        count(*) as registros_periodo,
        case when count(*) = 1 then
            max(energia_punta + energia_valle + energia_noche)
        end as energia_total
    from {{ ref('stg_generacion_planta') }}
    group by anho, id_mes, empresa_genera, planta
)

select
    mes,
    empresa_genera,
    planta,
    registros_periodo,
    energia_total,
    case
        when registros_periodo > 1 then 'MULTIPLES_REGISTROS'
        when energia_total is null then 'SIN_ENERGIA_COMPLETA'
        else 'UNICO'
    end as estado_periodo
from periodos
