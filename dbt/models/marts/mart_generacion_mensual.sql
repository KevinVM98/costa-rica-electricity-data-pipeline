select
    mes,
    sum(energia_total) as energia_reportada,
    count(*) as periodos_observados,
    count(energia_total) as periodos_con_energia,
    count(*) filter (where estado_periodo = 'MULTIPLES_REGISTROS') as periodos_ambiguos,
    count(*) filter (where estado_periodo = 'SIN_ENERGIA_COMPLETA') as periodos_sin_energia_completa
from {{ ref('mart_generacion_mensual_planta') }}
group by mes
