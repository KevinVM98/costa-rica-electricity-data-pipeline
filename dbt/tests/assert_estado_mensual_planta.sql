select mes, empresa_genera, planta, estado_periodo
from {{ ref('mart_generacion_mensual_planta') }}
where not coalesce(
    case estado_periodo
        when 'MULTIPLES_REGISTROS' then registros_periodo > 1 and energia_total is null
        when 'SIN_ENERGIA_COMPLETA' then registros_periodo = 1 and energia_total is null
        when 'UNICO' then registros_periodo = 1 and energia_total is not null
    end,
    false
)
