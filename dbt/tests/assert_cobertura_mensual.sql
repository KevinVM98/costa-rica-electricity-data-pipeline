select mes
from {{ ref('mart_generacion_mensual') }}
where periodos_observados <> periodos_con_energia + periodos_ambiguos + periodos_sin_energia_completa
    or (periodos_con_energia = 0) <> (energia_reportada is null)
