select mes, empresa_genera, planta
from {{ ref('mart_generacion_mensual_planta') }}
group by mes, empresa_genera, planta
having count(*) > 1
