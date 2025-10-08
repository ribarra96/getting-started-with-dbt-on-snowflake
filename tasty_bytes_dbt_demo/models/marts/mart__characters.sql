with src as (
  select *
  from {{ref('stg__characters')}}
)
select *
from src 
{% if is_incremental() %}
  where character_name not in (select character_name from {{ this }})
{% endif %}
