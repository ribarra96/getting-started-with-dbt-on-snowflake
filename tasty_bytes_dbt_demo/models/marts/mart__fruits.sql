with src as (
  
  select *
  from {{ref('stg__fruits')}}

)

select *
from src
{% if is_incremental() %}
  where fruit_name not in (select fruit_name from {{ this }})
{% endif %}
