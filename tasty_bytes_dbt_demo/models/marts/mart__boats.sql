{{ config(materialized='incremental', unique_key='name', on_schema_change='sync') }}

with src as (
  
  select *
  from {{ref('stg__boats')}}

)

select *
from src
{% if is_incremental() %}
  where name not in (select name from {{ this }})
{% endif %}
