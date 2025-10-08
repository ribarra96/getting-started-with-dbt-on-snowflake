-- models/dim_fruits.sql
{{ config(materialized='incremental', unique_key='fruit_name', on_schema_change='sync') }}

with src as (
  select payload from {{ source('onepiece_staging','STG_ONEPIECE_FRUITS') }}
),
flat as (
  select
    payload:name::string as fruit_name,
    payload:type::string as fruit_type,
    payload:description::string as fruit_description
  from src
)
select * from flat
{% if is_incremental() %}
where fruit_name not in (select fruit_name from {{ this }})
{% endif %}
