-- models/dim_characters.sql
{{ config(materialized='incremental', unique_key='character_name', on_schema_change='sync') }}

with src as (
  select payload from {{ source('onepiece_staging','STG_ONEPIECE_CHARACTERS') }}
),
flat as (
  select
    payload:name::string as character_name,
    payload:fruit::string as fruit, -- used to join to fruits.fruit_name
    coalesce(payload:character_captain.name::string, payload:captain.name::string, payload:captain::string) as character_captain_name,
    payload:affiliation::string as affiliation,
    payload:bounty::string as bounty
  from src
)
select * from flat
{% if is_incremental() %}
where character_name not in (select character_name from {{ this }})
{% endif %}
