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

