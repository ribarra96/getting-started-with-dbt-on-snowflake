with src as (
  select payload from {{ source('onepiece_staging','STG_ONEPIECE_BOATS') }}
),
flat as (
  select
    payload:name::string as name,
    payload:class::string as class,
    coalesce(payload:captain.name::string, payload:captain::string) as captain_name
  from src
)
select * from flat
