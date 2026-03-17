{{ config(materialized="table") }}
with
    organico as (
        select *, "tiktok" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "ORGANICO_TIKTOK_RAW") }}
        where 1 = 1 and date is not null and date > '2025-09-29'
    ),
    historico_tiktok as (
        select
            data as date,
            safe_cast(null as integer) as video_views,
            safe_cast(null as integer) as profile_views,
            safe_cast(null as integer) as total_engagement,
            safe_cast(null as integer) as likes,
            safe_cast(null as integer) as comments,
            safe_cast(null as integer) as shares,
            total_de_seguidores as new_followers,
            safe_cast(null as integer) as current_followers,
            "tiktok" as rede
        from
            {{
                source(
                    "SENAR_TEM_CAMPO_AQUI_SOURCES", "HISTORICO_SEGUIDORES_TIKTOK_RAW"
                )
            }}
        where 1 = 1 and data is not null
    ),
    obt as (
        select *
        from organico
        union all
        select *
        from historico_tiktok
    )
select *
from obt 
order by date asc
