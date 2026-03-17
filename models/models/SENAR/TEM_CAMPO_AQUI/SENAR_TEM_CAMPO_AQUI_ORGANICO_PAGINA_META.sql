{{ config(materialized="table") }}
with

    organico_fb as (
        select
            fetch_date as event_date, page_followers as seguidores, "facebook" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "ORGANICO_META_RAW") }}
        where 1 = 1 and fetch_date is not null
    ),

    historico_organico_ig as (
        select
            date as event_date, total_de_seguidores as seguidores, "instagram" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "HISTORICO_SEGUIDORES_IG_RAW") }}
        where 1 = 1 and date is not null
    ),

    historico_organico_fb as (
        select parse_date('%d/%m/%Y', data) as event_date, total_de_seguidores as seguidores, "facebook" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "HISTORICO_SEGUIDORES_FB_RAW") }}
        where 1 = 1 and data is not null
    ),

    organico_ig as (
        select
            fetch_date as event_date,
            profile_followers as seguidores,
            "instagram" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "ORGANICO-IG_RAW") }}
        where 1 = 1 and fetch_date is not null
    ),

    obt as (

        select *
        from organico_ig
        union all
        select *
        from organico_fb
        union all
        select *
        from historico_organico_ig
        union all
        select *
        from historico_organico_fb
    )

select *
from obt
