{{ config(materialized="table") }}
with

    organico_fb as (
        select
            fetch_date as event_date,
            coalesce(link_to_post, post_link) as post,
            post_type as tipo_post,
            safe_cast(likes as integer) as likes,
            safe_cast(comments as integer) as comments,
            safe_cast(likes_per_post as integer) as likes_per_post,
            safe_cast(comments_per_post as integer) as comments_per_post,
            "facebook" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "ORGANICO-VIDEOS-META_RAW") }}
        where 1 = 1 and fetch_date is not null
    ),

    organico_ig as (
        select
            date as event_date,
            link_to_post as post,
            post_type as tipo_post,
            likes,
            comments,
            likes_per_post,
            comments_per_post,
            "instagram" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "ORGANICO-POSTS-IG_RAW") }}
        where 1 = 1 and date is not null
    ),

    obt as (

        select *
        from organico_ig
        union all
        select *
        from organico_fb

    )

select *
from obt
