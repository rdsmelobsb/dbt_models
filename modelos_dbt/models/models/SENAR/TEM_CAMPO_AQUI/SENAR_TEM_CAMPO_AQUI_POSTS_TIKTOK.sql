{{ config(materialized="table") }}
with

    organico_tiktok as (
        select
            *, "tiktok" as rede

        from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "TIKTOK-POSTS_RAW") }}
        where 1 = 1 and share_url is not null
    )

    select * from organico_tiktok