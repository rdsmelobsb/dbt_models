{{ config(materialized="table") }}


select *, "twitter" as rede

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "TWITTER_SEGUIDORES_RAW") }}
where 1 = 1 and data is not null
