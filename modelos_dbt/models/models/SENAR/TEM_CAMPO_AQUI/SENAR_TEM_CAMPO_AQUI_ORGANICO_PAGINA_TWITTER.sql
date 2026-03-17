{{ config(materialized="table") }}


select *, "twitter" as rede

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "TWITTER_ORGANICO_PUBLICACOES_RAW") }}
where 1 = 1 and data_da_postagem is not null
