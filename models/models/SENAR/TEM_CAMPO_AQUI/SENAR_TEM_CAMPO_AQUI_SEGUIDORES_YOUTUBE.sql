{{ config(materialized="table") }}


select *, "youtube" as rede

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "HISTORICO_SEGUIDORES_YOUTUBE_RAW") }}
where 1 = 1 and date is not null
