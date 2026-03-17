{{ config(materialized="table") }}


select *, "meta" as rede

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "META_VISITAS_AO_PERFIL_RAW") }}
where 1 = 1 and dia is not null
