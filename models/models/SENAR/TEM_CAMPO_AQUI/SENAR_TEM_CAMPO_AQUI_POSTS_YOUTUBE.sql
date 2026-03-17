{{ config(materialized="table") }}

with 
video_yt as (

select 

data_da_postagem as data_da_postagem, 
link_da_publicacao,
"vídeos" as tipo,
tempo,
titulo,
"youtube" as rede,
descricao,
visualizacoes,
comentarios,
gostei,
nao_gostei,
inscritos,
engajamentos

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "YOUTUBE-ORGANICO-VIDEOS_RAW") }}
where 1 = 1 and data_da_postagem is not null
),

short_yt as (

select  

data_da_postagem as data_da_postagem, 
link_da_publicaco as link_da_publicacao,
tipo,
tempo,
titulo,
"youtube" as rede,
descricao,
visualizacoes,
comentarios,
gostei,
nao_gostei,
inscritos,
engajamentos

from {{ source("SENAR_TEM_CAMPO_AQUI_SOURCES", "YOUTUBE-ORGANICO-SHORTS_RAW") }}
where 1 = 1 and data_da_postagem is not null


),

obt as (

select * from video_yt
union all
select * from short_yt
)

select * from obt order by data_da_postagem desc
