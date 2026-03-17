{{ config(materialized="table") }}
with

    matriz_conteudo_data as (
        select
            veiculo,
            valor_liquido,
            campanha,
            criativo,
            status,
            objetivo,
            tipo,
            tipo_de_compra,
            url_criativo,
            texto_de_apoio,
            url_destino,
            periodo,
            url_parametrizada,
            idseed,
            conjunto,
            content
        from
            {{
                source(
                    "APEX_UNIFICADO_2026_raw_ads_sources", "MATRIZ-DE-CONTEUDO_RAW"
                )
            }}
        where 1 = 1 and url_parametrizada is not null
    ),

    obt as (select * from matriz_conteudo_data)

select *
from obt
where url_parametrizada is not null
