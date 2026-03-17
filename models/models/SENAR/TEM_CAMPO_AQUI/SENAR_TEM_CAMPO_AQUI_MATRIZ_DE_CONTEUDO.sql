{{ config(materialized="table") }}
with

    matriz_conteudo_data as (
        select

            nome_da_peca as criativo,
            rede_social as veiculo,
            formato as formato,
            (incio_da_veiculacao) as inicio,
            (final_da_veiculacao) as fim,
            objetivo as tipo_de_compra,
            segmentacao_do_publico as segmentacao,
            status_com_cliente as status_,
            objetivo as objetivo,
            link_com_peca_e_texto_de_apoio as link_peca,
            url_parametrizada as link_parametrizado,
            link_do_post as link_do_post,
            campanha,
            conjunto,
            criativo as content,
            tipo_de_publicacao as tipo_de_publicacao
        from
            {{
                source(
                    "SENAR_TEM_CAMPO_AQUI_SOURCES", "MATRIZ_DE_CONTEUDO_RAW"
                )
            }}
        where 1 = 1 and url_parametrizada is not null
    )

select * from  matriz_conteudo_data