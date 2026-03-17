{{ config(materialized="table") }}

with
    PLANEJADO_RAW as (
        select
            aba_referencia,	
            mes,
            id_veiculo,
            rede,	
            veiculo,
            idseed,
            veiculo_dsp,
            veiculo_inventario,
            fonte_dados_principal,
            fonte_dados_orcamento,
            uf,
            praca,
            canal,
            modelo_de_compra,
            segmentacao,
            objetivo,
            tipo_de_compra,
            formato,
            total_ins,
            valor_liquido,
            valor_unitario,
            data_inicio,
            data_fim,
            nome_campanha,
            viewability_contratado,
            secundagem_comprada,
            obs,
            tech_fee,
            regra_de_negocio,
            PI,
            numero_projeto,
            cliente,
            flight_campanha

        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "PLANEJADO_RAW") }}
        where 1 = 1 and aba_referencia is not null
    )

select *
from PLANEJADO_RAW
where 1 = 1 and aba_referencia is not null
