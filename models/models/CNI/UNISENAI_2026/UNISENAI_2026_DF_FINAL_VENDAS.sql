{{
    config(
        materialized="incremental",
        unique_key="id_pedido",
        on_schema_change="sync_all_columns",
    )
}}

with
    df_final_vendas as (
        select
            *,
            -- 1. Divide o valor por 100
            (valor_total_item / 100) as valor_total_item_ajustado,

            -- 2. Trata milhar (ponto), decimal (vírgula) e converte para FLOAT64
            safe_cast(
                replace(
                    replace(add_pagamento, '.', ''), 
                    ',', 
                    '.'
                ) as float64
            ) as add_pagamento_float

        from {{ source("CNI_UNISENAI_2026_raw_ads_sources", "df_final_vendas") }}
        where
            id_pedido is not null

            {% if is_incremental() %}
                -- Garante que só traremos dados novos em relação ao que já existe na tabela
                and data_pedido > (select max(data_pedido) from {{ this }})
            {% endif %}
    ),

    calculo_faturamento as (
        select
            * except (
                valor_total_item,
                add_pagamento,
                valor_total_item_ajustado,
                add_pagamento_float
            ),
            -- Retornando aos nomes originais com os valores transformados
            valor_total_item_ajustado as valor_total_item,
            
            -- Trata nulos como 0 para garantir que o cálculo do faturamento funcione
            coalesce(add_pagamento_float, 0) as add_pagamento,
            
            -- Cálculo do faturamento final
            (valor_total_item_ajustado + coalesce(add_pagamento_float, 0)) as faturamento
            
        from df_final_vendas
    )


select *
from calculo_faturamento

order by data_pedido DESC
