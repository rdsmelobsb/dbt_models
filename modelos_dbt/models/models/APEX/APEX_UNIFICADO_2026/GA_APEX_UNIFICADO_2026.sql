{{ config(materialized="table") }}

with
    ga_data as (
        select
            date,
            session_source__medium as source_medium,
            session_campaign_name as campaign,
            session_manual_ad_content as ad_content,
            sessions,
            total_users as users,
            new_users,
            average_session_duration as Avg_session_length,
            engaged_sessions as sessao_engajada,
            views as pageviews,
            bounces,
            idseed,
            content

        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "GA4_RAW") }}
        where 1 = 1 and date is not null
    )

select
    *,
from ga_data
where 1 = 1 and date is not null
