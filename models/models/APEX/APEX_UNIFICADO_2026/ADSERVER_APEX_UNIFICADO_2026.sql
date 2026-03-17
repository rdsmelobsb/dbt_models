{{ config(materialized="table") }}

with
    ADSERVER_RAW as (
        select
            date,
            campaign_name,
            site_name,
            format_name,
            dimension,
            type,
            impression,
            link_clicks,
            video_play_actions,
            video_watches_at_25,
            video_watches_at_50,
            video_watches_at_75,
            video_watches_at_100,
            viewables,
            content,
            idseed

        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "ADSERVER_RAW") }}
        where 1 = 1 and date is not null
    )

select *
from ADSERVER_RAW
where 1 = 1 and date is not null
