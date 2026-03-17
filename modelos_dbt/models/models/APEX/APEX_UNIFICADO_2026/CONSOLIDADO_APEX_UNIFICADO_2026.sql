{{ config(materialized="table") }}

with
    linkedin_data as (
        select
            plataforma,
            placement,
            date,
            campaign_name,
            ad_set_name,
            promoted_post_name,
            promoted_post_image_url,
            url_destino,
            utm_content,
            cost,
            impressions,
            link_clicks,
            post_engagements,
            post_shares,
            post_comments,
            post_reactions,
            page_subscribes,
            video_play_actions,
            video_watches_at_25,
            video_watches_at_50,
            video_watches_at_75,
            video_watches_at_100,
            content,
            idseed
        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "LINKEDIN_RAW") }}
        where 1 = 1 and date is not null
    ),

    meta_data as (
        select
            plataforma,
            placement,
            date,
            campaign_name,
            ad_set_name,
            promoted_post_name,
            promoted_post_image_url,
            url_destino,
            utm_content,
            cost,
            impressions,
            link_clicks,
            post_engagements,
            post_shares,
            post_comments,
            post_reactions,
            page_subscribes,
            video_play_actions,
            video_watches_at_25,
            video_watches_at_50,
            video_watches_at_75,
            video_watches_at_100,
            content,
            idseed
        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "META_RAW") }}
        where 1 = 1 and date is not null
    ),

    google_data as (
        select
            plataforma,
            placement,
            date,
            campaign_name,
            ad_set_name,
            promoted_post_name,
            promoted_post_image_url,
            url_destino,
            utm_content,
            cost,
            impressions,
            link_clicks,
            post_engagements,
            post_shares,
            post_comments,
            post_reactions,
            page_subscribes,
            video_play_actions,
            video_watches_at_25,
            video_watches_at_50,
            video_watches_at_75,
            video_watches_at_100,
            content,
            idseed
        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "GOOGLE_RAW") }}
        where 1 = 1 and date is not null
    ),

    tiktok_data as (
        select
            plataforma,
            placement,
            date,
            campaign_name,
            ad_set_name,
            promoted_post_name,
            promoted_post_image_url,
            url_destino,
            utm_content,
            cost,
            impressions,
            link_clicks,
            post_engagements,
            post_shares,
            post_comments,
            post_reactions,
            page_subscribes,
            video_play_actions,
            video_watches_at_25,
            video_watches_at_50,
            video_watches_at_75,
            video_watches_at_100,
            content,
            idseed
        from {{ source("APEX_UNIFICADO_2026_raw_ads_sources", "TIKTOK_RAW") }}
        where 1 = 1 and date is not null
    ),


    obt as (

        select *
        from meta_data
        union all
        select *
        from google_data
        union all
        select *
        from tiktok_data
        union all
        select *
        from linkedin_data

    )

select *
from obt
