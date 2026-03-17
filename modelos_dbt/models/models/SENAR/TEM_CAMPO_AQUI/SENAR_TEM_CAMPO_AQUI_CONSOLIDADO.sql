{{ config(materialized="table") }}
SELECT 

            plataforma,
            placement,
            date as event_date,
            campaign_name,
            ad_set_name,
            promoted_post_name,  -- Renomeando para padronizar
            promoted_post_image_url,
            url_destino,  -- Renomeando para padronizar
            cost,
            impressions,
            link_clicks,
            engajamento as engagements, 
            video_play_actions,
            video_watches_at_25 as video_watches_at_25_,
            video_watches_at_50 as video_watches_at_50_,
            video_watches_at_75 as video_watches_at_75_,
            video_watches_at_100 as video_watches_at_100_,
            idseed as seed,
            content
FROM {{source("SENAR_TEM_CAMPO_AQUI_SOURCES", "META_RAW")}}
WHERE date is not NULL