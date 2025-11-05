drop view if exists gold.daily_summary;
create view gold.daily_summary as 
    select 
        ga4.event_timestamp as "date",
        ga4.content_id as content_id,
        ga4.cnt_pageviews,
        ga4.cnt_session,
        ga4.cnt_user_engagment,
        gam.sum_impressions,
        gam.sum_clicks,
        gam.sum_revenue_usd
    from gold.ga4_daily_summary ga4
    left join gold.gam_daily_summary gam
    on ga4.event_timestamp::date = gam.served_at::date and ga4.content_id::int = gam.content_id::int