-- Metric: impression, click, revenue
with agg_daily as (
    select 
        served_at::date as served_at, 
        content_id::numeric as content_id,
        sum(impressions) as sum_impressions,
        sum(clicks) as sum_clicks,
        sum(revenue_usd) as sum_revenue_usd
    from bronze.gam_delivery
    where 1=1
        and _updated_at >= (select coalesce(max(_updated_at), '1999-01-01 00:00:01') from gold.gam_daily_summary)
    group by served_at::date, content_id
)
merge into gold.gam_daily_summary old
using agg_daily new
on old.served_at::date = new.served_at::date and old.content_id::numeric = new.content_id::numeric
when matched then 
    update set 
        sum_impressions = old.sum_impressions::numeric + new.sum_impressions::numeric,
        sum_clicks = old.sum_clicks::numeric + new.sum_clicks::numeric,
        sum_revenue_usd = old.sum_revenue_usd::numeric + new.sum_revenue_usd::numeric,
        _updated_at = current_timestamp
when not matched then
    insert (served_at, content_id, sum_impressions, sum_clicks, sum_revenue_usd)
    values (new.served_at, new.content_id, new.sum_impressions, new.sum_clicks, new.sum_revenue_usd) 