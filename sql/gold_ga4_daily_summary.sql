-- Metric: pageview, session, user_engagment
with agg_daily as (
    select 
        ga4.event_timestamp::date as event_timestamp, 
        ga4.content_id::numeric as content_id,
        ca.site,
        ca.title,
        ca.author,
        ca.publish_timestamp,
        ca.section,
        count(*) filter (where ga4.event_name = 'page_view') as cnt_pageviews,
        count(distinct ga4.session_id) as cnt_session,
        count(*) filter (where ga4.event_name = 'user_engagement') as cnt_user_engagment
    from bronze.ga4 ga4
    left join bronze.cms_articles ca
    using(content_id)
    where 1=1
        and ga4._updated_at >= (select coalesce(max(_updated_at), '1999-01-01 00:00:01') from gold.ga4_daily_summary)
    group by ga4.event_timestamp::date, ga4.content_id, ca.site, ca.title, ca.author, ca.publish_timestamp, ca.section
)
merge into gold.ga4_daily_summary old
using agg_daily new
on old.event_timestamp::date = new.event_timestamp::date and old.content_id::numeric = new.content_id::numeric
when matched then 
    update set 
        cnt_pageviews = old.cnt_pageviews::numeric + new.cnt_pageviews::numeric,
        cnt_session = old.cnt_session::numeric + new.cnt_session::numeric,
        cnt_user_engagment = old.cnt_user_engagment::numeric + new.cnt_user_engagment::numeric,
        _updated_at = current_timestamp
when not matched then
    insert (event_timestamp, content_id, site, title, author, publish_timestamp, section, cnt_pageviews, cnt_session, cnt_user_engagment)
    values (new.event_timestamp, new.content_id, new.site, new.title, new.author, new.publish_timestamp, new.section, new.cnt_pageviews, new.cnt_session, new.cnt_user_engagment) 