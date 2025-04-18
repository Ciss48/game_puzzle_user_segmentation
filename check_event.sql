select DISTINCT event_name from `coffee-please-b767a.analytics_454954686.events_intraday_20250416`;
-- Important event
-- engagement: user_engagement, screen_view, open_app, first_open, session_start, app_remove, daily_task_claim, daily_chest_claim, daily_login_claim
-- revenue: ad_impression, ads_inter_show, ads_reward_complete, booster_buy, in_app_purchase
-- level: lose_level, win_level, winstreak_start, start_level, revive, booster_use, start_level_phase
-- ?: ad_cgteam_impression, af_inters, campaign_firt_open, Total_Ads_Revenue, ads+revenue_0_02
## this is comment 

CREATE OR REPLACE TABLE `coffee-please-b767a.Dung_flatten_table.start_level`
PARTITION BY event_date
CLUSTER BY level, version, country AS
SELECT
    event_timestamp,
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) as event_date,
    app_info.version as version,
    geo.country as country,
    device.mobile_model_name as mobile_name,
    CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') AS INT64) as level
FROM `coffee-please-b767a.analytics_454954686.events_*`
WHERE CAST(_TABLE_SUFFIX AS INT64) BETWEEN 20250301 AND 20250415
AND event_name = 'start_level';



-- dau
SELECT 
    event_date,
        version,
        platform,
        COUNT(DISTINCT user_pseudo_id) as daily_active_users
    FROM `coffee-please-b767a.Dung_flatten_table.session_start`
    GROUP BY event_date, version, platform
    ORDER BY event_date DESC, version, platform    limit 100

-- user by country and use iap by country
with user_country as (
select 
    country ,
    COUNT(DISTINCT user_pseudo_id) as num_user 
 from `coffee-please-b767a.Dung_flatten_table.user_engagement`
where platform = 'ANDROID' and version = '1.4.1'
group by country
order by country),
iap_country as (
select 
    country ,
    COUNT(DISTINCT user_pseudo_id) as num_user 
 from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
 where platform = 'ANDROID' and version = '1.4.1'
group by country
order by country)
select 
    user_country.country,
    user_country.num_user,
    iap_country.num_user as num_iap
from user_country
left join iap_country on user_country.country = iap_country.country 
where user_country.num_user > 100

-- 


-- first iap by level
WITH first_purchases AS (
  SELECT 
    user_pseudo_id,
    event_timestamp as first_purchase_timestamp
  FROM `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  WHERE platform = 'ANDROID' AND version = '1.4.1' AND country = 'United States'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) = 1
),
level_ranges AS (
  SELECT
    user_pseudo_id,
    level,
    event_timestamp as level_start_time,
    LEAD(event_timestamp) OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY event_timestamp
    ) as next_level_start_time
  FROM `coffee-please-b767a.Dung_flatten_table.start_level`
  WHERE platform = 'ANDROID' AND version = '1.4.1' AND country = 'United States'
)
SELECT
  lr.level,
  COUNT(DISTINCT fp.user_pseudo_id) as num_first_iap
FROM level_ranges lr
INNER JOIN first_purchases fp
  ON lr.user_pseudo_id = fp.user_pseudo_id
  AND fp.first_purchase_timestamp >= lr.level_start_time
  AND (
    lr.next_level_start_time IS NULL 
    OR fp.first_purchase_timestamp < lr.next_level_start_time
  )
GROUP BY lr.level
ORDER BY lr.level

-- iap by level
WITH purchases AS (
  SELECT 
    user_pseudo_id,
    event_timestamp as purchase_timestamp
  FROM `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  WHERE platform = 'ANDROID' AND version = '1.4.1' AND country = 'United States'
),
level_ranges AS (
  SELECT
    user_pseudo_id,
    level,
    event_timestamp as level_start_time,
    LEAD(event_timestamp) OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY event_timestamp
    ) as next_level_start_time
  FROM `coffee-please-b767a.Dung_flatten_table.start_level`
  WHERE platform = 'ANDROID' AND version = '1.4.1' AND country = 'United States'
)
SELECT
  lr.level,
  COUNT(DISTINCT p.user_pseudo_id) as num_iap_users,
  COUNT(*) as num_purchases
FROM level_ranges lr
INNER JOIN purchases p
  ON lr.user_pseudo_id = p.user_pseudo_id
  AND p.purchase_timestamp >= lr.level_start_time
  AND (
    lr.next_level_start_time IS NULL 
    OR p.purchase_timestamp < lr.next_level_start_time
  )
GROUP BY lr.level
ORDER BY lr.level

select count(distinct user_pseudo_id) from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`


-- check 
select max(level) 
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
where user_pseudo_id = '0334D86505D04826A651B7F53512938A'







-- first iap by level
WITH first_iap AS (
  SELECT 
    user_pseudo_id,
    MIN(event_timestamp) as first_iap_timestamp
  FROM `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  GROUP BY user_pseudo_id
),
user_timeline AS (
  SELECT
    f.user_pseudo_id,
    f.event_timestamp as first_open_timestamp,
    i.first_iap_timestamp,
    TIMESTAMP_DIFF(TIMESTAMP_MICROS(i.first_iap_timestamp), TIMESTAMP_MICROS(f.event_timestamp), DAY) as days_to_first_iap
  FROM `coffee-please-b767a.Dung_flatten_table.first_open` f
  LEFT JOIN first_iap i
    ON f.user_pseudo_id = i.user_pseudo_id
)
SELECT
  days_to_first_iap,
  COUNT(DISTINCT user_pseudo_id) as num_users
FROM user_timeline
WHERE first_iap_timestamp IS NOT NULL
GROUP BY days_to_first_iap
ORDER BY days_to_first_iap

select count(distinct user_pseudo_id) from `coffee-please-b767a.Dung_flatten_table.first_open` 
where platform = 'ANDROID' and version = '1.4.1' and country = 'United States'
and event_date between '2025-03-15' and '2025-03-30'


with iap_user as (
select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.first_open`
where platform = 'ANDROID' and version = '1.4.1' and event_date between '2025-03-15' and '2025-03-30'
and user_pseudo_id in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'ANDROID' and version = '1.4.1')),
non_iap_user as (
select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.first_open`
where platform = 'ANDROID' and version = '1.4.1' and event_date between '2025-03-15' and '2025-03-30'
and user_pseudo_id not in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'ANDROID' and version = '1.4.1'))
-- select count(distinct user_pseudo_id) from iap_user
select count(distinct user_pseudo_id) from non_iap_user

--- winrate 
  WITH start_agg AS (
    SELECT 
      level,
      COUNT(DISTINCT user_pseudo_id) as user_start,
      COUNT(*) as event_start
    FROM `coffee-please-b767a.Dung_flatten_table.start_level`
    GROUP BY level
  ),
  win_agg AS (
    SELECT
      level, 
      COUNT(*) as event_win
    FROM `coffee-please-b767a.Dung_flatten_table.win_level`
    GROUP BY level order by level 
  ),
  lose_agg AS (
    SELECT
      level,
      COUNT(*) as event_lose 
    FROM `coffee-please-b767a.Dung_flatten_table.lose_level`
    GROUP BY level order by level 
  )
  SELECT
    s.level,
    s.user_start,
    s.event_start,
    COALESCE(w.event_win, 0) as event_win,
    COALESCE(l.event_lose, 0) as event_lose
  FROM start_agg s
  LEFT JOIN win_agg w ON s.level = w.level
  LEFT JOIN lose_agg l ON s.level = l.level
  ORDER BY s.level





-- DAU per two type
-- Simplified query to get DAU for IAP and non-IAP users
  with user_segments as (
    select distinct user_pseudo_id,
    case when user_pseudo_id in (
      select distinct user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
      where platform = 'ANDROID' and version = '1.4.1'
    ) then 'iap' else 'non_iap' end as user_type
    from `coffee-please-b767a.Dung_flatten_table.first_open`
    where platform = 'ANDROID' 
    and version = '1.4.1' 
    and event_date between '2025-03-15' and '2025-03-30'
  ),
  daily_active as (
    select 
      e.event_date,
      s.user_type,
      count(distinct e.user_pseudo_id) as dau
    from `coffee-please-b767a.Dung_flatten_table.user_engagement` e
    join user_segments s on e.user_pseudo_id = s.user_pseudo_id
    where e.platform = 'ANDROID' 
    and e.version = '1.4.1'
    and e.event_date between '2025-04-01' and '2025-04-15'
    group by e.event_date, s.user_type
  )
  select
    event_date,
    max(case when user_type = 'iap' then dau end) as dau_iap,
    max(case when user_type = 'non_iap' then dau end) as dau_non_iap
  from daily_active
  group by event_date
  order by event_date


-- revenue from country 
select 
  country,
  sum(revenue) as revenue
from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'IOS' and version = '1.0.5'
group by country



select 
  count(distinct user_pseudo_id) as num_user
from `coffee-please-b767a.Dung_flatten_table.user_engagement`
where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan') 
and user_pseudo_id not in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31')
and event_date between '2025-03-15' and '2025-03-31'


-- number of iap user in 15/3 - 31/3; fo in 15/3 - 20/3
select 
  distinct user_pseudo_id 
from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'IOS' AND version = '1.0.5' AND country in ('United States') 
and event_date between '2025-03-15' and '2025-03-31' 
and user_pseudo_id in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.first_open`
where platform = 'IOS' AND version = '1.0.5' AND country in ('United States') 
and event_date between '2025-03-15' and '2025-03-20')

-- number of not iap user in 15/3 - 31/3; fo in 15/3 - 20/3
select 
  distinct user_pseudo_id
from `coffee-please-b767a.Dung_flatten_table.user_engagement`
where platform = 'IOS' AND version = '1.0.5' AND country in ('United States') 
and event_date between '2025-03-15' and '2025-03-31' 
and user_pseudo_id in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.first_open`
where platform = 'IOS' AND version = '1.0.5' AND country in ('United States') 
and event_date between '2025-03-15' and '2025-03-20')
and user_pseudo_id not in (select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31')





with user_segments as (
  select distinct user_pseudo_id,
  case 
    when user_pseudo_id in (
      select distinct user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
      where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
      and event_date between '2025-03-15' and '2025-03-31'
      and user_pseudo_id in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.first_open`
        where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
        and event_date between '2025-03-15' and '2025-03-20'
      )
    ) then 'iap'
    when user_pseudo_id in (
      select distinct user_pseudo_id
      from `coffee-please-b767a.Dung_flatten_table.first_open`
      where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
      and event_date between '2025-03-15' and '2025-03-20'
    )
    and user_pseudo_id not in (
      select distinct user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
      where platform = 'IOS' and version = '1.0.5' 
      and event_date between '2025-03-15' and '2025-03-31'
    ) then 'non_iap'
  end as user_type
  from `coffee-please-b767a.Dung_flatten_table.user_engagement`
  where platform = 'IOS' AND version = '1.0.5'
),
daily_active as (
  select 
    e.event_date,
    s.user_type,
    count(distinct e.user_pseudo_id) as dau
  from `coffee-please-b767a.Dung_flatten_table.user_engagement` e
  join user_segments s on e.user_pseudo_id = s.user_pseudo_id
  where e.platform = 'IOS' 
  and e.version = '1.0.5'
  and e.event_date between '2025-03-20' and '2025-03-31'
  group by e.event_date, s.user_type
)
select
  event_date,
  max(case when user_type = 'iap' then dau end) / 15 * 100 as percent_dau_iap,
  max(case when user_type = 'non_iap' then dau end) / 2091 * 100 as percent_dau_non_iap
from daily_active
group by event_date
order by event_date








-- user reward 
with user_segments as (
  select distinct user_pseudo_id,
    case 
      when user_pseudo_id in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
        where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
        and event_date between '2025-03-15' and '2025-03-31'
        and user_pseudo_id in (
          select distinct user_pseudo_id 
          from `coffee-please-b767a.Dung_flatten_table.first_open`
          where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
          and event_date between '2025-03-15' and '2025-03-20'
        )
      ) then 'iap'
      when user_pseudo_id in (
        select distinct user_pseudo_id
        from `coffee-please-b767a.Dung_flatten_table.first_open`
        where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
        and event_date between '2025-03-15' and '2025-03-20'
      )
      and user_pseudo_id not in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
        where platform = 'IOS' and version = '1.0.5' 
        and event_date between '2025-03-15' and '2025-03-31'
      ) then 'non_iap'
    end as user_type
    from `coffee-please-b767a.Dung_flatten_table.user_engagement`
    where platform = 'IOS' AND version = '1.0.5'
)
select 
  a.placement,
  count(distinct case when s.user_type = 'iap' then a.user_pseudo_id end) as num_ads_by_iap,
  count(distinct case when s.user_type = 'non_iap' then a.user_pseudo_id end) as num_ads_by_non_iap
from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete` a
join user_segments s on a.user_pseudo_id = s.user_pseudo_id
where a.platform = 'IOS' and a.version = '1.0.5'
group by a.placement


-- ad rw segment 
-- by level 
with user_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
  where platform = 'IOS' and version = '1.0.5'
    and user_pseudo_id in (
      select user_pseudo_id
      from `coffee-please-b767a.Dung_flatten_table.first_open`
      where platform = 'IOS' and version = '1.0.5'
        and event_date between '2025-03-15' and '2025-03-20'
    )
), 
user_non_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.first_open`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-20'
    and user_pseudo_id not in (
      select user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
      where platform = 'IOS' and version = '1.0.5'
    )
), 
level_ad_rw as (
  select 
    level,
    count(distinct user_pseudo_id) as user_ad_rw
  from `coffee-please-b767a.Dung_flatten_table.start_level`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_ad_rw)
  group by level
), 
level_non_ad_rw as (
  select 
    level,
    count(distinct user_pseudo_id) as user_non_ad_rw
  from `coffee-please-b767a.Dung_flatten_table.start_level`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_non_ad_rw)
  group by level
)
select 
  a.level,
  a.user_ad_rw,
  b.user_non_ad_rw
from level_ad_rw a
join level_non_ad_rw b on a.level = b.level
order by a.level;


-- 


winrate_non_iap = client.query(query).to_dataframe()
winrate_non_iap['winrate'] = winrate_non_iap['event_win'] / (winrate_non_iap['event_win'] + winrate_non_iap['event_lose'])
# Filter level <= 50
winrate_non_iap_filtered = winrate_non_iap[winrate_non_iap['level'] <= 50]
winrate_iap_filtered = winrate_iap[winrate_iap['level'] <= 50]

# Create line plot
plt.figure(figsize=(12,6))
plt.plot(winrate_non_iap_filtered['level'], winrate_non_iap_filtered['winrate'], label='Non-IAP Users')
plt.plot(winrate_iap_filtered['level'], winrate_iap_filtered['winrate'], label='IAP Users')

plt.xlabel('Level')
plt.ylabel('Win Rate')
plt.title('Win Rate by Level, version 1.0.5, US, FO 15-20, regard: 15-21: IAP vs Non-IAP Users')
plt.legend()
plt.grid(True)
plt.show()



























-- iap drop level 
with user_segments as (
  select distinct user_pseudo_id,
    case 
      when user_pseudo_id in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
        where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
        and event_date between '2025-03-15' and '2025-03-31'
        and user_pseudo_id in (
          select distinct user_pseudo_id 
          from `coffee-please-b767a.Dung_flatten_table.first_open`
          where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
          and event_date between '2025-03-15' and '2025-03-20'
        )
      ) then 'iap'
      when user_pseudo_id in (
        select distinct user_pseudo_id
        from `coffee-please-b767a.Dung_flatten_table.first_open`
        where platform = 'IOS' AND version = '1.0.5' AND country in ('United States', 'United Kingdom','Japan')
        and event_date between '2025-03-15' and '2025-03-20'
      )
      and user_pseudo_id not in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
        where platform = 'IOS' and version = '1.0.5' 
        and event_date between '2025-03-15' and '2025-03-31'
      ) then 'non_iap'
    end as user_type
    from `coffee-please-b767a.Dung_flatten_table.user_engagement`
    where platform = 'IOS' AND version = '1.0.5'
),
level_iap as (
  select 
    level,
    count(distinct user_pseudo_id) as user_iap
  from `coffee-please-b767a.Dung_flatten_table.start_level`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_segments where user_type = 'iap')
  group by level
), 
level_non_iap as (
  select 
    level,
    count(distinct user_pseudo_id) as user_non_iap
  from `coffee-please-b767a.Dung_flatten_table.start_level`
  where platform = 'IOS' and version = '1.0.5'
  and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_segments where user_type = 'non_iap')
  group by level
)
select 
  a.level,
  a.user_iap,
  b.user_non_iap
from level_iap a
join level_non_iap b on a.level = b.level
order by a.level;



-- 



with user_segments as (
  select distinct user_pseudo_id,
  case 
    when user_pseudo_id in (
      select distinct user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
      where platform = 'IOS' AND version = '1.0.5' 
      and event_date between '2025-03-15' and '2025-03-31'
      and user_pseudo_id in (
        select distinct user_pseudo_id 
        from `coffee-please-b767a.Dung_flatten_table.first_open`
        where platform = 'IOS' AND version = '1.0.5' 
        and event_date between '2025-03-15' and '2025-03-20'
      )
    ) then 'adrw'
    when user_pseudo_id in (
      select distinct user_pseudo_id
      from `coffee-please-b767a.Dung_flatten_table.first_open`
      where platform = 'IOS' AND version = '1.0.5' 
      and event_date between '2025-03-15' and '2025-03-20'
    )
    and user_pseudo_id not in (
      select distinct user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
      where platform = 'IOS' and version = '1.0.5' 
      and event_date between '2025-03-15' and '2025-03-31'
    ) then 'non_adrw'
  end as user_type
  from `coffee-please-b767a.Dung_flatten_table.user_engagement`
  where platform = 'IOS' AND version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
),
daily_active as (
  select 
    e.event_date,
    s.user_type,
    count(distinct e.user_pseudo_id) as dau
  from `coffee-please-b767a.Dung_flatten_table.user_engagement` e
  join user_segments s on e.user_pseudo_id = s.user_pseudo_id
  where e.platform = 'IOS' 
  and e.version = '1.0.5'
  and e.event_date between '2025-03-20' and '2025-03-31'
  group by e.event_date, s.user_type
)
select
  event_date,
  max(case when user_type = 'adrw' then dau end) / 15 * 100 as percent_dau_adrw,
  max(case when user_type = 'non_adrw' then dau end) / 2091 * 100 as percent_dau_non_adrw
from daily_active
group by event_date
order by event_date




-- pay_rate 
with user_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
  where platform = 'IOS' and version = '1.0.5'
    and user_pseudo_id in (
      select user_pseudo_id
      from `coffee-please-b767a.Dung_flatten_table.first_open`
      where platform = 'IOS' and version = '1.0.5'
        and event_date between '2025-03-15' and '2025-03-20'
    )
), 
user_non_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.first_open`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-20'
    and user_pseudo_id not in (
      select user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
      where platform = 'IOS' and version = '1.0.5'
    )
)
-- ,pay_rate_adrw as (
  select 
    count(distinct user_pseudo_id) as num_user
  from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_ad_rw)
),
pay_rate_non_adrw as (
  select 
    count(distinct user_pseudo_id) as num_user
  from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_non_ad_rw)
)


with user_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
  where platform = 'IOS' and version = '1.0.5'
    and user_pseudo_id in (
      select user_pseudo_id
      from `coffee-please-b767a.Dung_flatten_table.first_open`
      where platform = 'IOS' and version = '1.0.5'
        and event_date between '2025-03-15' and '2025-03-20'
    )
), 
user_non_ad_rw as (
  select distinct user_pseudo_id
  from `coffee-please-b767a.Dung_flatten_table.first_open`
  where platform = 'IOS' and version = '1.0.5'
    and event_date between '2025-03-15' and '2025-03-20'
    and user_pseudo_id not in (
      select user_pseudo_id 
      from `coffee-please-b767a.Dung_flatten_table.ads_reward_complete`
      where platform = 'IOS' and version = '1.0.5'
    )
)
,pay_rate_adrw as (
  select 
    count(distinct user_pseudo_id) as num_user
  from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_ad_rw)
) ,pay_rate_non_adrw as (
  select 
    count(distinct user_pseudo_id) as num_user
  from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from user_non_ad_rw)
) 
select num_user as user_adrw_iap from pay_rate_adrw
union all
select num_user as user_non_adrw_iap from pay_rate_non_adrw
union all
select count(distinct user_pseudo_id)  from user_ad_rw
union all
select count(distinct user_pseudo_id)  from user_non_ad_rw






-- booster user 
with b as
(
  select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.win_level`
  where level >= 20 and platform = 'IOS' and version = '1.0.5' 
  AND user_pseudo_id IN (
    SELECT DISTINCT user_pseudo_id 
    FROM `coffee-please-b767a.Dung_flatten_table.first_open` 
    WHERE event_date BETWEEN '2025-03-15' AND '2025-03-20'  AND platform = 'IOS' AND version = '1.0.5'
  )
), a as (
SELECT 
  user_pseudo_id, 
  ROUND(COUNT(*) / 20, 4) AS start_time
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
WHERE level <= 20 
  AND platform = 'IOS' 
  AND version = '1.0.5'
  AND user_pseudo_id IN ( SELECT DISTINCT user_pseudo_id FROM b
  )
GROUP BY user_pseudo_id
)
select start_time, 
  count( distinct user_pseudo_id ) as num_user 
from a where start_time >= 1 group by start_time 
order by a.start_time



-- user segment by level  
with b as
(
  select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.win_level`
  where level >= 20 and platform = 'IOS' and version = '1.0.5' 
  AND user_pseudo_id IN (
    SELECT DISTINCT user_pseudo_id 
    FROM `coffee-please-b767a.Dung_flatten_table.first_open` 
    WHERE event_date BETWEEN '2025-03-15' AND '2025-03-20'  AND platform = 'IOS' AND version = '1.0.5'
  )
), a as (
SELECT 
  user_pseudo_id, 
  ROUND(COUNT(*) / 20, 2) AS start_time
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
WHERE level <= 20 
  AND platform = 'IOS' 
  AND version = '1.0.5'
  AND user_pseudo_id IN ( SELECT DISTINCT user_pseudo_id FROM b
  )
GROUP BY user_pseudo_id
), base as (
  select start_time, 
    count(distinct user_pseudo_id) as num_user 
  from a 
  where start_time >= 1 
  group by start_time
)
select 
  start_time,
  num_user,
  sum(num_user) over (order by start_time) as accumulate,
  round(100.0 * sum(num_user) over (order by start_time) / sum(num_user) over (), 2) as pct_accumulate
from base
order by start_time




-- drop 
with b as (
  select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.win_level`
  where level >= 20 and platform = 'IOS' and version = '1.0.5' 
  AND user_pseudo_id IN (
    SELECT DISTINCT user_pseudo_id 
    FROM `coffee-please-b767a.Dung_flatten_table.first_open` 
    WHERE event_date BETWEEN '2025-03-15' AND '2025-03-20'  AND platform = 'IOS' AND version = '1.0.5'
  )
), a as (
SELECT 
  user_pseudo_id, 
  ROUND(COUNT(*) / 20, 2) AS start_time
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
WHERE level <= 20 
  AND platform = 'IOS' 
  AND version = '1.0.5'
  AND user_pseudo_id IN ( SELECT DISTINCT user_pseudo_id FROM b
  )
GROUP BY user_pseudo_id
), best_user as (
  select distinct user_pseudo_id from a where start_time <= 1.05
), first_open_dates as (
  select 
    user_pseudo_id,
    min(event_date) as first_open_date
  from `coffee-please-b767a.Dung_flatten_table.first_open`
  where user_pseudo_id in (select user_pseudo_id from best_user)
  group by user_pseudo_id
), retention_data as (
  select
    f.user_pseudo_id,
    f.first_open_date,
    e.event_date,
    date_diff(e.event_date, f.first_open_date, DAY) as day_number
  from first_open_dates f
  left join `coffee-please-b767a.Dung_flatten_table.user_engagement` e
    on f.user_pseudo_id = e.user_pseudo_id
  where date_diff(e.event_date, f.first_open_date, DAY) between 0 and 7
)
  select
    day_number,
    count(distinct user_pseudo_id) as retained_users
  from retention_data
  group by day_number order by day_number








with b as (
  select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.win_level`
  where level >= 20 and platform = 'IOS' and version = '1.0.5' 
  AND user_pseudo_id IN (
    SELECT DISTINCT user_pseudo_id 
    FROM `coffee-please-b767a.Dung_flatten_table.first_open` 
    WHERE event_date BETWEEN '2025-03-15' AND '2025-03-20'  AND platform = 'IOS' AND version = '1.0.5'
  )
), a as (
SELECT 
  user_pseudo_id, 
  ROUND(COUNT(*) / 20, 2) AS start_time
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
WHERE level <= 20 
  AND platform = 'IOS' 
  AND version = '1.0.5'
  AND user_pseudo_id IN ( SELECT DISTINCT user_pseudo_id FROM b
  )
GROUP BY user_pseudo_id
), best_user as (
  select distinct user_pseudo_id from a where start_time <= 1.05
), worst_user as (
  select distinct user_pseudo_id from a where start_time >= 1.35
), start_level_agg as (
  select 
    level,
    user_pseudo_id,
    count(*) as start_count
  from `coffee-please-b767a.Dung_flatten_table.start_level`
  group by level, user_pseudo_id
), booster_agg as (
  select
    level,
    user_pseudo_id,
    count(*) as booster_count
  from `coffee-please-b767a.Dung_flatten_table.booster_use`
  group by level, user_pseudo_id
)
  select 
    s.level,
    count(distinct case when bu.user_pseudo_id is not null then s.user_pseudo_id end) as num_user_best_start,
    sum(case when bu.user_pseudo_id is not null then s.start_count else 0 end) as num_start_best,
    count(distinct case when wu.user_pseudo_id is not null then s.user_pseudo_id end) as num_user_worst_start,
    sum(case when wu.user_pseudo_id is not null then s.start_count else 0 end) as num_start_worst,
    sum(case when bu.user_pseudo_id is not null then COALESCE(bo.booster_count, 0) else 0 end) as num_booster_best,
    sum(case when wu.user_pseudo_id is not null then COALESCE(bo.booster_count, 0) else 0 end) as num_booster_worst
  from start_level_agg s
  left join best_user bu on s.user_pseudo_id = bu.user_pseudo_id
  left join worst_user wu on s.user_pseudo_id = wu.user_pseudo_id
  left join booster_agg bo 
    on s.level = bo.level 
    and s.user_pseudo_id = bo.user_pseudo_id
  group by s.level




-- payrate_best_worst 
with b as (
  select distinct user_pseudo_id from `coffee-please-b767a.Dung_flatten_table.win_level`
  where level >= 20 and platform = 'IOS' and version = '1.0.5' 
  AND user_pseudo_id IN (
    SELECT DISTINCT user_pseudo_id 
    FROM `coffee-please-b767a.Dung_flatten_table.first_open` 
    WHERE event_date BETWEEN '2025-03-15' AND '2025-03-20'  AND platform = 'IOS' AND version = '1.0.5'
  )
), a as (
SELECT 
  user_pseudo_id, 
  ROUND(COUNT(*) / 20, 2) AS start_time
FROM `coffee-please-b767a.Dung_flatten_table.start_level`
WHERE level <= 20 
  AND platform = 'IOS' 
  AND version = '1.0.5'
  AND user_pseudo_id IN ( SELECT DISTINCT user_pseudo_id FROM b
  )
GROUP BY user_pseudo_id
), best_user as (
  select distinct user_pseudo_id from a where start_time <= 1.05
), worst_user as (
  select distinct user_pseudo_id from a where start_time >= 1.35
) 
-- ,pay_rate_best as (
--   select 
--     count(distinct user_pseudo_id) as num_user
--   from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
--   where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
--     and user_pseudo_id in (select user_pseudo_id from best_user)
-- )
-- ,pay_rate_worst as (
  select 
    count(distinct user_pseudo_id) as num_user
  from `coffee-please-b767a.Dung_flatten_table.in_app_purchase`
  where platform = 'IOS' and version = '1.0.5' and event_date between '2025-03-15' and '2025-03-31'
    and user_pseudo_id in (select user_pseudo_id from worst_user)
)



-- test_avg_playtime 




