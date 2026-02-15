with silver as(
select 
  market_code,
  CASE
    WHEN LENGTH(TRIM(market_code)) = 2 THEN TRIM(market_code)
    ELSE NULL
  END AS market_code2,
  CASE
    WHEN LENGTH(TRIM(market_code)) = 3 THEN TRIM(market_code)
    ELSE NULL
  END AS market_code3,
team_id,	
country_name,	
image_id,	
file_name,	
update_timestamp

from {{ source('src_team_ext','silver_team_distinct_ingest') }}
)

select 
market_code,
coalesce(s.market_code2,m.alpha2) as market_code2,
coalesce(s.market_code3,m.alpha3) as market_code3,
team_id,	
country_name,	
image_id,	
file_name,	
update_timestamp
from silver as s
left join {{ ref('dim_market') }} as m 
on lower(s.market_code2)=lower(m.alpha2)
or lower(s.market_code3)=lower(m.alpha3)
or lower(s.country_name)=lower(m.country)