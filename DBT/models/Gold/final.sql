select * 
from {{ ref('stg_gold_team_international') }}  as i
left join {{ ref('stg_gold_team_players') }} as p
on i.team_id=p.id
left join {{ ref('stg_gold_team_results')}} as r
on i.image_id=r.match_id