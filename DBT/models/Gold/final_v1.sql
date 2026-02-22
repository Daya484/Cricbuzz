select i.*,p.*,r.* from {{ ref('stg_gold_team_international') }} as i
left join {{ ref('stg_gold_team_players') }}  as p 
on i.team_id=p.id 
or i.image_id=p.image_id
left join {{ ref('stg_gold_team_results') }} as r
on i.team_id=r.match_id