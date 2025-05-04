select date(datetime) as day, count(*) as pm2_5_violations
from {{ ref("joined_data_model") }}
where pm2_5 > 25
group by day
