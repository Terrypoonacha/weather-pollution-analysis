select date(datetime) as day, avg(pm2_5) as avg_pm2_5, avg(pm10) as avg_pm10
from {{ ref("joined_data_model") }}
group by day
