select date(datetime) as day, max(aqi) as max_aqi
from {{ ref("joined_data_model") }}
group by day
