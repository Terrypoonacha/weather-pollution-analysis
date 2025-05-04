select
    wp.datetime,
    wp.aqi,
    wp.co,
    wp.no,
    wp.no2,
    wp.o3,
    wp.so2,
    wp.pm2_5,
    wp.pm10,
    wp.nh3,
    sd.samplingpoint,
    sd.pollutant,
    sd.start,
    sd.end,
    sd.value as static_value,
    sd.unit,
    sd.aggtype,
    sd.validity,
    sd.verification,
    sd.resulttime
from {{ ref("weather_pollution_model") }} as wp
left join
    {{ ref("static_data_model") }} as sd
    on date(wp.datetime) between sd.start and sd.end
    and sd.pollutant in (1, 5)
