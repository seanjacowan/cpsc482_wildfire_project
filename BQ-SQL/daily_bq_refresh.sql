WITH combined AS (
  SELECT
    w.date,
    LPAD(CAST(w.geoid AS STRING), 5, '0') AS geoid,
    w.temp_max_f,
    w.temp_min_f,
    w.rain_sum_in,
    w.wind_speed_max_mph,
    w.wind_gusts_max_mph,
    w.sunshine_duration_s,
    f.num_fires,
    f.avg_bright,
    f.avg_frp
  FROM
    (SELECT DISTINCT * FROM `wildfire_data.weather`) w

    LEFT OUTER JOIN

    (SELECT
      geoid,
      date,
      COUNT(*) AS num_fires,
      AVG(bright_ti4) AS avg_bright,
      AVG(frp) AS avg_frp
    FROM
      (SELECT DISTINCT
        CAST(county_geoid AS INT64) AS geoid,
        DATE(acq_date) AS date,
        bright_ti4,
        confidence,
        frp,
        latitude,
        longitude
      FROM `wildfire_data.firms`
      WHERE confidence IN ('H', 'h', 'M', 'n')
        AND bright_ti4 IS NOT NULL
        AND frp IS NOT NULL
        AND acq_date IS NOT NULL)
      GROUP BY geoid, date) f

    ON w.geoid = f.geoid
    AND w.date = f.date
),

normalized AS (
  SELECT
    *,
    SAFE_DIVIDE(temp_max_f - MIN(temp_max_f) OVER(),
                MAX(temp_max_f) OVER() - MIN(temp_max_f) OVER()) AS n_temp_max,
    SAFE_DIVIDE(wind_gusts_max_mph - MIN(wind_gusts_max_mph) OVER(),
                MAX(wind_gusts_max_mph) OVER() - MIN(wind_gusts_max_mph) OVER()) AS n_wind_gust,
    SAFE_DIVIDE(sunshine_duration_s - MIN(sunshine_duration_s) OVER(),
                MAX(sunshine_duration_s) OVER() - MIN(sunshine_duration_s) OVER()) AS n_sunshine,
    SAFE_DIVIDE(IFNULL(num_fires, 0) - MIN(IFNULL(num_fires, 0)) OVER(),
                MAX(IFNULL(num_fires, 0)) OVER() - MIN(IFNULL(num_fires, 0)) OVER()) AS n_fires,
    SAFE_DIVIDE(IFNULL(avg_frp, 0) - MIN(IFNULL(avg_frp, 0)) OVER(),
                MAX(IFNULL(avg_frp, 0)) OVER() - MIN(IFNULL(avg_frp, 0)) OVER()) AS n_frp,
    SAFE_DIVIDE(IFNULL(avg_bright, 0) - MIN(IFNULL(avg_bright, 0)) OVER(),
                MAX(IFNULL(avg_bright, 0)) OVER() - MIN(IFNULL(avg_bright, 0)) OVER()) AS n_bright,
    SAFE_DIVIDE(rain_sum_in - MIN(rain_sum_in) OVER(),
                MAX(rain_sum_in) OVER() - MIN(rain_sum_in) OVER()) AS n_rain,
    SAFE_DIVIDE(temp_min_f - MIN(temp_min_f) OVER(),
                MAX(temp_min_f) OVER() - MIN(temp_min_f) OVER()) AS n_temp_min
  FROM combined
)

SELECT
  date,
  geoid,
  temp_max_f,
  temp_min_f,
  rain_sum_in,
  wind_speed_max_mph,
  wind_gusts_max_mph,
  sunshine_duration_s,
  num_fires,
  avg_bright,
  avg_frp,
  CASE
    WHEN num_fires IS NULL
      OR avg_bright IS NULL
      OR avg_frp IS NULL
    THEN NULL
    ELSE ROUND(
        0.20 * n_temp_max
      + 0.20 * n_wind_gust
      + 0.10 * n_sunshine
      + 0.10 * n_fires
      + 0.05 * n_frp
      + 0.05 * n_bright
      - 0.20 * n_rain
      - 0.10 * n_temp_min
    , 4)
  END AS risk_index
FROM normalized