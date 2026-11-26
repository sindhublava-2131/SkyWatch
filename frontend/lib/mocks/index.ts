import type { Meta, Station, AirIndex, Alert, StationStat, DailyTrend, WeatherCorrelation, QualityReport, HourlyTrend } from '../schemas'

export const meta: Meta = {
  run_id: 'run_20261123_190000',
  generated_at_utc: '2026-11-23T12:41:07Z',
  profile: 'modern',
  engine: 'APACHE_SPARK',
  is_simulation: false,
}

const names = ['BTM Layout', 'Silk Board', 'Hebbal', 'Peenya', 'Jayanagar 5th Block', 'Bapuji Nagar', 'City Railway Station', 'Sanegurava Halli']
const coords: [number, number][] = [
  [12.9166, 77.6101],
  [12.9177, 77.6233],
  [13.0358, 77.5970],
  [13.0329, 77.5279],
  [12.9250, 77.5838],
  [12.9450, 77.5350],
  [12.9770, 77.5710],
  [12.9590, 77.6410],
]

export const stations: Station[] = names.map((name, i) => ({
  id: String(2178 + i),
  station_id: String(2178 + i),
  name: name + ', Bengaluru - CPCB',
  latitude: coords[i][0],
  longitude: coords[i][1],
  parameters: ['PM2.5', 'PM10'],
  last_measurement_time_utc: i === 6 ? null : i === 4 ? '2026-11-23T07:00:00Z' : '2026-11-23T12:30:00Z',
  freshness_status: i === 6 ? 'offline' : i === 4 ? 'stale' : 'fresh',
}))

const values = [168, 142, 119, 94, 78, 62, 0, 45]
export const indexes: AirIndex[] = stations.map((s, i) => {
  const value = values[i]
  const category = value > 400 ? 'Severe' : value > 300 ? 'Very Poor' : value > 200 ? 'Poor' : value > 100 ? 'Moderate' : value > 50 ? 'Satisfactory' : 'Good'
  return {
    station_id: s.id,
    station_name: s.name,
    window_end_utc: '2026-11-23T12:30:00Z',
    pm25_24h_avg: i === 6 ? null : 18 + i * 4,
    pm25_subindex: i === 6 ? null : value,
    pm10_24h_avg: i === 6 ? null : 44 + i * 11,
    pm10_subindex: i === 6 ? null : Math.max(value - 10, 25),
    aqi_value: i === 6 || i === 7 ? null : value,
    aqi_category: (i === 6 || i === 7 ? null : category) as AirIndex['aqi_category'],
    dominant_pollutant: i % 2 ? 'pm10' : 'pm25',
    is_partial: false,
    valid_hours: i === 7 ? 12 : 24,
    suppression_reason: i === 7 ? '12 of 24 valid hours' : null,
  }
})

const alertRows: [string, string, string, string, number | null][] = [
  ['HEALTH_POOR', 'Severe', 'BTM Layout, Bengaluru - CPCB', 'PM-based index entered Poor category', 168],
  ['SPIKE', 'Warning', 'Silk Board, Bengaluru - CPCB', 'PM2.5 concentration increased 34% over baseline', 142],
  ['STALE', 'Warning', 'Jayanagar 5th Block, Bengaluru - CPCB', 'No new station reading in 4 hours', 78],
  ['OFFLINE', 'Severe', 'City Railway Station, Bengaluru - CPCB', 'Station has not reported in the last 24 hours', null],
  ['HEALTH_VERY_POOR', 'Severe', 'Hebbal, Bengaluru - CPCB', 'PM-based index entered Very Poor category', 119],
]

export const alerts: Alert[] = alertRows.map((a, i) => ({
  alert_id: 'alert_' + (i + 1),
  alert_type: a[0],
  severity: a[1],
  station_id: String(2178 + i),
  station_name: a[2],
  timestamp_utc: '2026-11-23T' + String(12 - i).padStart(2, '0') + ':30:00Z',
  message: a[3],
  metric_value: a[4],
}))

export const hourly: HourlyTrend[] = Array.from({ length: 24 }, (_, hour_of_day) => ({
  station_id: '2178',
  parameter: 'pm25',
  hour_of_day,
  average_value: Math.round(24 + Math.sin(((hour_of_day - 7) * Math.PI) / 12) * 15 + (hour_of_day % 3)),
  reading_count: 7,
  is_peak_hour: hour_of_day === 7 || hour_of_day === 8,
  ist_time_range: String((hour_of_day + 5) % 24).padStart(2, '0') + ':30 - ' + String((hour_of_day + 6) % 24).padStart(2, '0') + ':30 IST',
  ist_label: String((hour_of_day + 5) % 24).padStart(2, '0') + ':30 IST',
}))

export const stats: StationStat[] = [
  {
    "station_id": "2182",
    "station_name": "Jayanagar, Bengaluru - CPCB",
    "parameter": "pm10",
    "reading_count": 72,
    "average_value": 146.65,
    "minimum_value": 106,
    "maximum_value": 187.3,
    "pollution_rank": 1,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2181",
    "station_name": "Hebbal, Bengaluru - CPCB",
    "parameter": "pm10",
    "reading_count": 72,
    "average_value": 132.65,
    "minimum_value": 92,
    "maximum_value": 173.3,
    "pollution_rank": 2,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2180",
    "station_name": "Peenya, Bengaluru - CPCB",
    "parameter": "pm10",
    "reading_count": 72,
    "average_value": 118.65,
    "minimum_value": 78,
    "maximum_value": 159.3,
    "pollution_rank": 3,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2179",
    "station_name": "Silk Board, Bengaluru - CPCB",
    "parameter": "pm10",
    "reading_count": 72,
    "average_value": 104.65,
    "minimum_value": 64,
    "maximum_value": 145.3,
    "pollution_rank": 4,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2178",
    "station_name": "BTM Layout, Bengaluru - CPCB",
    "parameter": "pm10",
    "reading_count": 72,
    "average_value": 90.65,
    "minimum_value": 50,
    "maximum_value": 131.3,
    "pollution_rank": 5,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2182",
    "station_name": "Jayanagar, Bengaluru - CPCB",
    "parameter": "pm25",
    "reading_count": 72,
    "average_value": 78.9,
    "minimum_value": 55,
    "maximum_value": 102.8,
    "pollution_rank": 1,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2181",
    "station_name": "Hebbal, Bengaluru - CPCB",
    "parameter": "pm25",
    "reading_count": 72,
    "average_value": 71.4,
    "minimum_value": 47.5,
    "maximum_value": 95.3,
    "pollution_rank": 2,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2180",
    "station_name": "Peenya, Bengaluru - CPCB",
    "parameter": "pm25",
    "reading_count": 72,
    "average_value": 63.9,
    "minimum_value": 40,
    "maximum_value": 87.8,
    "pollution_rank": 3,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2179",
    "station_name": "Silk Board, Bengaluru - CPCB",
    "parameter": "pm25",
    "reading_count": 72,
    "average_value": 56.4,
    "minimum_value": 32.5,
    "maximum_value": 80.3,
    "pollution_rank": 4,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "station_id": "2178",
    "station_name": "BTM Layout, Bengaluru - CPCB",
    "parameter": "pm25",
    "reading_count": 72,
    "average_value": 48.9,
    "minimum_value": 25,
    "maximum_value": 72.8,
    "pollution_rank": 5,
    "generated_at_utc": "2026-11-24T18:13:27Z"
  }
]

export const daily: DailyTrend[] = [
  {
    "station_id": "2178",
    "parameter": "pm10",
    "day_of_week": "Friday",
    "average_value": 85.65,
    "reading_count": 24
  },
  {
    "station_id": "2178",
    "parameter": "pm10",
    "day_of_week": "Saturday",
    "average_value": 90.65,
    "reading_count": 24
  },
  {
    "station_id": "2178",
    "parameter": "pm10",
    "day_of_week": "Sunday",
    "average_value": 95.65,
    "reading_count": 24
  },
  {
    "station_id": "2178",
    "parameter": "pm25",
    "day_of_week": "Friday",
    "average_value": 45.7,
    "reading_count": 24
  },
  {
    "station_id": "2178",
    "parameter": "pm25",
    "day_of_week": "Saturday",
    "average_value": 48.9,
    "reading_count": 24
  },
  {
    "station_id": "2178",
    "parameter": "pm25",
    "day_of_week": "Sunday",
    "average_value": 52.1,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm10",
    "day_of_week": "Friday",
    "average_value": 99.65,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm10",
    "day_of_week": "Saturday",
    "average_value": 104.65,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm10",
    "day_of_week": "Sunday",
    "average_value": 109.65,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm25",
    "day_of_week": "Friday",
    "average_value": 53.2,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm25",
    "day_of_week": "Saturday",
    "average_value": 56.4,
    "reading_count": 24
  },
  {
    "station_id": "2179",
    "parameter": "pm25",
    "day_of_week": "Sunday",
    "average_value": 59.6,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm10",
    "day_of_week": "Friday",
    "average_value": 113.65,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm10",
    "day_of_week": "Saturday",
    "average_value": 118.65,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm10",
    "day_of_week": "Sunday",
    "average_value": 123.65,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm25",
    "day_of_week": "Friday",
    "average_value": 60.7,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm25",
    "day_of_week": "Saturday",
    "average_value": 63.9,
    "reading_count": 24
  },
  {
    "station_id": "2180",
    "parameter": "pm25",
    "day_of_week": "Sunday",
    "average_value": 67.1,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm10",
    "day_of_week": "Friday",
    "average_value": 127.65,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm10",
    "day_of_week": "Saturday",
    "average_value": 132.65,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm10",
    "day_of_week": "Sunday",
    "average_value": 137.65,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm25",
    "day_of_week": "Friday",
    "average_value": 68.2,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm25",
    "day_of_week": "Saturday",
    "average_value": 71.4,
    "reading_count": 24
  },
  {
    "station_id": "2181",
    "parameter": "pm25",
    "day_of_week": "Sunday",
    "average_value": 74.6,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm10",
    "day_of_week": "Friday",
    "average_value": 141.65,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm10",
    "day_of_week": "Saturday",
    "average_value": 146.65,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm10",
    "day_of_week": "Sunday",
    "average_value": 151.65,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm25",
    "day_of_week": "Friday",
    "average_value": 75.7,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm25",
    "day_of_week": "Saturday",
    "average_value": 78.9,
    "reading_count": 24
  },
  {
    "station_id": "2182",
    "parameter": "pm25",
    "day_of_week": "Sunday",
    "average_value": 82.1,
    "reading_count": 24
  }
]

export const correlations: WeatherCorrelation[] = [
  {
    "station_id": "2178",
    "pollutant": "pm10",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4681,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm10",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1332,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm10",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4686,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm10",
    "weather_variable": "wind_speed",
    "pearson_r": -0.1,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm25",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4664,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm25",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1327,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm25",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4668,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2178",
    "pollutant": "pm25",
    "weather_variable": "wind_speed",
    "pearson_r": -0.0997,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm10",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4681,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm10",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1332,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm10",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4686,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm10",
    "weather_variable": "wind_speed",
    "pearson_r": -0.1,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm25",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4664,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm25",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1327,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm25",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4668,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2179",
    "pollutant": "pm25",
    "weather_variable": "wind_speed",
    "pearson_r": -0.0997,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm10",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4681,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm10",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1332,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm10",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4686,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm10",
    "weather_variable": "wind_speed",
    "pearson_r": -0.1,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm25",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4664,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm25",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1327,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm25",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4668,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2180",
    "pollutant": "pm25",
    "weather_variable": "wind_speed",
    "pearson_r": -0.0997,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm10",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4681,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm10",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1332,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm10",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4686,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm10",
    "weather_variable": "wind_speed",
    "pearson_r": -0.1,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm25",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4664,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm25",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1327,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm25",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4668,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2181",
    "pollutant": "pm25",
    "weather_variable": "wind_speed",
    "pearson_r": -0.0997,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm10",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4681,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm10",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1332,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm10",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4686,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm10",
    "weather_variable": "wind_speed",
    "pearson_r": -0.1,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm25",
    "weather_variable": "humidity_pct",
    "pearson_r": -0.4664,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm25",
    "weather_variable": "precipitation_mm",
    "pearson_r": 0.1327,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm25",
    "weather_variable": "temperature_c",
    "pearson_r": 0.4668,
    "sample_count": 72,
    "confidence_tier": "Medium"
  },
  {
    "station_id": "2182",
    "pollutant": "pm25",
    "weather_variable": "wind_speed",
    "pearson_r": -0.0997,
    "sample_count": 72,
    "confidence_tier": "Medium"
  }
]

export const quality: QualityReport[] = [
  {
    "run_id": "4007d09e9c884b5f9c9a7297ef546c69",
    "stage": "ingestion",
    "accepted_count": 720,
    "rejected_count": 0,
    "duplicate_count": 0,
    "reason_counts_json": "{}",
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "run_id": "4007d09e9c884b5f9c9a7297ef546c69",
    "stage": "pig_etl",
    "accepted_count": 720,
    "rejected_count": 0,
    "duplicate_count": 0,
    "reason_counts_json": "{\"INVALID_FILTERED\": 0}",
    "generated_at_utc": "2026-11-24T18:13:27Z"
  },
  {
    "run_id": "4007d09e9c884b5f9c9a7297ef546c69",
    "stage": "spark_analytics",
    "accepted_count": 360,
    "rejected_count": 0,
    "duplicate_count": 0,
    "reason_counts_json": "{}",
    "generated_at_utc": "2026-11-24T18:13:27Z"
  }
]
