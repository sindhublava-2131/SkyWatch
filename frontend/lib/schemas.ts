import { z } from 'zod'

const utc = z.string().datetime({ offset: true }).or(
  z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/)
)

const stringId = z.union([z.string(), z.number()]).transform(v => String(v))

export const metaSchema = z.object({
  run_id: z.string(),
  generated_at_utc: utc,
  profile: z.string(),
  engine: z.string(),
  is_simulation: z.boolean().optional().default(false),
})

export const stationSchema = z.object({
  id: stringId.optional(),
  station_id: stringId.optional(),
  name: z.string(),
  latitude: z.number(),
  longitude: z.number(),
  parameters: z.array(z.string()).optional().default(['PM2.5', 'PM10']),
  last_measurement_time_utc: utc.nullable().optional(),
  freshness_status: z.string().transform(v => {
    const lower = v.toLowerCase()
    if (lower === 'fresh') return 'fresh'
    if (lower === 'stale') return 'stale'
    return 'offline'
  }),
}).transform(s => ({
  ...s,
  id: s.id ?? s.station_id ?? '',
  station_id: s.station_id ?? s.id ?? '',
}))

export const indexSchema = z.object({
  station_id: stringId,
  station_name: z.string(),
  window_end_utc: utc,
  pm25_24h_avg: z.number().nullable(),
  pm25_subindex: z.number().nullable(),
  pm10_24h_avg: z.number().nullable(),
  pm10_subindex: z.number().nullable(),
  aqi_value: z.number().nullable(),
  aqi_category: z.enum(['Good', 'Satisfactory', 'Moderate', 'Poor', 'Very Poor', 'Severe']).nullable(),
  dominant_pollutant: z.string().nullable(),
  is_partial: z.boolean().optional().default(false),
  valid_hours: z.number().optional().default(24),
  suppression_reason: z.string().nullable().optional(),
})

export const statSchema = z.object({
  station_id: stringId,
  station_name: z.string(),
  parameter: z.string(),
  reading_count: z.number().optional().default(0),
  average_value: z.number(),
  minimum_value: z.number(),
  maximum_value: z.number(),
  pollution_rank: z.number().optional().default(1),
  generated_at_utc: utc.optional(),
})

export const hourlySchema = z.object({
  station_id: stringId,
  parameter: z.string(),
  hour_of_day: z.number().int().min(0).max(23),
  hour_of_day_utc: z.number().int().optional(),
  ist_time_range: z.string().optional(),
  ist_label: z.string().optional(),
  average_value: z.number(),
  reading_count: z.number().optional().default(0),
  is_peak_hour: z.boolean().optional().default(false),
})

export const dailySchema = z.object({
  station_id: stringId,
  parameter: z.string(),
  day_of_week: z.string(),
  day_name: z.string().optional(),
  average_value: z.number(),
  reading_count: z.number().optional().default(0),
})

export const correlationSchema = z.object({
  station_id: stringId,
  pollutant: z.string(),
  weather_variable: z.string(),
  pearson_r: z.number().nullable(),
  sample_count: z.number().optional().default(0),
  confidence_tier: z.string(),
})

export const alertSchema = z.object({
  alert_id: z.string(),
  alert_type: z.string(),
  severity: z.string(),
  station_id: stringId,
  station_name: z.string(),
  timestamp_utc: utc.optional(),
  created_at_utc: utc.optional(),
  message: z.string(),
  metric_value: z.number().nullable(),
}).transform(a => ({
  ...a,
  timestamp_utc: a.timestamp_utc ?? a.created_at_utc ?? new Date().toISOString(),
}))

export const qualitySchema = z.object({
  run_id: z.string(),
  stage: z.string(),
  accepted_count: z.number().optional(),
  rejected_count: z.number().optional(),
  duplicate_count: z.number().optional(),
  records_received: z.number().optional(),
  records_passed: z.number().optional(),
  records_rejected: z.number().optional(),
  reason_counts_json: z.string().optional(),
  generated_at_utc: utc.optional(),
})

export type Meta = z.infer<typeof metaSchema>
export type Station = z.infer<typeof stationSchema>
export type AirIndex = z.infer<typeof indexSchema>
export type StationStat = z.infer<typeof statSchema>
export type HourlyTrend = z.infer<typeof hourlySchema>
export type DailyTrend = z.infer<typeof dailySchema>
export type WeatherCorrelation = z.infer<typeof correlationSchema>
export type Alert = z.infer<typeof alertSchema>
export type QualityReport = z.infer<typeof qualitySchema>
