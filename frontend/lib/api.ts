import {
  metaSchema,
  stationSchema,
  indexSchema,
  statSchema,
  hourlySchema,
  dailySchema,
  correlationSchema,
  alertSchema,
  qualitySchema,
  type Meta,
  type Station,
  type AirIndex,
  type StationStat,
  type HourlyTrend,
  type DailyTrend,
  type WeatherCorrelation,
  type Alert,
  type QualityReport,
} from './schemas'
import { meta, stations, indexes, alerts, hourly, stats, daily, correlations, quality } from './mocks'

const base = process.env.NEXT_PUBLIC_API_BASE_URL || ''

export async function getData<T>(
  path: string,
  schema: { safeParse: (v: unknown) => { success: boolean; data?: T; error?: unknown } },
  fixture: T
): Promise<T> {
  const parsedFixture = schema.safeParse(fixture)
  const safeFixture = parsedFixture.success && parsedFixture.data !== undefined ? (parsedFixture.data as T) : fixture

  // If explicitly requested to use mocks, or if in pure static build without API host
  if (process.env.NEXT_PUBLIC_USE_MOCKS === 'true') {
    return safeFixture
  }

  try {
    const url = base ? `${base.replace(/\/$/, '')}${path}` : path
    const response = await fetch(url, {
      headers: { accept: 'application/json' },
      cache: 'no-store',
    })

    if (!response.ok) {
      // Fallback gracefully to fixture if network/backend is unavailable
      console.warn(`Request to ${url} failed with status ${response.status}. Falling back to bundled fixture.`)
      return safeFixture
    }

    const json = await response.json()
    const parsed = schema.safeParse(json)
    if (!parsed.success) {
      console.warn(`Schema validation warning on ${path}:`, parsed.error)
      return safeFixture
    }
    return parsed.data as T
  } catch (err) {
    console.warn(`Fetch error on ${path}:`, err, 'Falling back to fixture.')
    return safeFixture
  }
}

export const loadMeta = (): Promise<Meta> =>
  getData<Meta>('/api/v1/meta', metaSchema, meta)

export const loadStations = async (status?: string, freshness?: string): Promise<Station[]> => {
  const params = new URLSearchParams()
  if (status) params.set('status', status)
  if (freshness) params.set('freshness', freshness)
  const qs = params.toString() ? `?${params.toString()}` : '?status=all'
  let data = await getData<Station[]>(`/api/v1/stations${qs}`, stationSchema.array(), stations)
  if (status && status !== 'all') {
    data = data.filter((s) => s.status?.toLowerCase() === status.toLowerCase())
  }
  if (freshness) {
    data = data.filter((s) => s.freshness_status?.toLowerCase() === freshness.toLowerCase())
  }
  return data
}

export const loadIndexes = async (stationId?: string, category?: string, limit?: number): Promise<AirIndex[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (category) params.set('aqi_category', category)
  if (limit) params.set('limit', String(limit))
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<AirIndex[]>(`/api/v1/aqi/cpcb-index${qs}`, indexSchema.array(), indexes)
  if (stationId) {
    data = data.filter((r) => r.station_id === stationId)
  }
  if (category) {
    data = data.filter((r) => r.aqi_category?.toLowerCase() === category.toLowerCase())
  }
  if (limit && limit > 0) {
    data = data.slice(0, limit)
  }
  return data
}

export const loadStationStatistics = async (parameter?: string): Promise<StationStat[]> => {
  const qs = parameter ? `?parameter=${parameter}` : ''
  let data = await getData<StationStat[]>(`/api/v1/analytics/station-statistics${qs}`, statSchema.array(), stats)
  if (parameter) {
    data = data.filter((s) => s.parameter?.toLowerCase() === parameter.toLowerCase())
  }
  return data
}

export const loadHourlyTrends = async (stationId?: string, parameter?: string): Promise<HourlyTrend[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (parameter) params.set('parameter', parameter)
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<HourlyTrend[]>(`/api/v1/analytics/trends/hourly${qs}`, hourlySchema.array(), hourly)
  if (stationId) {
    data = data.filter((h) => h.station_id === stationId)
  }
  if (parameter) {
    data = data.filter((h) => h.parameter?.toLowerCase() === parameter.toLowerCase())
  }
  return data
}

export const loadDailyTrends = async (stationId?: string, parameter?: string): Promise<DailyTrend[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (parameter) params.set('parameter', parameter)
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<DailyTrend[]>(`/api/v1/analytics/trends/daily${qs}`, dailySchema.array(), daily)
  if (stationId) {
    data = data.filter((d) => d.station_id === stationId)
  }
  if (parameter) {
    data = data.filter((d) => d.parameter?.toLowerCase() === parameter.toLowerCase())
  }
  return data
}

export const loadWeatherCorrelation = async (stationId?: string, pollutant?: string): Promise<WeatherCorrelation[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (pollutant) params.set('pollutant', pollutant)
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<WeatherCorrelation[]>(`/api/v1/analytics/weather-correlation${qs}`, correlationSchema.array(), correlations)
  if (stationId) {
    data = data.filter((c) => c.station_id === stationId)
  }
  if (pollutant) {
    data = data.filter((c) => c.pollutant?.toLowerCase() === pollutant.toLowerCase())
  }
  return data
}

export const loadAlerts = async (severity?: string, stationId?: string): Promise<Alert[]> => {
  const params = new URLSearchParams()
  if (severity) params.set('severity', severity)
  if (stationId) params.set('station_id', stationId)
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<Alert[]>(`/api/v1/alerts${qs}`, alertSchema.array(), alerts)
  if (severity) {
    data = data.filter((a) => a.severity?.toUpperCase() === severity.toUpperCase())
  }
  if (stationId) {
    data = data.filter((a) => a.station_id === stationId)
  }
  return data
}

export const loadQualityReport = async (runId?: string, stage?: string): Promise<QualityReport[]> => {
  const params = new URLSearchParams()
  if (runId) params.set('run_id', runId)
  if (stage) params.set('stage', stage)
  const qs = params.toString() ? `?${params.toString()}` : ''
  let data = await getData<QualityReport[]>(`/api/v1/audit/quality-report${qs}`, qualitySchema.array(), quality)
  if (runId) {
    data = data.filter((q) => q.run_id === runId)
  }
  if (stage) {
    data = data.filter((q) => q.stage?.toLowerCase() === stage.toLowerCase())
  }
  return data
}
