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
import { meta, stations, indexes, alerts, hourly } from './mocks'

const base = process.env.NEXT_PUBLIC_API_BASE_URL || ''

export async function getData<T>(
  path: string,
  schema: { safeParse: (v: unknown) => { success: boolean; data?: T; error?: unknown } },
  fixture: T
): Promise<T> {
  // If explicitly requested to use mocks, or if in pure static build without API host
  if (process.env.NEXT_PUBLIC_USE_MOCKS === 'true') {
    return fixture
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
      return fixture
    }

    const json = await response.json()
    const parsed = schema.safeParse(json)
    if (!parsed.success) {
      console.warn(`Schema validation warning on ${path}:`, parsed.error)
      return fixture
    }
    return parsed.data as T
  } catch (err) {
    console.warn(`Fetch error on ${path}:`, err, 'Falling back to fixture.')
    return fixture
  }
}

export const loadMeta = (): Promise<Meta> =>
  getData<Meta>('/api/v1/meta', metaSchema, meta)

export const loadStations = (status?: string, freshness?: string): Promise<Station[]> => {
  const params = new URLSearchParams()
  if (status) params.set('status', status)
  if (freshness) params.set('freshness', freshness)
  const qs = params.toString() ? `?${params.toString()}` : '?status=all'
  return getData<Station[]>(`/api/v1/stations${qs}`, stationSchema.array(), stations)
}

export const loadIndexes = (stationId?: string, category?: string, limit?: number): Promise<AirIndex[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (category) params.set('aqi_category', category)
  if (limit) params.set('limit', String(limit))
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<AirIndex[]>(`/api/v1/aqi/cpcb-index${qs}`, indexSchema.array(), indexes)
}

export const loadStationStatistics = (parameter?: string): Promise<StationStat[]> => {
  const qs = parameter ? `?parameter=${parameter}` : ''
  return getData<StationStat[]>(`/api/v1/analytics/station-statistics${qs}`, statSchema.array(), [])
}

export const loadHourlyTrends = (stationId?: string, parameter?: string): Promise<HourlyTrend[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (parameter) params.set('parameter', parameter)
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<HourlyTrend[]>(`/api/v1/analytics/trends/hourly${qs}`, hourlySchema.array(), hourly)
}

export const loadDailyTrends = (stationId?: string, parameter?: string): Promise<DailyTrend[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (parameter) params.set('parameter', parameter)
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<DailyTrend[]>(`/api/v1/analytics/trends/daily${qs}`, dailySchema.array(), [])
}

export const loadWeatherCorrelation = (stationId?: string, pollutant?: string): Promise<WeatherCorrelation[]> => {
  const params = new URLSearchParams()
  if (stationId) params.set('station_id', stationId)
  if (pollutant) params.set('pollutant', pollutant)
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<WeatherCorrelation[]>(`/api/v1/analytics/weather-correlation${qs}`, correlationSchema.array(), [])
}

export const loadAlerts = (severity?: string, stationId?: string): Promise<Alert[]> => {
  const params = new URLSearchParams()
  if (severity) params.set('severity', severity)
  if (stationId) params.set('station_id', stationId)
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<Alert[]>(`/api/v1/alerts${qs}`, alertSchema.array(), alerts)
}

export const loadQualityReport = (runId?: string, stage?: string): Promise<QualityReport[]> => {
  const params = new URLSearchParams()
  if (runId) params.set('run_id', runId)
  if (stage) params.set('stage', stage)
  const qs = params.toString() ? `?${params.toString()}` : ''
  return getData<QualityReport[]>(`/api/v1/audit/quality-report${qs}`, qualitySchema.array(), [])
}
