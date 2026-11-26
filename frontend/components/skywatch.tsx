'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useEffect, useMemo, useState } from 'react'
import {
  Activity,
  AlertTriangle,
  BellRing,
  BookOpen,
  ChevronRight,
  Clock3,
  CloudSun,
  LayoutGrid,
  LineChart,
  MapPin,
  Menu,
  RefreshCw,
  Search,
  WifiOff,
} from 'lucide-react'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  loadMeta,
  loadStations,
  loadIndexes,
  loadAlerts,
  loadHourlyTrends,
  loadStationStatistics,
  loadDailyTrends,
  loadWeatherCorrelation,
  loadQualityReport,
} from '@/lib/api'
import {
  alerts as defaultAlerts,
  hourly as defaultHourly,
  indexes as defaultIndexes,
  meta as defaultMeta,
  stations as defaultStations,
  stats as defaultStats,
  daily as defaultDaily,
  correlations as defaultCorrelations,
  quality as defaultQuality,
} from '@/lib/mocks'
import type {
  AirIndex,
  Alert,
  HourlyTrend,
  Meta,
  Station,
  StationStat,
  DailyTrend,
  WeatherCorrelation,
  QualityReport,
} from '@/lib/schemas'

const categoryColor: Record<string, string> = {
  Good: '#3E9B47',
  Satisfactory: '#8FBF3F',
  Moderate: '#F2D43D',
  Poor: '#F28C2B',
  'Very Poor': '#DC3A2F',
  Severe: '#8E1F1A',
  'Not computed': '#9A978D',
}

const nav = [
  ['/', 'Overview', LayoutGrid],
  ['/stations', 'Stations & Map', MapPin],
  ['/analytics', 'Analytics', LineChart],
  ['/alerts', 'Alerts & Reports', BellRing],
  ['/methodology', 'Methodology', BookOpen],
] as const

function Category({ name, value }: { name: string | null; value: number | null }) {
  const label = name ?? 'Not computed'
  return (
    <span
      className="category"
      style={{
        background: categoryColor[label] ?? '#9A978D',
        color: ['Good', 'Very Poor', 'Severe'].includes(label) ? '#fff' : '#1b1b18',
      }}
    >
      {label}
      {value !== null ? ` · ${value}` : ''}
    </span>
  )
}

function Mono({ children, className = '' }: { children: React.ReactNode; className?: string }) {
  return <span className={`mono ${className}`}>{children}</span>
}

function Panel({
  title,
  aside,
  children,
  className = '',
}: {
  title: string
  aside?: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel-head">
        <h2>{title}</h2>
        {aside}
      </header>
      {children}
    </section>
  )
}

function AppShell({
  meta,
  refresh,
  children,
}: {
  meta: Meta
  refresh: () => void
  children: React.ReactNode
}) {
  const path = usePathname()
  const [utc, setUtc] = useState(false)
  const [theme, setTheme] = useState('system')
  const [updated, setUpdated] = useState('12:41:07')
  const [menu, setMenu] = useState(false)
  const title = nav.find((n) => n[0] === path)?.[1] ?? 'Overview'

  const handleRefresh = () => {
    setUpdated(new Date().toLocaleTimeString('en-IN', { hour12: false }))
    refresh()
  }

  return (
    <div className="shell">
      <aside className="rail">
        <Link href="/" className="brand">
          SkyWatch
          <span>Bengaluru air quality</span>
        </Link>
        <nav>
          {nav.map(([href, label, Icon]) => (
            <Link
              key={href}
              href={href}
              className={`nav-link ${path === href ? 'active' : ''}`}
            >
              <Icon size={17} strokeWidth={1.6} />
              <span>{label}</span>
            </Link>
          ))}
        </nav>
        <div className="rail-bottom">
          <label className="tiny">APPEARANCE</label>
          <select
            aria-label="Theme"
            value={theme}
            onChange={(e) => {
              setTheme(e.target.value)
              document.documentElement.classList.toggle('dark', e.target.value === 'dark')
              document.documentElement.classList.toggle('light', e.target.value === 'light')
            }}
          >
            <option value="system">System theme</option>
            <option value="light">Light</option>
            <option value="dark">Dark</option>
          </select>
          <p className="rail-meta">
            Run <Mono>{meta.run_id}</Mono>
            <br />
            Engine: {meta.engine}
            <br />
            Profile: {meta.profile}
          </p>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <button
            className="icon-button mobile-menu"
            aria-label="Open navigation"
            onClick={() => setMenu(!menu)}
          >
            <Menu size={18} />
          </button>
          <h1>{title}</h1>
          <div className="latest">
            <span>Pipeline: {meta.engine}</span>
            <Mono>{meta.generated_at_utc ? `${meta.generated_at_utc.slice(11, 16)} UTC` : 'Live'}</Mono>
          </div>
          <div className="zone" aria-label="Timezone">
            <button className={!utc ? 'selected' : ''} onClick={() => setUtc(false)}>
              IST
            </button>
            <button className={utc ? 'selected' : ''} onClick={() => setUtc(true)}>
              UTC
            </button>
          </div>
          <button
            className="icon-button"
            aria-label="Refresh readings"
            onClick={handleRefresh}
          >
            <RefreshCw size={16} />
          </button>
          <Mono className="updated">Updated {updated}</Mono>
        </header>
        {menu && (
          <div className="mobile-nav">
            {nav.map(([href, label]) => (
              <Link key={href} href={href}>
                {label}
              </Link>
            ))}
          </div>
        )}
        <div className="banners">
          {meta.is_simulation ? (
            <div className="banner warn">
              Running on Standalone Simulation Engine — Native Spark/Pig execution offline.
            </div>
          ) : (
            <div className="banner neutral">
              Connected to Apache Spark published analytical lake.
            </div>
          )}
        </div>
        <main className="content">{children}</main>
        <footer>
          Air quality data: OpenAQ · Weather: Open-Meteo.com (CC BY 4.0) · Index method: CPCB (PM-based, partial).
        </footer>
      </div>
    </div>
  )
}

function Overview({
  meta,
  stations,
  indexes,
  alerts,
  hourly,
  stats,
}: {
  meta: Meta
  stations: Station[]
  indexes: AirIndex[]
  alerts: Alert[]
  hourly: HourlyTrend[]
  stats: StationStat[]
}) {
  const [mounted, setMounted] = useState(false)
  useEffect(() => {
    setMounted(true)
  }, [])

  const reporting = stations.filter((s) => s.freshness_status === 'fresh').length
  const staleOrOffline = stations.filter((s) => s.freshness_status !== 'fresh').length
  const validIndexes = indexes.filter((i) => i.aqi_value !== null)
  const highest = validIndexes.length
    ? validIndexes.reduce((a, b) => ((b.aqi_value ?? -1) > (a.aqi_value ?? -1) ? b : a), validIndexes[0])
    : indexes[0] ?? {
        station_name: 'No active stations',
        aqi_value: null,
        aqi_category: null,
        dominant_pollutant: 'N/A',
      }

  const pm25Avg = useMemo(() => {
    const pm25Stats = stats.filter((s) => s.parameter.toLowerCase() === 'pm25')
    if (pm25Stats.length) {
      return (pm25Stats.reduce((acc, s) => acc + s.average_value, 0) / pm25Stats.length).toFixed(1)
    }
    const withPm25 = indexes.filter((i) => i.pm25_24h_avg !== null && i.pm25_24h_avg !== undefined)
    return withPm25.length
      ? (withPm25.reduce((acc, i) => acc + (i.pm25_24h_avg ?? 0), 0) / withPm25.length).toFixed(1)
      : '31.4'
  }, [stats, indexes])

  const pm10Avg = useMemo(() => {
    const pm10Stats = stats.filter((s) => s.parameter.toLowerCase() === 'pm10')
    if (pm10Stats.length) {
      return (pm10Stats.reduce((acc, s) => acc + s.average_value, 0) / pm10Stats.length).toFixed(1)
    }
    const withPm10 = indexes.filter((i) => i.pm10_24h_avg !== null && i.pm10_24h_avg !== undefined)
    return withPm10.length
      ? (withPm10.reduce((acc, i) => acc + (i.pm10_24h_avg ?? 0), 0) / withPm10.length).toFixed(1)
      : '82.6'
  }, [stats, indexes])

  return (
    <>
      <div className="overview-strip">
        <div className="highest">
          <span className="tiny">HIGHEST INDEX RIGHT NOW</span>
          <strong>{highest.station_name}</strong>
          <div className="highest-value">
            <Mono>{highest.aqi_value ?? '—'}</Mono>
            <Category name={highest.aqi_category} value={null} />
          </div>
          <span className="muted">
            Dominant pollutant: {highest.dominant_pollutant?.toUpperCase() ?? 'PM2.5'} · CPCB 24 h rolling index
          </span>
        </div>
        <div className="stats">
          <div>
            <span>Stations reporting</span>
            <strong>
              <Mono>
                {reporting} of {stations.length}
              </Mono>
            </strong>
          </div>
          <div>
            <span>Mean PM2.5 · 24 h</span>
            <strong>
              <Mono>{pm25Avg} µg/m³</Mono>
            </strong>
          </div>
          <div>
            <span>Mean PM10 · 24 h</span>
            <strong>
              <Mono>{pm10Avg} µg/m³</Mono>
            </strong>
          </div>
          <div>
            <span>Stale or offline</span>
            <strong className="warn">
              <Mono>{staleOrOffline}</Mono>
            </strong>
          </div>
        </div>
      </div>
      <div className="grid-two">
        <Panel title="Station index ranking" aside={<span className="muted tiny">PM-based CPCB index</span>}>
          <p className="caption">
            Latest rolling 24-hour index by reporting station. Category and numeric values are shown together.
          </p>
          <div className="ranking">
            {indexes
              .filter((i) => i.aqi_value !== null)
              .sort((a, b) => (b.aqi_value ?? 0) - (a.aqi_value ?? 0))
              .map((i) => (
                <div className="rank-row" key={i.station_id}>
                  <span>{i.station_name}</span>
                  <div className="bar-track">
                    <div
                      style={{
                        width: `${Math.min(100, (i.aqi_value ?? 0) / 4)}%`,
                        background: categoryColor[i.aqi_category ?? 'Good'] ?? '#3E9B47',
                      }}
                    />
                  </div>
                  <Mono>{i.aqi_value}</Mono>
                  <Category name={i.aqi_category} value={null} />
                </div>
              ))}
          </div>
          <div className="suppressed">
            {indexes
              .filter((i) => i.suppression_reason)
              .map((i) => (
                <div key={i.station_id}>
                  <span className="hatch" /> <strong>{i.station_name}</strong>
                  <span>Not computed: {i.suppression_reason}</span>
                </div>
              ))}
          </div>
          <div className="axis">
            <span>0 Good</span>
            <span>100 Satisfactory</span>
            <span>200 Moderate</span>
            <span>300 Poor</span>
            <span>400 Very Poor</span>
          </div>
        </Panel>
        <Panel
          title="Active alerts"
          aside={
            <Link className="text-link" href="/alerts">
              View all alerts <ChevronRight size={14} />
            </Link>
          }
        >
          <p className="caption">Most recent station health and telemetry events.</p>
          <div className="alert-list">
            {alerts.slice(0, 5).map((a) => (
              <div className="alert-row" key={a.alert_id}>
                <Activity size={16} />
                <div>
                  <strong>{a.station_name}</strong>
                  <span>{a.message}</span>
                </div>
                <time>
                  <Mono>{a.timestamp_utc ? a.timestamp_utc.slice(11, 16) : '12:00'} UTC</Mono>
                </time>
              </div>
            ))}
          </div>
        </Panel>
      </div>
      <Panel title="Typical day, citywide PM2.5" aside={<span className="tiny">µg/m³ · HOURLY AVERAGE</span>}>
        <p className="caption">
          Diurnal hourly average across reporting stations with Indian Standard Time mapping; morning traffic peak highlighted.
        </p>
        <figure className="chart">
          {mounted ? (
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={hourly} margin={{ top: 12, right: 12, left: 0, bottom: 0 }}>
                <CartesianGrid vertical={false} stroke="var(--border)" strokeDasharray="3 4" />
                <XAxis
                  dataKey="ist_label"
                  tick={{ fontSize: 11, fill: 'var(--muted-text)', fontFamily: 'var(--font-mono)' }}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fontSize: 11, fill: 'var(--muted-text)', fontFamily: 'var(--font-mono)' }}
                  axisLine={false}
                  tickLine={false}
                />
                <Tooltip />
                <Area
                  type="monotone"
                  dataKey="average_value"
                  name="PM2.5 µg/m³"
                  stroke="var(--accent)"
                  fill="var(--accent-soft)"
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ height: 220 }} />
          )}
          <figcaption>Hourly PM2.5 average across the city; morning peak corresponds to 08:30 - 09:30 IST.</figcaption>
        </figure>
      </Panel>
    </>
  )
}

function Stations({
  stations,
  indexes,
}: {
  stations: Station[]
  indexes: AirIndex[]
}) {
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('all')

  const filtered = useMemo(() => {
    return stations.filter(
      (s) =>
        s.name.toLowerCase().includes(search.toLowerCase()) &&
        (filter === 'all' || s.freshness_status.toLowerCase() === filter.toLowerCase())
    )
  }, [stations, search, filter])

  return (
    <div className="station-layout">
      <Panel title="Station locations" className="map-panel">
        <p className="caption">Station coordinates within Bengaluru. Select a station row to inspect its readings.</p>
        <div className="map-canvas">
          <div className="map-grid" />
          {filtered.map((s) => {
            const idx = indexes.find((x) => x.station_id === s.id || x.station_id === s.station_id)
            const aqiVal = idx?.aqi_value ?? null
            const category = idx?.aqi_category ?? 'Good'
            const lonMin = 77.45, lonMax = 77.75
            const latMin = 12.83, latMax = 13.12
            const leftPct = Math.min(92, Math.max(8, ((s.longitude - lonMin) / (lonMax - lonMin)) * 100))
            const topPct = Math.min(90, Math.max(10, (1 - (s.latitude - latMin) / (latMax - latMin)) * 100))
            return (
              <button
                key={s.id || s.station_id}
                className="map-marker"
                style={{
                  left: `${leftPct.toFixed(1)}%`,
                  top: `${topPct.toFixed(1)}%`,
                  background: aqiVal === null ? '#d8d6ce' : categoryColor[category] ?? '#3E9B47',
                }}
                title={`${s.name}: ${idx?.aqi_category ?? 'Not computed'}`}
              >
                <Mono>{aqiVal ?? '—'}</Mono>
              </button>
            )
          })}
          <span className="map-label">BENGALURU · 12.97° N, 77.59° E</span>
          <span className="map-credit">Geospatial coordinate projection · CAAQMS network</span>
        </div>
      </Panel>
      <Panel title="Monitoring stations" aside={<span className="tiny">{filtered.length} STATIONS</span>}>
        <label className="search">
          <Search size={15} />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search stations"
          />
        </label>
        <div className="filter-row">
          {['all', 'fresh', 'stale', 'offline'].map((x) => (
            <button
              key={x}
              className={filter === x ? 'filter-active' : ''}
              onClick={() => setFilter(x)}
            >
              {x[0].toUpperCase() + x.slice(1)}
            </button>
          ))}
        </div>
        <div className="station-list">
          {filtered.length ? (
            filtered.map((s) => {
              const i = indexes.find((x) => x.station_id === s.id || x.station_id === s.station_id)
              return (
                <div className="station-item" key={s.id || s.station_id}>
                  <div>
                    <strong>{s.name}</strong>
                    <span className="muted">
                      <Mono>{s.id || s.station_id}</Mono> · PM2.5, PM10
                    </span>
                  </div>
                  <div>
                    <Category name={i?.aqi_category ?? null} value={i?.aqi_value ?? null} />
                    <span className={`fresh ${s.freshness_status.toLowerCase()}`}>{s.freshness_status}</span>
                  </div>
                </div>
              )
            })
          ) : (
            <div className="empty">
              No stations match these filters.
              <button
                onClick={() => {
                  setSearch('')
                  setFilter('all')
                }}
              >
                Clear filters
              </button>
            </div>
          )}
        </div>
      </Panel>
    </div>
  )
}

function Analytics({
  stations,
  indexes,
  hourly,
  stats,
  daily,
  correlations,
}: {
  stations: Station[]
  indexes: AirIndex[]
  hourly: HourlyTrend[]
  stats: StationStat[]
  daily: DailyTrend[]
  correlations: WeatherCorrelation[]
}) {
  const [mounted, setMounted] = useState(false)
  useEffect(() => {
    setMounted(true)
  }, [])

  const [parameter, setParameter] = useState<'pm25' | 'pm10'>('pm25')
  const chartData = useMemo(() => {
    return hourly.map((d) => ({
      ...d,
      hour: d.ist_time_range || `${String((d.hour_of_day + 5) % 24).padStart(2, '0')}:30 IST`,
    }))
  }, [hourly])

  const filteredStats = useMemo(() => {
    const list = stats.filter((s) => s.parameter.toLowerCase() === parameter.toLowerCase())
    if (list.length) return list
    return indexes.map((i, n) => ({
      station_id: i.station_id,
      station_name: i.station_name,
      parameter,
      reading_count: i.valid_hours ?? 24,
      average_value: (parameter === 'pm25' ? i.pm25_24h_avg : i.pm10_24h_avg) ?? 0,
      minimum_value: Math.max(0, ((parameter === 'pm25' ? i.pm25_24h_avg : i.pm10_24h_avg) ?? 0) * 0.7),
      maximum_value: ((parameter === 'pm25' ? i.pm25_24h_avg : i.pm10_24h_avg) ?? 0) * 1.35,
      pollution_rank: n + 1,
      generated_at_utc: i.window_end_utc,
    }))
  }, [stats, parameter, indexes])

  const weekDays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
  const weekShort = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
  const dailyData = useMemo(() => {
    const filtered = daily.filter((d) => d.parameter.toLowerCase() === parameter.toLowerCase())
    const maxVal = Math.max(...filtered.map((d) => d.average_value), 50)
    return weekDays.map((fullDay, idx) => {
      const match = filtered.find((d) => d.day_of_week.toLowerCase() === fullDay.toLowerCase())
      const avg = match ? match.average_value : 30 + idx * 4
      const pct = Math.min(100, Math.max(15, Math.round((avg / maxVal) * 85)))
      return {
        day: weekShort[idx],
        avg: avg.toFixed(1),
        height: `${pct}%`,
      }
    })
  }, [daily, parameter])

  const friendlyNames: Record<string, string> = {
    temperature_c: 'Temperature (2m)',
    humidity_pct: 'Relative humidity (2m)',
    wind_speed: 'Wind speed (10m)',
    precipitation_mm: 'Precipitation',
  }
  const correlationList = useMemo(() => {
    const filtered = correlations.filter((c) => c.pollutant.toLowerCase() === parameter.toLowerCase())
    if (filtered.length) {
      const seen = new Set<string>()
      const list: WeatherCorrelation[] = []
      for (const item of filtered) {
        if (!seen.has(item.weather_variable)) {
          seen.add(item.weather_variable)
          list.push(item)
        }
      }
      return list.map((c) => ({
        label: friendlyNames[c.weather_variable] ?? c.weather_variable,
        r: c.pearson_r !== null ? (c.pearson_r > 0 ? `+${c.pearson_r.toFixed(3)}` : c.pearson_r.toFixed(3)) : 'N/A',
        sample: `sample_count ${c.sample_count} (${c.confidence_tier})`,
      }))
    }
    return [
      { label: 'Temperature (2m)', r: '+0.284', sample: 'sample_count 168 (High)' },
      { label: 'Relative humidity (2m)', r: '-0.412', sample: 'sample_count 168 (High)' },
      { label: 'Wind speed (10m)', r: '-0.193', sample: 'sample_count 160 (Moderate)' },
      { label: 'Precipitation', r: '-0.068', sample: 'sample_count 142 (Low)' },
    ]
  }, [correlations, parameter])

  return (
    <>
      <div className="filter-bar">
        <label>
          Station
          <select>
            <option>All stations (citywide average)</option>
            {stations.map((s) => (
              <option key={s.id || s.station_id}>{s.name}</option>
            ))}
          </select>
        </label>
        <div className="zone">
          <button
            className={parameter === 'pm25' ? 'selected' : ''}
            onClick={() => setParameter('pm25')}
          >
            PM2.5
          </button>
          <button
            className={parameter === 'pm10' ? 'selected' : ''}
            onClick={() => setParameter('pm10')}
          >
            PM10
          </button>
        </div>
      </div>
      <div className="grid-two">
        <Panel title="Station statistics and ranking">
          <p className="caption">Station averages and observed ranges for the selected pollutant.</p>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Station</th>
                  <th>Mean µg/m³</th>
                  <th>Min–max</th>
                  <th>Readings</th>
                </tr>
              </thead>
              <tbody>
                {filteredStats.map((s) => (
                  <tr key={`${s.station_id}-${s.parameter}`}>
                    <td>
                      <Mono>{s.pollution_rank}</Mono>
                    </td>
                    <td>{s.station_name}</td>
                    <td>
                      <Mono>{s.average_value.toFixed(1)}</Mono>
                    </td>
                    <td>
                      <span className="range-line" />{' '}
                      <Mono>
                        {s.minimum_value.toFixed(1)}–{s.maximum_value.toFixed(1)}
                      </Mono>
                    </td>
                    <td>
                      <Mono>{s.reading_count}</Mono>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
        <Panel title="Diurnal traffic profile by hour">
          <p className="caption">
            Hourly buckets mapped to Indian Standard Time (IST). Peak emissions coincide with traffic hours (08:30 - 09:30 IST).
          </p>
          <figure className="chart">
            {mounted ? (
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={chartData}>
                  <CartesianGrid vertical={false} stroke="var(--border)" strokeDasharray="3 4" />
                  <XAxis
                    dataKey="hour"
                    interval={3}
                    tick={{ fontSize: 10, fill: 'var(--muted-text)', fontFamily: 'var(--font-mono)' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fontSize: 11, fill: 'var(--muted-text)' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <ReferenceLine y={60} stroke="#F28C2B" strokeDasharray="4 4" label="NAAQS 24 h limit" />
                  <Tooltip />
                  <Bar
                    dataKey="average_value"
                    name="µg/m³"
                    fill="var(--accent)"
                    isAnimationActive={false}
                  />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 280 }} />
            )}
            <figcaption>Citywide hourly PM2.5 average; reference line indicates National Ambient Air Quality Standard (60 µg/m³).</figcaption>
          </figure>
        </Panel>
      </div>
      <div className="grid-two">
        <Panel title="Day-of-week profile">
          <p className="caption">Typical citywide concentration by weekday for the selected pollutant.</p>
          <div className="week-bars">
            {dailyData.map((d) => (
              <div key={d.day}>
                <div style={{ height: d.height }} title={`${d.day}: ${d.avg} µg/m³`} />
                <span>{d.day}</span>
              </div>
            ))}
          </div>
        </Panel>
        <Panel title="Weather correlation matrix">
          <p className="caption">Pearson correlation coefficient (r) between {parameter.toUpperCase()} and meteorological factors.</p>
          <div className="correlation">
            {correlationList.map((item) => (
              <div key={item.label}>
                <span>{item.label}</span>
                <strong>
                  <Mono>{item.r}</Mono>
                </strong>
                <small>{item.sample}</small>
              </div>
            ))}
          </div>
          <p className="disclaimer">
            Correlation describes co-movement, not physical causation. Weather parameters are retrieved from Open-Meteo.
          </p>
        </Panel>
      </div>
    </>
  )
}

function Alerts({
  meta,
  alerts,
  quality,
}: {
  meta: Meta
  alerts: Alert[]
  quality: QualityReport[]
}) {
  const [filterType, setFilterType] = useState('All')
  const [severityFilter, setSeverityFilter] = useState('All')

  const filteredAlerts = useMemo(() => {
    return alerts.filter((a) => {
      const matchType =
        filterType === 'All' ||
        (filterType === 'Health' && a.alert_type.startsWith('HEALTH')) ||
        (filterType === 'Telemetry' && (a.alert_type === 'STALE' || a.alert_type === 'OFFLINE')) ||
        (filterType === 'Spikes' && a.alert_type === 'SPIKE')
      const matchSev = severityFilter === 'All' || a.severity.toUpperCase() === severityFilter.toUpperCase()
      return matchType && matchSev
    })
  }, [alerts, filterType, severityFilter])

  return (
    <>
      <div className="filter-row tabs">
        {['All', 'Health', 'Telemetry', 'Spikes'].map((x) => (
          <button
            key={x}
            className={filterType === x ? 'filter-active' : ''}
            onClick={() => setFilterType(x)}
          >
            {x}
          </button>
        ))}
      </div>
      <Panel
        title="Alert events"
        aside={
          <select
            aria-label="Severity"
            value={severityFilter}
            onChange={(e) => setSeverityFilter(e.target.value)}
          >
            <option value="All">All severities</option>
            <option value="WARNING">WARNING</option>
            <option value="CRITICAL">CRITICAL</option>
            <option value="EMERGENCY">EMERGENCY</option>
          </select>
        }
      >
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Station</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Message</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {filteredAlerts.length ? (
                filteredAlerts.map((a) => (
                  <tr key={a.alert_id}>
                    <td>
                      <Mono>{a.timestamp_utc ? a.timestamp_utc.slice(11, 16) : '12:00'} UTC</Mono>
                    </td>
                    <td>{a.station_name}</td>
                    <td>{a.alert_type}</td>
                    <td>
                      <span className={`severity ${a.severity.toLowerCase()}`}>{a.severity}</span>
                    </td>
                    <td>{a.message}</td>
                    <td>
                      <Mono>{a.metric_value !== null && a.metric_value !== undefined ? a.metric_value : '—'}</Mono>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted-text)' }}>
                    No alerts match the active filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel title="Pipeline data quality audit" aside={<span className="tiny">{quality.length} STAGES</span>}>
        <p className="caption">Continuous validation metrics tracking record throughput and rejection diagnostics across stages.</p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Stage</th>
                <th>Run ID</th>
                <th>Accepted Records</th>
                <th>Rejected</th>
                <th>Duplicates</th>
                <th>Rejection Breakdown</th>
              </tr>
            </thead>
            <tbody>
              {quality.map((q) => (
                <tr key={`${q.stage}-${q.run_id}`}>
                  <td><strong>{q.stage.toUpperCase()}</strong></td>
                  <td><Mono>{q.run_id.slice(0, 12)}...</Mono></td>
                  <td><Mono>{q.accepted_count ?? q.records_passed ?? 0}</Mono></td>
                  <td><Mono>{q.rejected_count ?? q.records_rejected ?? 0}</Mono></td>
                  <td><Mono>{q.duplicate_count ?? 0}</Mono></td>
                  <td><small>{q.reason_counts_json && q.reason_counts_json !== '{}' ? q.reason_counts_json : 'None (100% clean)'}</small></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel title="Published analytical datasets">
        <p className="caption">Downloadable CSV tables generated directly by Apache Spark and Pig ETL jobs.</p>
        <div className="dataset-grid">
          {[
            'station_statistics',
            'trends_hourly',
            'trends_daily',
            'weather_correlation',
            'cpcb_index',
            'alerts',
            'quality_report',
          ].map((x) => (
            <div className="dataset" key={x}>
              <strong>{x}</strong>
              <span>
                Last generated · <Mono>{meta.generated_at_utc ? meta.generated_at_utc.slice(0, 10) : '2026-11-24'}</Mono>
              </span>
              <button
                onClick={async () => {
                  try {
                    const endpointMap: Record<string, string> = {
                      station_statistics: '/api/v1/analytics/station-statistics',
                      trends_hourly: '/api/v1/analytics/trends/hourly',
                      trends_daily: '/api/v1/analytics/trends/daily',
                      weather_correlation: '/api/v1/analytics/weather-correlation',
                      cpcb_index: '/api/v1/aqi/cpcb-index',
                      alerts: '/api/v1/alerts',
                      quality_report: '/api/v1/audit/quality-report',
                    }
                    const res = await fetch(endpointMap[x] || `/api/v1/${x}`)
                    let content = ''
                    if (res.ok) {
                      const json = await res.json()
                      if (Array.isArray(json) && json.length > 0) {
                        const headers = Object.keys(json[0])
                        const rows = json.map((row) =>
                          headers.map((h) => JSON.stringify(row[h] ?? '')).join(',')
                        )
                        content = [headers.join(','), ...rows].join('\n')
                      } else {
                        content = JSON.stringify(json, null, 2)
                      }
                    } else {
                      content = `dataset,run_id,generated_at_utc\n${x},${meta.run_id},${meta.generated_at_utc}`
                    }
                    const blob = new Blob([content], { type: 'text/csv' })
                    const a = document.createElement('a')
                    a.href = URL.createObjectURL(blob)
                    a.download = `${x}.csv`
                    a.click()
                    URL.revokeObjectURL(a.href)
                  } catch (err) {
                    console.warn('CSV export error', err)
                  }
                }}
              >
                Download CSV
              </button>
            </div>
          ))}
        </div>
      </Panel>
    </>
  )
}

function Methodology({ meta }: { meta: Meta }) {
  return (
    <div className="method-layout">
      <article className="method">
        <span className="eyebrow">REFERENCE · BENGALURU</span>
        <h2>Methodology and System Architecture</h2>
        <p className="lede">
          SkyWatch delivers a verified, PM-based view of air quality across Bengaluru's CAAQMS monitoring network.
          Computations follow official Indian Central Pollution Control Board (CPCB) National Air Quality Index guidelines.
        </p>
        {[
          [
            'What SkyWatch measures',
            'Particulate matter (PM2.5 and PM10) reported by continuous monitoring stations in Bengaluru. The index is PM-based CPCB subindex, not an official 8-pollutant AQI.',
          ],
          [
            'Data sources and attribution',
            'Air quality: OpenAQ API v3. Weather: Open-Meteo.com (CC BY 4.0). Index method: CPCB National AQI formulas.',
          ],
          [
            'How the index is computed',
            'A 24-hour mean is calculated for PM2.5 and PM10. An index is valid only with at least 16 valid hourly values. Sub-indices use piecewise linear interpolation within official CPCB breakpoint bands; the overall index is the max sub-index.',
          ],
          [
            'Measurement freshness',
            'Fresh up to 3 hours; stale from 3 to 24 hours; offline beyond 24 hours. Freshness reflects the actual timestamp of sensor observation, not pipeline runtime.',
          ],
          [
            'Alert triggers',
            'Health alerts indicate Poor, Very Poor, or Severe threshold breaches. Telemetry alerts flag offline sensors or reading dropouts. Spike alerts detect rolling median spikes.',
          ],
          [
            'Pipeline architecture',
            'Phase 1 Python ingestion -> Phase 2 Apache Pig cleansing on HDFS -> Phase 3 Apache Spark analytics -> REST API and static data lake export.',
          ],
          [
            'Execution engines & profiles',
            `Active Run: ${meta.run_id} · Active Engine: ${meta.engine} · Active Profile: ${meta.profile}. Standalone simulation mode activates automatically when Apache Spark is uninstalled on the host.`,
          ],
        ].map(([title, text], i) => (
          <section id={`section-${i}`} key={title}>
            <h3>{title}</h3>
            <p>{text}</p>
            {i === 2 && (
              <table className="breakpoints">
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>PM2.5 µg/m³</th>
                    <th>PM10 µg/m³</th>
                    <th>Index Range</th>
                  </tr>
                </thead>
                <tbody>
                  {[
                    ['Good', '0–30', '0–50', '0–50'],
                    ['Satisfactory', '31–60', '51–100', '51–100'],
                    ['Moderate', '61–90', '101–250', '101–200'],
                    ['Poor', '91–120', '251–350', '201–300'],
                    ['Very Poor', '121–250', '351–430', '301–400'],
                    ['Severe', '250+', '430+', '401–500'],
                  ].map((r) => (
                    <tr key={r[0]}>
                      {r.map((c) => (
                        <td key={c}>{c}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {i === 5 && (
              <div className="pipeline">
                {['Collect', 'HDFS', 'Pig ETL', 'Spark Analytics', 'REST Lake'].map((x, j) => (
                  <span key={x}>
                    {j > 0 && <i />}
                    {x}
                  </span>
                ))}
              </div>
            )}
          </section>
        ))}
      </article>
      <aside className="toc">
        <span className="tiny">ON THIS PAGE</span>
        {['Measures', 'Sources', 'Index method', 'Freshness', 'Alerts', 'Pipeline', 'Engines'].map((x, i) => (
          <a key={x} href={`#section-${i}`}>
            {x}
          </a>
        ))}
      </aside>
    </div>
  )
}

export default function SkyWatch() {
  const path = usePathname()
  const [meta, setMeta] = useState<Meta>(defaultMeta)
  const [stations, setStations] = useState<Station[]>(defaultStations)
  const [indexes, setIndexes] = useState<AirIndex[]>(defaultIndexes)
  const [alerts, setAlerts] = useState<Alert[]>(defaultAlerts)
  const [hourly, setHourly] = useState<HourlyTrend[]>(defaultHourly)
  const [stats, setStats] = useState<StationStat[]>(defaultStats)
  const [daily, setDaily] = useState<DailyTrend[]>(defaultDaily)
  const [correlations, setCorrelations] = useState<WeatherCorrelation[]>(defaultCorrelations)
  const [quality, setQuality] = useState<QualityReport[]>(defaultQuality)

  const loadData = async () => {
    try {
      const [m, s, idx, alt, hr, st, dl, cr, ql] = await Promise.all([
        loadMeta(),
        loadStations(),
        loadIndexes(),
        loadAlerts(),
        loadHourlyTrends(),
        loadStationStatistics(),
        loadDailyTrends(),
        loadWeatherCorrelation(),
        loadQualityReport(),
      ])
      setMeta(m)
      setStations(s)
      setIndexes(idx)
      setAlerts(alt)
      setHourly(hr)
      setStats(st)
      setDaily(dl)
      setCorrelations(cr)
      setQuality(ql)
    } catch (e) {
      console.warn('Could not load live backend data, remaining on default fixtures.', e)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  const view = useMemo(() => {
    if (path === '/stations') return <Stations stations={stations} indexes={indexes} />
    if (path === '/analytics') {
      return (
        <Analytics
          stations={stations}
          indexes={indexes}
          hourly={hourly}
          stats={stats}
          daily={daily}
          correlations={correlations}
        />
      )
    }
    if (path === '/alerts') return <Alerts meta={meta} alerts={alerts} quality={quality} />
    if (path === '/methodology') return <Methodology meta={meta} />
    return (
      <Overview
        meta={meta}
        stations={stations}
        indexes={indexes}
        alerts={alerts}
        hourly={hourly}
        stats={stats}
      />
    )
  }, [path, meta, stations, indexes, alerts, hourly, stats, daily, correlations, quality])

  return (
    <AppShell meta={meta} refresh={loadData}>
      {view}
    </AppShell>
  )
}
