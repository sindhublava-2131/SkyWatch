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
} from '@/lib/api'
import { alerts as defaultAlerts, hourly as defaultHourly, indexes as defaultIndexes, meta as defaultMeta, stations as defaultStations } from '@/lib/mocks'
import type { AirIndex, Alert, HourlyTrend, Meta, Station } from '@/lib/schemas'

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
}: {
  meta: Meta
  stations: Station[]
  indexes: AirIndex[]
  alerts: Alert[]
  hourly: HourlyTrend[]
}) {
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
              <Mono>31.4 µg/m³</Mono>
            </strong>
          </div>
          <div>
            <span>Mean PM10 · 24 h</span>
            <strong>
              <Mono>82.6 µg/m³</Mono>
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
          {filtered.map((s, i) => {
            const idx = indexes.find((x) => x.station_id === s.id || x.station_id === s.station_id)
            const aqiVal = idx?.aqi_value ?? null
            const category = idx?.aqi_category ?? 'Good'
            return (
              <button
                key={s.id || s.station_id}
                className="map-marker"
                style={{
                  left: `${17 + ((i * 19) % 70)}%`,
                  top: `${19 + ((i * 23) % 62)}%`,
                  background: aqiVal === null ? '#d8d6ce' : categoryColor[category] ?? '#3E9B47',
                }}
                title={`${s.name}: ${idx?.aqi_category ?? 'Not computed'}`}
              >
                <Mono>{aqiVal ?? '—'}</Mono>
              </button>
            )
          })}
          <span className="map-label">BENGALURU · 12.97° N, 77.59° E</span>
          <span className="map-credit">Schematic geospatial positions · CAAQMS network</span>
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
}: {
  stations: Station[]
  indexes: AirIndex[]
  hourly: HourlyTrend[]
}) {
  const [parameter, setParameter] = useState<'pm25' | 'pm10'>('pm25')
  const chartData = useMemo(() => {
    return hourly.map((d) => ({
      ...d,
      hour: d.ist_time_range || `${String((d.hour_of_day + 5) % 24).padStart(2, '0')}:30 IST`,
    }))
  }, [hourly])

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
                {indexes.map((i, n) => (
                  <tr key={i.station_id}>
                    <td>
                      <Mono>{n + 1}</Mono>
                    </td>
                    <td>{i.station_name}</td>
                    <td>
                      <Mono>{(i.pm25_24h_avg ?? 0).toFixed(1)}</Mono>
                    </td>
                    <td>
                      <span className="range-line" />{' '}
                      <Mono>
                        {Math.max(0, Math.round((i.pm25_24h_avg ?? 0) - 8))}–
                        {Math.round((i.pm25_24h_avg ?? 0) + 14)}
                      </Mono>
                    </td>
                    <td>
                      <Mono>24</Mono>
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
            <figcaption>Citywide hourly PM2.5 average; reference line indicates National Ambient Air Quality Standard (60 µg/m³).</figcaption>
          </figure>
        </Panel>
      </div>
      <div className="grid-two">
        <Panel title="Day-of-week profile">
          <p className="caption">Typical citywide PM2.5 concentration by weekday, Monday first.</p>
          <div className="week-bars">
            {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((d, i) => (
              <div key={d}>
                <div style={{ height: `${45 + i * 6}%` }} />
                <span>{d}</span>
              </div>
            ))}
          </div>
        </Panel>
        <Panel title="Weather correlation matrix">
          <p className="caption">Pearson correlation coefficient (r) between PM2.5 and meteorological factors.</p>
          <div className="correlation">
            {[
              ['Temperature (2m)', '0.284', 'sample_count 168 (High)'],
              ['Relative humidity (2m)', '−0.412', 'sample_count 168 (High)'],
              ['Wind speed (10m)', '−0.193', 'sample_count 160 (Moderate)'],
              ['Precipitation', '−0.068', 'sample_count 142 (Low)'],
            ].map(([x, v, n]) => (
              <div key={x}>
                <span>{x}</span>
                <strong>
                  <Mono>{v}</Mono>
                </strong>
                <small>{n}</small>
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
}: {
  meta: Meta
  alerts: Alert[]
}) {
  return (
    <>
      <div className="filter-row tabs">
        {['All', 'Health', 'Telemetry', 'Spikes'].map((x, i) => (
          <button key={x} className={i === 0 ? 'filter-active' : ''}>
            {x}
          </button>
        ))}
      </div>
      <Panel
        title="Alert events"
        aside={
          <select aria-label="Severity">
            <option>All severities</option>
            <option>WARNING</option>
            <option>CRITICAL</option>
            <option>EMERGENCY</option>
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
              {alerts.map((a) => (
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
                    <Mono>{a.metric_value ?? '—'}</Mono>
                  </td>
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
                onClick={() => {
                  const blob = new Blob([`dataset,generated\n${x},${meta.generated_at_utc}`], {
                    type: 'text/csv',
                  })
                  const a = document.createElement('a')
                  a.href = URL.createObjectURL(blob)
                  a.download = `${x}.csv`
                  a.click()
                  URL.revokeObjectURL(a.href)
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

  const loadData = async () => {
    try {
      const [m, s, idx, alt, hr] = await Promise.all([
        loadMeta(),
        loadStations(),
        loadIndexes(),
        loadAlerts(),
        loadHourlyTrends(),
      ])
      setMeta(m)
      setStations(s)
      setIndexes(idx)
      setAlerts(alt)
      setHourly(hr)
    } catch (e) {
      console.warn('Could not load live backend data, remaining on default fixtures.', e)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  const view = useMemo(() => {
    if (path === '/stations') return <Stations stations={stations} indexes={indexes} />
    if (path === '/analytics') return <Analytics stations={stations} indexes={indexes} hourly={hourly} />
    if (path === '/alerts') return <Alerts meta={meta} alerts={alerts} />
    if (path === '/methodology') return <Methodology meta={meta} />
    return <Overview meta={meta} stations={stations} indexes={indexes} alerts={alerts} hourly={hourly} />
  }, [path, meta, stations, indexes, alerts, hourly])

  return (
    <AppShell meta={meta} refresh={loadData}>
      {view}
    </AppShell>
  )
}
