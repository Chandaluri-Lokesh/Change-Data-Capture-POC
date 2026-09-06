import { useEffect, useState } from 'react'
import { fetchPipelineStatus } from '../lib/api'

type Task   = { id: number; state: string; worker_id?: string }
type Connector = { name: string; state: string; tasks: Task[]; worker_id?: string }
type LagRow = { topic: string; partition: number; lag: number; committed: number; end: number }
type Status = {
  overall:    string
  connectors: Connector[]
  kafka_lag:  LagRow[]
  total_lag:  number
}

function StateBadge({ state }: { state: string }) {
  const cls =
    state === 'RUNNING'    ? 'badge badge-green'  :
    state === 'FAILED'     ? 'badge badge-red'    :
    state === 'PAUSED'     ? 'badge badge-yellow' :
    state === 'UNASSIGNED' ? 'badge badge-gray'   : 'badge badge-yellow'
  return <span className={cls}>{state}</span>
}

export default function Pipeline() {
  const [status, setStatus]       = useState<Status | null>(null)
  const [loading, setLoading]     = useState(true)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)

  const load = () => {
    setLoading(true)
    fetchPipelineStatus()
      .then(d => { setStatus(d); setLastRefresh(new Date()) })
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 15000)
    return () => clearInterval(id)
  }, [])

  const overall = status?.overall ?? '—'
  const overallCls =
    overall === 'HEALTHY'  ? 'text-green-400' :
    overall === 'DEGRADED' ? 'text-yellow-400' : 'text-gray-400'

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Pipeline Status</h1>
        <div className="flex items-center gap-3">
          {lastRefresh && (
            <span className="text-xs text-gray-500">
              Refreshed {lastRefresh.toLocaleTimeString()}
            </span>
          )}
          <button className="btn-secondary text-sm" onClick={load} disabled={loading}>
            {loading ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
      </div>

      {/* Overall status banner */}
      <div className="card flex items-center gap-4">
        <div className={`text-3xl font-bold ${overallCls}`}>{overall}</div>
        <div className="text-sm text-gray-400">
          <p>Total Kafka lag: <span className="text-white font-medium">{status?.total_lag ?? '—'}</span> messages</p>
          <p>{status?.connectors.length ?? 0} connector(s) registered</p>
        </div>
      </div>

      {/* Connectors */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-3">Kafka Connect Connectors</h2>
        {status?.connectors.length === 0 && (
          <p className="text-gray-500 text-sm">No connectors registered.</p>
        )}
        <div className="space-y-4">
          {status?.connectors.map(c => (
            <div key={c.name} className="border border-gray-800 rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="font-mono text-white text-sm">{c.name}</span>
                <StateBadge state={c.state} />
              </div>
              {c.worker_id && (
                <p className="text-xs text-gray-500 mb-2">Worker: {c.worker_id}</p>
              )}
              {c.tasks.length > 0 && (
                <div className="space-y-1">
                  <p className="text-xs text-gray-500 uppercase tracking-wider">Tasks</p>
                  {c.tasks.map((t: Task) => (
                    <div key={t.id} className="flex items-center gap-2 text-xs">
                      <span className="text-gray-500">Task {t.id}</span>
                      <StateBadge state={t.state} />
                      {t.worker_id && <span className="text-gray-600">{t.worker_id}</span>}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Kafka Lag */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-3">Kafka Consumer Lag</h2>
        {!status?.kafka_lag.length ? (
          <p className="text-gray-500 text-sm">
            No lag data — consumer group 'p2p-pipeline-consumer' may not have committed offsets yet.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 text-xs uppercase border-b border-gray-800">
                  <th className="py-2 text-left">Topic</th>
                  <th className="py-2 text-left">Partition</th>
                  <th className="py-2 text-right">Committed</th>
                  <th className="py-2 text-right">End</th>
                  <th className="py-2 text-right">Lag</th>
                </tr>
              </thead>
              <tbody>
                {status.kafka_lag.map((row, i) => (
                  <tr key={i} className="border-b border-gray-800/50">
                    <td className="py-1.5 font-mono text-xs text-gray-300">{row.topic}</td>
                    <td className="py-1.5 text-gray-400">{row.partition}</td>
                    <td className="py-1.5 text-right text-gray-400">{row.committed.toLocaleString()}</td>
                    <td className="py-1.5 text-right text-gray-400">{row.end.toLocaleString()}</td>
                    <td className="py-1.5 text-right">
                      <span className={row.lag === 0 ? 'text-green-400' : row.lag < 100 ? 'text-yellow-400' : 'text-red-400'}>
                        {row.lag.toLocaleString()}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Architecture reference */}
      <div className="card text-sm text-gray-400 space-y-2">
        <p className="text-gray-300 font-medium">Data Flow</p>
        <div className="font-mono text-xs space-y-1 text-gray-500">
          <p>MongoDB (rs0, :27018)  →  Debezium (Kafka Connect :8083)</p>
          <p>→  Kafka (:9092, topics: poc.mydb.[rfqs|purchase_orders|asns|grns|invoices])</p>
          <p>→  Python Consumer  →  PostgreSQL (:5432, analytics DB)</p>
          <p>                    →  Neo4j (:7687, bolt)</p>
        </div>
      </div>
    </div>
  )
}
