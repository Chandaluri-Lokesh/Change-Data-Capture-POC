import { useCallback, useEffect, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { fetchSubgraph, fetchGraphOverview } from '../lib/api'

type GraphData = {
  nodes: { id: string; label: string; properties: Record<string, unknown> }[]
  links: { source: string; target: string; type: string }[]
}

type Overview = {
  node_counts: { label: string; count: number }[]
  relationship_counts: { type: string; count: number }[]
}

const COLLECTION_MAP: Record<string, string> = {
  rfqs: 'rfqs', purchase_orders: 'purchase_orders',
  asns: 'asns', grns: 'grns', invoices: 'invoices',
}

const LABEL_COLORS: Record<string, string> = {
  RFQ:           '#f59e0b',
  PurchaseOrder: '#3b82f6',
  ASN:           '#8b5cf6',
  GRN:           '#10b981',
  Invoice:       '#ef4444',
  Vendor:        '#06b6d4',
  Material:      '#ec4899',
}

export default function Graph() {
  const [collection, setCollection] = useState('purchase_orders')
  const [docId, setDocId]           = useState('')
  const [graphData, setGraphData]   = useState<GraphData | null>(null)
  const [overview, setOverview]     = useState<Overview | null>(null)
  const [selected, setSelected]     = useState<GraphData['nodes'][0] | null>(null)
  const [loading, setLoading]       = useState(false)
  const [error, setError]           = useState<string | null>(null)
  const containerRef                = useRef<HTMLDivElement>(null)

  useEffect(() => {
    fetchGraphOverview().then(setOverview).catch(() => {})
  }, [])

  const handleSearch = async () => {
    if (!docId.trim()) return
    setLoading(true)
    setError(null)
    setSelected(null)
    try {
      const data = await fetchSubgraph(collection, docId.trim())
      setGraphData(data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Graph unavailable')
      setGraphData(null)
    } finally {
      setLoading(false)
    }
  }

  const handleNodeClick = useCallback((node: unknown) => {
    setSelected(node as GraphData['nodes'][0])
  }, [])

  const nodeColor = (node: { label?: string }) =>
    LABEL_COLORS[node.label ?? ''] ?? '#6b7280'

  const width  = containerRef.current?.clientWidth  ?? 700
  const height = 460

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-white">Neo4j Graph</h1>

      {/* Overview stats */}
      {overview && (
        <div className="grid md:grid-cols-2 gap-4">
          <div className="card">
            <h2 className="text-sm text-gray-400 mb-2">Node counts</h2>
            <div className="space-y-1">
              {overview.node_counts.map(n => (
                <div key={n.label} className="flex justify-between text-sm">
                  <span className="flex items-center gap-2">
                    <span
                      className="w-2.5 h-2.5 rounded-full inline-block"
                      style={{ background: LABEL_COLORS[n.label] ?? '#6b7280' }}
                    />
                    <span className="text-gray-300">{n.label}</span>
                  </span>
                  <span className="text-blue-400 font-medium">{n.count}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="card">
            <h2 className="text-sm text-gray-400 mb-2">Relationship counts</h2>
            <div className="space-y-1">
              {overview.relationship_counts.map(r => (
                <div key={r.type} className="flex justify-between text-sm">
                  <span className="text-gray-300 font-mono text-xs">{r.type}</span>
                  <span className="text-purple-400 font-medium">{r.count}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Search */}
      <div className="card">
        <h2 className="text-sm text-gray-400 mb-3">Explore Subgraph</h2>
        <div className="flex gap-3">
          <select
            className="input-field w-48"
            value={collection}
            onChange={e => setCollection(e.target.value)}
          >
            {Object.keys(COLLECTION_MAP).map(c => (
              <option key={c} value={c}>{c.replace('_', ' ')}</option>
            ))}
          </select>
          <input
            className="input-field flex-1"
            placeholder="Document ID e.g. PO-2026-05512"
            value={docId}
            onChange={e => setDocId(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSearch()}
          />
          <button className="btn-primary" onClick={handleSearch} disabled={loading}>
            {loading ? 'Loading…' : 'Explore'}
          </button>
        </div>
        {error && <p className="text-red-400 text-sm mt-2">{error}</p>}
      </div>

      {/* Graph canvas + node inspector */}
      {graphData && (
        <div className="grid md:grid-cols-3 gap-4">
          <div className="md:col-span-2 card p-0 overflow-hidden rounded-xl" ref={containerRef}>
            {graphData.nodes.length === 0 ? (
              <div className="flex items-center justify-center h-64 text-gray-500">
                No nodes found for this document ID.
              </div>
            ) : (
              <ForceGraph2D
                width={width}
                height={height}
                graphData={graphData}
                nodeLabel={(n: object) => (n as { id?: string }).id ?? ''}
                nodeColor={(n: object) => nodeColor(n as { label?: string })}
                nodeRelSize={6}
                linkLabel={(l: object) => (l as { type?: string }).type ?? ''}
                linkDirectionalArrowLength={5}
                linkDirectionalArrowRelPos={1}
                linkColor={() => '#4b5563'}
                onNodeClick={handleNodeClick}
                backgroundColor="#111827"
              />
            )}
          </div>

          <div className="card">
            <h2 className="text-sm text-gray-400 mb-3">
              {selected ? 'Node Properties' : 'Click a node to inspect'}
            </h2>
            {selected ? (
              <div className="space-y-2 text-sm">
                <div>
                  <span
                    className="badge text-xs"
                    style={{ background: nodeColor(selected) + '33', color: nodeColor(selected) }}
                  >
                    {selected.label}
                  </span>
                </div>
                {Object.entries(selected.properties || {}).map(([k, v]) => (
                  <div key={k}>
                    <span className="text-gray-500 text-xs">{k}</span>
                    <p className="text-gray-200 font-mono text-xs break-all">
                      {v === null ? 'null' : String(v)}
                    </p>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-gray-500 text-sm">Select a node in the graph.</p>
            )}
          </div>
        </div>
      )}

      {/* Legend */}
      <div className="card">
        <h2 className="text-sm text-gray-400 mb-2">Node Legend</h2>
        <div className="flex flex-wrap gap-3">
          {Object.entries(LABEL_COLORS).map(([label, color]) => (
            <span key={label} className="flex items-center gap-1.5 text-sm text-gray-300">
              <span className="w-3 h-3 rounded-full" style={{ background: color }} />
              {label}
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
