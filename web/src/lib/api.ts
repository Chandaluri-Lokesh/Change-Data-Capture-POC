import axios from 'axios'

const api = axios.create({ baseURL: '/api' })

// ── Metrics ──────────────────────────────────────────────────────────────────
export const fetchMetricsSummary = () =>
  api.get('/metrics/summary').then(r => r.data)

export const fetchMetricsRecent = (limit = 50) =>
  api.get('/metrics/recent', { params: { limit } }).then(r => r.data)

export const fetchEventsByCollection = () =>
  api.get('/metrics/collections').then(r => r.data)

// ── Pipeline ─────────────────────────────────────────────────────────────────
export const fetchPipelineStatus = () =>
  api.get('/pipeline/status').then(r => r.data)

// ── Graph ────────────────────────────────────────────────────────────────────
export const fetchSubgraph = (collection: string, docId: string) =>
  api.get(`/graph/${collection}/${encodeURIComponent(docId)}`).then(r => r.data)

export const fetchGraphOverview = () =>
  api.get('/graph/stats/overview').then(r => r.data)

// ── Documents ────────────────────────────────────────────────────────────────
export const insertDocument = (docType: string, fields: Record<string, unknown>) =>
  api.post(`/documents/${docType}`, { fields }).then(r => r.data)

export const deleteDocument = (docType: string, docId: string) =>
  api.delete(`/documents/${docType}/${encodeURIComponent(docId)}`).then(r => r.data)

// ── Simulator ────────────────────────────────────────────────────────────────
export const simulateChain = () =>
  api.post('/simulate/chain').then(r => r.data)

export const simulateUpdate = () =>
  api.post('/simulate/update').then(r => r.data)
