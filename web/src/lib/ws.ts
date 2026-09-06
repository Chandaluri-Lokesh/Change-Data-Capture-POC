export type MetricRow = {
  id: number
  doc_id: string
  collection: string
  operation: string
  e2e_lat_ms: number
  debezium_lat_ms: number
  write_lat_ms: number
  recorded_at: string
}

type WsMessage = { type: 'init' | 'update'; data: MetricRow[] }
type Handler = (rows: MetricRow[], isInit: boolean) => void

export function connectMetricsWs(onData: Handler): () => void {
  const protocol = location.protocol === 'https:' ? 'wss' : 'ws'
  const url = `${protocol}://${location.host}/ws/metrics`
  let ws: WebSocket
  let stopped = false
  let retryTimer: ReturnType<typeof setTimeout>

  function connect() {
    ws = new WebSocket(url)

    ws.onmessage = (ev) => {
      try {
        const msg: WsMessage = JSON.parse(ev.data)
        onData(msg.data, msg.type === 'init')
      } catch {
        // ignore malformed frames
      }
    }

    ws.onerror = () => ws.close()

    ws.onclose = () => {
      if (!stopped) {
        retryTimer = setTimeout(connect, 3000)
      }
    }
  }

  connect()

  return () => {
    stopped = true
    clearTimeout(retryTimer)
    ws?.close()
  }
}
