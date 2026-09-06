import { useState } from 'react'
import { insertDocument, simulateChain, simulateUpdate } from '../lib/api'

type DocType = 'rfq' | 'po' | 'asn' | 'grn' | 'invoice'

const TEMPLATES: Record<DocType, Record<string, string | number | unknown[]>> = {
  rfq: {
    rfq_number: 'RFQ-2026-XXXXX',
    requested_date: '2026-09-06',
    requested_by: 'procurement@buyerco.com',
    plant: 'Plant-Chennai-01',
    status: 'OPEN',
    response_due_date: '2026-09-16',
    invited_vendors: [
      { vendor_id: 'V-1042', vendor_name: 'Ashford Industrial Supplies', invited_on: '2026-09-06' }
    ],
    line_items: [
      { line_no: 1, material_code: 'MAT-7734', description: 'Stainless Steel Pipe Fittings', quantity: 100, unit_of_measure: 'EA', target_delivery_date: '2026-10-06' }
    ],
  },
  po: {
    po_number: 'PO-2026-XXXXX',
    rfq_number: 'RFQ-2026-XXXXX',
    vendor_id: 'V-1042',
    vendor_name: 'Ashford Industrial Supplies',
    order_date: '2026-09-08',
    delivery_date: '2026-10-08',
    plant: 'Plant-Chennai-01',
    currency: 'USD',
    status: 'OPEN',
    payment_terms: 'NET-30',
    order_total: 1875.00,
    line_items: [
      { line_no: 1, material_code: 'MAT-7734', description: 'Stainless Steel Pipe Fittings', quantity: 100, unit_of_measure: 'EA', unit_price: 18.75, line_total: 1875.00 }
    ],
  },
  asn: {
    asn_number: 'ASN-2026-XXXXX',
    po_number: 'PO-2026-XXXXX',
    vendor_id: 'V-1042',
    ship_date: '2026-09-18',
    carrier: 'BlueDart Logistics',
    tracking_number: 'BD0000000001',
    expected_arrival: '2026-09-28',
    status: 'IN_TRANSIT',
    line_items: [
      { line_no: 1, material_code: 'MAT-7734', quantity_shipped: 100, unit_of_measure: 'EA' }
    ],
  },
  grn: {
    grn_number: 'GRN-2026-XXXXX',
    po_number: 'PO-2026-XXXXX',
    asn_number: 'ASN-2026-XXXXX',
    receipt_date: '2026-09-29',
    received_by: 'warehouse.chennai@buyerco.com',
    plant: 'Plant-Chennai-01',
    status: 'COMPLETED',
    line_items: [
      { line_no: 1, material_code: 'MAT-7734', quantity_received: 100, unit_of_measure: 'EA', condition: 'ACCEPTED', remarks: null }
    ],
  },
  invoice: {
    invoice_number: 'INV-2026-XXXXX',
    po_number: 'PO-2026-XXXXX',
    grn_number: 'GRN-2026-XXXXX',
    vendor_id: 'V-1042',
    invoice_date: '2026-10-01',
    due_date: '2026-10-31',
    currency: 'USD',
    status: 'PENDING_PAYMENT',
    subtotal: 1875.00,
    tax_rate: 0.08,
    tax_amount: 150.00,
    total_amount: 2025.00,
    line_items: [
      { line_no: 1, material_code: 'MAT-7734', quantity: 100, unit_price: 18.75, amount: 1875.00 }
    ],
  },
}

export default function Documents() {
  const [docType, setDocType]     = useState<DocType>('po')
  const [jsonText, setJsonText]   = useState(() => JSON.stringify(TEMPLATES['po'], null, 2))
  const [status, setStatus]       = useState<string | null>(null)
  const [error, setError]         = useState<string | null>(null)
  const [chainResult, setChainResult] = useState<Record<string, string> | null>(null)

  const handleTypeChange = (t: DocType) => {
    setDocType(t)
    setJsonText(JSON.stringify(TEMPLATES[t], null, 2))
    setStatus(null)
    setError(null)
  }

  const handleInsert = async () => {
    setStatus(null)
    setError(null)
    try {
      const fields = JSON.parse(jsonText)
      const result = await insertDocument(docType, fields)
      setStatus(`Inserted ${result._id} into ${result.collection}`)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const handleSimulateChain = async () => {
    setChainResult(null)
    setError(null)
    try {
      const r = await simulateChain()
      setChainResult(r)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const handleSimulateUpdate = async () => {
    setStatus(null)
    setError(null)
    try {
      const r = await simulateUpdate()
      setStatus(`Updated PO ${r.po_id} → status=${r.new_status}`)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <div className="space-y-6 max-w-4xl">
      <h1 className="text-xl font-semibold text-white">Documents</h1>

      {/* Quick actions */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-3">Quick Simulation</h2>
        <div className="flex gap-3">
          <button className="btn-primary" onClick={handleSimulateChain}>
            Insert Full P2P Chain
          </button>
          <button className="btn-secondary" onClick={handleSimulateUpdate}>
            Random PO Status Update
          </button>
        </div>
        {chainResult && (
          <div className="mt-3 p-3 bg-green-950 border border-green-800 rounded-lg text-sm space-y-1">
            <p className="text-green-300 font-medium">Chain inserted successfully</p>
            {Object.entries(chainResult)
              .filter(([k]) => k.endsWith('_id'))
              .map(([k, v]) => (
                <p key={k} className="text-gray-300">
                  <span className="text-gray-500">{k}:</span>{' '}
                  <span className="font-mono">{v as string}</span>
                </p>
              ))}
          </div>
        )}
      </div>

      {/* Manual insert */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-3">Manual Document Insert</h2>

        <div className="flex gap-2 mb-4">
          {(['rfq', 'po', 'asn', 'grn', 'invoice'] as DocType[]).map(t => (
            <button
              key={t}
              onClick={() => handleTypeChange(t)}
              className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                docType === t
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-800 text-gray-400 hover:text-white'
              }`}
            >
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        <textarea
          className="input-field font-mono text-xs h-72 resize-none"
          value={jsonText}
          onChange={e => setJsonText(e.target.value)}
          spellCheck={false}
        />

        <div className="flex items-center gap-3 mt-3">
          <button className="btn-primary" onClick={handleInsert}>
            Insert into MongoDB
          </button>
          <button
            className="btn-secondary text-sm"
            onClick={() => setJsonText(JSON.stringify(TEMPLATES[docType], null, 2))}
          >
            Reset Template
          </button>
        </div>

        {status && (
          <p className="mt-3 text-green-400 text-sm">{status}</p>
        )}
        {error && (
          <p className="mt-3 text-red-400 text-sm">{error}</p>
        )}
      </div>

      <div className="card text-sm text-gray-400 space-y-1">
        <p className="text-gray-300 font-medium">How it works</p>
        <p>Inserting a document writes directly to MongoDB. Debezium detects the change,
           produces a Kafka message, and the consumer propagates it to Postgres and Neo4j.
           Check the Dashboard to see the event appear in real time.</p>
      </div>
    </div>
  )
}
