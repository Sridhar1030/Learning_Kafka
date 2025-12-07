import { useEffect, useMemo, useState } from 'react'
import './App.css'

const API_BASE = 'http://localhost:8000'
const WS_BASE = 'ws://localhost:8000'

function App() {
  const [view, setView] = useState('buy')
  const [form, setForm] = useState({ user: '', product: '', quantity: 1 })
  const [placing, setPlacing] = useState(false)
  const [placeResult, setPlaceResult] = useState(null)
  const [orders, setOrders] = useState([])
  const [loadingOrders, setLoadingOrders] = useState(false)
  const [error, setError] = useState('')
  const [wsStatus, setWsStatus] = useState('disconnected')

  const isFormValid = useMemo(
    () => form.user.trim() && form.product.trim() && Number(form.quantity) > 0,
    [form]
  )

  const handleChange = (e) => {
    const { name, value } = e.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!isFormValid) return
    setPlacing(true)
    setError('')
    setPlaceResult(null)

    try {
      const res = await fetch(`${API_BASE}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user: form.user.trim(),
          product: form.product.trim(),
          quantity: Number(form.quantity),
        }),
      })

      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        throw new Error(body.detail || 'Failed to place order')
      }

      const body = await res.json()
      setPlaceResult(body)
      setForm({ user: '', product: '', quantity: 1 })
    } catch (err) {
      setError(err.message)
    } finally {
      setPlacing(false)
    }
  }

  const fetchOrders = async () => {
    setLoadingOrders(true)
    setError('')
    try {
      const res = await fetch(`${API_BASE}/orders`)
      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        throw new Error(body.detail || 'Failed to load orders')
      }
      const body = await res.json()
      setOrders(body.orders || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoadingOrders(false)
    }
  }

  useEffect(() => {
    if (view !== 'get') return

    const ws = new WebSocket(`${WS_BASE}/ws/orders`)

    ws.onopen = () => {
      setWsStatus('connected')
      console.log('✅ Connected to live order stream')
      fetchOrders()  // Load initial orders
    }

    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data)
        
        // If it's an order message from Kafka consumer
        if (message.type === 'order') {
          const order = message.order
          // Add to front of orders list (most recent first)
          setOrders((prev) => [
            {
              order_id: order.order_id,
              status: 'pending',
              payload: order,
              kafka_metadata: null,
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString(),
            },
            ...prev,
          ])
        } else if (message.type === 'status_update') {
          // Status update for existing order
          setOrders((prev) =>
            prev.map((order) =>
              order.order_id === message.order_id
                ? { ...order, status: message.status }
                : order
            )
          )
        }
      } catch (err) {
        console.error('Failed to parse WebSocket message:', err)
      }
    }

    ws.onerror = () => {
      setWsStatus('error')
      setError('WebSocket connection error')
    }

    ws.onclose = () => {
      setWsStatus('disconnected')
      console.log('❌ Disconnected from live order stream')
    }

    return () => {
      ws.close()
    }
  }, [view])

  return (
    <div className="page">
      <header className="header">
        <div>
          <p className="eyebrow">Kafka Orders</p>
          <h1>Order Portal</h1>
          <p className="muted">Post orders to Kafka and view them live.</p>
        </div>
        <nav className="tabs">
          <button
            className={view === 'buy' ? 'tab active' : 'tab'}
            onClick={() => setView('buy')}
          >
            Buy
          </button>
          <button
            className={view === 'get' ? 'tab active' : 'tab'}
            onClick={() => setView('get')}
          >
            Live Orders
          </button>
        </nav>
      </header>

      {error && <div className="alert">{error}</div>}

      {view === 'buy' && (
        <section className="card form-card">
          <h2>Create Order</h2>
          <form className="form" onSubmit={handleSubmit}>
            <label>
              User
              <input
                name="user"
                value={form.user}
                onChange={handleChange}
                placeholder="alice"
                required
              />
            </label>
            <label>
              Product
              <input
                name="product"
                value={form.product}
                onChange={handleChange}
                placeholder="laptop"
                required
              />
            </label>
            <label>
              Quantity
              <input
                name="quantity"
                type="number"
                min="1"
                value={form.quantity}
                onChange={handleChange}
                required
              />
            </label>
            <button type="submit" disabled={!isFormValid || placing}>
              {placing ? 'Placing…' : 'Submit Order'}
            </button>
          </form>

          {placeResult && (
            <div className="result">
              <p className="muted">Order accepted</p>
              <p><strong>Order ID:</strong> {placeResult.order_id}</p>
              <p><strong>Status:</strong> {placeResult.status}</p>
              {placeResult.order && (
                <pre className="payload">{JSON.stringify(placeResult.order, null, 2)}</pre>
              )}
            </div>
          )}
        </section>
      )}

      {view === 'get' && (
        <section className="card list-card">
          <div className="list-header">
            <div>
              <h2>Live Orders</h2>
              <p className="muted ws-status">
                {wsStatus === 'connected' && '🟢 Live (streaming from Kafka)'}
                {wsStatus === 'disconnected' && '⚪ Offline'}
                {wsStatus === 'error' && '🔴 Connection error'}
              </p>
            </div>
            <button onClick={fetchOrders} disabled={loadingOrders}>
              {loadingOrders ? 'Loading…' : 'Refresh'}
            </button>
          </div>
          <div className="table">
            <div className="table-row head">
              <span>Order ID</span>
              <span>Status</span>
              <span>User</span>
              <span>Product</span>
              <span>Qty</span>
            </div>
            {orders.map((o) => (
              <div key={o.order_id} className="table-row">
                <span className="mono">{o.order_id.slice(0, 8)}...</span>
                <span className={`badge badge-${o.status}`}>{o.status}</span>
                <span>{o.payload?.user}</span>
                <span>{o.payload?.product}</span>
                <span>{o.payload?.quantity}</span>
              </div>
            ))}
            {!orders.length && !loadingOrders && (
              <p className="muted center">No orders yet. Submit one to get started!</p>
            )}
          </div>
        </section>
      )}
    </div>
  )
}

export default App
