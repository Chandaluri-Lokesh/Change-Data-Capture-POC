import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import Dashboard  from './pages/Dashboard'
import Documents  from './pages/Documents'
import Graph      from './pages/Graph'
import Pipeline   from './pages/Pipeline'

const navCls = ({ isActive }: { isActive: boolean }) =>
  `px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
    isActive
      ? 'bg-blue-600 text-white'
      : 'text-gray-400 hover:text-white hover:bg-gray-800'
  }`

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col">
        {/* Nav */}
        <header className="bg-gray-900 border-b border-gray-800 px-6 py-3 flex items-center gap-6">
          <span className="text-lg font-bold text-white tracking-tight">P2P CDC</span>
          <nav className="flex gap-1">
            <NavLink to="/"          end className={navCls}>Dashboard</NavLink>
            <NavLink to="/documents"     className={navCls}>Documents</NavLink>
            <NavLink to="/graph"         className={navCls}>Graph</NavLink>
            <NavLink to="/pipeline"      className={navCls}>Pipeline</NavLink>
          </nav>
        </header>

        {/* Page */}
        <main className="flex-1 p-6">
          <Routes>
            <Route path="/"          element={<Dashboard />} />
            <Route path="/documents" element={<Documents />} />
            <Route path="/graph"     element={<Graph />} />
            <Route path="/pipeline"  element={<Pipeline />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
