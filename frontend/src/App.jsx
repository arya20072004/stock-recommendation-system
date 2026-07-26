import React from 'react';
import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from './theme/ThemeContext';
import { ThemeToggle } from './components/ThemeToggle';
import { Dashboard } from './pages/Dashboard';
import { Portfolio } from './pages/Portfolio';
import { Activity, LayoutDashboard } from 'lucide-react';

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

const NavLink = ({ to, icon, label }) => {
  const location = useLocation();
  const isActive = location.pathname === to;
  
  return (
    <Link 
      to={to} 
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
        padding: '0.5rem 1rem',
        borderRadius: '0.5rem',
        color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)',
        backgroundColor: isActive ? 'var(--bg-hover)' : 'transparent',
        textDecoration: 'none',
        fontWeight: isActive ? 600 : 500,
        transition: 'background-color 0.2s',
      }}
    >
      {icon}
      <span>{label}</span>
    </Link>
  );
};

const Layout = ({ children }) => {
  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header style={{ 
        borderBottom: '1px solid var(--border-color)', 
        backgroundColor: 'var(--bg-card)',
        padding: '1rem 0'
      }}>
        <div className="container" style={{ 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          paddingTop: 0,
          paddingBottom: 0
        }}>
          <div className="flex items-center gap-6">
            <Link to="/" style={{ textDecoration: 'none', color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <div style={{ 
                width: '32px', 
                height: '32px', 
                backgroundColor: 'var(--accent-blue)', 
                borderRadius: '0.5rem',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'white'
              }}>
                <Activity size={20} />
              </div>
              <span style={{ fontWeight: 700, fontSize: '1.25rem', letterSpacing: '-0.025em' }}>QuantSignal</span>
            </Link>
            
            <nav className="flex items-center gap-2" style={{ marginLeft: '2rem' }}>
              <NavLink to="/" icon={<Activity size={18} />} label="Terminal" />
              <NavLink to="/portfolio" icon={<LayoutDashboard size={18} />} label="Portfolio" />
            </nav>
          </div>
          
          <div>
            <ThemeToggle />
          </div>
        </div>
      </header>
      
      <main style={{ flex: 1, padding: '2rem 0' }}>
        {children}
      </main>
    </div>
  );
};

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <BrowserRouter>
          <Layout>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/portfolio" element={<Portfolio />} />
            </Routes>
          </Layout>
        </BrowserRouter>
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;
