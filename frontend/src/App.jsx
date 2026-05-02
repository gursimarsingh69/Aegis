import { useState } from 'react';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import Assets from './pages/Assets';
import Scan from './pages/Scan';
import Feed from './pages/Feed';
import ToastContainer from './components/ToastContainer';
import './index.css';

export default function App() {
  const [page, setPage] = useState('dashboard');
  const [toasts, setToasts] = useState([]);

  const addToast = (message, type = 'info') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000);
  };

  const renderPage = () => {
    switch (page) {
      case 'dashboard': return <Dashboard addToast={addToast} />;
      case 'assets':    return <Assets addToast={addToast} />;
      case 'scan':      return <Scan addToast={addToast} />;
      case 'feed':      return <Feed addToast={addToast} />;
      default:          return <Dashboard addToast={addToast} />;
    }
  };

  return (
    <>
      <Sidebar activePage={page} onNavigate={setPage} />
      <main className="main-content">
        {renderPage()}
      </main>
      <ToastContainer toasts={toasts} />
    </>
  );
}
