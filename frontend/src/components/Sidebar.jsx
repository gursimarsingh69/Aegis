import { useState, useEffect } from 'react';
import { getStatus } from '../api';

export default function Sidebar({ activePage, onNavigate }) {
  const [online, setOnline] = useState(null);

  useEffect(() => {
    const check = async () => {
      try {
        await getStatus();
        setOnline(true);
      } catch {
        setOnline(false);
      }
    };
    check();
    const interval = setInterval(check, 10000);
    return () => clearInterval(interval);
  }, []);

  const links = [
    { id: 'dashboard', icon: '📊', label: 'Dashboard' },
    { id: 'assets',    icon: '🗂️', label: 'Asset Library' },
    { id: 'scan',      icon: '🔍', label: 'Manual Scan' },
    { id: 'feed',      icon: '🚨', label: 'Infringement Feed' },
  ];

  return (
    <nav className="sidebar">
      <div className="sidebar-brand">
        <div className="brand-icon">🛡️</div>
        <span className="brand-text">AEGIS</span>
      </div>

      <ul className="nav-links">
        {links.map(link => (
          <li key={link.id}>
            <a
              className={`nav-link ${activePage === link.id ? 'active' : ''}`}
              onClick={() => onNavigate(link.id)}
            >
              <span className="nav-icon">{link.icon}</span>
              <span className="nav-label">{link.label}</span>
            </a>
          </li>
        ))}
      </ul>

      <div className="sidebar-status">
        <div className={`status-dot ${online === true ? 'online' : online === false ? 'offline' : ''}`} />
        <span>{online === true ? 'Engine Online' : online === false ? 'Engine Offline' : 'Checking…'}</span>
      </div>
    </nav>
  );
}
