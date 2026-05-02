import { useState, useEffect } from 'react';
import { getStatus, getHistory } from '../api';

export default function Dashboard() {
  const [status, setStatus] = useState(null);
  const [history, setHistory] = useState([]);

  useEffect(() => {
    const load = async () => {
      try {
        const [s, h] = await Promise.all([getStatus(), getHistory()]);
        setStatus(s.data);
        setHistory(h.data);
      } catch { /* engine offline */ }
    };
    load();
    const interval = setInterval(load, 8000);
    return () => clearInterval(interval);
  }, []);

  const totalScans = status?.total_scans || 0;
  const totalMatches = status?.total_matches || 0;
  const totalAssets = status?.total_assets || 0;
  const threatLevel = totalScans > 0 ? Math.round((totalMatches / totalScans) * 100) : 0;

  const gaugeOffset = 251 - (251 * Math.min(threatLevel, 100)) / 100;

  const threatLabel = threatLevel < 20 ? 'SAFE' : threatLevel < 50 ? 'MODERATE' : threatLevel < 80 ? 'HIGH' : 'CRITICAL';
  const threatColor = threatLevel < 20 ? '#22c55e' : threatLevel < 50 ? '#f59e0b' : '#ef4444';

  const formatTime = (iso) => {
    if (!iso) return '';
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div>
      <h1 className="page-title">Command Center</h1>

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-icon">🛡️</div>
          <div className="stat-value">{totalAssets}</div>
          <div className="stat-label">Assets Protected</div>
        </div>
        <div className="stat-card">
          <div className="stat-icon">🔬</div>
          <div className="stat-value">{totalScans}</div>
          <div className="stat-label">Scans Performed</div>
        </div>
        <div className="stat-card accent">
          <div className="stat-icon">🚨</div>
          <div className="stat-value">{totalMatches}</div>
          <div className="stat-label">Infringements</div>
        </div>
      </div>

      <div className="gauge-row">
        <div className="skeu-card">
          <h2 className="section-title">Threat Level</h2>
          <svg viewBox="0 0 200 120" className="gauge-svg">
            <defs>
              <linearGradient id="gaugeGrad" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#22c55e" />
                <stop offset="50%" stopColor="#f59e0b" />
                <stop offset="100%" stopColor="#ef4444" />
              </linearGradient>
            </defs>
            <path d="M 20 100 A 80 80 0 0 1 180 100" fill="none" stroke="#1a1a2e" strokeWidth="14" strokeLinecap="round" />
            <path d="M 20 100 A 80 80 0 0 1 180 100" fill="none" stroke="url(#gaugeGrad)" strokeWidth="14" strokeLinecap="round"
              strokeDasharray="251" strokeDashoffset={gaugeOffset}
              style={{ transition: 'stroke-dashoffset 1s ease' }} />
            <text x="100" y="90" textAnchor="middle" className="gauge-text">{threatLevel}</text>
            <text x="100" y="112" textAnchor="middle" className="gauge-sublabel" fill={threatColor}>{threatLabel}</text>
          </svg>
        </div>

        <div className="skeu-card">
          <h2 className="section-title">Recent Activity</h2>
          <div className="activity-feed">
            {history.length === 0 ? (
              <div className="activity-empty">No scan activity yet. Run a scan to see results here.</div>
            ) : (
              history.slice(0, 12).map((item, i) => (
                <div key={i} className="activity-item">
                  <div className={`activity-dot ${item.match ? 'match' : 'clean'}`} />
                  <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {item.match ? '🚨 Match' : '✅ Clean'} — {item.reason?.slice(0, 60) || 'Scanned'}
                  </span>
                  <span className="activity-time">{formatTime(item.scanned_at)}</span>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
