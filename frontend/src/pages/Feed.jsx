import { useState, useEffect } from 'react';
import { getHistory, markFalsePositive, getSuspiciousImageUrl, getAssetImageUrl } from '../api';

export default function Feed({ addToast }) {
  const [history, setHistory] = useState([]);
  const [platform, setPlatform] = useState('all');
  const [minConf, setMinConf] = useState(0);

  const load = async () => {
    try {
      const res = await getHistory();
      setHistory(res.data);
    } catch { /* offline */ }
  };

  useEffect(() => {
    load();
    const interval = setInterval(load, 10000);
    return () => clearInterval(interval);
  }, []);

  const filtered = history.filter(h => {
    if (platform !== 'all' && h.source !== platform) return false;
    if (h.confidence < minConf) return false;
    if (h.false_positive) return false;
    return true;
  });

  const confClass = (c) => c >= 70 ? 'high' : c >= 40 ? 'medium' : 'low';

  const handleDismiss = async (id) => {
    try {
      await markFalsePositive(id);
      addToast('Marked as false positive', 'info');
      load();
    } catch {
      addToast('Failed to update', 'error');
    }
  };

  const exportCSV = () => {
    const rows = [['Date', 'Source', 'Match', 'Confidence', 'Reason', 'Modifications', 'Post URL']];
    filtered.forEach(h => {
      const reason = (h.reason || '').replaceAll('"', "'");
      const mods = (h.modifications || []).join(', ');
      rows.push([
        h.scanned_at, h.source, h.match, h.confidence,
        `"${reason}"`,
        `"${mods}"`,
        h.post_url || ''
      ]);
    });
    const csv = rows.map(r => r.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `aegis_report_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    addToast('📥 Report exported', 'success');
  };

  return (
    <div>
      <h1 className="page-title">Infringement Feed</h1>

      <div className="feed-toolbar">
        <div className="feed-filters">
          <select className="skeu-select" value={platform} onChange={(e) => setPlatform(e.target.value)}>
            <option value="all">All Platforms</option>
            <option value="reddit">Reddit</option>
            <option value="twitter">Twitter / X</option>
            <option value="manual">Manual</option>
          </select>
          <div className="confidence-filter">
            <label>Min Confidence:</label>
            <input type="range" min="0" max="100" value={minConf} onChange={(e) => setMinConf(Number(e.target.value))} />
            <span>{minConf}%</span>
          </div>
        </div>
        <button className="btn-export" onClick={exportCSV}>📥 Export CSV</button>
      </div>

      {filtered.length === 0 ? (
        <div className="skeu-card">
          <div className="activity-empty">
            No infringements match your filters. Try lowering the confidence threshold.
          </div>
        </div>
      ) : (
        filtered.map((item) => (
          <div key={item.id} className={`feed-card ${item.match ? 'is-match' : 'is-clean'}`}>
            <img
              className="feed-thumb"
              src={getSuspiciousImageUrl(item.suspicious_file)}
              alt="Suspicious"
              onError={(e) => { e.target.style.display = 'none'; }}
            />
            <div className="feed-info">
              <div className="feed-source">
                {item.source === 'reddit' ? '🔴 Reddit' : item.source === 'twitter' ? '🐦 Twitter' : '📎 Manual'}
                {' · '}{item.match ? 'INFRINGEMENT' : 'CLEAN'}
              </div>
              <div className="feed-reason">{item.reason}</div>
              <div className="feed-meta">
                {item.modifications?.length > 0 && (
                  <div className="mod-tags" style={{ marginTop: 0 }}>
                    {item.modifications.map((m, i) => <span key={i} className="mod-tag">{m}</span>)}
                  </div>
                )}
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 6 }}>
                {new Date(item.scanned_at).toLocaleString()}
              </div>
            </div>
            <div className="feed-actions">
              <div className={`feed-conf ${confClass(item.confidence)}`}>{item.confidence}%</div>
              {item.post_url && (
                <a href={item.post_url} target="_blank" rel="noopener noreferrer" className="btn-sm">
                  View Post ↗
                </a>
              )}
              {item.match && (
                <button className="btn-sm danger" onClick={() => handleDismiss(item.id)}>
                  False Positive
                </button>
              )}
            </div>
          </div>
        ))
      )}
    </div>
  );
}
