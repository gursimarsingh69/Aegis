import { useState, useEffect, useRef } from 'react';
import { scanImage, getHistory, getAssetImageUrl, getSuspiciousImageUrl } from '../api';

export default function Scan({ addToast }) {
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [scanning, setScanning] = useState(false);
  const [result, setResult] = useState(null);
  const [recentScans, setRecentScans] = useState([]);
  const [dragOver, setDragOver] = useState(false);
  const fileRef = useRef();

  useEffect(() => {
    loadHistory();
  }, []);

  const loadHistory = async () => {
    try {
      const res = await getHistory();
      setRecentScans(res.data.filter(h => h.source === 'manual').slice(0, 5));
    } catch { /* offline */ }
  };

  const handleFile = (f) => {
    setFile(f);
    setResult(null);
    const reader = new FileReader();
    reader.onload = (e) => setPreview(e.target.result);
    reader.readAsDataURL(f);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
  };

  const handleScan = async () => {
    if (!file) return;
    setScanning(true);
    try {
      const res = await scanImage(file);
      setResult(res.data);
      addToast(res.data.match ? '🚨 Infringement detected!' : '✅ Image is clean', res.data.match ? 'error' : 'success');
      loadHistory();
    } catch {
      addToast('❌ Scan failed — is the engine running?', 'error');
    }
    setScanning(false);
  };

  return (
    <div>
      <h1 className="page-title">Manual Scan</h1>

      <div className="scan-layout">
        {/* Left: Upload */}
        <div className="skeu-card">
          {!preview ? (
            <div
              className={`upload-zone ${dragOver ? 'drag-over' : ''}`}
              onClick={() => fileRef.current?.click()}
              onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={handleDrop}
            >
              <span className="upload-icon">🔍</span>
              <span className="upload-text">Drop a suspicious image to scan</span>
              <input
                ref={fileRef}
                type="file"
                accept="image/*"
                style={{ display: 'none' }}
                onChange={(e) => e.target.files[0] && handleFile(e.target.files[0])}
              />
            </div>
          ) : (
            <>
              <img src={preview} alt="Preview" className="scan-preview" />
              <button className="btn-scan" onClick={handleScan} disabled={scanning}>
                {scanning ? 'Analyzing…' : '🔬 Analyze with AI'}
              </button>
              <button
                className="btn-export"
                style={{ width: '100%', marginTop: 8 }}
                onClick={() => { setFile(null); setPreview(null); setResult(null); }}
              >
                Clear
              </button>
            </>
          )}
        </div>

        {/* Right: Result */}
        <div className="skeu-card" style={{ position: 'relative', minHeight: 300 }}>
          <h2 className="section-title">AI Verdict</h2>

          {scanning ? (
            <div className="inline-loader">
              <div className="spinner" />
              <p>Analyzing with Gemini AI…</p>
            </div>
          ) : !result ? (
            <div className="activity-empty">
              Upload and scan an image to see the AI verdict here.
            </div>
          ) : (
            <>
              <div className={`verdict-badge ${result.match ? 'match' : 'clean'}`}>
                {result.match ? (result.confidence >= 99 ? '✅ VERIFIED ORIGINAL' : '🚨 ALTERATION DETECTED') : '✅ CLEAN — NO MATCH'}
              </div>

              <div className="metrics-row">
                <div className="metric-item">
                  <div className="confidence-label">Alteration Index</div>
                  <div className="confidence-bar-wrap">
                    <div className="confidence-bar secondary" style={{ width: `${result.alteration || 0}%` }} />
                    <span className="confidence-text">{result.alteration || 0}%</span>
                  </div>
                </div>
                
                <div className="metric-item">
                  <div className="confidence-label">Match Confidence</div>
                  <div className="confidence-bar-wrap">
                    <div className="confidence-bar" style={{ width: `${result.confidence || 0}%` }} />
                    <span className="confidence-text">{result.confidence || 0}%</span>
                  </div>
                </div>
              </div>

              <div className="verdict-reason">{result.reason}</div>

              {result.modifications?.length > 0 && (
                <div className="mod-tags">
                  {result.modifications.map((mod, i) => (
                    <span key={i} className="mod-tag">{mod}</span>
                  ))}
                </div>
              )}

              {result.match && result.matched_asset && (
                <div>
                  <h3 className="section-title" style={{ marginTop: 16 }}>Side-by-Side</h3>
                  <div className="comparison-images">
                    <div className="comp-img-wrap">
                      <img src={preview} alt="Suspicious" />
                      <span className="comp-label">Suspicious</span>
                    </div>
                    <div className="comp-vs">VS</div>
                    <div className="comp-img-wrap">
                      <img src={getAssetImageUrl(result.matched_asset)} alt="Original" />
                      <span className="comp-label">Original</span>
                    </div>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* Recent manual scans */}
      <div className="skeu-card">
        <h2 className="section-title">Recent Manual Scans</h2>
        {recentScans.length === 0 ? (
          <div className="activity-empty">No manual scans yet.</div>
        ) : (
          recentScans.map((s, i) => (
            <div key={i} className="scan-history-item">
              <div className={`activity-dot ${s.match ? 'match' : 'clean'}`} />
              <span>{s.match ? '🚨' : '✅'} {s.original_filename}</span>
              <span style={{ marginLeft: 'auto', color: 'var(--text-muted)', fontSize: 11 }}>
                {s.confidence}% — {new Date(s.scanned_at).toLocaleString()}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
