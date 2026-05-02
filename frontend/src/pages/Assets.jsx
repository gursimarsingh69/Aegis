import { useState, useEffect, useRef } from 'react';
import { getAssets, registerAsset, deleteAsset, getAssetImageUrl } from '../api';

export default function Assets({ addToast }) {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const fileRef = useRef();

  const load = async () => {
    try {
      const res = await getAssets();
      setAssets(res.data);
    } catch { /* offline */ }
  };

  useEffect(() => { load(); }, []);

  const handleFiles = async (files) => {
    setLoading(true);
    for (const file of files) {
      try {
        await registerAsset(file);
        addToast(`✅ Registered: ${file.name}`, 'success');
      } catch {
        addToast(`❌ Failed: ${file.name}`, 'error');
      }
    }
    setLoading(false);
    load();
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    handleFiles([...e.dataTransfer.files]);
  };

  const handleDelete = async (id) => {
    try {
      await deleteAsset(id);
      addToast('🗑️ Asset deleted', 'info');
      load();
    } catch {
      addToast('❌ Delete failed', 'error');
    }
  };

  return (
    <div>
      <h1 className="page-title">Asset Library</h1>

      <div className="asset-toolbar">
        <div
          className={`upload-zone ${dragOver ? 'drag-over' : ''}`}
          onClick={() => fileRef.current?.click()}
          onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
          onDragLeave={() => setDragOver(false)}
          onDrop={handleDrop}
          style={{ flex: 1 }}
        >
          <span className="upload-icon">📤</span>
          <span className="upload-text">
            {loading ? 'Uploading...' : <>Drop images here or <strong>click to upload</strong></>}
          </span>
          <input
            ref={fileRef}
            type="file"
            multiple
            accept="image/*"
            style={{ display: 'none' }}
            onChange={(e) => handleFiles([...e.target.files])}
          />
        </div>
      </div>

      <div className="asset-count" style={{ marginBottom: 16 }}>
        {assets.length} asset{assets.length !== 1 ? 's' : ''} registered
      </div>

      <div className="asset-grid">
        {assets.map((asset) => (
          <div key={asset.asset_id} className="asset-thumb">
            <img
              src={getAssetImageUrl(asset.asset_id)}
              alt={asset.filename || asset.asset_id}
              loading="lazy"
            />
            <button className="delete-btn" onClick={() => handleDelete(asset.asset_id)}>✕</button>
            <div className="asset-info">
              {asset.width}×{asset.height}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
