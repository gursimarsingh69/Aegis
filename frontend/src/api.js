import axios from 'axios';

const API = axios.create({
  baseURL: 'http://localhost:8000',
  timeout: 120000, // 2 minutes for AI scans
});

export const getStatus = () => API.get('/status');
export const getHistory = () => API.get('/history');
export const getAssets = () => API.get('/assets');
export const deleteAsset = (id) => API.delete(`/assets/${id}`);
export const getAssetImageUrl = (id) => `http://localhost:8000/assets/${id}/image`;
export const getSuspiciousImageUrl = (filename) => `http://localhost:8000/suspicious/${filename}`;

export const registerAsset = (file) => {
  const form = new FormData();
  form.append('file', file);
  return API.post('/register', form);
};

export const scanImage = (file) => {
  const form = new FormData();
  form.append('file', file);
  return API.post('/scan', form);
};

export const markFalsePositive = (scanId) => API.patch(`/history/${scanId}`);

export default API;
