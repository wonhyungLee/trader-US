import axios from 'axios';

// Flask가 동일 오리진으로 dist를 서빙하는 구성을 기본으로 합니다.
// 필요하면 .env에 VITE_API_BASE="http://..." 로 분리 구성 가능
const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE || '' });

export const fetchUniverse = (sector) =>
  api.get('/universe', { params: sector ? { sector } : {} }).then(r => r.data);

export const fetchSectors = () => api.get('/sectors').then(r => r.data);

export const fetchPrices = (code, days = 180) =>
  api.get('/prices', { params: { code, days } }).then(r => r.data);

export const fetchSelection = () => api.get('/selection').then(r => r.data);
export const fetchStatus = () => api.get('/status').then(r => r.data);
export const fetchJobs = (limit = 20) => api.get('/jobs', { params: { limit } }).then(r => r.data);
export const fetchStrategy = () => api.get('/strategy').then(r => r.data);
