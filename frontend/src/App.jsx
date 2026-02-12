import { useEffect, useMemo, useState } from 'react';
import {
  fetchUniverse,
  fetchSectors,
  fetchPrices,
  fetchSelection,
  fetchStatus,
} from './api';
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
} from 'recharts';
import './App.css';

const asArray = (v) => (Array.isArray(v) ? v : []);

const formatNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return num.toLocaleString();
};

const formatCurrency = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  try {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(num);
  } catch {
    return `$${formatNumber(num)}`;
  }
};

const formatPct = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`;
};

function App() {
  const [universe, setUniverse] = useState([]);
  const [sectors, setSectors] = useState([]);
  const [selection, setSelection] = useState({ date: null, candidates: [], summary: {} });
  const [status, setStatus] = useState(null);

  const [groupFilter, setGroupFilter] = useState('NASDAQ100');
  const [sectorFilter, setSectorFilter] = useState('ALL');
  const [search, setSearch] = useState('');

  const [selected, setSelected] = useState(null);
  const [prices, setPrices] = useState([]);
  const [days, setDays] = useState(240);
  const [modalOpen, setModalOpen] = useState(false);

  const marketFilter = useMemo(() => {
    if (groupFilter === 'ALL') return 'ALL';
    return groupFilter === 'NASDAQ100' ? 'NASDAQ' : 'SP500';
  }, [groupFilter]);

  const sectorOptions = useMemo(() => {
    const list = asArray(sectors);
    if (marketFilter === 'ALL') return list;
    return list.filter((s) => String(s.market || '').toUpperCase().includes(marketFilter));
  }, [sectors, marketFilter]);

  const reload = async () => {
    try {
      const [u, s, sel, st] = await Promise.all([
        fetchUniverse(sectorFilter !== 'ALL' ? sectorFilter : undefined),
        fetchSectors(),
        fetchSelection(),
        fetchStatus(),
      ]);
      setUniverse(asArray(u));
      setSectors(asArray(s));
      setSelection(sel && typeof sel === 'object' ? sel : { date: null, candidates: [], summary: {} });
      setStatus(st && typeof st === 'object' ? st : null);
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    reload();
    const id = setInterval(() => reload(), 30000);
    return () => clearInterval(id);
  }, [sectorFilter]);

  useEffect(() => {
    if (!selected) return;
    fetchPrices(selected.code, days)
      .then((data) => setPrices(asArray(data)))
      .catch(() => setPrices([]));
  }, [selected, days]);

  useEffect(() => {
    if (!modalOpen) {
      setSelected(null);
      setPrices([]);
    }
  }, [modalOpen]);

  const universeFiltered = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    return asArray(universe)
      .filter((row) => (groupFilter === 'ALL' ? true : row.group === groupFilter))
      .filter((row) => (sectorFilter === 'ALL' ? true : String(row.sector_name || 'UNKNOWN') === sectorFilter))
      .filter((row) => {
        if (!keyword) return true;
        return String(row.code || '').toLowerCase().includes(keyword)
          || String(row.name || '').toLowerCase().includes(keyword)
          || String(row.sector_name || '').toLowerCase().includes(keyword);
      });
  }, [universe, groupFilter, sectorFilter, search]);

  const candidates = useMemo(() => asArray(selection?.candidates), [selection]);
  const chartData = useMemo(() => [...asArray(prices)].reverse(), [prices]);
  const latest = chartData.length ? chartData[chartData.length - 1] : null;
  const prev = chartData.length > 1 ? chartData[chartData.length - 2] : null;
  const deltaPct = latest && prev && prev.close ? ((latest.close - prev.close) / prev.close) * 100 : null;

  return (
    <div className="page">
      <header className="header">
        <div>
          <div className="title">BNF Viewer (US)</div>
          <div className="subtitle">NASDAQ100 + S&P500 · 자동매매/잔고 기능 제거</div>
        </div>
        <div className="status">
          {status?.daily_price?.date?.max ? (
            <div>일봉 최신: <b>{status.daily_price.date.max}</b> · 유니버스: <b>{status.universe.total}</b></div>
          ) : (
            <div>데이터를 준비해 주세요 (universe_loader / bulk_loader)</div>
          )}
        </div>
      </header>

      <div className="grid">
        <section className="panel">
          <div className="panelTitle">필터</div>
          <div className="controls">
            <label>
              그룹
              <select value={groupFilter} onChange={(e) => setGroupFilter(e.target.value)}>
                <option value="NASDAQ100">NASDAQ100</option>
                <option value="SP500">S&P500</option>
                <option value="ALL">전체</option>
              </select>
            </label>
            <label>
              섹터
              <select value={sectorFilter} onChange={(e) => setSectorFilter(e.target.value)}>
                <option value="ALL">전체</option>
                {sectorOptions.map((s) => (
                  <option key={`${s.market}-${s.sector_name}`} value={s.sector_name}>{s.sector_name} ({s.count})</option>
                ))}
              </select>
            </label>
            <label>
              검색
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="티커/회사/섹터" />
            </label>
          </div>
        </section>

        <section className="panel">
          <div className="panelTitle">매수 후보 (Selection)</div>
          <div className="hint">선정일: {selection?.date || '-'} · 후보: {candidates.length}</div>
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>R</th><th>코드</th><th>종목</th><th>시장</th><th>괴리율</th><th>거래대금</th><th>종가</th>
                </tr>
              </thead>
              <tbody>
                {candidates.length === 0 ? (
                  <tr><td colSpan="7" className="empty">후보가 없습니다 (데이터/전략 조건 확인)</td></tr>
                ) : candidates.map((r) => (
                  <tr key={`${r.code}-${r.rank}`} onClick={() => { setSelected({ code: r.code, name: r.name, market: r.market }); setModalOpen(true); }}>
                    <td>{r.rank}</td>
                    <td className="mono">{r.code}</td>
                    <td>{r.name}</td>
                    <td>{r.market}</td>
                    <td>{formatPct((Number(r.disparity) || 0) * 100)}</td>
                    <td>{formatCurrency(r.amount)}</td>
                    <td>{formatCurrency(r.close)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel span2">
          <div className="panelTitle">유니버스</div>
          <div className="hint">{universeFiltered.length} / {asArray(universe).length} 종목</div>
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>코드</th><th>종목</th><th>그룹</th><th>시장</th><th>섹터</th>
                </tr>
              </thead>
              <tbody>
                {universeFiltered.length === 0 ? (
                  <tr><td colSpan="5" className="empty">표시할 종목이 없습니다</td></tr>
                ) : universeFiltered.slice(0, 500).map((r) => (
                  <tr key={r.code} onClick={() => { setSelected({ code: r.code, name: r.name, market: r.market }); setModalOpen(true); }}>
                    <td className="mono">{r.code}</td>
                    <td>{r.name}</td>
                    <td>{r.group}</td>
                    <td>{r.market}</td>
                    <td>{r.sector_name || 'UNKNOWN'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {universeFiltered.length > 500 && <div className="hint">※ 성능을 위해 500개까지만 표시</div>}
        </section>
      </div>

      {modalOpen && selected && (
        <div className="modal" onClick={() => setModalOpen(false)}>
          <div className="modalBody" onClick={(e) => e.stopPropagation()}>
            <div className="modalHeader">
              <div>
                <div className="modalTitle">{selected.name} <span className="mono">({selected.code})</span></div>
                <div className="modalSub">{selected.market} · {latest?.date ? `최신 ${latest.date}` : '데이터 없음'} · {latest?.close ? `종가 ${formatCurrency(latest.close)}` : ''} {deltaPct !== null ? `(${formatPct(deltaPct)})` : ''}</div>
              </div>
              <div className="modalControls">
                <label>기간
                  <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
                    <option value={120}>120일</option>
                    <option value={240}>240일</option>
                    <option value={500}>500일</option>
                  </select>
                </label>
                <button onClick={() => setModalOpen(false)}>닫기</button>
              </div>
            </div>
            <div className="chartArea">
              {chartData.length === 0 ? (
                <div className="empty">일봉 데이터가 없습니다. bulk_loader로 적재해 주세요.</div>
              ) : (
                <ResponsiveContainer width="100%" height={360}>
                  <ComposedChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" hide={true} />
                    <YAxis yAxisId="price" domain={['auto', 'auto']} />
                    <YAxis yAxisId="vol" orientation="right" hide={true} />
                    <Tooltip />
                    <Legend />
                    <Area yAxisId="price" type="monotone" dataKey="close" name="Close" fillOpacity={0.15} />
                    <Line yAxisId="price" type="monotone" dataKey="ma25" name="MA25" dot={false} />
                    <Bar yAxisId="vol" dataKey="volume" name="Volume" opacity={0.25} />
                  </ComposedChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;