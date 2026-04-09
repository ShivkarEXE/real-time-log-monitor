/* ──────────────────────────────────────────
   Spark Log Dashboard — app.js
   Polls http://localhost:5000/api/metrics
   every 2 s and updates all UI components.
   ────────────────────────────────────────── */

// Relative URL — works whether opened via Flask (localhost:5000)
// or any other port, with zero CORS issues.
const API_URL    = '/api/metrics';
const POLL_MS    = 2000;
const MAX_EVENTS = 50;       // rows in event stream
const HISTORY_N  = 20;       // bars in throughput chart

// ── State ────────────────────────────────
let batchNum         = 0;
let prevTotal        = 0;
let throughputHistory = Array(HISTORY_N).fill(0);
let activeFilter     = 'all';   // 'all' | 'errors' | 'warnings'
let allEvents        = [];
let connected        = false;

// ── DOM refs ─────────────────────────────
const $ = id => document.getElementById(id);

const dom = {
  totalLogs    : $('totalLogs'),
  logsPerSec   : $('logsPerSec'),
  errorCount   : $('errorCount'),
  errorPct     : $('errorPct'),
  warningCount : $('warningCount'),
  warningPct   : $('warningPct'),
  activeServices: $('activeServices'),
  throughput   : $('throughputChart'),
  levelBars    : $('logLevelBars'),
  errorBars    : $('serviceErrorBars'),
  streamBody   : $('eventStreamBody'),
  batchId      : $('batchId'),
  clock        : $('live-clock'),
  connBanner   : $('conn-banner'),
};

// ── Clock ────────────────────────────────
function tickClock() {
  if (!dom.clock) return;
  dom.clock.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
}
setInterval(tickClock, 1000);
tickClock();

// ── Throughput chart ─────────────────────
function buildThroughputChart() {
  dom.throughput.innerHTML = '';
  throughputHistory.forEach((v, i) => {
    const wrap  = document.createElement('div');
    wrap.className = 't-bar-wrap';
    wrap.id = `tbar-${i}`;

    const bar  = document.createElement('div');
    bar.className = 't-bar' + (i === HISTORY_N - 1 ? ' active' : '');
    bar.style.height = '0px';

    const lbl  = document.createElement('div');
    lbl.className = 't-bar-label';
    lbl.textContent = i === HISTORY_N - 1 ? 'now' : `-${HISTORY_N - 1 - i}s`;

    wrap.appendChild(bar);
    wrap.appendChild(lbl);
    dom.throughput.appendChild(wrap);
  });
}

function updateThroughputChart() {
  const max = Math.max(...throughputHistory, 1);
  throughputHistory.forEach((v, i) => {
    const wrap = dom.throughput.children[i];
    if (!wrap) return;
    const bar  = wrap.querySelector('.t-bar');
    const pct  = v / max;
    bar.style.height = Math.max(4, Math.round(pct * 72)) + 'px';
    bar.title = `${v} logs`;
  });
}

// ── Bar charts ────────────────────────────
const LEVEL_COLORS = {
  INFO : 'var(--blue)',
  WARN : 'var(--orange)',
  ERROR: 'var(--red)',
  DEBUG: 'var(--purple)',
};

const SVC_COLORS = [
  'var(--red)', 'var(--orange)', 'var(--blue)', 'var(--purple)', 'var(--cyan)'
];

function renderBarChart(container, rows, colorFn) {
  if (!rows.length) { container.innerHTML = '<p style="color:var(--muted);font-size:11px">No data yet…</p>'; return; }
  const max = Math.max(...rows.map(r => r.count), 1);
  container.innerHTML = rows.map((r, i) => `
    <div class="bar-row">
      <div class="bar-row-label" title="${r.label}">${r.label}</div>
      <div class="bar-track">
        <div class="bar-fill" style="width:${(r.count / max * 100).toFixed(1)}%;background:${colorFn(r.label, i)}"></div>
      </div>
      <div class="bar-row-count">${r.count.toLocaleString()}</div>
    </div>
  `).join('');
}

// ── Event stream table ────────────────────
function levelClass(lvl) {
  return 'level-' + (lvl || 'INFO').trim().toUpperCase();
}

function renderStream(events) {
  const filtered = events.filter(ev => {
    if (activeFilter === 'errors')   return ev.level === 'ERROR';
    if (activeFilter === 'warnings') return ev.level === 'WARN';
    return true;
  });

  // Build rows — newest first
  dom.streamBody.innerHTML = filtered.slice(0, MAX_EVENTS).map((ev, idx) => {
    const isNew = idx === 0 && batchNum > 1;
    const rowCls = isNew && ev.level === 'ERROR' ? 'row-error' : '';
    return `
      <tr class="${rowCls}">
        <td class="ts-cell">${ev.timestamp}</td>
        <td><span class="level-badge ${levelClass(ev.level)}">${ev.level}</span></td>
        <td class="svc-cell">${ev.service}</td>
        <td class="msg-cell">${ev.message}</td>
      </tr>`;
  }).join('');
}

// ── Main render ───────────────────────────
function render(data) {
  const total    = data.total_logs   || 0;
  const levels   = data.levels       || {};
  const errors   = data.errors       || [];
  const svcCount = data.active_services || 0;
  const events   = data.events       || [];

  // KPIs
  const errCount  = levels['ERROR'] || 0;
  const warnCount = levels['WARN']  || 0;
  const delta     = Math.max(0, total - prevTotal);
  prevTotal = total;

  dom.totalLogs.textContent     = total.toLocaleString();
  dom.logsPerSec.textContent    = `${(delta / (POLL_MS / 1000)).toFixed(1)} logs/s`;
  dom.errorCount.textContent    = errCount.toLocaleString();
  dom.errorPct.textContent      = total ? `${(errCount / total * 100).toFixed(1)}% of total` : '0% of total';
  dom.warningCount.textContent  = warnCount.toLocaleString();
  dom.warningPct.textContent    = total ? `${(warnCount / total * 100).toFixed(1)}% of total` : '0% of total';
  dom.activeServices.textContent = svcCount;

  // Throughput history — shift in new delta
  throughputHistory.shift();
  throughputHistory.push(delta);
  updateThroughputChart();

  // Level breakdown bar chart
  const levelRows = Object.entries(levels)
    .sort((a, b) => b[1] - a[1])
    .map(([label, count]) => ({ label, count }));
  renderBarChart(dom.levelBars, levelRows, (lbl) => LEVEL_COLORS[lbl] || 'var(--muted)');

  // Errors per service bar chart
  const errorRows = errors.map(e => ({ label: e.service, count: e.count }));
  renderBarChart(dom.errorBars, errorRows, (_, i) => SVC_COLORS[i % SVC_COLORS.length]);

  // Event stream
  allEvents = events;
  renderStream(allEvents);

  batchNum++;
  dom.batchId.textContent = batchNum;
}

// ── Polling ───────────────────────────────
async function poll() {
  try {
    const res  = await fetch(API_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    if (!connected) {
      connected = true;
      if (dom.connBanner) dom.connBanner.classList.remove('visible');
    }

    render(data);
  } catch (err) {
    connected = false;
    if (dom.connBanner) dom.connBanner.classList.add('visible');
    console.warn('[Dashboard] Cannot reach API:', err.message);
  }
}

// ── Filter buttons ────────────────────────
function initControls() {
  const btns = document.querySelectorAll('.controls-btn-group .btn');
  const filterMap = ['all', 'errors', 'warnings'];

  btns.forEach((btn, i) => {
    btn.addEventListener('click', () => {
      btns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeFilter = filterMap[i];
      renderStream(allEvents);
    });
  });
}

// ── Init ──────────────────────────────────
function init() {
  buildThroughputChart();
  initControls();
  poll();
  setInterval(poll, POLL_MS);
}

document.addEventListener('DOMContentLoaded', init);
