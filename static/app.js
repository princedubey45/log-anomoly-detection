/* ── STATE ─────────────────────────────────────────────── */
const state = {
  currentPage : 1,
  totalPages  : 1,
  liveCount   : 0,
  streaming   : false,
  eventSource : null,
  cpuHistory  : [],
  memHistory  : [],
  timeLabels  : [],
  maxHistory  : 60,
};

/* ── CHARTS ────────────────────────────────────────────── */
let cpuChart, memChart, donutChart;

function initCharts() {
  const gridColor  = 'rgba(46,50,80,0.6)';
  const tickColor  = '#8892b0';
  const chartFont  = { family: "'Segoe UI', system-ui, sans-serif", size: 11 };

  const lineDefaults = (label, color) => ({
    type: 'line',
    data: {
      labels  : [],
      datasets: [{
        label,
        data            : [],
        borderColor     : color,
        backgroundColor : color + '22',
        borderWidth     : 2,
        pointRadius     : 0,
        fill            : true,
        tension         : 0.4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation : { duration: 0 },
      plugins   : { legend: { display: false } },
      scales    : {
        x: {
          ticks : { color: tickColor, font: chartFont, maxTicksLimit: 6, maxRotation: 0 },
          grid  : { color: gridColor },
        },
        y: {
          min   : 0,
          max   : 100,
          ticks : { color: tickColor, font: chartFont, callback: v => v + '%' },
          grid  : { color: gridColor },
        },
      },
    },
  });

  cpuChart = new Chart(
    document.getElementById('chart-cpu').getContext('2d'),
    lineDefaults('CPU Max %', '#3b82f6')
  );
  memChart = new Chart(
    document.getElementById('chart-mem').getContext('2d'),
    lineDefaults('Memory %', '#a855f7')
  );

  donutChart = new Chart(
    document.getElementById('chart-donut').getContext('2d'),
    {
      type: 'doughnut',
      data: {
        labels  : ['Normal', 'CPU Spike', 'Mem Spike'],
        datasets: [{
          data           : [1, 0, 0],
          backgroundColor: ['#22c55e33', '#f59e0b33', '#a855f733'],
          borderColor    : ['#22c55e',   '#f59e0b',   '#a855f7'],
          borderWidth    : 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position : 'bottom',
            labels   : { color: tickColor, font: chartFont, padding: 12, boxWidth: 12 },
          },
        },
        cutout: '65%',
      },
    }
  );
}

function pushChartPoint(time, cpu, mem) {
  const label = time ? time.slice(11, 19) : new Date().toLocaleTimeString();
  state.timeLabels.push(label);
  state.cpuHistory.push(cpu);
  state.memHistory.push(mem);

  if (state.timeLabels.length > state.maxHistory) {
    state.timeLabels.shift();
    state.cpuHistory.shift();
    state.memHistory.shift();
  }

  cpuChart.data.labels         = [...state.timeLabels];
  cpuChart.data.datasets[0].data = [...state.cpuHistory];
  cpuChart.update('none');

  memChart.data.labels         = [...state.timeLabels];
  memChart.data.datasets[0].data = [...state.memHistory];
  memChart.update('none');
}

function updateDonut(normal, cpuSpike, memSpike) {
  donutChart.data.datasets[0].data = [normal, cpuSpike, memSpike];
  donutChart.update();
}

/* ── STATS ─────────────────────────────────────────────── */
async function loadStats() {
  try {
    const res  = await fetch('/api/stats');
    if (!res.ok) return;
    const data = await res.json();

    setText('stat-total',  data.total);
    setText('stat-normal', data.normals);
    setText('stat-anomaly',data.anomalies);
    setText('stat-rate',   data.anomaly_pct + '%');
    setText('stat-cpu',    data.cpu_avg + '%');
    setText('stat-mem',    data.mem_avg + '%');

    const modelBadge = document.getElementById('model-badge');
    if (data.model_loaded) {
      modelBadge.className = 'badge badge-green';
      modelBadge.textContent = 'Model: Ready';
    } else {
      modelBadge.className = 'badge badge-red';
      modelBadge.textContent = 'Model: Not Loaded';
    }

    // Update donut
    const types = data.anomaly_types || {};
    updateDonut(data.normals, types['CPU_SPIKE'] || 0, types['MEM_SPIKE'] || 0);

  } catch (e) {
    console.error('Stats error:', e);
  }
}

/* ── LOG TABLE ─────────────────────────────────────────── */
async function loadLogs(page = 1) {
  state.currentPage = page;
  const filter = document.getElementById('log-filter').value;

  try {
    const res  = await fetch(`/api/logs?page=${page}&per_page=20&filter=${filter}`);
    if (!res.ok) { renderTableError('No data available.'); return; }
    const data = await res.json();

    state.totalPages = data.pages;
    renderTable(data.records);
    renderPagination(data.page, data.pages, data.total);
  } catch (e) {
    renderTableError('Failed to load logs.');
  }
}

function renderTable(records) {
  const tbody = document.getElementById('log-tbody');
  if (!records.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="empty-row">No records found.</td></tr>';
    return;
  }

  const search = document.getElementById('search-box').value.toLowerCase();

  tbody.innerHTML = records
    .filter(r => !search || (r.top_proc_name || '').toLowerCase().includes(search))
    .map(r => {
      const isAnom = r.is_anomaly === 1;
      const atype  = r.anomaly_type || 'none';
      const atag   = atype === 'CPU_SPIKE' ? 'tag-cpu'
                   : atype === 'MEM_SPIKE' ? 'tag-mem'
                   : 'tag-none';
      return `
        <tr class="${isAnom ? 'row-anomaly' : ''}">
          <td style="font-family:monospace;color:var(--muted)">${r.timestamp || '—'}</td>
          <td><span class="tag ${isAnom ? 'tag-anomaly' : 'tag-normal'}">${isAnom ? 'ANOMALY' : 'NORMAL'}</span></td>
          <td>${fmt(r.cpu_max_pct)}%</td>
          <td>${fmt(r.mem_used_pct)}%</td>
          <td>${fmt(r.disk_read_mb)} MB</td>
          <td>${fmt(r.disk_write_mb)} MB</td>
          <td>${fmt(r.net_sent_mb)} MB</td>
          <td>${fmt(r.net_recv_mb)} MB</td>
          <td style="color:var(--purple);font-family:monospace">${r.top_proc_name || '—'}</td>
          <td><span class="tag ${atag}">${atype}</span></td>
          <td style="font-family:monospace;color:var(--muted)">${r.score !== undefined ? r.score : '—'}</td>
        </tr>`;
    }).join('');
}

function renderTableError(msg) {
  document.getElementById('log-tbody').innerHTML =
    `<tr><td colspan="11" class="empty-row">${msg}</td></tr>`;
}

function renderPagination(page, pages, total) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.innerHTML = `<span class="page-info">${total} records</span>`; return; }

  let html = `<span class="page-info">${total} records</span>`;
  html += `<button class="page-btn" onclick="loadLogs(${page-1})" ${page<=1?'disabled':''}>‹</button>`;

  const start = Math.max(1, page - 2);
  const end   = Math.min(pages, page + 2);
  for (let i = start; i <= end; i++) {
    html += `<button class="page-btn ${i===page?'active':''}" onclick="loadLogs(${i})">${i}</button>`;
  }
  html += `<button class="page-btn" onclick="loadLogs(${page+1})" ${page>=pages?'disabled':''}>›</button>`;
  el.innerHTML = html;
}

function filterTable() {
  loadLogs(state.currentPage);
}

/* ── LIVE STREAM (polling — Vercel compatible) ─────────────── */
function startStream() {
  if (state.streaming) {
    stopStream();
    return;
  }

  state.streaming = true;
  state.liveCount = 0;
  document.getElementById('btn-stream').innerHTML = '<span class="spinner"></span> Loading...';
  document.getElementById('btn-stream').disabled = true;
  document.getElementById('live-feed').innerHTML = '';
  setStreamBadge('streaming');

  fetch('/api/logs-stream')
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        showToast('anomaly', '❌ Error', data.error);
        stopStream();
        return;
      }

      showToast('info', '▶ Stream Started', `Replaying ${data.total} log entries...`);

      // Replay entries with a small delay between each for visual effect
      let i = 0;
      const entries = data.entries;

      function playNext() {
        if (i >= entries.length || !state.streaming) {
          showToast('success', '✅ Stream Complete',
            `${data.normals} normal · ${data.anomalies} anomalies`);
          stopStream();
          loadStats();
          return;
        }

        const entry = entries[i++];
        state.liveCount++;
        document.getElementById('live-count').textContent = `${state.liveCount} events`;

        pushChartPoint(entry.timestamp, entry.cpu, entry.mem);
        addFeedItem(entry);

        if (entry.is_anomaly) {
          showToast('anomaly', '🔴 Anomaly Detected',
            `${entry.anomaly_type} · CPU ${entry.cpu}% · Mem ${entry.mem}%`);
        }

        // Pace the replay — faster for normal, slight pause on anomaly
        const delay = entry.is_anomaly ? 300 : 80;
        setTimeout(playNext, delay);
      }

      playNext();
    })
    .catch(err => {
      showToast('anomaly', '❌ Stream Error', err.message);
      stopStream();
    });
}

function stopStream() {
  state.streaming = false;
  document.getElementById('btn-stream').textContent = '▶ Start Live Stream';
  document.getElementById('btn-stream').disabled = false;
  setStreamBadge('idle');
}

function addFeedItem(data) {
  const feed = document.getElementById('live-feed');
  const cls  = data.is_anomaly ? 'anomaly' : 'normal';
  const item = document.createElement('div');
  item.className = `feed-item ${cls}`;
  item.innerHTML = `
    <span class="feed-time">${data.timestamp || '—'}</span>
    <span class="feed-label ${cls}">${data.label}</span>
    <span class="feed-metrics">CPU ${data.cpu}% · Mem ${data.mem}%</span>
    <span class="feed-proc">${data.proc || '—'}</span>
    <span class="feed-score">${data.score}</span>
  `;
  feed.prepend(item);

  // Keep feed from growing too large
  while (feed.children.length > 100) {
    feed.removeChild(feed.lastChild);
  }
}

/* ── PIPELINE ──────────────────────────────────────────── */
function runPipeline() {
  showToast('info', 'ℹ️ Pipeline Info',
    'Pipeline runs locally. Run: python3 phase4_ml_model.py then redeploy.');
  closePipeline();
}

function handlePipelineEvent(data) {}

function closePipeline() {
  document.getElementById('pipeline-status').classList.add('hidden');
}

/* ── TOAST ─────────────────────────────────────────────── */
function showToast(type, title, msg) {
  const container = document.getElementById('toast-container');
  const icons = { anomaly: '🔴', info: 'ℹ️', success: '✅' };
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `
    <span class="toast-icon">${icons[type] || '•'}</span>
    <div class="toast-body">
      <div class="toast-title">${title}</div>
      <div class="toast-msg">${msg}</div>
    </div>`;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.animation = 'toastOut 0.3s ease forwards';
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

/* ── BADGE HELPERS ─────────────────────────────────────── */
function setStreamBadge(status) {
  const el = document.getElementById('stream-badge');
  const map = {
    idle      : ['badge-gray',   'Stream: Idle'],
    streaming : ['badge-green',  'Stream: Live ●'],
    error     : ['badge-red',    'Stream: Error'],
  };
  const [cls, text] = map[status] || map.idle;
  el.className = `badge ${cls}`;
  el.textContent = text;
}

/* ── UTILS ─────────────────────────────────────────────── */
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
function fmt(v) {
  if (v === null || v === undefined || v === '') return '—';
  return parseFloat(v).toFixed(1);
}

/* ── INIT ──────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  loadStats();
  loadLogs(1);
});
