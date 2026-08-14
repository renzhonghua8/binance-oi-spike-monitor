const state = {
  rows: [],
  signals: [],
  alerts: [],
  sortKey: "signalStrength",
  sortDir: "desc",
  search: "",
};

const fields = {
  monitorAll: document.querySelector("#monitorAll"),
  topSymbols: document.querySelector("#topSymbols"),
  oiThreshold: document.querySelector("#oiThreshold"),
  volumeThreshold: document.querySelector("#volumeThreshold"),
  strengthThreshold: document.querySelector("#strengthThreshold"),
  maxAge: document.querySelector("#maxAge"),
  seedTopSymbols: document.querySelector("#seedTopSymbols"),
  klineTopSymbols: document.querySelector("#klineTopSymbols"),
  minVolume: document.querySelector("#minVolume"),
  refreshSeconds: document.querySelector("#refreshSeconds"),
};

const rowsEl = document.querySelector("#rows");
const statusEl = document.querySelector("#status");

function initStream() {
  const events = new EventSource("/events");
  events.onmessage = (event) => applySnapshot(JSON.parse(event.data));
  events.onerror = () => setStatus(false, "SSE重连中");
}

function applySnapshot(snapshot) {
  const { config, status, rows, signals, alerts } = snapshot;
  state.rows = rows || [];
  state.signals = signals || [];
  state.alerts = alerts || [];
  fields.monitorAll.checked = Boolean(config.monitor_all);
  fields.topSymbols.value = config.top_symbols;
  fields.oiThreshold.value = config.oi_5m_threshold;
  fields.volumeThreshold.value = config.volume_multiple_threshold;
  fields.strengthThreshold.value = config.signal_strength_threshold;
  fields.maxAge.value = config.max_data_age_seconds;
  fields.seedTopSymbols.value = config.seed_top_symbols;
  fields.klineTopSymbols.value = config.kline_top_symbols;
  fields.minVolume.value = config.min_24h_quote_volume;
  fields.refreshSeconds.value = config.refresh_seconds;
  fields.topSymbols.disabled = fields.monitorAll.checked;
  setStatus(status.ok, status.message);
  document.querySelector("#tracked").textContent = status.tracked || 0;
  document.querySelector("#highlighted").textContent = state.rows.filter((row) => row.isHighlighted).length;
  document.querySelector("#strongSignals").textContent = state.rows.filter((row) => row.isStrongSignal).length;
  document.querySelector("#directionSignals").textContent = state.signals.length;
  document.querySelector("#staleRows").textContent = status.staleRows || 0;
  document.querySelector("#updatedAt").textContent = status.updatedAt || "-";
  const latestAlert = state.alerts[0];
  document.querySelector("#alertStatus").textContent = latestAlert
    ? `${latestAlert.ok ? "成功" : "失败"} ${latestAlert.symbol}`
    : "待触发";
  renderRows();
  renderSignals();
}

function setStatus(ok, message) {
  statusEl.classList.toggle("ok", ok);
  statusEl.querySelector("span:last-child").textContent = message || (ok ? "live" : "等待数据");
}

function renderRows() {
  const query = state.search.trim().toUpperCase();
  const rows = state.rows
    .filter((row) => !query || row.symbol.includes(query))
    .sort((a, b) => compareRows(a, b));

  rowsEl.innerHTML = rows
    .map((row) => {
      const directionClass = directionToClass(row.signalDirection);
      return `
        <tr class="${row.isStale ? "staleRow" : row.isStrongSignal ? "strongRow" : row.isHighlighted ? "highlight" : ""}">
          <td class="symbol">${row.symbol}</td>
          <td>${price(row.latestPrice)}</td>
          <td class="${tone(row.oiChange1m)}">${pct(row.oiChange1m, "实时采样中")}</td>
          <td class="${tone(row.oiChange3m)}">${pct(row.oiChange3m, "实时采样中")}</td>
          <td class="${tone(row.oiChange5m)}">${pct(row.oiChange5m, "回填中")}</td>
          <td class="${tone(row.priceChange5m)}">${pct(row.priceChange5m, "回填中")}</td>
          <td class="${row.volumeMultiple5m >= Number(fields.volumeThreshold.value) ? "hot" : ""}">
            ${multiple(row.volumeMultiple5m, "回填中")}
          </td>
          <td class="${tone(row.fundingRate)}">${pct(row.fundingRate)}</td>
          <td>${money(row.quoteVolume24h)}</td>
          <td><span class="badge ${directionClass}">${row.signalDirection}</span></td>
          <td>
            <div class="strength"><span style="width:${row.signalStrength}%"></span></div>
            <b>${row.signalStrength}</b>
          </td>
          <td class="${row.triggerCount1h >= 3 ? "hot" : ""}">${row.triggerCount1h || 0} ${row.repeatSignalLevel || ""}</td>
          <td>${row.lastSignalAt || "-"}</td>
          <td class="${row.isStale ? "down" : ""}">${age(row.dataAgeSeconds)}</td>
          <td>${row.updatedAt}</td>
        </tr>
      `;
    })
    .join("");
}

function renderSignals() {
  const signalRowsEl = document.querySelector("#signalRows");
  signalRowsEl.innerHTML = state.signals
    .slice(0, 80)
    .map((signal) => {
      const directionClass = directionToClass(signal.signalDirection);
      return `
        <tr>
          <td>${signal.createdAt}</td>
          <td class="symbol">${signal.symbol}</td>
          <td><span class="badge ${directionClass}">${signal.signalDirection}</span></td>
          <td><b>${signal.signalStrength}</b></td>
          <td class="${signal.triggerCount1h >= 3 ? "hot" : ""}">${signal.triggerCount1h}</td>
          <td class="${tone(signal.oiChange5m)}">${pct(signal.oiChange5m, "-")}</td>
          <td class="${tone(signal.priceChange5m)}">${pct(signal.priceChange5m, "-")}</td>
          <td class="hot">${multiple(signal.volumeMultiple5m, "-")}</td>
          <td class="${tone(signal.fundingRate)}">${pct(signal.fundingRate)}</td>
          <td>${money(signal.quoteVolume24h)}</td>
        </tr>
      `;
    })
    .join("");
}

function compareRows(a, b) {
  const av = a[state.sortKey];
  const bv = b[state.sortKey];
  let result;
  if (typeof av === "string") {
    result = av.localeCompare(bv);
  } else {
    result = (Number(av) || 0) - (Number(bv) || 0);
  }
  return state.sortDir === "asc" ? result : -result;
}

function pct(value, emptyText = "采样中") {
  return value === null || value === undefined ? emptyText : `${Number(value).toFixed(2)}%`;
}

function price(value) {
  const num = Number(value);
  if (num >= 100) return num.toFixed(2);
  if (num >= 1) return num.toFixed(4);
  return num.toPrecision(5);
}

function multiple(value, emptyText = "-") {
  return value === null || value === undefined ? emptyText : `${Number(value).toFixed(2)}x`;
}

function age(value) {
  if (value === null || value === undefined) return "-";
  return `${Number(value).toFixed(0)}s`;
}

function money(value) {
  const num = Number(value) || 0;
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  return num.toLocaleString();
}

function tone(value) {
  const num = Number(value) || 0;
  if (num > 0) return "up";
  if (num < 0) return "down";
  return "";
}

function directionToClass(direction) {
  return {
    "多头增仓": "long",
    "空头增仓": "short",
    "挤空": "squeezeUp",
    "挤多": "squeezeDown",
    "仅OI增长": "oiOnly",
  }[direction] || "watch";
}

document.querySelector("#search").addEventListener("input", (event) => {
  state.search = event.target.value;
  renderRows();
});

document.querySelectorAll("th[data-sort]").forEach((th) => {
  th.addEventListener("click", () => {
    const key = th.dataset.sort;
    if (state.sortKey === key) {
      state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    } else {
      state.sortKey = key;
      state.sortDir = "desc";
    }
    renderRows();
  });
});

document.querySelector("#saveConfig").addEventListener("click", async () => {
  const payload = {
    monitor_all: fields.monitorAll.checked,
    top_symbols: Number(fields.topSymbols.value),
    oi_5m_threshold: Number(fields.oiThreshold.value),
    volume_multiple_threshold: Number(fields.volumeThreshold.value),
    signal_strength_threshold: Number(fields.strengthThreshold.value),
    max_data_age_seconds: Number(fields.maxAge.value),
    seed_top_symbols: Number(fields.seedTopSymbols.value),
    kline_top_symbols: Number(fields.klineTopSymbols.value),
    min_24h_quote_volume: Number(fields.minVolume.value),
    refresh_seconds: Number(fields.refreshSeconds.value),
  };
  await fetch("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
});

fields.monitorAll.addEventListener("change", () => {
  fields.topSymbols.disabled = fields.monitorAll.checked;
});

initStream();
