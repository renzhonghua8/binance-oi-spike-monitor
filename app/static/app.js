const state = {
  rows: [],
  signals: [],
  alerts: [],
  paper: null,
  api: null,
  sortKey: "signalStrength",
  sortDir: "desc",
  search: "",
  settingsDirty: false,
  dirtyFieldIds: new Set(),
  saving: false,
  liveUnlocked: Boolean(sessionStorage.getItem("liveAccessToken")),
  liveToken: sessionStorage.getItem("liveAccessToken") || "",
  events: null,
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
  dailyLossLimit: document.querySelector("#dailyLossLimit"),
  maxLossStreak: document.querySelector("#maxLossStreak"),
  lossPauseMinutes: document.querySelector("#lossPauseMinutes"),
  apiTradingEnabled: document.querySelector("#apiTradingEnabled"),
  apiTradingTestnet: document.querySelector("#apiTradingTestnet"),
  apiLongEnabled: document.querySelector("#apiLongEnabled"),
  apiShortEnabled: document.querySelector("#apiShortEnabled"),
  apiMaxNotional: document.querySelector("#apiMaxNotional"),
  apiMaxOpen: document.querySelector("#apiMaxOpen"),
  apiLeverage: document.querySelector("#apiLeverage"),
  apiKeyInput: document.querySelector("#apiKeyInput"),
  apiSecretInput: document.querySelector("#apiSecretInput"),
  apiLiveConfirm: document.querySelector("#apiLiveConfirm"),
  adminKey: document.querySelector("#adminKey"),
};

const rowsEl = document.querySelector("#rows");
const statusEl = document.querySelector("#status");

function initStream() {
  if (state.events) state.events.close();
  const suffix = state.liveToken ? `?live_token=${encodeURIComponent(state.liveToken)}` : "";
  state.events = new EventSource(`/events${suffix}`);
  state.events.onmessage = (event) => applySnapshot(JSON.parse(event.data));
  state.events.onerror = () => setStatus(false, "SSE重连中");
}

function applySnapshot(snapshot) {
  const { config, status, rows, signals, alerts, paper, api } = snapshot;
  state.rows = rows || [];
  state.signals = signals || [];
  state.alerts = alerts || [];
  state.paper = paper || null;
  state.api = api || null;
  if (!state.saving && !state.settingsDirty) {
    syncConfigFields(config);
  }
  syncDependentSettingState();
  setStatus(status.ok, status.message);
  document.querySelector("#tracked").textContent = status.tracked || 0;
  document.querySelector("#highlighted").textContent = state.rows.filter((row) => row.isHighlighted).length;
  document.querySelector("#strongSignals").textContent = state.rows.filter((row) => row.isStrongSignal).length;
  document.querySelector("#directionSignals").textContent = state.signals.length;
  document.querySelector("#staleRows").textContent = status.staleRows || 0;
  document.querySelector("#updatedAt").textContent = status.updatedAt || "-";
  const latestAlert = state.alerts[0];
  document.querySelector("#alertStatus").textContent = latestAlert
    ? formatAlertStatus(latestAlert)
    : "待触发";
  renderRows();
  renderSignals();
  renderApi();
  renderPaper();
  renderAdminHint();
}

function syncConfigFields(config) {
  syncChecked(fields.monitorAll, Boolean(config.monitor_all));
  syncValue(fields.topSymbols, config.top_symbols);
  syncValue(fields.oiThreshold, config.oi_5m_threshold);
  syncValue(fields.volumeThreshold, config.volume_multiple_threshold);
  syncValue(fields.strengthThreshold, config.signal_strength_threshold);
  syncValue(fields.maxAge, config.max_data_age_seconds);
  syncValue(fields.seedTopSymbols, config.seed_top_symbols);
  syncValue(fields.klineTopSymbols, config.kline_top_symbols);
  syncValue(fields.minVolume, config.min_24h_quote_volume);
  syncValue(fields.refreshSeconds, config.refresh_seconds);
  syncValue(fields.dailyLossLimit, config.paper_daily_loss_limit_pct);
  syncValue(fields.maxLossStreak, config.paper_max_consecutive_losses);
  syncValue(fields.lossPauseMinutes, config.paper_loss_pause_minutes);
  syncChecked(fields.apiTradingEnabled, Boolean(config.api_trading_enabled));
  syncChecked(fields.apiTradingTestnet, Boolean(config.api_trading_testnet));
  syncChecked(fields.apiLongEnabled, Boolean(config.api_trading_long_enabled));
  syncChecked(fields.apiShortEnabled, Boolean(config.api_trading_short_enabled));
  syncValue(fields.apiMaxNotional, config.api_max_notional_per_trade);
  syncValue(fields.apiMaxOpen, config.api_max_open_positions);
  syncValue(fields.apiLeverage, config.api_leverage);
  syncDependentSettingState();
}

function fieldIsLocked(field) {
  return state.dirtyFieldIds.has(field.id) || document.activeElement === field;
}

function syncValue(field, value) {
  if (!fieldIsLocked(field)) field.value = value;
}

function syncChecked(field, value) {
  if (!fieldIsLocked(field)) field.checked = value;
}

function markSettingDirty(target) {
  if (!target || !target.id || target.id === "adminKey" || target.id === "liveAccessKey") return;
  if (!target.closest(".settings") && !target.closest("#liveUnlocked")) return;
  state.settingsDirty = true;
  state.dirtyFieldIds.add(target.id);
  syncDependentSettingState();
  setSaveStatus("有未保存修改，请点击保存设置", "warningTextInline");
}

function syncDependentSettingState() {
  fields.topSymbols.disabled = fields.monitorAll.checked;
}

function setSaveStatus(text, className = "") {
  const el = document.querySelector("#saveStatus");
  el.textContent = text;
  el.className = className;
}

function setStatus(ok, message) {
  statusEl.classList.toggle("ok", ok);
  statusEl.querySelector("span:last-child").textContent = message || (ok ? "live" : "等待数据");
}

function formatAlertStatus(alert) {
  const result = `${alert.ok ? "成功" : "失败"} ${alert.symbol}`;
  if (alert.ok) return result;
  return `${result}：${alert.message || "未知原因"}`;
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

function renderPaper() {
  const paper = state.paper;
  if (!paper) return;
  document.querySelector("#paperEquity").textContent = moneyFull(paper.equity);
  document.querySelector("#paperPnl").textContent = `${moneyFull(paper.totalPnl)} (${paper.totalPnlPct.toFixed(2)}%)`;
  document.querySelector("#paperPnl").className = paper.totalPnl >= 0 ? "up" : "down";
  document.querySelector("#paperOpen").textContent = paper.openCount;
  document.querySelector("#paperClosed").textContent = paper.closedCount;
  document.querySelector("#paperWinRate").textContent = `${paper.winRate.toFixed(1)}%`;
  document.querySelector("#paperDrawdown").textContent = `${paper.maxDrawdownPct.toFixed(2)}%`;
  document.querySelector("#paperDailyPnl").textContent = `${moneyFull(paper.dailyPnl)} (${paper.dailyPnlPct.toFixed(2)}%)`;
  document.querySelector("#paperDailyPnl").className = paper.dailyPnl >= 0 ? "up" : "down";
  document.querySelector("#paperLossStreak").textContent = paper.consecutiveLosses;
  document.querySelector("#paperOpenStatus").textContent = paper.openingPaused ? paper.pauseReason : "正常";
  document.querySelector("#paperOpenStatus").className = paper.openingPaused ? "down" : "up";
  renderPaperStats("#paperSignalStats", paper.statsBySignal || []);
  renderPaperStats("#paperEntryStats", paper.statsByEntryType || []);

  document.querySelector("#paperPositions").innerHTML = paper.positions
    .map((position) => `
      <tr>
        <td class="symbol">${position.symbol}</td>
        <td><span class="badge ${position.side === "long" ? "long" : "short"}">${position.side === "long" ? "做多" : "做空"}</span> ${position.entryType || ""}</td>
        <td>${price(position.entryPrice)}</td>
        <td>${price(position.latestPrice)}</td>
        <td>${price(position.stopPrice)}</td>
        <td>${price(position.takeProfitPrice)}</td>
        <td class="${position.unrealizedPnl >= 0 ? "up" : "down"}">${moneyFull(position.unrealizedPnl)} (${position.unrealizedPnlPct.toFixed(2)}%)</td>
        <td>${position.ageMinutes.toFixed(1)}m</td>
      </tr>
    `)
    .join("");

  document.querySelector("#paperTrades").innerHTML = paper.trades
    .slice(0, 30)
    .map((trade) => `
      <tr>
        <td>${trade.exitTime}</td>
        <td class="symbol">${trade.symbol}</td>
        <td><span class="badge ${trade.side === "long" ? "long" : "short"}">${trade.side === "long" ? "做多" : "做空"}</span> ${trade.entryType || ""}</td>
        <td>${price(trade.entryPrice)}</td>
        <td>${price(trade.exitPrice)}</td>
        <td>${trade.exitReason}</td>
        <td class="${trade.pnl >= 0 ? "up" : "down"}">${moneyFull(trade.pnl)} (${trade.pnlPct.toFixed(2)}%)</td>
      </tr>
    `)
    .join("");
}

function renderApi() {
  const api = state.api;
  if (!api) return;
  setLiveUnlocked(Boolean(api.detailsUnlocked));
  document.querySelector("#apiMode").textContent = api.mode === "testnet" ? "Testnet" : "主网";
  document.querySelector("#apiStatus").textContent = `${api.enabled ? api.message : "未开启"}`;
  document.querySelector("#apiStatus").className = api.ready ? "up" : api.enabled ? "down" : "";
  document.querySelector("#apiKeys").textContent = api.hasKeys ? `已配置（${api.keySource || "未知"}）` : "未配置";
  document.querySelector("#apiKeys").className = api.hasKeys ? "up" : "down";
  document.querySelector("#apiOpen").textContent = api.openCount || 0;
  document.querySelector("#apiExchangeOpen").textContent = (api.exchangePositions || []).length;
  document.querySelector("#apiClosed").textContent = api.closedCount || 0;

  document.querySelector("#apiExchangePositions").innerHTML = (api.exchangePositions || [])
    .map((position) => `
      <tr>
        <td class="symbol">${position.symbol}</td>
        <td><span class="badge ${position.side === "long" ? "long" : "short"}">${position.side === "long" ? "做多" : "做空"}</span></td>
        <td>${price(position.entryPrice)}</td>
        <td>${price(position.markPrice)}</td>
        <td>${Number(position.amount).toFixed(6)}</td>
        <td>${moneyFull(position.notional)}</td>
        <td class="${position.unrealizedPnl >= 0 ? "up" : "down"}">${moneyFull(position.unrealizedPnl)}</td>
        <td>${position.leverage}x</td>
      </tr>
    `)
    .join("");

  document.querySelector("#apiPositions").innerHTML = (api.positions || [])
    .map((position) => `
      <tr>
        <td class="symbol">${position.symbol}</td>
        <td><span class="badge ${position.side === "long" ? "long" : "short"}">${position.side === "long" ? "做多" : "做空"}</span> ${position.entryType || ""}</td>
        <td>${price(position.entryPrice)}</td>
        <td>${Number(position.qty).toFixed(6)}</td>
        <td>${price(position.stopPrice)}</td>
        <td>${price(position.takeProfitPrice)}</td>
        <td>${((Date.now() / 1000 - position.entryAt) / 60).toFixed(1)}m</td>
      </tr>
    `)
    .join("");

  document.querySelector("#apiTrades").innerHTML = (api.trades || [])
    .slice(0, 30)
    .map((trade) => `
      <tr>
        <td>${trade.exitTime}</td>
        <td class="symbol">${trade.symbol}</td>
        <td><span class="badge ${trade.side === "long" ? "long" : "short"}">${trade.side === "long" ? "做多" : "做空"}</span> ${trade.entryType || ""}</td>
        <td>${price(trade.entryPrice)}</td>
        <td>${price(trade.exitPrice)}</td>
        <td>${trade.exitReason}</td>
        <td class="${trade.pnl >= 0 ? "up" : "down"}">${moneyFull(trade.pnl)} (${trade.pnlPct.toFixed(2)}%)</td>
      </tr>
    `)
    .join("");
}

function renderAdminHint() {
  const hint = document.querySelector("#adminKeyHint");
  if (!hint || !state.api) return;
  hint.textContent = state.api.adminKeyProtected
    ? "操作密钥就是服务器 .env 中的 ADMIN_ACTION_KEY；修改实盘交易、Testnet、方向、金额、持仓、杠杆、密钥时必须填写。"
    : "服务器未设置 ADMIN_ACTION_KEY，API真实交易参数暂未启用操作密钥保护。";
  hint.className = state.api.adminKeyProtected ? "dangerText" : "warningText";
}

function renderPaperStats(selector, stats) {
  document.querySelector(selector).innerHTML = stats
    .slice(0, 8)
    .map((item) => `
      <tr>
        <td class="symbol">${item.name}</td>
        <td>${item.total}</td>
        <td>${item.winRate.toFixed(1)}%</td>
        <td class="${item.pnl >= 0 ? "up" : "down"}">${moneyFull(item.pnl)}</td>
        <td class="${item.avgPnl >= 0 ? "up" : "down"}">${moneyFull(item.avgPnl)}</td>
        <td>${item.profitFactor === null ? "-" : Number(item.profitFactor).toFixed(2)}</td>
      </tr>
    `)
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

function moneyFull(value) {
  const num = Number(value) || 0;
  return `${num >= 0 ? "" : "-"}${Math.abs(num).toFixed(2)}`;
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

["pointerdown", "keydown", "input", "change"].forEach((eventName) => {
  document.body.addEventListener(
    eventName,
    (event) => markSettingDirty(event.target),
    true,
  );
});

async function saveConfig(saveButton) {
  const idleText = saveButton.textContent;
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
    paper_daily_loss_limit_pct: Number(fields.dailyLossLimit.value),
    paper_max_consecutive_losses: Number(fields.maxLossStreak.value),
    paper_loss_pause_minutes: Number(fields.lossPauseMinutes.value),
    api_trading_enabled: fields.apiTradingEnabled.checked,
    api_trading_testnet: fields.apiTradingTestnet.checked,
    api_trading_long_enabled: fields.apiLongEnabled.checked,
    api_trading_short_enabled: fields.apiShortEnabled.checked,
    api_max_notional_per_trade: Number(fields.apiMaxNotional.value),
    api_max_open_positions: Number(fields.apiMaxOpen.value),
    api_leverage: Number(fields.apiLeverage.value),
    binance_api_key: fields.apiKeyInput.value.trim(),
    binance_api_secret: fields.apiSecretInput.value.trim(),
    binance_live_trading_confirm: fields.apiLiveConfirm.value.trim(),
    admin_key: fields.adminKey.value,
  };
  state.saving = true;
  saveButton.textContent = "保存中";
  setSaveStatus("正在保存", "");
  const response = await fetch("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    const message = error.detail || "保存失败";
    alert(message);
    setSaveStatus(message, "dangerTextInline");
    saveButton.textContent = idleText;
    state.saving = false;
    return;
  }
  const snapshot = await response.json();
  state.settingsDirty = false;
  state.dirtyFieldIds.clear();
  state.saving = false;
  if (snapshot.config) {
    syncConfigFields(snapshot.config);
  }
  fields.apiKeyInput.value = "";
  fields.apiSecretInput.value = "";
  fields.apiLiveConfirm.value = "";
  fields.adminKey.value = "";
  saveButton.textContent = "已保存";
  setSaveStatus("保存成功，配置已生效", "successTextInline");
  setTimeout(() => {
    saveButton.textContent = idleText;
    if (!state.settingsDirty) setSaveStatus("实时同步中", "");
  }, 1200);
}

document.querySelector("#saveConfig").addEventListener("click", async () => {
  await saveConfig(document.querySelector("#saveConfig"));
});

document.querySelector("#saveLiveConfig").addEventListener("click", async () => {
  await saveConfig(document.querySelector("#saveLiveConfig"));
});

document.querySelector("#unlockLive").addEventListener("click", async () => {
  const status = document.querySelector("#liveUnlockStatus");
  status.textContent = "验证中";
  status.className = "";
  const response = await fetch("/api/live/unlock", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ access_key: document.querySelector("#liveAccessKey").value }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    status.textContent = error.detail || "口令错误";
    status.className = "dangerTextInline";
    return;
  }
  const data = await response.json();
  state.liveToken = data.token;
  sessionStorage.setItem("liveAccessToken", data.token);
  document.querySelector("#liveAccessKey").value = "";
  status.textContent = "已解锁";
  status.className = "successTextInline";
  setLiveUnlocked(true);
  initStream();
});

function setLiveUnlocked(unlocked) {
  state.liveUnlocked = unlocked;
  document.querySelector("#liveLocked").classList.toggle("hidden", unlocked);
  document.querySelector("#liveUnlocked").classList.toggle("hidden", !unlocked);
}

fields.monitorAll.addEventListener("change", () => {
  syncDependentSettingState();
});

document.querySelector("#resetPaper").addEventListener("click", async () => {
  await fetch("/api/paper/reset", { method: "POST" });
});

initStream();
