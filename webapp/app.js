const calendarView = document.getElementById("calendarView");
const statusText = document.getElementById("statusText");
const refreshBtn = document.getElementById("refreshBtn");
const detailsPanel = document.getElementById("detailsPanel");
const closeDetails = document.getElementById("closeDetails");
const recalcBtn = document.getElementById("recalcBtn");
const detailsTitle = document.getElementById("detailsTitle");
const detailsMeta = document.getElementById("detailsMeta");
const linesContainer = document.getElementById("linesContainer");
const prevDayBtn = document.getElementById("prevDayBtn");
const nextDayBtn = document.getElementById("nextDayBtn");
const dayLabel = document.getElementById("dayLabel");

let gamesById = new Map();
let allDays = [];
let currentDayIndex = 0;
let selectedMarketId = null;

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function parseApiResponse(res) {
  const contentType = (res.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    try {
      return await res.json();
    } catch (_err) {
      return { error: "Invalid JSON response from server." };
    }
  }
  const text = await res.text();
  return { error: String(text || "Unexpected response") };
}

function clampDayIndex(index) {
  if (!allDays.length) return 0;
  return Math.max(0, Math.min(index, allDays.length - 1));
}

async function loadGames() {
  statusText.textContent = "Loading games...";
  try {
    const res = await fetch("/api/games");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load games");
    allDays = payload.days || [];
    currentDayIndex = clampDayIndex(currentDayIndex);
    renderCurrentDay();
    statusText.textContent = `Loaded ${payload.total_games || 0} games`;

    if (selectedMarketId && !gamesById.has(selectedMarketId)) {
      selectedMarketId = null;
      detailsPanel.classList.add("hidden");
    }
  } catch (err) {
    statusText.textContent = String(err.message || err);
  }
}

function renderCurrentDay() {
  gamesById = new Map();
  calendarView.innerHTML = "";

  if (!allDays.length) {
    dayLabel.textContent = "No games";
    prevDayBtn.disabled = true;
    nextDayBtn.disabled = true;
    calendarView.innerHTML = "<p>No games found for selected leagues.</p>";
    return;
  }

  currentDayIndex = clampDayIndex(currentDayIndex);
  const day = allDays[currentDayIndex];
  dayLabel.textContent = `${day.date_label} (${day.games.length})`;
  prevDayBtn.disabled = currentDayIndex === 0;
  nextDayBtn.disabled = currentDayIndex === allDays.length - 1;

  const block = document.createElement("section");
  block.className = "day-block";

  const header = document.createElement("h2");
  header.className = "day-header";
  header.textContent = day.date_label;

  const table = document.createElement("table");
  table.className = "games-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Kickoff (UTC)</th>
        <th>League</th>
        <th>Game</th>
        <th>Mainline</th>
        <th>Market</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector("tbody");
  for (const game of day.games) {
    gamesById.set(game.market_id, game);
    const row = document.createElement("tr");
    row.dataset.marketId = game.market_id;
    if (selectedMarketId === game.market_id) row.classList.add("selected");
    row.innerHTML = `
      <td>${escapeHtml(game.kickoff_utc)}</td>
      <td>${escapeHtml(game.competition)}</td>
      <td>${escapeHtml(game.event_name)}</td>
      <td>${escapeHtml(game.mainline || "-")}</td>
      <td>${escapeHtml(game.market_name)}</td>
    `;
    row.addEventListener("click", () => loadGameXgd(game.market_id));
    tbody.appendChild(row);
  }

  block.appendChild(header);
  block.appendChild(table);
  calendarView.appendChild(block);
}

function renderXgd(payload) {
  const warning = payload.warning ? `<div class="warning">${escapeHtml(payload.warning)}</div>` : "";
  const s = payload.summary || {};
  const metricHtml = payload.summary
    ? `
    <section class="metric-grid">
      <article class="metric-card"><div class="metric-label">Home xG</div><div class="metric-value">${Number(s.home_xg || 0).toFixed(2)}</div></article>
      <article class="metric-card"><div class="metric-label">Away xG</div><div class="metric-value">${Number(s.away_xg || 0).toFixed(2)}</div></article>
      <article class="metric-card"><div class="metric-label">Total xG</div><div class="metric-value">${Number(s.total_xg || 0).toFixed(2)}</div></article>
      <article class="metric-card"><div class="metric-label">xGD</div><div class="metric-value">${Number(s.xgd || 0).toFixed(2)}</div></article>
    </section>
  `
    : "";

  const periodRows = Array.isArray(payload.period_rows) ? payload.period_rows : [];
  const periodTable = periodRows.length
    ? `
    <h3 class="section-title">xGD Output</h3>
    <table class="lines-table">
      <thead>
        <tr>
          <th>Period</th>
          <th>Home xG</th>
          <th>Away xG</th>
          <th>Total xG</th>
          <th>xGD</th>
          <th>Min</th>
          <th>Max</th>
          <th>Home Games</th>
          <th>Away Games</th>
          <th>Warning</th>
        </tr>
      </thead>
      <tbody>
        ${periodRows
          .map(
            (r) => `
          <tr>
            <td>${escapeHtml(r.period)}</td>
            <td>${Number(r.home_xg || 0).toFixed(2)}</td>
            <td>${Number(r.away_xg || 0).toFixed(2)}</td>
            <td>${Number(r.total_xg || 0).toFixed(2)}</td>
            <td>${Number(r.xgd || 0).toFixed(2)}</td>
            <td>${r.total_min_xg == null ? "-" : Number(r.total_min_xg).toFixed(2)}</td>
            <td>${r.total_max_xg == null ? "-" : Number(r.total_max_xg).toFixed(2)}</td>
            <td>${r.home_games_used == null ? "-" : escapeHtml(r.home_games_used)}</td>
            <td>${r.away_games_used == null ? "-" : escapeHtml(r.away_games_used)}</td>
            <td>${escapeHtml(r.model_warning || "")}</td>
          </tr>
        `
          )
          .join("")}
      </tbody>
    </table>
  `
    : "";

  const mappingRows = Array.isArray(payload.mapping_rows) ? payload.mapping_rows : [];
  const mappingTable = mappingRows.length
    ? `
    <h3 class="section-title">Team Mapping</h3>
    <table class="lines-table">
      <thead>
        <tr>
          <th>Betfair Home</th>
          <th>Betfair Away</th>
          <th>Sofa Home</th>
          <th>Sofa Away</th>
          <th>Home Method</th>
          <th>Away Method</th>
          <th>Home Score</th>
          <th>Away Score</th>
          <th>Fixture Found</th>
          <th>Fixture Competition</th>
        </tr>
      </thead>
      <tbody>
        ${mappingRows
          .map(
            (r) => `
          <tr>
            <td>${escapeHtml(r.home_raw)}</td>
            <td>${escapeHtml(r.away_raw)}</td>
            <td>${escapeHtml(r.home_sofa)}</td>
            <td>${escapeHtml(r.away_sofa)}</td>
            <td>${escapeHtml(r.home_match_method)}</td>
            <td>${escapeHtml(r.away_match_method)}</td>
            <td>${r.home_match_score == null ? "-" : Number(r.home_match_score).toFixed(3)}</td>
            <td>${r.away_match_score == null ? "-" : Number(r.away_match_score).toFixed(3)}</td>
            <td>${r.fixture_found ? "Yes" : "No"}</td>
            <td>${escapeHtml(r.fixture_competition || "")}</td>
          </tr>
        `
          )
          .join("")}
      </tbody>
    </table>
  `
    : "";

  linesContainer.innerHTML = `${warning}${metricHtml}${periodTable}${mappingTable}`;
}

async function loadGameXgd(marketId) {
  const game = gamesById.get(marketId);
  if (!game) return;

  selectedMarketId = marketId;
  renderCurrentDay();

  detailsPanel.classList.remove("hidden");
  detailsTitle.textContent = game.event_name;
  detailsMeta.textContent = `${game.competition} | ${game.kickoff_raw} | Mainline: ${game.mainline || "-"}`;
  linesContainer.innerHTML = "<p>Calculating xGD...</p>";

  try {
    const res = await fetch(`/api/game-xgd?market_id=${encodeURIComponent(marketId)}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load xGD");
    renderXgd(payload);
  } catch (err) {
    linesContainer.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
  }
}

refreshBtn.addEventListener("click", () => loadGames());
prevDayBtn.addEventListener("click", () => {
  currentDayIndex = clampDayIndex(currentDayIndex - 1);
  renderCurrentDay();
});
nextDayBtn.addEventListener("click", () => {
  currentDayIndex = clampDayIndex(currentDayIndex + 1);
  renderCurrentDay();
});
closeDetails.addEventListener("click", () => {
  detailsPanel.classList.add("hidden");
});
recalcBtn.addEventListener("click", () => {
  if (selectedMarketId) loadGameXgd(selectedMarketId);
});

loadGames();
