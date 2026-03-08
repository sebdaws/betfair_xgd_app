const calendarView = document.getElementById("calendarView");
const statusText = document.getElementById("statusText");
const refreshBtn = document.getElementById("refreshBtn");
const leagueFilter = document.getElementById("leagueFilter");
const leagueFilterBtn = document.getElementById("leagueFilterBtn");
const leagueFilterMenu = document.getElementById("leagueFilterMenu");
const sortModeBtn = document.getElementById("sortModeBtn");
const tableControls = document.querySelector(".table-controls");
const detailsPanel = document.getElementById("detailsPanel");
const closeDetails = document.getElementById("closeDetails");
const recalcBtn = document.getElementById("recalcBtn");
const detailsTitle = document.getElementById("detailsTitle");
const detailsMeta = document.getElementById("detailsMeta");
const recentMatchesInput = document.getElementById("recentMatchesInput");
const linesContainer = document.getElementById("linesContainer");
const prevDayBtn = document.getElementById("prevDayBtn");
const nextDayBtn = document.getElementById("nextDayBtn");
const dayLabel = document.getElementById("dayLabel");

let gamesById = new Map();
let rawDays = [];
let availableLeagues = [];
let allDays = [];
let currentDayIndex = 0;
let selectedMarketId = null;
let selectedLeagues = new Set();
let sortMode = "kickoff";
let recentMatchesCount = 5;
const AUTO_REFRESH_MS = 2 * 60 * 1000;

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

function clampRecentMatchesCount(value) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (Number.isNaN(parsed)) return 5;
  return Math.max(1, Math.min(20, parsed));
}

function formatMetricValue(value, decimals = 2) {
  if (value == null || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return escapeHtml(value);
  return num.toFixed(decimals);
}

function buildRecentMatchesTableHtml(title, rows) {
  if (!rows.length) {
    return `
      <section class="recent-team-block">
        <h4>${escapeHtml(title)}</h4>
        <p class="recent-empty">No recent matches available.</p>
      </section>
    `;
  }

  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(title)}</h4>
      <table class="lines-table recent-lines-table">
        <thead>
          <tr>
            <th>Date (UTC)</th>
            <th>Competition</th>
            <th>Venue</th>
            <th>Opponent</th>
            <th>GF</th>
            <th>GA</th>
            <th>xG</th>
            <th>xGA</th>
            <th>xGoT</th>
            <th>xGoTA</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (r) => `
            <tr>
              <td>${escapeHtml(r.date_time || "-")}</td>
              <td>${escapeHtml(r.competition_name || "-")}</td>
              <td>${escapeHtml(r.venue || "-")}</td>
              <td>${escapeHtml(r.opponent || "-")}</td>
              <td>${formatMetricValue(r.GF, 0)}</td>
              <td>${formatMetricValue(r.GA, 0)}</td>
              <td>${formatMetricValue(r.xG, 2)}</td>
              <td>${formatMetricValue(r.xGA, 2)}</td>
              <td>${formatMetricValue(r.xGoT, 2)}</td>
              <td>${formatMetricValue(r.xGoTA, 2)}</td>
            </tr>
          `
            )
            .join("")}
        </tbody>
      </table>
    </section>
  `;
}

function updateSortButtonLabel() {
  sortModeBtn.textContent = sortMode === "kickoff" ? "Sort: Kickoff" : "Sort: League";
}

function setLeagueFilterOpen(isOpen) {
  leagueFilterMenu.classList.toggle("hidden", !isOpen);
  leagueFilterBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
}

function updateLeagueFilterButtonLabel() {
  if (!selectedLeagues.size) {
    leagueFilterBtn.textContent = "All Leagues";
    return;
  }
  if (selectedLeagues.size === 1) {
    leagueFilterBtn.textContent = Array.from(selectedLeagues)[0];
    return;
  }
  leagueFilterBtn.textContent = `${selectedLeagues.size} leagues`;
}

function updateLeagueFilterMenu(leagues) {
  leagueFilterMenu.innerHTML = "";

  const allRow = document.createElement("label");
  allRow.className = "league-option";
  const allInput = document.createElement("input");
  allInput.type = "checkbox";
  allInput.value = "__ALL__";
  allInput.checked = selectedLeagues.size === 0;
  const allText = document.createElement("span");
  allText.textContent = "All Leagues";
  allRow.appendChild(allInput);
  allRow.appendChild(allText);
  leagueFilterMenu.appendChild(allRow);

  for (const league of leagues) {
    const row = document.createElement("label");
    row.className = "league-option";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = league;
    input.checked = selectedLeagues.has(league);
    const text = document.createElement("span");
    text.textContent = league;
    row.appendChild(input);
    row.appendChild(text);
    leagueFilterMenu.appendChild(row);
  }
}

function updateLeagueFilterOptions() {
  const previous = new Set(selectedLeagues);
  const leagues = new Set();
  for (const day of rawDays) {
    for (const game of day.games || []) {
      const league = String(game.competition || "").trim();
      if (league) leagues.add(league);
    }
  }
  availableLeagues = Array.from(leagues).sort((a, b) => a.localeCompare(b));
  selectedLeagues = new Set(Array.from(previous).filter((league) => leagues.has(league)));
  updateLeagueFilterMenu(availableLeagues);
  updateLeagueFilterButtonLabel();
}

function applyLeagueFilter() {
  if (!selectedLeagues.size) {
    allDays = rawDays.map((day) => ({ ...day, games: Array.isArray(day.games) ? [...day.games] : [] }));
  } else {
    allDays = rawDays
      .map((day) => ({
        ...day,
        games: (day.games || []).filter((game) => selectedLeagues.has(String(game.competition || ""))),
      }))
      .filter((day) => day.games.length > 0);
  }
  currentDayIndex = clampDayIndex(currentDayIndex);
}

function kickoffSortKey(game) {
  const ts = Date.parse(String(game.kickoff_raw || ""));
  return Number.isNaN(ts) ? Number.MAX_SAFE_INTEGER : ts;
}

function sortGamesForDay(games) {
  const out = [...games];
  if (sortMode === "league") {
    out.sort((a, b) => {
      const leagueCmp = String(a.competition || "").localeCompare(String(b.competition || ""));
      if (leagueCmp !== 0) return leagueCmp;
      return kickoffSortKey(a) - kickoffSortKey(b);
    });
    return out;
  }
  out.sort((a, b) => kickoffSortKey(a) - kickoffSortKey(b));
  return out;
}

async function loadGames() {
  statusText.textContent = "Loading games...";
  try {
    const res = await fetch("/api/games");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load games");
    rawDays = payload.days || [];
    updateLeagueFilterOptions();
    applyLeagueFilter();
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
  const sortedDayGames = sortGamesForDay(day.games || []);
  dayLabel.textContent = `${day.date_label} (${sortedDayGames.length})`;
  prevDayBtn.disabled = currentDayIndex === 0;
  nextDayBtn.disabled = currentDayIndex === allDays.length - 1;

  const block = document.createElement("section");
  block.className = "day-block";

  const header = document.createElement("div");
  header.className = "day-header day-header-bar";
  const headerTitle = document.createElement("h2");
  headerTitle.className = "day-header-title";
  headerTitle.textContent = day.date_label;
  header.appendChild(headerTitle);
  if (tableControls) {
    header.appendChild(tableControls);
  }

  const table = document.createElement("table");
  table.className = "games-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Kickoff (UTC)</th>
        <th>League</th>
        <th>Game</th>
        <th class="home-price-col">Home</th>
        <th class="line-col">Handicap</th>
        <th class="away-price-col">Away</th>
        <th class="goal-under-price-col">Under</th>
        <th class="goal-line-col">Goals</th>
        <th class="goal-over-price-col">Over</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector("tbody");
  for (const game of sortedDayGames) {
    gamesById.set(game.market_id, game);
    const row = document.createElement("tr");
    row.dataset.marketId = game.market_id;
    if (selectedMarketId === game.market_id) row.classList.add("selected");
    row.innerHTML = `
      <td>${escapeHtml(game.kickoff_utc)}</td>
      <td>${escapeHtml(game.competition)}</td>
      <td>${escapeHtml(game.event_name)}</td>
      <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
      <td class="line-col">${escapeHtml(game.mainline || "-")}</td>
      <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
      <td class="goal-under-price-col">${escapeHtml(game.goal_under_price || "-")}</td>
      <td class="goal-line-col">${escapeHtml(game.goal_mainline || "-")}</td>
      <td class="goal-over-price-col">${escapeHtml(game.goal_over_price || "-")}</td>
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

  const recentN = clampRecentMatchesCount(payload.recent_n ?? recentMatchesCount);
  recentMatchesCount = recentN;
  if (recentMatchesInput) recentMatchesInput.value = String(recentN);

  const homeRecentRows = Array.isArray(payload.home_recent_rows) ? payload.home_recent_rows : [];
  const awayRecentRows = Array.isArray(payload.away_recent_rows) ? payload.away_recent_rows : [];
  const mappingHead = mappingRows[0] || {};
  const homeLabel = mappingHead.home_sofa || mappingHead.home_raw || "Home team";
  const awayLabel = mappingHead.away_sofa || mappingHead.away_raw || "Away team";
  const recentMatchesSection = `
    <h3 class="section-title">Previous ${recentN} Matches</h3>
    <div class="recent-grid">
      ${buildRecentMatchesTableHtml(homeLabel, homeRecentRows)}
      ${buildRecentMatchesTableHtml(awayLabel, awayRecentRows)}
    </div>
  `;

  linesContainer.innerHTML = `${warning}${metricHtml}${periodTable}${recentMatchesSection}${mappingTable}`;
}

async function loadGameXgd(marketId) {
  const game = gamesById.get(marketId);
  if (!game) return;

  selectedMarketId = marketId;
  renderCurrentDay();

  detailsPanel.classList.remove("hidden");
  detailsTitle.textContent = game.event_name;
  detailsMeta.textContent = `${game.competition} | ${game.kickoff_raw} | Home: ${game.home_price || "-"} | Handicap: ${game.mainline || "-"} | Away: ${game.away_price || "-"} | Under: ${game.goal_under_price || "-"} | Goals: ${game.goal_mainline || "-"} | Over: ${game.goal_over_price || "-"}`;
  linesContainer.innerHTML = "<p>Calculating xGD...</p>";
  recentMatchesCount = clampRecentMatchesCount(recentMatchesInput ? recentMatchesInput.value : recentMatchesCount);
  if (recentMatchesInput) recentMatchesInput.value = String(recentMatchesCount);

  try {
    const res = await fetch(
      `/api/game-xgd?market_id=${encodeURIComponent(marketId)}&recent_n=${encodeURIComponent(String(recentMatchesCount))}`
    );
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load xGD");
    renderXgd(payload);
  } catch (err) {
    linesContainer.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
  }
}

refreshBtn.addEventListener("click", () => loadGames());
leagueFilterBtn.addEventListener("click", (event) => {
  event.stopPropagation();
  const willOpen = leagueFilterMenu.classList.contains("hidden");
  setLeagueFilterOpen(willOpen);
});
leagueFilterMenu.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
  const value = String(target.value || "");
  if (!value) return;

  if (value === "__ALL__") {
    selectedLeagues.clear();
  } else if (target.checked) {
    selectedLeagues.add(value);
  } else {
    selectedLeagues.delete(value);
  }

  updateLeagueFilterMenu(availableLeagues);
  updateLeagueFilterButtonLabel();
  currentDayIndex = 0;
  applyLeagueFilter();
  renderCurrentDay();
});
document.addEventListener("click", (event) => {
  if (!leagueFilter.contains(event.target)) {
    setLeagueFilterOpen(false);
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") setLeagueFilterOpen(false);
});
sortModeBtn.addEventListener("click", () => {
  sortMode = sortMode === "kickoff" ? "league" : "kickoff";
  updateSortButtonLabel();
  renderCurrentDay();
});
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
if (recentMatchesInput) {
  recentMatchesInput.addEventListener("change", () => {
    recentMatchesCount = clampRecentMatchesCount(recentMatchesInput.value);
    recentMatchesInput.value = String(recentMatchesCount);
    if (selectedMarketId) loadGameXgd(selectedMarketId);
  });
}

updateSortButtonLabel();
updateLeagueFilterButtonLabel();
loadGames();
setInterval(() => {
  loadGames();
}, AUTO_REFRESH_MS);
