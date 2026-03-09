const calendarView = document.getElementById("calendarView");
const statusText = document.getElementById("statusText");
const refreshBtn = document.getElementById("refreshBtn");
const leagueFilter = document.getElementById("leagueFilter");
const leagueFilterBtn = document.getElementById("leagueFilterBtn");
const leagueFilterMenu = document.getElementById("leagueFilterMenu");
const tierFilter = document.getElementById("tierFilter");
const tierFilterBtn = document.getElementById("tierFilterBtn");
const tierFilterMenu = document.getElementById("tierFilterMenu");
const sortModeBtn = document.getElementById("sortModeBtn");
const tableControls = document.querySelector(".table-controls");
const gamesTabBtn = document.getElementById("gamesTabBtn");
const manualMappingTabBtn = document.getElementById("manualMappingTabBtn");
const gamesTabPane = document.getElementById("gamesTabPane");
const manualMappingTabPane = document.getElementById("manualMappingTabPane");
const mappingRefreshBtn = document.getElementById("mappingRefreshBtn");
const mappingStatus = document.getElementById("mappingStatus");
const unmatchedTeamsContainer = document.getElementById("unmatchedTeamsContainer");
const savedMappingsContainer = document.getElementById("savedMappingsContainer");
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
let rawDays = [];
let availableLeagues = [];
let availableTiers = [];
let allDays = [];
let currentDayIndex = 0;
let selectedMarketId = null;
let selectedLeagues = new Set();
let selectedTiers = new Set();
let sortMode = "kickoff";
let gamesShownCount = 5;
let recentTeamView = "home";
let lastXgdPayload = null;
let activeTab = "games";
let lastManualMappingPayload = null;
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

function setActiveTab(tabName) {
  activeTab = tabName === "mapping" ? "mapping" : "games";
  const gamesActive = activeTab === "games";
  gamesTabBtn.classList.toggle("active", gamesActive);
  manualMappingTabBtn.classList.toggle("active", !gamesActive);
  gamesTabPane.classList.toggle("hidden", !gamesActive);
  manualMappingTabPane.classList.toggle("hidden", gamesActive);
  if (!gamesActive) {
    detailsPanel.classList.add("hidden");
  }
}

function renderManualMappingSections(payload) {
  lastManualMappingPayload = payload;
  const mappings = Array.isArray(payload?.mappings) ? payload.mappings : [];
  const unmatched = Array.isArray(payload?.unmatched) ? payload.unmatched : [];
  const sofaTeams = Array.isArray(payload?.sofa_teams) ? payload.sofa_teams : [];

  mappingStatus.textContent = `${unmatched.length} unmatched teams | ${mappings.length} saved mappings`;

  unmatchedTeamsContainer.innerHTML = "";
  if (!unmatched.length) {
    unmatchedTeamsContainer.innerHTML = `<p class="mapping-empty">No unmatched teams in current games.</p>`;
  } else {
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Game</th>
          <th>League</th>
          <th>Kickoff</th>
          <th>Side</th>
          <th>Betfair Team</th>
          <th>SofaScore Team</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");

    const optionsHtml =
      `<option value="">Select team...</option>` +
      sofaTeams.map((team) => `<option value="${escapeHtml(team)}">${escapeHtml(team)}</option>`).join("");

    for (const row of unmatched) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const existing = mappings.find((m) => String(m.raw_name || "") === rawName);
      tr.innerHTML = `
        <td>${escapeHtml(String(row.event_name || "-"))}</td>
        <td>${escapeHtml(String(row.competition || "-"))}</td>
        <td>${escapeHtml(String(row.kickoff_raw || "-"))}</td>
        <td>${escapeHtml(String(row.side || "-"))}</td>
        <td>${escapeHtml(rawName)}</td>
        <td>
          <select class="mapping-select">
            ${optionsHtml}
          </select>
        </td>
        <td><button type="button" class="mapping-save-btn">Save</button></td>
      `;
      const select = tr.querySelector(".mapping-select");
      if (select && existing?.sofa_name) {
        select.value = String(existing.sofa_name);
      }
      const saveBtn = tr.querySelector(".mapping-save-btn");
      if (saveBtn && select) {
        saveBtn.addEventListener("click", async () => {
          const sofaName = String(select.value || "").trim();
          if (!sofaName) {
            mappingStatus.textContent = "Select a SofaScore team before saving.";
            return;
          }
          await upsertManualMapping(rawName, sofaName);
        });
      }
      tbody.appendChild(tr);
    }
    unmatchedTeamsContainer.appendChild(table);
  }

  savedMappingsContainer.innerHTML = "";
  if (!mappings.length) {
    savedMappingsContainer.innerHTML = `<p class="mapping-empty">No manual mappings saved yet.</p>`;
  } else {
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Betfair Team</th>
          <th>SofaScore Team</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const row of mappings) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      tr.innerHTML = `
        <td>${escapeHtml(rawName)}</td>
        <td>${escapeHtml(String(row.sofa_name || ""))}</td>
        <td><button type="button" class="mapping-delete-btn">Delete</button></td>
      `;
      const deleteBtn = tr.querySelector(".mapping-delete-btn");
      if (deleteBtn) {
        deleteBtn.addEventListener("click", async () => {
          await deleteManualMapping(rawName);
        });
      }
      tbody.appendChild(tr);
    }
    savedMappingsContainer.appendChild(table);
  }
}

async function loadManualMappings() {
  mappingStatus.textContent = "Loading mapping data...";
  try {
    const res = await fetch("/api/manual-mappings");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load manual mappings");
    renderManualMappingSections(payload);
  } catch (err) {
    mappingStatus.textContent = String(err.message || err);
  }
}

async function upsertManualMapping(rawName, sofaName) {
  try {
    const res = await fetch("/api/manual-mappings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ raw_name: rawName, sofa_name: sofaName }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to save mapping");
    await Promise.all([loadGames(), loadManualMappings()]);
  } catch (err) {
    mappingStatus.textContent = String(err.message || err);
  }
}

async function deleteManualMapping(rawName) {
  try {
    const res = await fetch("/api/manual-mappings/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ raw_name: rawName }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to delete mapping");
    await Promise.all([loadGames(), loadManualMappings()]);
  } catch (err) {
    mappingStatus.textContent = String(err.message || err);
  }
}

function clampDayIndex(index) {
  if (!allDays.length) return 0;
  return Math.max(0, Math.min(index, allDays.length - 1));
}

function toDayDate(dayIso) {
  const d = new Date(`${dayIso}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function formatDayLabelFromIso(dayIso) {
  const d = toDayDate(dayIso);
  if (!d) return dayIso;
  const weekday = d.toLocaleDateString("en-GB", { weekday: "short", timeZone: "UTC" });
  return `${weekday} ${dayIso}`;
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

function buildPeriodMetricStackCell(seasonValue, last5Value, last3Value) {
  const isMissing = (value) => {
    const text = String(value ?? "").trim();
    return !text || text === "-";
  };

  const rows = [
    { label: "S", value: seasonValue },
    { label: "5", value: last5Value },
    { label: "3", value: last3Value },
  ].filter((row) => !isMissing(row.value));

  if (!rows.length) {
    return `
      <div class="metric-stack">
        <div class="metric-stack-row">
          <span class="metric-stack-value">-</span>
        </div>
      </div>
    `;
  }

  return `
    <div class="metric-stack">
      ${rows
        .map(
          (row) => `
            <div class="metric-stack-row">
              <span class="metric-stack-label">${escapeHtml(row.label)}</span>
              <span class="metric-stack-value">${escapeHtml(row.value || "-")}</span>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function buildRecentMatchesTableHtml(title, rows, relevantTeamName = "") {
  const headingPrefix = relevantTeamName
    ? `<strong>${escapeHtml(relevantTeamName)}</strong> · `
    : "";
  if (!rows.length) {
    return `
      <section class="recent-team-block">
        <h4>${headingPrefix}${escapeHtml(title)}</h4>
        <p class="recent-empty">No recent matches available.</p>
      </section>
    `;
  }

  return `
    <section class="recent-team-block">
      <h4>${headingPrefix}${escapeHtml(title)}</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Competition</th>
              <th>Home</th>
              <th>Away</th>
              <th>G<span class="metric-suffix">H</span></th>
              <th>G<span class="metric-suffix">A</span></th>
              <th>xG<span class="metric-suffix">H</span></th>
              <th>xG<span class="metric-suffix">A</span></th>
              <th>xGD</th>
              <th>xGoT<span class="metric-suffix">H</span></th>
              <th>xGoT<span class="metric-suffix">A</span></th>
            </tr>
          </thead>
          <tbody>
            ${rows
              .map(
                (r) => {
                  const isHome = String(r.venue || "").toLowerCase() === "home";
                  const relevantTeam = String(relevantTeamName || r.team || "").trim();
                  const opponentTeam = String(r.opponent || "").trim();
                  const homeTeam = isHome ? relevantTeam : opponentTeam;
                  const awayTeam = isHome ? opponentTeam : relevantTeam;
                  const homeTeamCell = isHome ? `<strong>${escapeHtml(homeTeam || "-")}</strong>` : escapeHtml(homeTeam || "-");
                  const awayTeamCell = isHome ? escapeHtml(awayTeam || "-") : `<strong>${escapeHtml(awayTeam || "-")}</strong>`;
                  const gHome = isHome ? r.GF : r.GA;
                  const gAway = isHome ? r.GA : r.GF;
                  const xgHome = isHome ? r.xG : r.xGA;
                  const xgAway = isHome ? r.xGA : r.xG;
                  const xgotHome = isHome ? r.xGoT : r.xGoTA;
                  const xgotAway = isHome ? r.xGoTA : r.xGoT;
                  const computedXgd = Number(xgHome) - Number(xgAway);
                  const xgd = Number.isFinite(computedXgd) ? computedXgd : r.xgd;
                  return `
              <tr>
                <td>${escapeHtml(r.date_time || "-")}</td>
                <td>${escapeHtml(r.competition_name || "-")}</td>
                <td>${homeTeamCell}</td>
                <td>${awayTeamCell}</td>
                <td>${formatMetricValue(gHome, 0)}</td>
                <td>${formatMetricValue(gAway, 0)}</td>
                <td>${formatMetricValue(xgHome, 2)}</td>
                <td>${formatMetricValue(xgAway, 2)}</td>
                <td>${formatMetricValue(xgd, 2)}</td>
                <td>${formatMetricValue(xgotHome, 2)}</td>
                <td>${formatMetricValue(xgotAway, 2)}</td>
              </tr>
            `;
                }
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildRecentSwitchHtml(homeLabel, awayLabel) {
  const homeActive = recentTeamView !== "away";
  const awayActive = recentTeamView === "away";
  return `
    <div class="recent-switch" role="tablist" aria-label="Recent matches team view">
      <button
        type="button"
        class="recent-switch-btn${homeActive ? " active" : ""}"
        data-team-view="home"
        role="tab"
        aria-selected="${homeActive ? "true" : "false"}"
      >
        Home: ${escapeHtml(homeLabel)}
      </button>
      <button
        type="button"
        class="recent-switch-btn${awayActive ? " active" : ""}"
        data-team-view="away"
        role="tab"
        aria-selected="${awayActive ? "true" : "false"}"
      >
        Away: ${escapeHtml(awayLabel)}
      </button>
    </div>
  `;
}

function buildVenueSplitSectionHtml(teamLabel, venueRecentN, homeRows, awayRows) {
  const mergedRows = [...homeRows, ...awayRows].sort((a, b) =>
    String(b.date_time || "").localeCompare(String(a.date_time || ""))
  );
  return `
    <h3 class="section-title">${escapeHtml(teamLabel)}: Home & Away Matches</h3>
    ${buildRecentMatchesTableHtml("All matches", mergedRows, teamLabel)}
  `;
}

function buildGamesShownControlHtml(value) {
  const safeValue = clampRecentMatchesCount(value);
  return `
    <div class="details-options">
      <label for="gamesShownInputInline">Last X games</label>
      <input id="gamesShownInputInline" type="number" min="1" max="20" step="1" value="${safeValue}" />
    </div>
  `;
}

function updateSortButtonLabel() {
  sortModeBtn.textContent = sortMode === "kickoff" ? "Sort: Kickoff" : "Sort: League";
}

function setLeagueFilterOpen(isOpen) {
  leagueFilterMenu.classList.toggle("hidden", !isOpen);
  leagueFilterBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
}

function setTierFilterOpen(isOpen) {
  tierFilterMenu.classList.toggle("hidden", !isOpen);
  tierFilterBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
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

function updateTierFilterButtonLabel() {
  if (!selectedTiers.size) {
    tierFilterBtn.textContent = "All Tiers";
    return;
  }
  if (selectedTiers.size === 1) {
    tierFilterBtn.textContent = Array.from(selectedTiers)[0];
    return;
  }
  tierFilterBtn.textContent = `${selectedTiers.size} tiers`;
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

function updateTierFilterMenu(tiers) {
  tierFilterMenu.innerHTML = "";

  const allRow = document.createElement("label");
  allRow.className = "league-option";
  const allInput = document.createElement("input");
  allInput.type = "checkbox";
  allInput.value = "__ALL__";
  allInput.checked = selectedTiers.size === 0;
  const allText = document.createElement("span");
  allText.textContent = "All Tiers";
  allRow.appendChild(allInput);
  allRow.appendChild(allText);
  tierFilterMenu.appendChild(allRow);

  for (const tier of tiers) {
    const row = document.createElement("label");
    row.className = "league-option";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = tier;
    input.checked = selectedTiers.has(tier);
    const text = document.createElement("span");
    text.textContent = tier;
    row.appendChild(input);
    row.appendChild(text);
    tierFilterMenu.appendChild(row);
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

function updateTierFilterOptions(serverTiers = []) {
  const previous = new Set(selectedTiers);
  const tiers = new Set();

  if (Array.isArray(serverTiers)) {
    for (const tier of serverTiers) {
      const normalized = String(tier || "").trim();
      if (normalized) tiers.add(normalized);
    }
  }

  for (const day of rawDays) {
    for (const game of day.games || []) {
      const tier = String(game.tier || "").trim();
      if (tier) tiers.add(tier);
    }
  }

  availableTiers = Array.from(tiers).sort((a, b) => a.localeCompare(b));
  selectedTiers = new Set(Array.from(previous).filter((tier) => tiers.has(tier)));
  updateTierFilterMenu(availableTiers);
  updateTierFilterButtonLabel();
}

function applyGameFilters() {
  const previousDayIso =
    allDays[currentDayIndex] && allDays[currentDayIndex].date
      ? String(allDays[currentDayIndex].date)
      : null;
  const filteredDays = rawDays.map((day) => ({
    ...day,
    games: (day.games || []).filter((game) => {
      const leagueName = String(game.competition || "");
      const tierName = String(game.tier || "").trim();
      const leaguePass = !selectedLeagues.size || selectedLeagues.has(leagueName);
      const tierPass = !selectedTiers.size || selectedTiers.has(tierName);
      return leaguePass && tierPass;
    }),
  }));

  if (!filteredDays.length) {
    allDays = [];
    currentDayIndex = 0;
    return;
  }

  const dayMap = new Map();
  for (const day of filteredDays) {
    if (!day || !day.date) continue;
    dayMap.set(String(day.date), day);
  }

  const dateKeys = Array.from(dayMap.keys()).sort((a, b) => a.localeCompare(b));
  const first = toDayDate(dateKeys[0]);
  const last = toDayDate(dateKeys[dateKeys.length - 1]);
  if (!first || !last) {
    allDays = filteredDays;
    if (previousDayIso) {
      const nextIndex = allDays.findIndex((d) => String(d.date || "") === previousDayIso);
      currentDayIndex = nextIndex >= 0 ? nextIndex : clampDayIndex(currentDayIndex);
    } else {
      currentDayIndex = clampDayIndex(currentDayIndex);
    }
    return;
  }

  const rangedDays = [];
  for (let cursor = new Date(first); cursor <= last; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    const dayIso = cursor.toISOString().slice(0, 10);
    if (dayMap.has(dayIso)) {
      const existing = dayMap.get(dayIso);
      rangedDays.push({
        ...existing,
        games: Array.isArray(existing.games) ? existing.games : [],
      });
      continue;
    }
    rangedDays.push({
      date: dayIso,
      date_label: formatDayLabelFromIso(dayIso),
      games: [],
    });
  }

  allDays = rangedDays;
  if (previousDayIso) {
    const nextIndex = allDays.findIndex((d) => String(d.date || "") === previousDayIso);
    currentDayIndex = nextIndex >= 0 ? nextIndex : clampDayIndex(currentDayIndex);
  } else {
    currentDayIndex = clampDayIndex(currentDayIndex);
  }
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
    updateTierFilterOptions(payload.tiers || []);
    applyGameFilters();
    renderCurrentDay();
    statusText.textContent = `Loaded ${payload.total_games || 0} games`;
    if (activeTab === "mapping") {
      loadManualMappings();
    }

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
    calendarView.innerHTML = "<p>No games found for selected filters.</p>";
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
        <th>xGD</th>
        <th class="goal-under-price-col">Under</th>
        <th class="goal-line-col">Goals</th>
        <th class="goal-over-price-col">Over</th>
        <th>Min</th>
        <th>Max</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector("tbody");
  if (!sortedDayGames.length) {
    const row = document.createElement("tr");
    row.innerHTML = `<td class="no-games-row" colspan="12">No games</td>`;
    tbody.appendChild(row);
  } else {
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
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_xgd, game.last5_xgd, game.last3_xgd)}</td>
        <td class="goal-under-price-col">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="goal-line-col">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="goal-over-price-col">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg)}</td>
      `;
      row.addEventListener("click", () => loadGameXgd(game.market_id));
      tbody.appendChild(row);
    }
  }

  block.appendChild(header);
  block.appendChild(table);
  calendarView.appendChild(block);
}

function renderXgd(payload) {
  lastXgdPayload = payload;
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
  const periodRowsDisplay = [...periodRows].reverse();
  const periodTable = periodRows.length
    ? `
    <h3 class="section-title">xGD Output</h3>
    <table class="lines-table">
      <thead>
        <tr>
          <th>Period</th>
          <th>xGD</th>
          <th>Home xG</th>
          <th>Away xG</th>
          <th>Total xG</th>
          <th>Min</th>
          <th>Max</th>
        </tr>
      </thead>
      <tbody>
        ${periodRowsDisplay
          .map(
            (r) => `
          <tr>
            <td>${escapeHtml(r.period)}</td>
            <td>${Number(r.xgd || 0).toFixed(2)}</td>
            <td>${Number(r.home_xg || 0).toFixed(2)}</td>
            <td>${Number(r.away_xg || 0).toFixed(2)}</td>
            <td>${Number(r.total_xg || 0).toFixed(2)}</td>
            <td>${r.total_min_xg == null ? "-" : Number(r.total_min_xg).toFixed(2)}</td>
            <td>${r.total_max_xg == null ? "-" : Number(r.total_max_xg).toFixed(2)}</td>
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

  const recentN = clampRecentMatchesCount(payload.recent_n ?? gamesShownCount);
  const venueRecentN = clampRecentMatchesCount(payload.venue_recent_n ?? recentN);
  gamesShownCount = recentN;

  const homeRecentRows = Array.isArray(payload.home_recent_rows) ? payload.home_recent_rows : [];
  const awayRecentRows = Array.isArray(payload.away_recent_rows) ? payload.away_recent_rows : [];
  const homeTeamVenueRows = payload.home_team_venue_rows && typeof payload.home_team_venue_rows === "object" ? payload.home_team_venue_rows : {};
  const awayTeamVenueRows = payload.away_team_venue_rows && typeof payload.away_team_venue_rows === "object" ? payload.away_team_venue_rows : {};
  const mappingHead = mappingRows[0] || {};
  const homeLabel = mappingHead.home_sofa || mappingHead.home_raw || "Home team";
  const awayLabel = mappingHead.away_sofa || mappingHead.away_raw || "Away team";
  const activeIsAway = recentTeamView === "away";
  const activeLabel = activeIsAway ? awayLabel : homeLabel;
  const activeRows = activeIsAway ? awayRecentRows : homeRecentRows;
  const activeVenueRows = activeIsAway ? awayTeamVenueRows : homeTeamVenueRows;
  const activeHomeVenueRows = Array.isArray(activeVenueRows.home) ? activeVenueRows.home : [];
  const activeAwayVenueRows = Array.isArray(activeVenueRows.away) ? activeVenueRows.away : [];
  const recentMatchesSection = `
    <h3 class="section-title">Model Source Matches</h3>
    ${buildGamesShownControlHtml(gamesShownCount)}
    ${buildRecentSwitchHtml(homeLabel, awayLabel)}
    ${buildRecentMatchesTableHtml("Fixture-side matches", activeRows, activeLabel)}
    ${buildVenueSplitSectionHtml(activeLabel, venueRecentN, activeHomeVenueRows, activeAwayVenueRows)}
  `;

  linesContainer.innerHTML = `${warning}${metricHtml}${periodTable}${recentMatchesSection}${mappingTable}`;

  const switchButtons = linesContainer.querySelectorAll(".recent-switch-btn");
  for (const button of switchButtons) {
    button.addEventListener("click", () => {
      const targetView = button.dataset.teamView === "away" ? "away" : "home";
      if (targetView === recentTeamView) return;
      recentTeamView = targetView;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const gamesShownInputInline = linesContainer.querySelector("#gamesShownInputInline");
  if (gamesShownInputInline) {
    gamesShownInputInline.addEventListener("change", () => {
      gamesShownCount = clampRecentMatchesCount(gamesShownInputInline.value);
      gamesShownInputInline.value = String(gamesShownCount);
      if (selectedMarketId) loadGameXgd(selectedMarketId);
    });
  }
}

async function loadGameXgd(marketId) {
  const game = gamesById.get(marketId);
  if (!game) return;

  selectedMarketId = marketId;
  renderCurrentDay();

  detailsPanel.classList.remove("hidden");
  detailsTitle.textContent = game.event_name;
  recentTeamView = "home";
  lastXgdPayload = null;
  detailsMeta.textContent = `${game.competition} | ${game.kickoff_raw}`;
  linesContainer.innerHTML = "<p>Calculating xGD...</p>";
  const gamesShownInputInline = linesContainer.querySelector("#gamesShownInputInline");
  gamesShownCount = clampRecentMatchesCount(gamesShownInputInline ? gamesShownInputInline.value : gamesShownCount);

  try {
    const res = await fetch(
      `/api/game-xgd?market_id=${encodeURIComponent(marketId)}&recent_n=${encodeURIComponent(
        String(gamesShownCount)
      )}&venue_recent_n=${encodeURIComponent(String(gamesShownCount))}`
    );
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load xGD");
    renderXgd(payload);
  } catch (err) {
    linesContainer.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
  }
}

refreshBtn.addEventListener("click", () => loadGames());
gamesTabBtn.addEventListener("click", () => {
  setActiveTab("games");
});
manualMappingTabBtn.addEventListener("click", () => {
  setActiveTab("mapping");
  loadManualMappings();
});
mappingRefreshBtn.addEventListener("click", () => {
  loadManualMappings();
});
leagueFilterBtn.addEventListener("click", (event) => {
  event.stopPropagation();
  const willOpen = leagueFilterMenu.classList.contains("hidden");
  setLeagueFilterOpen(willOpen);
  if (willOpen) setTierFilterOpen(false);
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
  applyGameFilters();
  renderCurrentDay();
});
tierFilterBtn.addEventListener("click", (event) => {
  event.stopPropagation();
  const willOpen = tierFilterMenu.classList.contains("hidden");
  setTierFilterOpen(willOpen);
  if (willOpen) setLeagueFilterOpen(false);
});
tierFilterMenu.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
  const value = String(target.value || "");
  if (!value) return;

  if (value === "__ALL__") {
    selectedTiers.clear();
  } else if (target.checked) {
    selectedTiers.add(value);
  } else {
    selectedTiers.delete(value);
  }

  updateTierFilterMenu(availableTiers);
  updateTierFilterButtonLabel();
  applyGameFilters();
  renderCurrentDay();
});
document.addEventListener("click", (event) => {
  if (!leagueFilter.contains(event.target)) {
    setLeagueFilterOpen(false);
  }
  if (!tierFilter.contains(event.target)) {
    setTierFilterOpen(false);
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    setLeagueFilterOpen(false);
    setTierFilterOpen(false);
  }
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

updateSortButtonLabel();
updateLeagueFilterButtonLabel();
updateTierFilterButtonLabel();
setActiveTab("games");
loadGames();
setInterval(() => {
  loadGames();
}, AUTO_REFRESH_MS);
