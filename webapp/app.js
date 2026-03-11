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
const unmatchedCompetitionsContainer = document.getElementById("unmatchedCompetitionsContainer");
const savedCompetitionMappingsContainer = document.getElementById("savedCompetitionMappingsContainer");
const teamMappingsSubTabBtn = document.getElementById("teamMappingsSubTabBtn");
const competitionMappingsSubTabBtn = document.getElementById("competitionMappingsSubTabBtn");
const teamMappingsPane = document.getElementById("teamMappingsPane");
const competitionMappingsPane = document.getElementById("competitionMappingsPane");
const detailsPanel = document.getElementById("detailsPanel");
const closeDetails = document.getElementById("closeDetails");
const recalcBtn = document.getElementById("recalcBtn");
const detailsTitle = document.getElementById("detailsTitle");
const detailsMeta = document.getElementById("detailsMeta");
const linesContainer = document.getElementById("linesContainer");
const prevDayBtn = document.getElementById("prevDayBtn");
const todayBtn = document.getElementById("todayBtn");
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
let gamesShownCount = 0;
let gamesShownAuto = true;
let rollingWindowCount = 3;
let showTrendCharts = false;
let recentTeamView = "home";
let statsGamesShownCount = 0;
let statsGamesShownAuto = true;
let statsTeamView = "home";
let lastXgdPayload = null;
let activeXgdViewId = null;
let detailsMainTab = "xgd";
let activeTab = "games";
let mappingSubTab = "teams";
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
    setMappingSubTab(mappingSubTab);
  }
}

function setMappingSubTab(tabName) {
  mappingSubTab = tabName === "competitions" ? "competitions" : "teams";
  const teamsActive = mappingSubTab === "teams";
  teamMappingsSubTabBtn.classList.toggle("active", teamsActive);
  competitionMappingsSubTabBtn.classList.toggle("active", !teamsActive);
  teamMappingsPane.classList.toggle("hidden", !teamsActive);
  competitionMappingsPane.classList.toggle("hidden", teamsActive);
}

function renderManualMappingSections(payload) {
  lastManualMappingPayload = payload;
  const teamMappings = Array.isArray(payload?.mappings) ? payload.mappings : [];
  const unmatchedTeams = Array.isArray(payload?.unmatched) ? payload.unmatched : [];
  const sofaTeams = Array.isArray(payload?.sofa_teams) ? payload.sofa_teams : [];
  const manualMappings = teamMappings.filter((row) => row?.is_manual !== false);
  const autoMappings = teamMappings.filter((row) => row?.is_manual === false);
  const manualCountRaw = Number(payload?.manual_count);
  const autoCountRaw = Number(payload?.auto_count);
  const manualCount = Number.isFinite(manualCountRaw) ? manualCountRaw : manualMappings.length;
  const autoCount = Number.isFinite(autoCountRaw) ? autoCountRaw : autoMappings.length;
  const mappedSofaNames = new Set(
    teamMappings
      .map((row) => String(row?.sofa_name || "").trim())
      .filter((name) => !!name)
  );
  const availableSofaTeams = sofaTeams
    .map((team) => String(team || "").trim())
    .filter((team) => !!team && !mappedSofaNames.has(team));
  const competitionMappings = Array.isArray(payload?.competition_mappings) ? payload.competition_mappings : [];
  const unmatchedCompetitions = Array.isArray(payload?.unmatched_competitions)
    ? payload.unmatched_competitions
    : [];
  const sofaCompetitions = Array.isArray(payload?.sofa_competitions) ? payload.sofa_competitions : [];
  const manualCompetitionMappings = competitionMappings.filter((row) => row?.is_manual !== false);
  const autoCompetitionMappings = competitionMappings.filter((row) => row?.is_manual === false);
  const manualCompetitionCountRaw = Number(payload?.manual_competition_count);
  const autoCompetitionCountRaw = Number(payload?.auto_competition_count);
  const manualCompetitionCount = Number.isFinite(manualCompetitionCountRaw)
    ? manualCompetitionCountRaw
    : manualCompetitionMappings.length;
  const autoCompetitionCount = Number.isFinite(autoCompetitionCountRaw)
    ? autoCompetitionCountRaw
    : autoCompetitionMappings.length;
  const mappedSofaCompetitionNames = new Set(
    competitionMappings
      .map((row) => String(row?.sofa_name || "").trim())
      .filter((name) => !!name)
  );
  const availableSofaCompetitions = sofaCompetitions
    .map((competition) => String(competition || "").trim())
    .filter((competition) => !!competition && !mappedSofaCompetitionNames.has(competition));

  mappingStatus.textContent =
    `${unmatchedTeams.length} unmatched teams | ${manualCount} manual, ${autoCount} auto teams | ` +
    `${unmatchedCompetitions.length} unmatched competitions | ` +
    `${manualCompetitionCount} manual, ${autoCompetitionCount} auto competitions`;

  unmatchedTeamsContainer.innerHTML = "";
  if (!unmatchedTeams.length) {
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

    for (const row of unmatchedTeams) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const existing = teamMappings.find((m) => String(m.raw_name || "") === rawName);
      const rowOptions = [...availableSofaTeams];
      const existingSofaName = String(existing?.sofa_name || "").trim();
      if (existingSofaName && !rowOptions.includes(existingSofaName)) {
        rowOptions.unshift(existingSofaName);
      }
      const optionsHtml =
        `<option value="">Select team...</option>` +
        rowOptions.map((team) => `<option value="${escapeHtml(team)}">${escapeHtml(team)}</option>`).join("");
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
          await upsertManualTeamMapping(rawName, sofaName);
        });
      }
      tbody.appendChild(tr);
    }
    unmatchedTeamsContainer.appendChild(table);
  }

  savedMappingsContainer.innerHTML = "";
  if (!teamMappings.length) {
    savedMappingsContainer.innerHTML = `<p class="mapping-empty">No mappings available yet.</p>`;
  } else {
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Betfair Team</th>
          <th>SofaScore Team</th>
          <th>Type</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const row of teamMappings) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const isManual = row?.is_manual !== false;
      const method = String(row.match_method || "").trim().toLowerCase();
      const methodLabel = method ? `${method.charAt(0).toUpperCase()}${method.slice(1)}` : "Auto";
      const typeLabel = isManual ? "Manual" : `Auto (${methodLabel})`;
      const actionHtml = isManual
        ? `<button type="button" class="mapping-delete-btn">Delete</button>`
        : `<span class="mapping-auto-badge">Auto</span>`;
      tr.innerHTML = `
        <td>${escapeHtml(rawName)}</td>
        <td>${escapeHtml(String(row.sofa_name || ""))}</td>
        <td>${escapeHtml(typeLabel)}</td>
        <td>${actionHtml}</td>
      `;
      const deleteBtn = tr.querySelector(".mapping-delete-btn");
      if (deleteBtn && isManual) {
        deleteBtn.addEventListener("click", async () => {
          await deleteManualTeamMapping(rawName);
        });
      }
      tbody.appendChild(tr);
    }
    savedMappingsContainer.appendChild(table);
  }

  unmatchedCompetitionsContainer.innerHTML = "";
  if (!unmatchedCompetitions.length) {
    unmatchedCompetitionsContainer.innerHTML = `<p class="mapping-empty">No unmatched competitions in current games.</p>`;
  } else {
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Betfair Competition</th>
          <th>Games</th>
          <th>Next Kickoff</th>
          <th>SofaScore Competition</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const row of unmatchedCompetitions) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const existing = competitionMappings.find((m) => String(m.raw_name || "") === rawName);
      const rowOptions = [...availableSofaCompetitions];
      const existingSofaName = String(existing?.sofa_name || "").trim();
      if (existingSofaName && !rowOptions.includes(existingSofaName)) {
        rowOptions.unshift(existingSofaName);
      }
      const optionsHtml =
        `<option value="">Select competition...</option>` +
        rowOptions.map((competition) => `<option value="${escapeHtml(competition)}">${escapeHtml(competition)}</option>`).join("");
      tr.innerHTML = `
        <td>${escapeHtml(rawName)}</td>
        <td>${escapeHtml(String(row.games_count ?? "-"))}</td>
        <td>${escapeHtml(String(row.next_kickoff || "-"))}</td>
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
            mappingStatus.textContent = "Select a SofaScore competition before saving.";
            return;
          }
          await upsertManualCompetitionMapping(rawName, sofaName);
        });
      }
      tbody.appendChild(tr);
    }
    unmatchedCompetitionsContainer.appendChild(table);
  }

  savedCompetitionMappingsContainer.innerHTML = "";
  if (!competitionMappings.length) {
    savedCompetitionMappingsContainer.innerHTML = `<p class="mapping-empty">No competition mappings available yet.</p>`;
  } else {
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Betfair Competition</th>
          <th>SofaScore Competition</th>
          <th>Type</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const row of competitionMappings) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const isManual = row?.is_manual !== false;
      const method = String(row.match_method || "").trim().toLowerCase();
      const methodLabel = method ? `${method.charAt(0).toUpperCase()}${method.slice(1)}` : "Auto";
      const typeLabel = isManual ? "Manual" : `Auto (${methodLabel})`;
      const actionHtml = isManual
        ? `<button type="button" class="mapping-delete-btn">Delete</button>`
        : `<span class="mapping-auto-badge">Auto</span>`;
      tr.innerHTML = `
        <td>${escapeHtml(rawName)}</td>
        <td>${escapeHtml(String(row.sofa_name || ""))}</td>
        <td>${escapeHtml(typeLabel)}</td>
        <td>${actionHtml}</td>
      `;
      const deleteBtn = tr.querySelector(".mapping-delete-btn");
      if (deleteBtn && isManual) {
        deleteBtn.addEventListener("click", async () => {
          await deleteManualCompetitionMapping(rawName);
        });
      }
      tbody.appendChild(tr);
    }
    savedCompetitionMappingsContainer.appendChild(table);
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

async function upsertManualTeamMapping(rawName, sofaName) {
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

async function deleteManualTeamMapping(rawName) {
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

async function upsertManualCompetitionMapping(rawName, sofaName) {
  try {
    const res = await fetch("/api/manual-competition-mappings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ raw_name: rawName, sofa_name: sofaName }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to save competition mapping");
    await Promise.all([loadGames(), loadManualMappings()]);
  } catch (err) {
    mappingStatus.textContent = String(err.message || err);
  }
}

async function deleteManualCompetitionMapping(rawName) {
  try {
    const res = await fetch("/api/manual-competition-mappings/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ raw_name: rawName }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to delete competition mapping");
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

function getTodayIsoUtc() {
  return new Date().toISOString().slice(0, 10);
}

function jumpToTodayDay() {
  if (!allDays.length) return;
  const todayIso = getTodayIsoUtc();
  const targetIndex = allDays.findIndex((day) => String(day.date || "") === todayIso);
  if (targetIndex < 0) return;
  currentDayIndex = targetIndex;
  renderCurrentDay();
}

function clampRecentMatchesCount(value) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (Number.isNaN(parsed)) return 1;
  return Math.max(1, parsed);
}

function formatMetricValue(value, decimals = 2) {
  if (value == null || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return escapeHtml(value);
  return num.toFixed(decimals);
}

function buildXgdPeriodTableHtml(periodRows, title, warningText = "") {
  const rows = Array.isArray(periodRows) ? periodRows : [];
  if (!rows.length) return "";
  const heading = String(title || "").trim();
  const warning = String(warningText || "").trim();
  return `
    ${heading ? `<h3 class="section-title">${escapeHtml(heading)}</h3>` : ""}
    ${warning ? `<div class="warning">${escapeHtml(warning)}</div>` : ""}
    <table class="lines-table">
      <thead>
        <tr>
          <th>Period</th>
          <th>xStrength</th>
          <th>xGD Perf</th>
          <th>Home xG</th>
          <th>Away xG</th>
          <th>Total xG</th>
          <th>Min</th>
          <th>Max</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (r) => `
          <tr>
            <td>${escapeHtml(r.period)}</td>
            <td>${formatMetricValue(r.strength, 2)}</td>
            <td>${formatMetricValue(r.xgd, 2)}</td>
            <td>${formatMetricValue(r.home_xg, 2)}</td>
            <td>${formatMetricValue(r.away_xg, 2)}</td>
            <td>${formatMetricValue(r.total_xg, 2)}</td>
            <td>${formatMetricValue(r.total_min_xg, 2)}</td>
            <td>${formatMetricValue(r.total_max_xg, 2)}</td>
          </tr>
        `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function buildPeriodMetricStackCell(seasonValue, last5Value, last3Value, highlighted = false) {
  const isMissing = (value) => {
    const text = String(value ?? "").trim();
    return !text || text === "-";
  };
  const stackClass = highlighted ? "metric-stack metric-stack-mismatch" : "metric-stack";

  const rows = [
    { label: "S", value: seasonValue },
    { label: "5", value: last5Value },
    { label: "3", value: last3Value },
  ].filter((row) => !isMissing(row.value));

  if (!rows.length) {
    return `
      <div class="${stackClass}">
        <div class="metric-stack-row">
          <span class="metric-stack-value">-</span>
        </div>
      </div>
    `;
  }

  return `
    <div class="${stackClass}">
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
    ? `<strong>${escapeHtml(relevantTeamName)}</strong> - `
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

function toMetricNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function buildMetricTrendPlotHtml(rows, title, relevantTeamName = "", rollingWindowGames = 5) {
  const headingPrefix = relevantTeamName ? `<strong>${escapeHtml(relevantTeamName)}</strong> - ` : "";
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${headingPrefix}${escapeHtml(title)}</h4>
        <p class="recent-empty">No trend data available.</p>
      </section>
    `;
  }

  const parseSortTs = (rawValue) => {
    const text = String(rawValue || "").trim();
    if (!text) return Number.NaN;
    const isoLike = text.includes("T") ? text : text.replace(" ", "T");
    const utcTry = Date.parse(`${isoLike}Z`);
    if (Number.isFinite(utcTry)) return utcTry;
    const localTry = Date.parse(isoLike);
    if (Number.isFinite(localTry)) return localTry;
    return Number.NaN;
  };

  const points = safeRows
    .map((row, idx) => {
      const xg = toMetricNumber(row.xG);
      const xga = toMetricNumber(row.xGA);
      return {
        idx,
        row,
        sortTs: parseSortTs(row.date_time),
        dateText: String(row.date_time || "").trim(),
        xg,
        xga,
      };
    })
    .sort((a, b) => {
      const aTsValid = Number.isFinite(a.sortTs);
      const bTsValid = Number.isFinite(b.sortTs);
      if (aTsValid && bTsValid) return a.sortTs - b.sortTs;
      if (aTsValid) return -1;
      if (bTsValid) return 1;
      return a.idx - b.idx;
    });

  const values = [];
  for (const point of points) {
    if (point.xg != null) values.push(point.xg);
    if (point.xga != null) values.push(point.xga);
  }
  if (!values.length) {
    return `
      <section class="recent-team-block">
        <h4>${headingPrefix}${escapeHtml(title)}</h4>
        <p class="recent-empty">No trend data available.</p>
      </section>
    `;
  }

  let yMin = Math.min(...values);
  let yMax = Math.max(...values);
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }

  const width = 860;
  const height = 220;
  const padLeft = 44;
  const padRight = 14;
  const padTop = 12;
  const padBottom = 30;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;

  const xForIndex = (index) => {
    if (points.length <= 1) return padLeft + (plotWidth / 2);
    return padLeft + ((index / (points.length - 1)) * plotWidth);
  };
  const yForValue = (value) => padTop + (((yMax - value) / (yMax - yMin)) * plotHeight);

  const buildSmoothedTrendPath = (metricKey) => {
    const rawValues = points.map((point) => {
      const value = point[metricKey];
      return Number.isFinite(value) ? value : null;
    });
    const validCount = rawValues.reduce((count, value) => (value == null ? count : count + 1), 0);
    if (!validCount) return "";

    const windowSize = Math.max(1, Math.min(points.length, clampRecentMatchesCount(rollingWindowGames)));
    const smoothed = rawValues.map((_, index) => {
      let sum = 0;
      let count = 0;
      const start = Math.max(0, index - windowSize + 1);
      const end = index;
      for (let pos = start; pos <= end; pos += 1) {
        const value = rawValues[pos];
        if (value == null) continue;
        sum += value;
        count += 1;
      }
      if (!count) return null;
      return sum / count;
    });

    const parts = [];
    let drawMode = "M";
    for (let i = 0; i < smoothed.length; i += 1) {
      const value = smoothed[i];
      if (!Number.isFinite(value)) {
        drawMode = "M";
        continue;
      }
      parts.push(`${drawMode} ${xForIndex(i).toFixed(2)} ${yForValue(value).toFixed(2)}`);
      drawMode = "L";
    }
    return parts.join(" ");
  };

  const yTicks = [yMax, (yMax + yMin) / 2, yMin];
  const firstLabel = points[0]?.dateText ? String(points[0].dateText).slice(0, 10) : "-";
  const lastLabel = points[points.length - 1]?.dateText ? String(points[points.length - 1].dateText).slice(0, 10) : "-";

  const xgColor = "#0f766e";
  const xgaColor = "#b91c1c";
  const xgTrendPath = buildSmoothedTrendPath("xg");
  const xgaTrendPath = buildSmoothedTrendPath("xga");

  return `
    <section class="recent-team-block">
      <h4>${headingPrefix}${escapeHtml(title)}</h4>
      <div class="recent-table-wrap">
        <div class="trend-plot-wrap">
          <svg class="trend-plot" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(
            `${relevantTeamName || "Team"} trendlines for xG and xGA`
          )}">
            ${yTicks
              .map(
                (tick) => `
              <line x1="${padLeft}" y1="${yForValue(tick).toFixed(2)}" x2="${width - padRight}" y2="${yForValue(tick).toFixed(
                2
              )}" class="trend-grid-line" />
              <text x="${padLeft - 6}" y="${(yForValue(tick) + 4).toFixed(2)}" text-anchor="end" class="trend-axis-text">${formatMetricValue(
                tick,
                2
              )}</text>
            `
              )
              .join("")}
            <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}" class="trend-axis-line" />
            <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" class="trend-axis-line" />
            ${xgaTrendPath ? `<path d="${xgaTrendPath}" fill="none" stroke="${xgaColor}" stroke-width="2.5" />` : ""}
            ${xgTrendPath ? `<path d="${xgTrendPath}" fill="none" stroke="${xgColor}" stroke-width="2.5" />` : ""}
            <text x="${padLeft}" y="${height - 8}" text-anchor="start" class="trend-axis-text">${escapeHtml(firstLabel)}</text>
            <text x="${width - padRight}" y="${height - 8}" text-anchor="end" class="trend-axis-text">${escapeHtml(lastLabel)}</text>
          </svg>
          <div class="trend-legend">
            <span class="trend-legend-item"><i class="trend-swatch" style="--swatch-color:${xgColor};"></i>xG</span>
            <span class="trend-legend-item"><i class="trend-swatch" style="--swatch-color:${xgaColor};"></i>xGA</span>
          </div>
        </div>
      </div>
    </section>
  `;
}

function buildRecentAveragesTableHtml(rows, sampleSize, relevantTeamName = "") {
  const headingPrefix = relevantTeamName ? `<strong>${escapeHtml(relevantTeamName)}</strong> - ` : "";
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${headingPrefix}Averages</h4>
        <p class="recent-empty">No recent matches available.</p>
      </section>
    `;
  }

  const limitedRows = safeRows.slice(0, Math.max(1, Math.min(safeRows.length, clampRecentMatchesCount(sampleSize))));
  const averageOf = (reader) => {
    let total = 0;
    let count = 0;
    for (const row of limitedRows) {
      const value = reader(row);
      if (value == null) continue;
      total += value;
      count += 1;
    }
    if (!count) return null;
    return total / count;
  };

  const avgGf = averageOf((row) => toMetricNumber(row.GF));
  const avgGa = averageOf((row) => toMetricNumber(row.GA));
  const avgXg = averageOf((row) => toMetricNumber(row.xG));
  const avgXga = averageOf((row) => toMetricNumber(row.xGA));
  const avgXgot = averageOf((row) => toMetricNumber(row.xGoT));
  const avgXgota = averageOf((row) => toMetricNumber(row.xGoTA));
  const avgXgd = averageOf((row) => {
    const xg = toMetricNumber(row.xG);
    const xga = toMetricNumber(row.xGA);
    if (xg == null || xga == null) return null;
    return xg - xga;
  });

  return `
    <section class="recent-team-block">
      <h4>${headingPrefix}Averages (Last ${limitedRows.length} Games)</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Games</th>
              <th>GF</th>
              <th>GA</th>
              <th>xG</th>
              <th>xGA</th>
              <th>xGD</th>
              <th>xGoT</th>
              <th>xGoTA</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${limitedRows.length}</td>
              <td>${formatMetricValue(avgGf, 2)}</td>
              <td>${formatMetricValue(avgGa, 2)}</td>
              <td>${formatMetricValue(avgXg, 2)}</td>
              <td>${formatMetricValue(avgXga, 2)}</td>
              <td>${formatMetricValue(avgXgd, 2)}</td>
              <td>${formatMetricValue(avgXgot, 2)}</td>
              <td>${formatMetricValue(avgXgota, 2)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function averageMetric(rows, metricKey) {
  const safeRows = Array.isArray(rows) ? rows : [];
  let total = 0;
  let count = 0;
  for (const row of safeRows) {
    const value = toMetricNumber(row?.[metricKey]);
    if (value == null) continue;
    total += value;
    count += 1;
  }
  if (!count) return null;
  return total / count;
}

function limitStatsRows(rows, sampleSize) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const maxRows = safeRows.length;
  if (sampleSize == null || !Number.isFinite(sampleSize)) return safeRows;
  const safeSample = Math.max(1, Math.min(maxRows, clampRecentMatchesCount(sampleSize)));
  return safeRows.slice(0, safeSample);
}

function buildCardsCornersAveragesTableHtml(homeRows, awayRows, homeLabel, awayLabel, sampleSize = null) {
  const sampleLabel = sampleSize == null ? "All Previous Games" : `Last ${clampRecentMatchesCount(sampleSize)} Games`;
  const entries = [
    { label: homeLabel || "Home team", rows: limitStatsRows(homeRows || [], sampleSize) },
    { label: awayLabel || "Away team", rows: limitStatsRows(awayRows || [], sampleSize) },
  ];
  return `
    <section class="recent-team-block">
      <h4>Average Corners/Cards (${escapeHtml(sampleLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Team</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
            </tr>
          </thead>
          <tbody>
            ${entries
              .map(
                (entry) => `
              <tr>
                <td><strong>${escapeHtml(entry.label)}</strong></td>
                <td>${formatMetricValue(averageMetric(entry.rows, "corners_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "corners_against"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "cards_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "cards_against"), 2)}</td>
              </tr>
            `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildCardsCornersVenueTableHtml(homeVenueRows, awayVenueRows, homeLabel, awayLabel, sampleSize = null) {
  const sampleLabel = sampleSize == null ? "All Previous Games" : `Last ${clampRecentMatchesCount(sampleSize)} Games`;
  const entries = [
    { label: homeLabel || "Home team", venue: "Home", rows: limitStatsRows(homeVenueRows, sampleSize) },
    { label: awayLabel || "Away team", venue: "Away", rows: limitStatsRows(awayVenueRows, sampleSize) },
  ];
  return `
    <section class="recent-team-block">
      <h4>Average Corners/Cards At Fixture Venues (${escapeHtml(sampleLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Team</th>
              <th>Venue</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
            </tr>
          </thead>
          <tbody>
            ${entries
              .map(
                (entry) => `
              <tr>
                <td><strong>${escapeHtml(entry.label)}</strong></td>
                <td>${escapeHtml(entry.venue)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "corners_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "corners_against"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "cards_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "cards_against"), 2)}</td>
              </tr>
            `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildCardsCornersMatchesTableHtml(teamLabel, rows, sampleSize = null) {
  const safeRows = Array.isArray(rows) ? [...rows] : [];
  safeRows.sort((a, b) => String(b?.date_time || "").localeCompare(String(a?.date_time || "")));
  const shownRows = limitStatsRows(safeRows, sampleSize);
  const titleSuffix = sampleSize == null ? "" : ` (Last ${clampRecentMatchesCount(sampleSize)})`;
  if (!shownRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${escapeHtml(teamLabel || "Team")} - Previous Games${escapeHtml(titleSuffix)}</h4>
        <p class="recent-empty">No previous games available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} - Previous Games${escapeHtml(titleSuffix)}</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Competition</th>
              <th>Venue</th>
              <th>Opponent</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
            </tr>
          </thead>
          <tbody>
            ${shownRows
              .map(
                (row) => `
              <tr>
                <td>${escapeHtml(row?.date_time || "-")}</td>
                <td>${escapeHtml(row?.competition_name || "-")}</td>
                <td>${escapeHtml(row?.venue || "-")}</td>
                <td>${escapeHtml(row?.opponent || "-")}</td>
                <td>${formatMetricValue(row?.corners_for, 2)}</td>
                <td>${formatMetricValue(row?.corners_against, 2)}</td>
                <td>${formatMetricValue(row?.cards_for, 2)}</td>
                <td>${formatMetricValue(row?.cards_against, 2)}</td>
              </tr>
            `
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

function buildStatsSwitchHtml(homeLabel, awayLabel) {
  const homeActive = statsTeamView !== "away";
  const awayActive = statsTeamView === "away";
  return `
    <div class="recent-switch" role="tablist" aria-label="Stats previous games team view">
      <button
        type="button"
        class="recent-switch-btn stats-switch-btn${homeActive ? " active" : ""}"
        data-stats-team-view="home"
        role="tab"
        aria-selected="${homeActive ? "true" : "false"}"
      >
        Home: ${escapeHtml(homeLabel)}
      </button>
      <button
        type="button"
        class="recent-switch-btn stats-switch-btn${awayActive ? " active" : ""}"
        data-stats-team-view="away"
        role="tab"
        aria-selected="${awayActive ? "true" : "false"}"
      >
        Away: ${escapeHtml(awayLabel)}
      </button>
    </div>
  `;
}

function buildVenueSplitSectionHtml(teamLabel, homeRows, awayRows, rollingWindowGames = 5, includeChart = true) {
  const mergedRows = [...homeRows, ...awayRows].sort((a, b) =>
    String(b.date_time || "").localeCompare(String(a.date_time || ""))
  );
  return `
    <h3 class="section-title">${escapeHtml(teamLabel)}: Home & Away Matches</h3>
    ${includeChart ? buildMetricTrendPlotHtml(mergedRows, "Total games trend", teamLabel, rollingWindowGames) : ""}
    ${buildRecentMatchesTableHtml("All matches", mergedRows, teamLabel)}
  `;
}

function buildGamesShownControlHtml(value, maxValue, rollingMaxValue = maxValue) {
  const safeMax = Number.isFinite(maxValue) && maxValue > 0 ? Math.floor(maxValue) : 1;
  const safeValue = Math.max(1, Math.min(safeMax, clampRecentMatchesCount(value)));
  const safeRollingMax = Number.isFinite(rollingMaxValue) && rollingMaxValue > 0 ? Math.floor(rollingMaxValue) : safeMax;
  const safeRollingValue = Math.max(1, Math.min(safeRollingMax, clampRecentMatchesCount(rollingWindowCount)));
  const showChartsChecked = showTrendCharts ? " checked" : "";
  return `
    <div class="details-options">
      <label for="gamesShownInputInline">Last X games</label>
      <input id="gamesShownInputInline" type="number" min="1" max="${safeMax}" step="1" value="${safeValue}" />
      <label class="checkbox-label" for="showTrendChartsToggle">Show charts</label>
      <input id="showTrendChartsToggle" type="checkbox"${showChartsChecked} />
      <label for="rollingWindowInputInline">Rolling avg games</label>
      <input id="rollingWindowInputInline" type="number" min="1" max="${safeRollingMax}" step="1" value="${safeRollingValue}" />
    </div>
  `;
}

function buildStatsGamesShownControlHtml(value, maxValue) {
  const safeMax = Number.isFinite(maxValue) && maxValue > 0 ? Math.floor(maxValue) : 1;
  const safeValue = Math.max(1, Math.min(safeMax, clampRecentMatchesCount(value)));
  return `
    <div class="details-options">
      <label for="statsGamesShownInput">Last X games</label>
      <input id="statsGamesShownInput" type="number" min="1" max="${safeMax}" step="1" value="${safeValue}" />
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
    todayBtn.disabled = true;
    nextDayBtn.disabled = true;
    calendarView.innerHTML = "<p>No games found for selected filters.</p>";
    return;
  }

  currentDayIndex = clampDayIndex(currentDayIndex);
  const day = allDays[currentDayIndex];
  const sortedDayGames = sortGamesForDay(day.games || []);
  const todayIndex = allDays.findIndex((entry) => String(entry.date || "") === getTodayIsoUtc());
  dayLabel.textContent = `${day.date_label} (${sortedDayGames.length})`;
  prevDayBtn.disabled = currentDayIndex === 0;
  todayBtn.disabled = todayIndex < 0 || currentDayIndex === todayIndex;
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
        <th>xStrength</th>
        <th>xGD Perf</th>
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
    row.innerHTML = `<td class="no-games-row" colspan="13">No games</td>`;
    tbody.appendChild(row);
  } else {
    for (const game of sortedDayGames) {
      gamesById.set(game.market_id, game);
      const row = document.createElement("tr");
      row.dataset.marketId = game.market_id;
      if (selectedMarketId === game.market_id) row.classList.add("selected");
      const xgdMismatch = Boolean(game.xgd_competition_mismatch);
      const mismatchMetricCellClass = xgdMismatch ? "metric-stack-cell xgd-mismatch-cell" : "metric-stack-cell";
      const mismatchMetricCellTitle = xgdMismatch
        ? ` title="xGD derived from a different competition than this fixture."`
        : "";
      row.innerHTML = `
        <td>${escapeHtml(game.kickoff_utc)}</td>
        <td>${escapeHtml(game.competition)}</td>
        <td>${escapeHtml(game.event_name)}</td>
        <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
        <td class="line-col">${escapeHtml(game.mainline || "-")}</td>
        <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
        <td class="${mismatchMetricCellClass}"${mismatchMetricCellTitle}>${buildPeriodMetricStackCell(game.season_strength, game.last5_strength, game.last3_strength, xgdMismatch)}</td>
        <td class="${mismatchMetricCellClass}"${mismatchMetricCellTitle}>${buildPeriodMetricStackCell(game.season_xgd, game.last5_xgd, game.last3_xgd, xgdMismatch)}</td>
        <td class="goal-under-price-col">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="goal-line-col">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="goal-over-price-col">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="${mismatchMetricCellClass}"${mismatchMetricCellTitle}>${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg, xgdMismatch)}</td>
        <td class="${mismatchMetricCellClass}"${mismatchMetricCellTitle}>${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg, xgdMismatch)}</td>
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
  const payloadPeriodRows = Array.isArray(payload.period_rows) ? payload.period_rows : [];
  const payloadHomeRecentRows = Array.isArray(payload.home_recent_rows) ? payload.home_recent_rows : [];
  const payloadAwayRecentRows = Array.isArray(payload.away_recent_rows) ? payload.away_recent_rows : [];
  const payloadHomeVenueRows =
    payload.home_team_venue_rows && typeof payload.home_team_venue_rows === "object"
      ? payload.home_team_venue_rows
      : { home: [], away: [] };
  const payloadAwayVenueRows =
    payload.away_team_venue_rows && typeof payload.away_team_venue_rows === "object"
      ? payload.away_team_venue_rows
      : { home: [], away: [] };
  const fallbackView = {
    id: "fixture",
    label: "Fixture",
    summary: payload.summary || null,
    period_rows: payloadPeriodRows,
    warning: payload.warning || "",
    home_recent_rows: payloadHomeRecentRows,
    away_recent_rows: payloadAwayRecentRows,
    home_team_venue_rows: payloadHomeVenueRows,
    away_team_venue_rows: payloadAwayVenueRows,
  };

  const providedViews = Array.isArray(payload.xgd_views) ? payload.xgd_views : [];
  const xgdViewsRaw = providedViews.length ? providedViews : [fallbackView];
  const xgdViews = xgdViewsRaw.map((view, idx) => ({
    id: String(view?.id || `view-${idx + 1}`),
    label: String(view?.label || `View ${idx + 1}`),
    summary: view?.summary || null,
    period_rows: Array.isArray(view?.period_rows) ? view.period_rows : [],
    warning: String(view?.warning || ""),
    home_recent_rows: Array.isArray(view?.home_recent_rows) ? view.home_recent_rows : [],
    away_recent_rows: Array.isArray(view?.away_recent_rows) ? view.away_recent_rows : [],
    home_team_venue_rows:
      view?.home_team_venue_rows && typeof view.home_team_venue_rows === "object"
        ? view.home_team_venue_rows
        : { home: [], away: [] },
    away_team_venue_rows:
      view?.away_team_venue_rows && typeof view.away_team_venue_rows === "object"
        ? view.away_team_venue_rows
        : { home: [], away: [] },
  }));

  if (!xgdViews.length) {
    linesContainer.innerHTML = "<p>No xGD output available.</p>";
    return;
  }
  if (!activeXgdViewId || !xgdViews.some((view) => view.id === activeXgdViewId)) {
    activeXgdViewId = xgdViews[0].id;
  }
  const activeView = xgdViews.find((view) => view.id === activeXgdViewId) || xgdViews[0];

  const viewTabsHtml = xgdViews.length > 1
    ? `
    <section class="page-tabs xgd-view-tabs">
      ${xgdViews
        .map(
          (view) => `
        <button type="button" class="tab-btn xgd-view-tab ${view.id === activeView.id ? "active" : ""}" data-xgd-view-id="${escapeHtml(view.id)}">
          ${escapeHtml(view.label)}
        </button>
      `
        )
        .join("")}
    </section>
  `
    : "";

  const warning = activeView.warning ? `<div class="warning">${escapeHtml(activeView.warning)}</div>` : "";
  const s = activeView.summary || {};
  const metricHtml = activeView.summary
    ? `
    <section class="metric-grid">
      <article class="metric-card"><div class="metric-label">Home xG</div><div class="metric-value">${formatMetricValue(s.home_xg, 2)}</div></article>
      <article class="metric-card"><div class="metric-label">Away xG</div><div class="metric-value">${formatMetricValue(s.away_xg, 2)}</div></article>
      <article class="metric-card"><div class="metric-label">Total xG</div><div class="metric-value">${formatMetricValue(s.total_xg, 2)}</div></article>
      <article class="metric-card"><div class="metric-label">xGD</div><div class="metric-value">${formatMetricValue(s.xgd, 2)}</div></article>
      <article class="metric-card"><div class="metric-label">Strength</div><div class="metric-value">${formatMetricValue(s.strength, 2)}</div></article>
    </section>
  `
    : "";

  const periodRowsRaw = Array.isArray(activeView.period_rows) ? activeView.period_rows : [];
  const periodRows = periodRowsRaw.length ? periodRowsRaw : payloadPeriodRows;
  const periodTable = buildXgdPeriodTableHtml(periodRows, "xGD Output");

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

  const normalizeVenueRows = (rowsObj) => ({
    home: Array.isArray(rowsObj?.home) ? rowsObj.home : [],
    away: Array.isArray(rowsObj?.away) ? rowsObj.away : [],
  });
  const hasAnyVenueRows = (rowsObj) => rowsObj.home.length > 0 || rowsObj.away.length > 0;
  const homeRecentRowsRaw = Array.isArray(activeView.home_recent_rows) ? activeView.home_recent_rows : [];
  const awayRecentRowsRaw = Array.isArray(activeView.away_recent_rows) ? activeView.away_recent_rows : [];
  const homeRecentRows = homeRecentRowsRaw.length ? homeRecentRowsRaw : payloadHomeRecentRows;
  const awayRecentRows = awayRecentRowsRaw.length ? awayRecentRowsRaw : payloadAwayRecentRows;
  const homeTeamVenueRowsRaw = normalizeVenueRows(activeView.home_team_venue_rows);
  const awayTeamVenueRowsRaw = normalizeVenueRows(activeView.away_team_venue_rows);
  const fallbackHomeTeamVenueRows = normalizeVenueRows(payloadHomeVenueRows);
  const fallbackAwayTeamVenueRows = normalizeVenueRows(payloadAwayVenueRows);
  const homeTeamVenueRows = hasAnyVenueRows(homeTeamVenueRowsRaw) ? homeTeamVenueRowsRaw : fallbackHomeTeamVenueRows;
  const awayTeamVenueRows = hasAnyVenueRows(awayTeamVenueRowsRaw) ? awayTeamVenueRowsRaw : fallbackAwayTeamVenueRows;
  const homeFixtureVenueRows = Array.isArray(homeTeamVenueRows.home) ? homeTeamVenueRows.home : [];
  const awayFixtureVenueRows = Array.isArray(awayTeamVenueRows.away) ? awayTeamVenueRows.away : [];
  const mappingHead = mappingRows[0] || {};
  const homeLabel = mappingHead.home_sofa || mappingHead.home_raw || "Home team";
  const awayLabel = mappingHead.away_sofa || mappingHead.away_raw || "Away team";
  const activeIsAway = recentTeamView === "away";
  const activeLabel = activeIsAway ? awayLabel : homeLabel;
  const activeRows = activeIsAway ? awayRecentRows : homeRecentRows;
  const activeVenueRows = activeIsAway ? awayTeamVenueRows : homeTeamVenueRows;
  const activeHomeVenueRows = Array.isArray(activeVenueRows.home) ? activeVenueRows.home : [];
  const activeAwayVenueRows = Array.isArray(activeVenueRows.away) ? activeVenueRows.away : [];
  const allVenueRowsCount = activeHomeVenueRows.length + activeAwayVenueRows.length;
  const maxGamesShown = Math.max(1, activeRows.length);
  const maxRollingWindow = Math.max(1, activeRows.length, allVenueRowsCount);
  if (gamesShownAuto) {
    gamesShownCount = activeRows.length ? activeRows.length : 1;
  } else if (!Number.isFinite(gamesShownCount) || gamesShownCount < 1) {
    gamesShownCount = 1;
  }
  gamesShownCount = Math.max(1, Math.min(maxGamesShown, clampRecentMatchesCount(gamesShownCount)));
  rollingWindowCount = Math.max(1, Math.min(maxRollingWindow, clampRecentMatchesCount(rollingWindowCount)));
  const shownRows = activeRows.slice(0, gamesShownCount);
  const recentMatchesSection = `
    <h3 class="section-title">Model Source Matches</h3>
    ${buildGamesShownControlHtml(gamesShownCount, maxGamesShown, maxRollingWindow)}
    ${buildRecentSwitchHtml(homeLabel, awayLabel)}
    ${buildRecentAveragesTableHtml(activeRows, gamesShownCount, activeLabel)}
    ${showTrendCharts ? buildMetricTrendPlotHtml(shownRows, "Venue games trend", activeLabel, rollingWindowCount) : ""}
    ${buildRecentMatchesTableHtml("Fixture-side matches", shownRows, activeLabel)}
    ${buildVenueSplitSectionHtml(activeLabel, activeHomeVenueRows, activeAwayVenueRows, rollingWindowCount, showTrendCharts)}
  `;

  const xgdTabContent = `${warning}${metricHtml}${periodTable}${recentMatchesSection}${mappingTable}`;
  const statsIsAway = statsTeamView === "away";
  const statsActiveLabel = statsIsAway ? awayLabel : homeLabel;
  const statsActiveRows = statsIsAway ? awayRecentRows : homeRecentRows;
  const statsMaxGamesShown = Math.max(1, homeRecentRows.length, awayRecentRows.length);
  if (statsGamesShownAuto) {
    statsGamesShownCount = statsMaxGamesShown;
  } else if (!Number.isFinite(statsGamesShownCount) || statsGamesShownCount < 1) {
    statsGamesShownCount = 1;
  }
  statsGamesShownCount = Math.max(1, Math.min(statsMaxGamesShown, clampRecentMatchesCount(statsGamesShownCount)));
  const cardsTabContent = `
    <h3 class="section-title">Cards & Corners</h3>
    ${buildStatsGamesShownControlHtml(statsGamesShownCount, statsMaxGamesShown)}
    ${buildCardsCornersAveragesTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsGamesShownCount)}
    ${buildCardsCornersVenueTableHtml(
      homeFixtureVenueRows,
      awayFixtureVenueRows,
      homeLabel,
      awayLabel,
      statsGamesShownCount
    )}
    ${buildStatsSwitchHtml(homeLabel, awayLabel)}
    ${buildCardsCornersMatchesTableHtml(statsActiveLabel, statsActiveRows, statsGamesShownCount)}
  `;
  if (detailsMainTab !== "cards") {
    detailsMainTab = "xgd";
  }
  const detailsTabNav = `
    <section class="page-tabs details-main-tabs">
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "xgd" ? "active" : ""}" data-details-tab="xgd">xGD</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "cards" ? "active" : ""}" data-details-tab="cards">Stats</button>
    </section>
  `;
  const activeTabContent = detailsMainTab === "cards" ? cardsTabContent : xgdTabContent;
  linesContainer.innerHTML = `${detailsTabNav}${viewTabsHtml}${activeTabContent}`;

  const detailsTabButtons = linesContainer.querySelectorAll(".details-main-tab");
  for (const button of detailsTabButtons) {
    button.addEventListener("click", () => {
      const targetTab = button.dataset.detailsTab === "cards" ? "cards" : "xgd";
      if (targetTab === detailsMainTab) return;
      detailsMainTab = targetTab;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const viewButtons = linesContainer.querySelectorAll(".xgd-view-tab");
  for (const button of viewButtons) {
    button.addEventListener("click", () => {
      const targetViewId = String(button.dataset.xgdViewId || "").trim();
      if (!targetViewId || targetViewId === activeXgdViewId) return;
      activeXgdViewId = targetViewId;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const switchButtons = linesContainer.querySelectorAll(".recent-switch-btn[data-team-view]");
  for (const button of switchButtons) {
    button.addEventListener("click", () => {
      const targetView = button.dataset.teamView === "away" ? "away" : "home";
      if (targetView === recentTeamView) return;
      recentTeamView = targetView;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const statsSwitchButtons = linesContainer.querySelectorAll(".stats-switch-btn");
  for (const button of statsSwitchButtons) {
    button.addEventListener("click", () => {
      const targetView = button.dataset.statsTeamView === "away" ? "away" : "home";
      if (targetView === statsTeamView) return;
      statsTeamView = targetView;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const gamesShownInputInline = linesContainer.querySelector("#gamesShownInputInline");
  if (gamesShownInputInline) {
    gamesShownInputInline.addEventListener("change", () => {
      gamesShownAuto = false;
      gamesShownCount = clampRecentMatchesCount(gamesShownInputInline.value);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const rollingWindowInputInline = linesContainer.querySelector("#rollingWindowInputInline");
  if (rollingWindowInputInline) {
    rollingWindowInputInline.addEventListener("change", () => {
      rollingWindowCount = clampRecentMatchesCount(rollingWindowInputInline.value);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const showTrendChartsToggle = linesContainer.querySelector("#showTrendChartsToggle");
  if (showTrendChartsToggle) {
    showTrendChartsToggle.addEventListener("change", () => {
      showTrendCharts = Boolean(showTrendChartsToggle.checked);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const statsGamesShownInput = linesContainer.querySelector("#statsGamesShownInput");
  if (statsGamesShownInput) {
    statsGamesShownInput.addEventListener("change", () => {
      statsGamesShownAuto = false;
      statsGamesShownCount = clampRecentMatchesCount(statsGamesShownInput.value);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
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
  gamesShownCount = 0;
  gamesShownAuto = true;
  rollingWindowCount = 3;
  showTrendCharts = false;
  detailsMainTab = "xgd";
  recentTeamView = "home";
  statsGamesShownCount = 0;
  statsGamesShownAuto = true;
  statsTeamView = "home";
  activeXgdViewId = null;
  lastXgdPayload = null;
  detailsMeta.textContent = `${game.competition} | ${game.kickoff_raw}`;
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
gamesTabBtn.addEventListener("click", () => {
  setActiveTab("games");
});
manualMappingTabBtn.addEventListener("click", () => {
  setActiveTab("mapping");
  loadManualMappings();
});
teamMappingsSubTabBtn.addEventListener("click", () => {
  setMappingSubTab("teams");
});
competitionMappingsSubTabBtn.addEventListener("click", () => {
  setMappingSubTab("competitions");
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
todayBtn.addEventListener("click", () => {
  jumpToTodayDay();
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
setMappingSubTab("teams");
setActiveTab("games");
loadGames();
setInterval(() => {
  loadGames();
}, AUTO_REFRESH_MS);
