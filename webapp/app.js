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
const teamSearchInput = document.getElementById("teamSearchInput");
const tableControls = document.querySelector(".table-controls");
const gamesTabBtn = document.getElementById("gamesTabBtn");
const savedGamesTabBtn = document.getElementById("savedGamesTabBtn");
const teamHcRankingsTabBtn = document.getElementById("teamHcRankingsTabBtn");
const manualMappingTabBtn = document.getElementById("manualMappingTabBtn");
const gamesTabPane = document.getElementById("gamesTabPane");
const savedGamesTabPane = document.getElementById("savedGamesTabPane");
const savedGamesView = document.getElementById("savedGamesView");
const teamHcRankingsTabPane = document.getElementById("teamHcRankingsTabPane");
const teamHcRankingsView = document.getElementById("teamHcRankingsView");
const teamHcRankingsGeneralTabBtn = document.getElementById("teamHcRankingsGeneralTabBtn");
const teamHcRankingsHomeTabBtn = document.getElementById("teamHcRankingsHomeTabBtn");
const teamHcRankingsAwayTabBtn = document.getElementById("teamHcRankingsAwayTabBtn");
const teamHcRankingsSortSelect = document.getElementById("teamHcRankingsSortSelect");
const teamHcRankingsLeagueFilter = document.getElementById("teamHcRankingsLeagueFilter");
const teamHcRankingsLeagueFilterBtn = document.getElementById("teamHcRankingsLeagueFilterBtn");
const teamHcRankingsLeagueFilterMenu = document.getElementById("teamHcRankingsLeagueFilterMenu");
const teamHcRankingsRefreshBtn = document.getElementById("teamHcRankingsRefreshBtn");
const teamHcPerfPanel = document.getElementById("teamHcPerfPanel");
const teamHcPerfTitle = document.getElementById("teamHcPerfTitle");
const teamHcPerfMeta = document.getElementById("teamHcPerfMeta");
const teamHcPerfContent = document.getElementById("teamHcPerfContent");
const teamHcPerfCloseBtn = document.getElementById("teamHcPerfCloseBtn");
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
const saveGameBtn = document.getElementById("saveGameBtn");
const recalcBtn = document.getElementById("recalcBtn");
const detailsTitle = document.getElementById("detailsTitle");
const detailsMeta = document.getElementById("detailsMeta");
const linesContainer = document.getElementById("linesContainer");
const prevDayBtn = document.getElementById("prevDayBtn");
const todayBtn = document.getElementById("todayBtn");
const nextDayBtn = document.getElementById("nextDayBtn");
const dayLabel = document.getElementById("dayLabel");
const upcomingModeBtn = document.getElementById("upcomingModeBtn");
const historicalModeBtn = document.getElementById("historicalModeBtn");

let gamesById = new Map();
let rawDays = [];
let availableLeagues = [];
let availableTiers = [];
let allDays = [];
let currentDayIndex = 0;
let selectedMarketId = null;
let gamesMode = "upcoming";
let fillMissingDays = true;
let historicalHasMoreOlder = false;
let selectedLeagues = new Set();
let selectedTiers = new Set();
let sortMode = "kickoff";
let leagueFilterSearch = "";
let teamSearchQuery = "";
let gamesShownCount = 0;
let gamesShownAuto = true;
let rollingWindowCount = 3;
let showTrendCharts = false;
let recentTeamView = "home";
let statsGamesShownCount = 0;
let statsGamesShownAuto = true;
let statsTeamView = "home";
let hcPerfTeamView = "home";
const hcPerfPayloadByMarket = new Map();
let hcPerfLoadingMarketId = null;
let hcPerfRescanInFlight = false;
let lastXgdPayload = null;
let activeXgdViewId = null;
let detailsMainTab = "xgd";
let activeTab = "games";
let savedDays = [];
let savedGamesLoaded = false;
let savedGamesLoading = false;
let savedGamesErrorText = "";
let savedGamesCount = 0;
let savedMarketIds = new Set();
let teamHcRankingsLeagues = [];
let teamHcRankingsRows = [];
let teamHcRankingsLoading = false;
let teamHcRankingsLoaded = false;
let teamHcRankingsErrorText = "";
let teamHcRankingsVenueMode = "overall";
let teamHcRankingsSortMetric = "result";
let selectedTeamHcRankingsLeague = "";
let teamHcRankingsLeagueSearch = "";
let mappingSubTab = "teams";
let lastManualMappingPayload = null;
let teamMappingSearchBetfair = "";
let teamMappingSearchSavedTeams = "";
const historicalDayCalcInFlight = new Set();
let teamHcPerfDetailLoadingKey = "";
const AUTO_REFRESH_MS = 2 * 60 * 1000;
const MAPPING_UNMATCHED_TEAM_RENDER_LIMIT = 250;
const MAPPING_SAVED_TEAM_RENDER_LIMIT = 400;
const MAPPING_UNMATCHED_COMPETITION_RENDER_LIMIT = 200;
const MAPPING_SAVED_COMPETITION_RENDER_LIMIT = 400;

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
  const tabRaw = String(tabName || "").trim().toLowerCase();
  if (tabRaw === "saved") {
    activeTab = "saved";
  } else if (tabRaw === "rankings") {
    activeTab = "rankings";
  } else if (tabRaw === "mapping") {
    activeTab = "mapping";
  } else {
    activeTab = "games";
  }
  const gamesActive = activeTab === "games";
  const savedActive = activeTab === "saved";
  const rankingsActive = activeTab === "rankings";
  const mappingActive = activeTab === "mapping";
  gamesTabBtn.classList.toggle("active", gamesActive);
  if (savedGamesTabBtn) {
    savedGamesTabBtn.classList.toggle("active", savedActive);
  }
  if (teamHcRankingsTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsTabBtn.classList.toggle("active", rankingsActive);
  }
  manualMappingTabBtn.classList.toggle("active", mappingActive);
  gamesTabPane.classList.toggle("hidden", !gamesActive);
  if (savedGamesTabPane) {
    savedGamesTabPane.classList.toggle("hidden", !savedActive);
  }
  if (teamHcRankingsTabPane) {
    teamHcRankingsTabPane.classList.toggle("hidden", !rankingsActive);
  }
  manualMappingTabPane.classList.toggle("hidden", !mappingActive);
  if (activeTab !== "games") {
    detailsPanel.classList.add("hidden");
  }
  if (activeTab !== "rankings" && teamHcPerfPanel) {
    teamHcPerfPanel.classList.add("hidden");
  }
  if (activeTab !== "rankings") {
    setTeamHcRankingsLeagueFilterOpen(false);
  }
  if (mappingActive) {
    setMappingSubTab(mappingSubTab);
  }
  if (gamesActive) {
    renderCurrentDay();
  }
  if (savedActive && !savedGamesLoading) {
    loadSavedGames({ silent: savedGamesLoaded });
  }
  if (rankingsActive && !teamHcRankingsLoading) {
    loadTeamHcRankings({ silent: teamHcRankingsLoaded });
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

function setTeamHcRankingsVenueMode(mode) {
  const modeText = String(mode || "").trim().toLowerCase();
  if (modeText === "home") {
    teamHcRankingsVenueMode = "home";
  } else if (modeText === "away") {
    teamHcRankingsVenueMode = "away";
  } else {
    teamHcRankingsVenueMode = "overall";
  }
  if (teamHcRankingsGeneralTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsGeneralTabBtn.classList.toggle("active", teamHcRankingsVenueMode === "overall");
  }
  if (teamHcRankingsHomeTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsHomeTabBtn.classList.toggle("active", teamHcRankingsVenueMode === "home");
  }
  if (teamHcRankingsAwayTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsAwayTabBtn.classList.toggle("active", teamHcRankingsVenueMode === "away");
  }
}

function setGamesMode(mode, reload = true) {
  const nextMode = mode === "historical" ? "historical" : "upcoming";
  const changed = nextMode !== gamesMode;
  gamesMode = nextMode;
  fillMissingDays = gamesMode !== "historical";
  if (gamesMode !== "historical") {
    historicalHasMoreOlder = false;
  }
  upcomingModeBtn.classList.toggle("active", gamesMode === "upcoming");
  historicalModeBtn.classList.toggle("active", gamesMode === "historical");
  todayBtn.textContent = gamesMode === "historical" ? "Latest" : "Today";

  if (!changed) {
    if (reload) loadGames();
    return;
  }

  selectedMarketId = null;
  detailsPanel.classList.add("hidden");
  currentDayIndex = gamesMode === "historical" ? Number.MAX_SAFE_INTEGER : 0;
  if (reload) loadGames();
}

function rerenderManualMappingsPreserveSearchFocus(selectionStart = null, selectionEnd = null) {
  if (!lastManualMappingPayload) return;
  renderManualMappingSections(lastManualMappingPayload);
  const input = unmatchedTeamsContainer.querySelector('input.mapping-search-input[data-search="betfair"]');
  if (!(input instanceof HTMLInputElement)) return;
  input.focus();
  if (selectionStart == null || selectionEnd == null) return;
  const maxLen = input.value.length;
  const start = Math.max(0, Math.min(selectionStart, maxLen));
  const end = Math.max(start, Math.min(selectionEnd, maxLen));
  try {
    input.setSelectionRange(start, end);
  } catch (_err) {
    // Ignore browsers/inputs that do not support explicit selection ranges.
  }
}

function rerenderManualMappingsPreserveSavedSearchFocus(selectionStart = null, selectionEnd = null) {
  if (!lastManualMappingPayload) return;
  renderManualMappingSections(lastManualMappingPayload);
  const input = savedMappingsContainer.querySelector(
    'input.mapping-search-input[data-search="saved-team-mappings"]'
  );
  if (!(input instanceof HTMLInputElement)) return;
  input.focus();
  if (selectionStart == null || selectionEnd == null) return;
  const maxLen = input.value.length;
  const start = Math.max(0, Math.min(selectionStart, maxLen));
  const end = Math.max(start, Math.min(selectionEnd, maxLen));
  try {
    input.setSelectionRange(start, end);
  } catch (_err) {
    // Ignore browsers/inputs that do not support explicit selection ranges.
  }
}

function appendMappingLimitNotice(container, shown, total, label) {
  if (!(container instanceof HTMLElement)) return;
  if (!Number.isFinite(shown) || !Number.isFinite(total) || shown >= total) return;
  const note = document.createElement("p");
  note.className = "mapping-empty";
  note.textContent = `Showing ${shown} of ${total} ${label}. Refine your search to narrow results.`;
  container.appendChild(note);
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
  const normalizedSofaTeams = sofaTeams
    .map((team) => String(team || "").trim())
    .filter((team) => !!team);
  const availableSofaTeams = normalizedSofaTeams
    .map((team) => String(team || "").trim())
    .filter((team) => !!team && !mappedSofaNames.has(team));
  const availableSofaTeamLookup = new Map(
    availableSofaTeams.map((team) => [team.toLowerCase(), team])
  );
  const competitionMappings = Array.isArray(payload?.competition_mappings) ? payload.competition_mappings : [];
  const unmatchedCompetitions = Array.isArray(payload?.unmatched_competitions)
    ? payload.unmatched_competitions
    : [];
  const sofaCompetitions = Array.isArray(payload?.sofa_competitions) ? payload.sofa_competitions : [];
  const competitionMappingsByRawName = new Map();
  for (const row of competitionMappings) {
    const key = String(row?.raw_name || "").trim();
    if (!key || competitionMappingsByRawName.has(key)) continue;
    competitionMappingsByRawName.set(key, row);
  }
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
  const normalizedSofaCompetitions = sofaCompetitions
    .map((competition) => String(competition || "").trim())
    .filter((competition) => !!competition);
  const availableSofaCompetitions = normalizedSofaCompetitions
    .map((competition) => String(competition || "").trim())
    .filter((competition) => !!competition && !mappedSofaCompetitionNames.has(competition));
  const availableSofaCompetitionLookup = new Map(
    availableSofaCompetitions.map((competition) => [competition.toLowerCase(), competition])
  );
  const sofascoreDbPath = String(payload?.sofascore_db_path || "").trim();
  const sofascoreDbLabel = sofascoreDbPath
    ? sofascoreDbPath.split("/").filter(Boolean).slice(-2).join("/")
    : "";
  const unmatchedTeamsTotalRaw = Number(payload?.unmatched_total);
  const unmatchedTeamsTotal = Number.isFinite(unmatchedTeamsTotalRaw)
    ? unmatchedTeamsTotalRaw
    : unmatchedTeams.length;
  const unmatchedCompetitionsTotalRaw = Number(payload?.unmatched_competitions_total);
  const unmatchedCompetitionsTotal = Number.isFinite(unmatchedCompetitionsTotalRaw)
    ? unmatchedCompetitionsTotalRaw
    : unmatchedCompetitions.length;

  mappingStatus.textContent =
    `${unmatchedTeamsTotal} unmatched teams | ${manualCount} manual, ${autoCount} auto teams | ` +
    `${unmatchedCompetitionsTotal} unmatched competitions | ` +
    `${manualCompetitionCount} manual, ${autoCompetitionCount} auto competitions` +
    (sofascoreDbLabel ? ` | DB: ${sofascoreDbLabel}` : "");

  unmatchedTeamsContainer.innerHTML = "";
  const teamSearchControls = document.createElement("div");
  teamSearchControls.className = "mapping-search-row";
  teamSearchControls.innerHTML = `
    <label class="mapping-search-field">
      <span>Search Betfair Teams</span>
      <input type="search" class="mapping-search-input" data-search="betfair" placeholder="Type Betfair team..." />
    </label>
  `;
  const betfairSearchInput = teamSearchControls.querySelector('input.mapping-search-input[data-search="betfair"]');
  if (betfairSearchInput) {
    betfairSearchInput.value = teamMappingSearchBetfair;
    betfairSearchInput.addEventListener("input", () => {
      teamMappingSearchBetfair = String(betfairSearchInput.value || "");
      rerenderManualMappingsPreserveSearchFocus(
        betfairSearchInput.selectionStart,
        betfairSearchInput.selectionEnd
      );
    });
  }
  unmatchedTeamsContainer.innerHTML = "";
  unmatchedTeamsContainer.appendChild(teamSearchControls);

  const betfairQuery = teamMappingSearchBetfair.trim().toLowerCase();
  const filteredUnmatchedTeams = !betfairQuery
    ? unmatchedTeams
    : unmatchedTeams.filter((row) => {
        const raw = String(row.raw_name || "").toLowerCase();
        const event = String(row.event_name || "").toLowerCase();
        const league = String(row.competition || "").toLowerCase();
        return raw.includes(betfairQuery) || event.includes(betfairQuery) || league.includes(betfairQuery);
      });
  const visibleUnmatchedTeams = filteredUnmatchedTeams.slice(0, MAPPING_UNMATCHED_TEAM_RENDER_LIMIT);

  if (!unmatchedTeams.length) {
    const empty = document.createElement("p");
    empty.className = "mapping-empty";
    empty.textContent = "No unmatched teams in current games.";
    unmatchedTeamsContainer.appendChild(empty);
  } else if (!filteredUnmatchedTeams.length) {
    const empty = document.createElement("p");
    empty.className = "mapping-empty";
    empty.textContent = "No unmatched teams match this filter.";
    unmatchedTeamsContainer.appendChild(empty);
  } else if (!visibleUnmatchedTeams.length) {
    const empty = document.createElement("p");
    empty.className = "mapping-empty";
    empty.textContent = "No unmatched teams available to render.";
    unmatchedTeamsContainer.appendChild(empty);
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

    for (const row of visibleUnmatchedTeams) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const resolveSofaTeam = (inputValue) => {
        const normalized = String(inputValue || "").trim();
        if (!normalized) return "";
        return availableSofaTeamLookup.get(normalized.toLowerCase()) || "";
      };
      tr.innerHTML = `
        <td>${escapeHtml(String(row.event_name || "-"))}</td>
        <td>${escapeHtml(String(row.competition || "-"))}</td>
        <td>${escapeHtml(String(row.kickoff_raw || "-"))}</td>
        <td>${escapeHtml(String(row.side || "-"))}</td>
        <td>${escapeHtml(rawName)}</td>
        <td>
          <div class="mapping-team-picker-wrap">
            <input type="text" class="mapping-team-picker" placeholder="Type to search teams..." autocomplete="off" />
            <div class="mapping-team-dropdown hidden"></div>
          </div>
        </td>
        <td><button type="button" class="mapping-save-btn">Save</button></td>
      `;
      const input = tr.querySelector(".mapping-team-picker");
      const dropdown = tr.querySelector(".mapping-team-dropdown");
      const renderDropdownOptions = (query = "") => {
        if (!dropdown) return;
        const q = String(query || "").trim().toLowerCase();
        const options = !q
          ? availableSofaTeams
          : availableSofaTeams.filter((team) => team.toLowerCase().includes(q));
        if (!options.length) {
          dropdown.innerHTML = `<div class="mapping-team-option-empty">No matching teams</div>`;
          return;
        }
        dropdown.innerHTML = options
          .map(
            (team) =>
              `<button type="button" class="mapping-team-option" data-team="${escapeHtml(team)}">${escapeHtml(team)}</button>`
          )
          .join("");
      };
      if (input && dropdown) {
        input.addEventListener("focus", () => {
          renderDropdownOptions(input.value);
          dropdown.classList.remove("hidden");
        });
        input.addEventListener("input", () => {
          renderDropdownOptions(input.value);
          dropdown.classList.remove("hidden");
        });
        input.addEventListener("keydown", (evt) => {
          if (evt.key === "Escape") {
            dropdown.classList.add("hidden");
          }
        });
        input.addEventListener("blur", () => {
          window.setTimeout(() => {
            dropdown.classList.add("hidden");
          }, 120);
        });
        dropdown.addEventListener("mousedown", (evt) => {
          evt.preventDefault();
        });
        dropdown.addEventListener("click", (evt) => {
          const target = evt.target;
          if (!(target instanceof HTMLElement)) return;
          const optionBtn = target.closest(".mapping-team-option");
          if (!(optionBtn instanceof HTMLElement)) return;
          const selectedTeam = String(optionBtn.dataset.team || "").trim();
          if (!selectedTeam) return;
          input.value = selectedTeam;
          dropdown.classList.add("hidden");
          input.focus();
        });
      }
      const saveBtn = tr.querySelector(".mapping-save-btn");
      if (saveBtn && input) {
        saveBtn.addEventListener("click", async () => {
          const inputValue = String(input.value || "").trim();
          const sofaName = resolveSofaTeam(inputValue);
          if (!sofaName) {
            mappingStatus.textContent = "Pick a SofaScore team from the dropdown suggestions before saving.";
            return;
          }
          await upsertManualTeamMapping(rawName, sofaName);
        });
      }
      tbody.appendChild(tr);
    }
    unmatchedTeamsContainer.appendChild(table);
    appendMappingLimitNotice(
      unmatchedTeamsContainer,
      visibleUnmatchedTeams.length,
      filteredUnmatchedTeams.length,
      "unmatched teams"
    );
  }

  savedMappingsContainer.innerHTML = "";
  if (!teamMappings.length) {
    savedMappingsContainer.innerHTML = `<p class="mapping-empty">No mappings available yet.</p>`;
  } else {
    const savedQuery = teamMappingSearchSavedTeams.trim().toLowerCase();
    const filteredTeamMappings = !savedQuery
      ? teamMappings
      : teamMappings.filter((row) => {
          const raw = String(row?.raw_name || "").toLowerCase();
          const sofa = String(row?.sofa_name || "").toLowerCase();
          return raw.includes(savedQuery) || sofa.includes(savedQuery);
        });
    const visibleTeamMappings = filteredTeamMappings.slice(0, MAPPING_SAVED_TEAM_RENDER_LIMIT);

    const savedSearchControls = document.createElement("div");
    savedSearchControls.className = "mapping-search-row";
    savedSearchControls.innerHTML = `
      <label class="mapping-search-field">
        <span>Search Saved Mappings</span>
        <input type="search" class="mapping-search-input" data-search="saved-team-mappings" placeholder="Type Betfair or SofaScore team..." />
      </label>
    `;
    const savedSearchInput = savedSearchControls.querySelector(
      'input.mapping-search-input[data-search="saved-team-mappings"]'
    );
    if (savedSearchInput) {
      savedSearchInput.value = teamMappingSearchSavedTeams;
      savedSearchInput.addEventListener("input", () => {
        teamMappingSearchSavedTeams = String(savedSearchInput.value || "");
        rerenderManualMappingsPreserveSavedSearchFocus(
          savedSearchInput.selectionStart,
          savedSearchInput.selectionEnd
        );
      });
    }
    savedMappingsContainer.appendChild(savedSearchControls);

    if (!filteredTeamMappings.length) {
      const empty = document.createElement("p");
      empty.className = "mapping-empty";
      empty.textContent = "No saved mappings match this filter.";
      savedMappingsContainer.appendChild(empty);
    } else if (!visibleTeamMappings.length) {
      const empty = document.createElement("p");
      empty.className = "mapping-empty";
      empty.textContent = "No saved mappings available to render.";
      savedMappingsContainer.appendChild(empty);
    } else {
      const teamDatalistId = "mappingSofaTeamOptions";
      const teamDatalist = document.createElement("datalist");
      teamDatalist.id = teamDatalistId;
      teamDatalist.innerHTML = availableSofaTeams
        .map((teamName) => `<option value="${escapeHtml(teamName)}"></option>`)
        .join("");
      savedMappingsContainer.appendChild(teamDatalist);

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
      for (const row of visibleTeamMappings) {
        const tr = document.createElement("tr");
        const rawName = String(row.raw_name || "");
        const sofaName = String(row.sofa_name || "");
        const sofaNameLower = sofaName.toLowerCase();
        const isManual = row?.is_manual !== false;
        const method = String(row.match_method || "").trim().toLowerCase();
        const methodLabel = method ? `${method.charAt(0).toUpperCase()}${method.slice(1)}` : "Auto";
        const typeLabel = isManual ? "Manual" : `Auto (${methodLabel})`;
        const actionHtml = isManual
          ? `
              <button type="button" class="mapping-save-btn">Save</button>
              <button type="button" class="mapping-delete-btn">Delete</button>
            `
          : `<button type="button" class="mapping-save-btn">Override</button>`;
        tr.innerHTML = `
          <td>${escapeHtml(rawName)}</td>
          <td>
            <input type="text" class="mapping-team-input" list="${teamDatalistId}" value="${escapeHtml(sofaName)}" />
          </td>
          <td>${escapeHtml(typeLabel)}</td>
          <td>${actionHtml}</td>
        `;
        const teamInput = tr.querySelector(".mapping-team-input");
        const saveBtn = tr.querySelector(".mapping-save-btn");
        if (saveBtn && teamInput) {
          saveBtn.addEventListener("click", async () => {
            const selectedRaw = String(teamInput.value || "").trim();
            const selectedLower = selectedRaw.toLowerCase();
            const selectedSofaName = availableSofaTeamLookup.get(selectedLower)
              || (selectedLower && selectedLower === sofaNameLower ? sofaName : "");
            if (!selectedSofaName) {
              mappingStatus.textContent = "Select a valid SofaScore team before saving.";
              return;
            }
            await upsertManualTeamMapping(rawName, selectedSofaName);
          });
        }
        const deleteBtn = tr.querySelector(".mapping-delete-btn");
        if (deleteBtn && isManual) {
          deleteBtn.addEventListener("click", async () => {
            await deleteManualTeamMapping(rawName);
          });
        }
        tbody.appendChild(tr);
      }
      savedMappingsContainer.appendChild(table);
      appendMappingLimitNotice(
        savedMappingsContainer,
        visibleTeamMappings.length,
        filteredTeamMappings.length,
        "saved team mappings"
      );
    }
  }

  unmatchedCompetitionsContainer.innerHTML = "";
  const visibleUnmatchedCompetitions = unmatchedCompetitions.slice(0, MAPPING_UNMATCHED_COMPETITION_RENDER_LIMIT);
  if (!unmatchedCompetitions.length) {
    unmatchedCompetitionsContainer.innerHTML = `<p class="mapping-empty">No unmatched competitions in current games.</p>`;
  } else if (!visibleUnmatchedCompetitions.length) {
    unmatchedCompetitionsContainer.innerHTML = `<p class="mapping-empty">No unmatched competitions available to render.</p>`;
  } else {
    const competitionDatalistId = "mappingSofaCompetitionOptions";
    const competitionDatalist = document.createElement("datalist");
    competitionDatalist.id = competitionDatalistId;
    competitionDatalist.innerHTML = availableSofaCompetitions
      .map((competition) => `<option value="${escapeHtml(competition)}"></option>`)
      .join("");
    unmatchedCompetitionsContainer.appendChild(competitionDatalist);

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
    for (const row of visibleUnmatchedCompetitions) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const existing = competitionMappingsByRawName.get(rawName);
      const existingSofaName = String(existing?.sofa_name || "").trim();
      const existingSofaNameLower = existingSofaName.toLowerCase();
      tr.innerHTML = `
        <td>${escapeHtml(rawName)}</td>
        <td>${escapeHtml(String(row.games_count ?? "-"))}</td>
        <td>${escapeHtml(String(row.next_kickoff || "-"))}</td>
        <td>
          <input
            type="text"
            class="mapping-team-input"
            list="${competitionDatalistId}"
            value="${escapeHtml(existingSofaName)}"
            placeholder="Type competition..."
          />
        </td>
        <td><button type="button" class="mapping-save-btn">Save</button></td>
      `;
      const input = tr.querySelector(".mapping-team-input");
      const saveBtn = tr.querySelector(".mapping-save-btn");
      if (saveBtn && input) {
        saveBtn.addEventListener("click", async () => {
          const inputValue = String(input.value || "").trim();
          const inputValueLower = inputValue.toLowerCase();
          const sofaName = availableSofaCompetitionLookup.get(inputValueLower)
            || (inputValueLower && inputValueLower === existingSofaNameLower ? existingSofaName : "");
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
    appendMappingLimitNotice(
      unmatchedCompetitionsContainer,
      visibleUnmatchedCompetitions.length,
      unmatchedCompetitions.length,
      "unmatched competitions"
    );
  }

  savedCompetitionMappingsContainer.innerHTML = "";
  if (!competitionMappings.length) {
    savedCompetitionMappingsContainer.innerHTML = `<p class="mapping-empty">No competition mappings available yet.</p>`;
  } else {
    const visibleCompetitionMappings = competitionMappings.slice(0, MAPPING_SAVED_COMPETITION_RENDER_LIMIT);
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
    for (const row of visibleCompetitionMappings) {
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
    appendMappingLimitNotice(
      savedCompetitionMappingsContainer,
      visibleCompetitionMappings.length,
      competitionMappings.length,
      "saved competition mappings"
    );
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
  if (targetIndex < 0) {
    if (gamesMode === "historical") {
      currentDayIndex = allDays.length - 1;
      renderCurrentDay();
    }
    return;
  }
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

function buildHistoricalResultSection(payload) {
  if (!payload || payload.is_historical !== true) return "";
  const result = payload.historical_result && typeof payload.historical_result === "object"
    ? payload.historical_result
    : {};
  const comparison = payload.prediction_vs_actual && typeof payload.prediction_vs_actual === "object"
    ? payload.prediction_vs_actual
    : null;

  const resultHtml = `
    <h3 class="section-title">Actual Match Result</h3>
    <table class="lines-table">
      <thead>
        <tr>
          <th>Score</th>
          <th>Home Goals</th>
          <th>Away Goals</th>
          <th>Home xG</th>
          <th>Away xG</th>
          <th>Home Corners</th>
          <th>Away Corners</th>
          <th>Home Cards</th>
          <th>Away Cards</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>${escapeHtml(result.score || "-")}</td>
          <td>${formatMetricValue(result.home_goals, 0)}</td>
          <td>${formatMetricValue(result.away_goals, 0)}</td>
          <td>${formatMetricValue(result.home_xg, 2)}</td>
          <td>${formatMetricValue(result.away_xg, 2)}</td>
          <td>${formatMetricValue(result.home_corners, 1)}</td>
          <td>${formatMetricValue(result.away_corners, 1)}</td>
          <td>${formatMetricValue(result.home_cards, 1)}</td>
          <td>${formatMetricValue(result.away_cards, 1)}</td>
        </tr>
      </tbody>
    </table>
  `;

  if (!comparison) {
    return resultHtml;
  }

  const comparisonHtml = `
    <h3 class="section-title">Prediction vs Actual</h3>
    <table class="lines-table">
      <thead>
        <tr>
          <th>Metric</th>
          <th>Predicted</th>
          <th>Actual</th>
          <th>Delta</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>Home xG</td>
          <td>${formatMetricValue(comparison.pred_home_xg, 2)}</td>
          <td>${formatMetricValue(comparison.actual_home_xg, 2)}</td>
          <td>${formatMetricValue(comparison.delta_home_xg, 2)}</td>
        </tr>
        <tr>
          <td>Away xG</td>
          <td>${formatMetricValue(comparison.pred_away_xg, 2)}</td>
          <td>${formatMetricValue(comparison.actual_away_xg, 2)}</td>
          <td>${formatMetricValue(comparison.delta_away_xg, 2)}</td>
        </tr>
        <tr>
          <td>Total xG</td>
          <td>${formatMetricValue(comparison.pred_total_xg, 2)}</td>
          <td>${formatMetricValue(comparison.actual_total_xg, 2)}</td>
          <td>${formatMetricValue(comparison.delta_total_xg, 2)}</td>
        </tr>
        <tr>
          <td>xGD</td>
          <td>${formatMetricValue(comparison.pred_xgd, 2)}</td>
          <td>${formatMetricValue(comparison.actual_xgd, 2)}</td>
          <td>${formatMetricValue(comparison.delta_xgd, 2)}</td>
        </tr>
      </tbody>
    </table>
  `;

  return `${resultHtml}${comparisonHtml}`;
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

function toMetricNumberOrNull(value) {
  const text = String(value ?? "").trim();
  if (!text || text === "-") return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function hasNoXgMetricSignal(game) {
  const coreValues = [
    game?.season_strength,
    game?.last5_strength,
    game?.last3_strength,
    game?.season_xgd,
    game?.last5_xgd,
    game?.last3_xgd,
  ];
  const rangeValues = [
    game?.season_min_xg,
    game?.last5_min_xg,
    game?.last3_min_xg,
    game?.season_max_xg,
    game?.last5_max_xg,
    game?.last3_max_xg,
  ];
  const coreNumeric = coreValues.map(toMetricNumberOrNull).filter((v) => v != null);
  if (!coreNumeric.length) return false;
  const coreAllZero = coreNumeric.every((v) => Math.abs(v) < 1e-9);
  if (!coreAllZero) return false;

  // No-xG leagues usually produce all-zero xG bands too.
  const rangeNumeric = rangeValues.map(toMetricNumberOrNull).filter((v) => v != null);
  if (!rangeNumeric.length) return true;
  return rangeNumeric.every((v) => Math.abs(v) < 1e-9);
}

function getHcMetricConsensusDirection(game, periodValues) {
  const handicap = toMetricNumberOrNull(game?.mainline);
  if (handicap == null) return null;
  const metricValues = periodValues.map(toMetricNumberOrNull);
  if (metricValues.some((value) => value == null)) return null;
  const sums = metricValues.map((value) => handicap + value);
  const threshold = 0.1;
  if (sums.every((value) => value > threshold)) return "positive";
  if (sums.every((value) => value < -threshold)) return "negative";
  return null;
}

function getHcXgdConsensusDirection(game) {
  return getHcMetricConsensusDirection(game, [
    game?.season_strength,
    game?.last5_strength,
    game?.last3_strength,
  ]);
}

function getHcXgdPerfConsensusDirection(game) {
  return getHcMetricConsensusDirection(game, [
    game?.season_xgd,
    game?.last5_xgd,
    game?.last3_xgd,
  ]);
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

function sumMetric(rows, metricKey) {
  const safeRows = Array.isArray(rows) ? rows : [];
  let total = 0;
  let count = 0;
  for (const row of safeRows) {
    const value = Number(row?.[metricKey]);
    if (!Number.isFinite(value)) continue;
    total += value;
    count += 1;
  }
  if (!count) return null;
  return total;
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

function buildGamestateTableHtml(homeRows, awayRows, homeLabel, awayLabel, sampleSize = null) {
  const sampleLabel = sampleSize == null ? "All Previous Games" : `Last ${clampRecentMatchesCount(sampleSize)} Games`;
  const entries = [
    { label: homeLabel || "Home team", rows: limitStatsRows(homeRows || [], sampleSize) },
    { label: awayLabel || "Away team", rows: limitStatsRows(awayRows || [], sampleSize) },
  ];
  const states = [
    { key: "drawing", label: "D" },
    { key: "winning", label: "W" },
    { key: "losing", label: "L" },
  ];
  const metricCols = [
    { metric: "corners", direction: "for", label: "Corners For" },
    { metric: "corners", direction: "against", label: "Corners Against" },
    { metric: "cards", direction: "for", label: "Cards For" },
    { metric: "cards", direction: "against", label: "Cards Against" },
  ];

  const toSafeNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const formatRate = (total, minutes, factor) => {
    if (!Number.isFinite(total) || !Number.isFinite(minutes) || minutes <= 0) return "-";
    return formatMetricValue((total / minutes) * factor, 2);
  };
  const formatMinutesPerStat = (total, minutes) => {
    if (!Number.isFinite(total) || !Number.isFinite(minutes) || total <= 0 || minutes <= 0) return "-";
    return formatMetricValue(minutes / total, 2);
  };

  const renderTeamTable = (entry) => `
    <section class="recent-team-block">
      <h4>${escapeHtml(entry.label)} - Gamestate Totals (${escapeHtml(sampleLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>State</th>
              <th>Minutes</th>
              <th>Time %</th>
            </tr>
          </thead>
          <tbody>
            ${(() => {
              const minutesByState = {};
              for (const state of states) {
                const key = `minutes_${state.key}`;
                minutesByState[state.key] = toSafeNumber(sumMetric(entry.rows, key));
              }
              const totalMinutes = states.reduce((sum, state) => sum + minutesByState[state.key], 0);
              return states
                .map((state) => {
                  const minutes = minutesByState[state.key];
                  const pct = totalMinutes > 0 ? (minutes / totalMinutes) * 100 : null;
                  return `
                    <tr>
                      <td>${escapeHtml(state.label)}</td>
                      <td>${formatMetricValue(minutes, 1)}</td>
                      <td>${pct == null ? "-" : `${formatMetricValue(pct, 1)}%`}</td>
                    </tr>
                  `;
                })
                .join("");
            })()}
          </tbody>
        </table>
      </div>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th>State</th>
              <th>Total</th>
              <th>Per 90</th>
              <th>Mins/Stat</th>
            </tr>
          </thead>
          <tbody>
            ${metricCols
              .map((metric) => {
                return states
                  .map((state) => {
                    const metricKey = `${metric.metric}_${metric.direction}_${state.key}`;
                    const minutesKey = `minutes_${state.key}`;
                    const total = toSafeNumber(sumMetric(entry.rows, metricKey));
                    const minutes = toSafeNumber(sumMetric(entry.rows, minutesKey));
                    return `
                      <tr>
                        <td>${escapeHtml(metric.label)}</td>
                        <td>${escapeHtml(state.label)}</td>
                        <td>${formatMetricValue(total, 0)}</td>
                        <td>${formatRate(total, minutes, 90)}</td>
                        <td>${formatMinutesPerStat(total, minutes)}</td>
                      </tr>
                    `;
                  })
                  .join("");
              })
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;

  return `
    ${entries.map((entry) => renderTeamTable(entry)).join("")}
  `;
}

function buildCardsCornersMatchesTableHtml(teamLabel, rows, sampleSize = null, titleOverride = "") {
  const safeRows = Array.isArray(rows) ? [...rows] : [];
  safeRows.sort((a, b) => String(b?.date_time || "").localeCompare(String(a?.date_time || "")));
  const shownRows = limitStatsRows(safeRows, sampleSize);
  const titleSuffix = sampleSize == null ? "" : ` (Last ${clampRecentMatchesCount(sampleSize)})`;
  const defaultTitle = `${escapeHtml(teamLabel || "Team")} - Previous Games${escapeHtml(titleSuffix)}`;
  const title = titleOverride ? `${escapeHtml(titleOverride)}${escapeHtml(titleSuffix)}` : defaultTitle;
  if (!shownRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${title}</h4>
        <p class="recent-empty">No previous games available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <h4>${title}</h4>
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
              <th>Corners<span class="metric-suffix">H</span></th>
              <th>Corners<span class="metric-suffix">A</span></th>
              <th>Cards<span class="metric-suffix">H</span></th>
              <th>Cards<span class="metric-suffix">A</span></th>
            </tr>
          </thead>
          <tbody>
            ${shownRows
              .map(
                (row) => {
                  const isHome = String(row?.venue || "").toLowerCase() === "home";
                  const relevantTeam = String(teamLabel || row?.team || "").trim();
                  const opponentTeam = String(row?.opponent || "").trim();
                  const homeTeam = isHome ? relevantTeam : opponentTeam;
                  const awayTeam = isHome ? opponentTeam : relevantTeam;
                  const homeTeamCell = isHome
                    ? `<strong>${escapeHtml(homeTeam || "-")}</strong>`
                    : escapeHtml(homeTeam || "-");
                  const awayTeamCell = isHome
                    ? escapeHtml(awayTeam || "-")
                    : `<strong>${escapeHtml(awayTeam || "-")}</strong>`;
                  const goalsHome = isHome ? row?.GF : row?.GA;
                  const goalsAway = isHome ? row?.GA : row?.GF;
                  const cornersHome = isHome ? row?.corners_for : row?.corners_against;
                  const cornersAway = isHome ? row?.corners_against : row?.corners_for;
                  const cardsHome = isHome ? row?.cards_for : row?.cards_against;
                  const cardsAway = isHome ? row?.cards_against : row?.cards_for;
                  return `
              <tr>
                <td>${escapeHtml(row?.date_time || "-")}</td>
                <td>${escapeHtml(row?.competition_name || "-")}</td>
                <td>${homeTeamCell}</td>
                <td>${awayTeamCell}</td>
                <td>${formatMetricValue(goalsHome, 0)}</td>
                <td>${formatMetricValue(goalsAway, 0)}</td>
                <td>${formatMetricValue(cornersHome, 2)}</td>
                <td>${formatMetricValue(cornersAway, 2)}</td>
                <td>${formatMetricValue(cardsHome, 2)}</td>
                <td>${formatMetricValue(cardsAway, 2)}</td>
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

function formatSignedMetricValue(value, decimals = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  const fixed = num.toFixed(decimals);
  return num > 0 ? `+${fixed}` : fixed;
}

function toFiniteNumberOrNull(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const text = value.trim();
    if (!text || text === "-") return null;
  }
  const out = Number(value);
  return Number.isFinite(out) ? out : null;
}

function classifyDelta(delta, pushThreshold = 0) {
  if (!Number.isFinite(delta)) return null;
  const threshold = Number.isFinite(pushThreshold) ? Math.max(0, pushThreshold) : 0;
  if (delta > threshold) return "win";
  if (delta < -threshold) return "loss";
  return "push";
}

function emptyHcPerfCounts() {
  return { win: 0, half_win: 0, half_loss: 0, loss: 0, push: 0, total: 0 };
}

function incrementHcPerfCounts(target, verdict) {
  if (!target || !verdict) return;
  if (
    verdict === "win"
    || verdict === "half_win"
    || verdict === "half_loss"
    || verdict === "loss"
    || verdict === "push"
  ) {
    target[verdict] += 1;
    target.total += 1;
  }
}

function classifyResultHandicapDelta(delta) {
  if (!Number.isFinite(delta)) return null;
  if (Math.abs(delta) <= 1e-9) return "push";
  const absDelta = Math.abs(delta);
  const isHalf = Math.abs(absDelta - 0.25) <= 1e-6;
  if (delta > 0) return isHalf ? "half_win" : "win";
  return isHalf ? "half_loss" : "loss";
}

function computeTeamDeltaVsHandicap(row, metric = "result", relevantTeam = "") {
  const data = row && typeof row === "object" ? row : {};
  const isXgMetric = metric === "xg";
  const preset = toFiniteNumberOrNull(isXgMetric ? data.xg_vs_hc : data.result_vs_hc);
  if (preset != null) return preset;

  const relevantNorm = String(relevantTeam || "").trim().toLowerCase();
  const homeTeamNorm = String(data.home_team || "").trim().toLowerCase();
  const awayTeamNorm = String(data.away_team || "").trim().toLowerCase();
  let isHome = Boolean(relevantNorm && homeTeamNorm && relevantNorm === homeTeamNorm);
  let isAway = Boolean(relevantNorm && awayTeamNorm && relevantNorm === awayTeamNorm);
  if (!isHome && !isAway) {
    const venue = String(data.venue || "").trim().toLowerCase();
    isHome = venue === "home";
    isAway = venue === "away";
  }
  if (!isHome && !isAway) return null;

  const homeHandicap = toFiniteNumberOrNull(data.closing_mainline);
  const homeValue = toFiniteNumberOrNull(isXgMetric ? data.home_xg : data.home_goals);
  const awayValue = toFiniteNumberOrNull(isXgMetric ? data.away_xg : data.away_goals);
  if (homeHandicap == null || homeValue == null || awayValue == null) return null;

  const teamHandicap = isHome ? homeHandicap : -homeHandicap;
  const teamMetric = isHome ? homeValue : awayValue;
  const oppMetric = isHome ? awayValue : homeValue;
  return (teamMetric + teamHandicap) - oppMetric;
}

function computeHcPerfSummary(rows, relevantTeam = "") {
  const safeRows = Array.isArray(rows) ? rows : [];
  const summary = {
    result: {
      home: emptyHcPerfCounts(),
      away: emptyHcPerfCounts(),
      overall: emptyHcPerfCounts(),
    },
    xg: {
      home: emptyHcPerfCounts(),
      away: emptyHcPerfCounts(),
      overall: emptyHcPerfCounts(),
    },
  };
  for (const row of safeRows) {
    const venue = String(row?.venue || "").trim().toLowerCase();
    const venueKey = venue === "home" || venue === "away" ? venue : null;

    const resultDelta = computeTeamDeltaVsHandicap(row, "result", relevantTeam);
    const resultVerdict = classifyResultHandicapDelta(resultDelta);
    if (resultVerdict) {
      incrementHcPerfCounts(summary.result.overall, resultVerdict);
      if (venueKey) incrementHcPerfCounts(summary.result[venueKey], resultVerdict);
    }

    const xgDelta = computeTeamDeltaVsHandicap(row, "xg", relevantTeam);
    const xgVerdict = classifyDelta(xgDelta, 0.1);
    if (xgVerdict) {
      incrementHcPerfCounts(summary.xg.overall, xgVerdict);
      if (venueKey) incrementHcPerfCounts(summary.xg[venueKey], xgVerdict);
    }
  }
  return summary;
}

function formatHcPerfSummaryCell(counts) {
  const safe = counts && typeof counts === "object" ? counts : emptyHcPerfCounts();
  const weightedWin = safe.win + (safe.half_win * 0.5);
  const weightedLoss = safe.loss + (safe.half_loss * 0.5);
  const weightedPush = safe.push + (safe.half_win * 0.5) + (safe.half_loss * 0.5);
  const formatWeighted = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "0";
    return Math.abs(num - Math.round(num)) < 1e-9 ? String(Math.round(num)) : num.toFixed(1);
  };
  return `
    <span class="hcperf-summary-pill hcperf-summary-pill-win">W ${formatWeighted(weightedWin)}</span>
    <span class="hcperf-summary-pill hcperf-summary-pill-loss">L ${formatWeighted(weightedLoss)}</span>
    <span class="hcperf-summary-pill hcperf-summary-pill-push">P ${formatWeighted(weightedPush)}</span>
  `;
}

function formatHcPerfSummaryCellBasic(counts) {
  const safe = counts && typeof counts === "object" ? counts : emptyHcPerfCounts();
  return `
    <span class="hcperf-summary-pill hcperf-summary-pill-win">W ${safe.win}</span>
    <span class="hcperf-summary-pill hcperf-summary-pill-loss">L ${safe.loss}</span>
    <span class="hcperf-summary-pill hcperf-summary-pill-push">P ${safe.push}</span>
  `;
}

function buildHcPerfSummaryTableHtml(teamLabel, rows, options = {}) {
  const safeTeamLabel = String(teamLabel || "Team");
  const focusVenue = String(options?.focusVenue || "").trim().toLowerCase();
  const focusHome = focusVenue === "home";
  const focusAway = focusVenue === "away";
  const homeHeaderClass = focusHome ? ' class="hcperf-summary-focus hcperf-summary-focus-top"' : "";
  const awayHeaderClass = focusAway ? ' class="hcperf-summary-focus hcperf-summary-focus-top"' : "";
  const homeCellClassMid = `hcperf-summary-cell${focusHome ? " hcperf-summary-focus" : ""}`;
  const awayCellClassMid = `hcperf-summary-cell${focusAway ? " hcperf-summary-focus" : ""}`;
  const homeCellClassBottom = `hcperf-summary-cell${focusHome ? " hcperf-summary-focus hcperf-summary-focus-bottom" : ""}`;
  const awayCellClassBottom = `hcperf-summary-cell${focusAway ? " hcperf-summary-focus hcperf-summary-focus-bottom" : ""}`;
  const summary = computeHcPerfSummary(rows, safeTeamLabel);
  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(safeTeamLabel)} - Summary</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table hcperf-summary-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th${homeHeaderClass}>Home</th>
              <th${awayHeaderClass}>Away</th>
              <th>Overall</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="hcperf-summary-metric">Result</td>
              <td class="${homeCellClassMid}">${formatHcPerfSummaryCell(summary.result.home)}</td>
              <td class="${awayCellClassMid}">${formatHcPerfSummaryCell(summary.result.away)}</td>
              <td class="hcperf-summary-cell">${formatHcPerfSummaryCell(summary.result.overall)}</td>
            </tr>
            <tr>
              <td class="hcperf-summary-metric">xG (threshold = 0.10)</td>
              <td class="${homeCellClassBottom}">${formatHcPerfSummaryCellBasic(summary.xg.home)}</td>
              <td class="${awayCellClassBottom}">${formatHcPerfSummaryCellBasic(summary.xg.away)}</td>
              <td class="hcperf-summary-cell">${formatHcPerfSummaryCellBasic(summary.xg.overall)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildSeasonHandicapPerformanceTableHtml(teamLabel, rows, options = {}) {
  const titleOverride = String(options?.title || "").trim();
  const relevantTeam = String(options?.relevantTeam || teamLabel || "").trim();
  const safeRows = Array.isArray(rows) ? [...rows] : [];
  safeRows.sort((a, b) => String(b?.date_time || "").localeCompare(String(a?.date_time || "")));
  const title = titleOverride || `${escapeHtml(teamLabel || "Team")} - Season Previous Games`;
  if (!safeRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${title}</h4>
        <p class="recent-empty">No previous games found for this season.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <h4>${title}</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Competition</th>
              <th>Home</th>
              <th>Away</th>
              <th>HG</th>
              <th>AG</th>
              <th>xG H</th>
              <th>xG A</th>
              <th>xGD</th>
              <th class="hcperf-odds-col hcperf-odds-col-start">H Px</th>
              <th class="hcperf-odds-line-col">HC</th>
              <th class="hcperf-odds-col hcperf-odds-col-end">A Px</th>
              <th>Result pick</th>
              <th>xG pick</th>
            </tr>
          </thead>
          <tbody>
            ${safeRows
              .map(
                (row) => {
                  const homeTeam = String(row?.home_team || "-");
                  const awayTeam = String(row?.away_team || "-");
                  const homeIsRelevant = relevantTeam && homeTeam === relevantTeam;
                  const awayIsRelevant = relevantTeam && awayTeam === relevantTeam;
                  const homeCell = homeIsRelevant ? `<strong>${escapeHtml(homeTeam)}</strong>` : escapeHtml(homeTeam);
                  const awayCell = awayIsRelevant ? `<strong>${escapeHtml(awayTeam)}</strong>` : escapeHtml(awayTeam);
                  const homeGoalsNum = Number(row?.home_goals);
                  const awayGoalsNum = Number(row?.away_goals);
                  const homeXgNum = Number(row?.home_xg);
                  const awayXgNum = Number(row?.away_xg);
                  const homeHcNum = Number(row?.closing_mainline);
                  const xgd = Number.isFinite(homeXgNum) && Number.isFinite(awayXgNum)
                    ? (homeXgNum - awayXgNum)
                    : null;
                  const resultDelta = (
                    Number.isFinite(homeGoalsNum)
                    && Number.isFinite(awayGoalsNum)
                    && Number.isFinite(homeHcNum)
                  )
                    ? ((homeGoalsNum + homeHcNum) - awayGoalsNum)
                    : null;
                  const resultPickSide = Number.isFinite(resultDelta)
                    ? (resultDelta > 1e-9 ? "home" : (resultDelta < -1e-9 ? "away" : "push"))
                    : "none";
                  const resultOutcome = classifyResultHandicapDelta(resultDelta);
                  const resultIsHalf = resultOutcome === "half_win" || resultOutcome === "half_loss";
                  const resultPickText = (() => {
                    if (resultPickSide === "home") {
                      return `Home (${formatSignedMetricValue(resultDelta, 2)})`;
                    }
                    if (resultPickSide === "away") {
                      return `Away (${formatSignedMetricValue(resultDelta, 2)})`;
                    }
                    if (resultPickSide === "push") return "Push (0.00)";
                    return "-";
                  })();
                  const resultPickClass = (() => {
                    if (resultPickSide === "home") {
                      if (resultIsHalf) return homeIsRelevant ? "hcperf-pick-relevant-half" : "hcperf-pick-other-half";
                      return homeIsRelevant ? "hcperf-pick-relevant" : "hcperf-pick-other";
                    }
                    if (resultPickSide === "away") {
                      if (resultIsHalf) return awayIsRelevant ? "hcperf-pick-relevant-half" : "hcperf-pick-other-half";
                      return awayIsRelevant ? "hcperf-pick-relevant" : "hcperf-pick-other";
                    }
                    return "hcperf-pick-neutral";
                  })();
                  const xgDelta = (
                    Number.isFinite(homeXgNum)
                    && Number.isFinite(awayXgNum)
                    && Number.isFinite(homeHcNum)
                  )
                    ? ((homeXgNum + homeHcNum) - awayXgNum)
                    : null;
                  const xgNoBet = Number.isFinite(xgDelta) && Math.abs(xgDelta) < 0.2;
                  const xgPickSide = Number.isFinite(xgDelta)
                    ? (
                      xgNoBet
                        ? "no_bet"
                        : (xgDelta > 1e-9 ? "home" : (xgDelta < -1e-9 ? "away" : "push"))
                    )
                    : "none";
                  const xgPickText = (() => {
                    if (xgPickSide === "no_bet") return `No bet (${formatSignedMetricValue(xgDelta, 2)})`;
                    if (xgPickSide === "home") return `Home (${formatSignedMetricValue(xgDelta, 2)})`;
                    if (xgPickSide === "away") return `Away (${formatSignedMetricValue(xgDelta, 2)})`;
                    if (xgPickSide === "push") return "Push (0.00)";
                    return "-";
                  })();
                  const xgPickClass = (() => {
                    if (xgPickSide === "home") return homeIsRelevant ? "hcperf-pick-relevant" : "hcperf-pick-other";
                    if (xgPickSide === "away") return awayIsRelevant ? "hcperf-pick-relevant" : "hcperf-pick-other";
                    return "hcperf-pick-neutral";
                  })();
                  return `
              <tr>
                <td>${escapeHtml(row?.date_time || "-")}</td>
                <td>${escapeHtml(row?.competition_name || "-")}</td>
                <td>${homeCell}</td>
                <td>${awayCell}</td>
                <td>${formatMetricValue(row?.home_goals, 0)}</td>
                <td>${formatMetricValue(row?.away_goals, 0)}</td>
                <td>${formatMetricValue(row?.home_xg, 2)}</td>
                <td>${formatMetricValue(row?.away_xg, 2)}</td>
                <td>${formatSignedMetricValue(xgd, 2)}</td>
                <td class="hcperf-odds-col hcperf-odds-col-start">${escapeHtml(row?.closing_home_price || "-")}</td>
                <td class="hcperf-odds-line-col">${escapeHtml(row?.closing_mainline || "-")}</td>
                <td class="hcperf-odds-col hcperf-odds-col-end">${escapeHtml(row?.closing_away_price || "-")}</td>
                <td><span class="${resultPickClass}">${escapeHtml(resultPickText)}</span></td>
                <td><span class="${xgPickClass}">${escapeHtml(xgPickText)}</span></td>
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

function buildHcPerfSwitchHtml(homeLabel, awayLabel) {
  const homeActive = hcPerfTeamView !== "away";
  const awayActive = hcPerfTeamView === "away";
  return `
    <div class="recent-switch" role="tablist" aria-label="HC performance team view">
      <button
        type="button"
        class="recent-switch-btn hcperf-switch-btn${homeActive ? " active" : ""}"
        data-hcperf-team-view="home"
        role="tab"
        aria-selected="${homeActive ? "true" : "false"}"
      >
        Home: ${escapeHtml(homeLabel)}
      </button>
      <button
        type="button"
        class="recent-switch-btn hcperf-switch-btn${awayActive ? " active" : ""}"
        data-hcperf-team-view="away"
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
  sortModeBtn.value = sortMode;
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

  const searchWrap = document.createElement("div");
  searchWrap.className = "league-filter-search";
  const searchInput = document.createElement("input");
  searchInput.type = "search";
  searchInput.className = "league-filter-search-input";
  searchInput.placeholder = "Search leagues...";
  searchInput.value = leagueFilterSearch;
  searchInput.addEventListener("input", () => {
    leagueFilterSearch = String(searchInput.value || "");
    updateLeagueFilterMenu(leagues);
    const nextInput = leagueFilterMenu.querySelector(".league-filter-search-input");
    if (nextInput instanceof HTMLInputElement) {
      nextInput.focus();
    }
  });
  searchWrap.appendChild(searchInput);
  leagueFilterMenu.appendChild(searchWrap);

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

  const query = leagueFilterSearch.trim().toLowerCase();
  const visibleLeagues = !query
    ? leagues
    : leagues.filter((league) => String(league || "").toLowerCase().includes(query));
  if (!visibleLeagues.length) {
    const empty = document.createElement("p");
    empty.className = "league-filter-empty";
    empty.textContent = "No leagues match search.";
    leagueFilterMenu.appendChild(empty);
    return;
  }

  for (const league of visibleLeagues) {
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
  const normalizedTeamQuery = teamSearchQuery.trim().toLowerCase();
  const filteredDays = rawDays
    .map((day) => ({
      ...day,
      games: (day.games || []).filter((game) => {
        const leagueName = String(game.competition || "");
        const tierName = String(game.tier || "").trim();
        const leaguePass = !selectedLeagues.size || selectedLeagues.has(leagueName);
        const tierPass = !selectedTiers.size || selectedTiers.has(tierName);
        const teamText = [
          String(game.home_team || ""),
          String(game.away_team || ""),
          String(game.event_name || ""),
        ]
          .join(" ")
          .toLowerCase();
        const teamPass = !normalizedTeamQuery || teamText.includes(normalizedTeamQuery);
        return leaguePass && tierPass && teamPass;
      }),
    }))
    .sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));

  if (!filteredDays.length) {
    allDays = [];
    currentDayIndex = 0;
    return;
  }

  if (!fillMissingDays) {
    allDays = filteredDays;
    if (previousDayIso) {
      const nextIndex = allDays.findIndex((d) => String(d.date || "") === previousDayIso);
      currentDayIndex = nextIndex >= 0 ? nextIndex : clampDayIndex(currentDayIndex);
    } else {
      currentDayIndex = clampDayIndex(currentDayIndex);
    }
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
  const tierSortRank = (tierValue) => {
    const tierText = String(tierValue || "").trim().toLowerCase();
    if (!tierText) return Number.MAX_SAFE_INTEGER;
    const match = tierText.match(/(?:^|\s)tier\s*([0-9]+)(?:\s|$)/i) || tierText.match(/^([0-9]+)$/);
    if (!match) return Number.MAX_SAFE_INTEGER;
    const tierNum = Number.parseInt(match[1], 10);
    return Number.isFinite(tierNum) ? tierNum : Number.MAX_SAFE_INTEGER;
  };
  if (sortMode === "tier") {
    out.sort((a, b) => {
      const tierCmp = tierSortRank(a.tier) - tierSortRank(b.tier);
      if (tierCmp !== 0) return tierCmp;
      const leagueCmp = String(a.competition || "").localeCompare(String(b.competition || ""));
      if (leagueCmp !== 0) return leagueCmp;
      return kickoffSortKey(a) - kickoffSortKey(b);
    });
    return out;
  }
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

function tierRowClass(tierValue) {
  const tierText = String(tierValue || "").trim().toLowerCase();
  if (!tierText) return "";
  const match = tierText.match(/(?:^|\s)tier\s*([0-9]+)(?:\s|$)/i) || tierText.match(/^([0-9]+)$/);
  if (!match) return "";
  const tierNum = Number.parseInt(match[1], 10);
  if (tierNum === 1) return "tier-row-1";
  if (tierNum === 2) return "tier-row-2";
  if (tierNum === 3) return "tier-row-3";
  return "";
}

function dayHasComputedXgdMetrics(dayGames) {
  const metricKeys = [
    "season_xgd",
    "last5_xgd",
    "last3_xgd",
    "season_strength",
    "last5_strength",
    "last3_strength",
  ];
  return (dayGames || []).some((game) => {
    return metricKeys.some((metricKey) => {
      const value = game?.[metricKey];
      if (value == null) return false;
      return String(value).trim() !== "";
    });
  });
}

async function calculateHistoricalDayXgd(dayIso) {
  const dayText = String(dayIso || "").trim();
  if (!dayText || gamesMode !== "historical") return;
  if (historicalDayCalcInFlight.has(dayText)) return;

  historicalDayCalcInFlight.add(dayText);
  renderCurrentDay();
  statusText.textContent = `Calculating historical xGD for ${dayText}...`;

  try {
    const res = await fetch("/api/historical-day-xgd", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: dayText }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to calculate historical day xGD");
    await loadGames();
    const computedGames = Number(payload?.computed_games) || 0;
    const gamesCount = Number(payload?.games_count) || 0;
    statusText.textContent = `Computed xGD for ${computedGames}/${gamesCount} games on ${dayText}`;
  } catch (err) {
    statusText.textContent = String(err.message || err);
  } finally {
    historicalDayCalcInFlight.delete(dayText);
    renderCurrentDay();
  }
}

async function loadGames(options = {}) {
  const loadMoreHistorical = Boolean(options && options.loadMoreHistorical);
  const shouldLoadMoreHistorical = gamesMode === "historical" && loadMoreHistorical;
  if (shouldLoadMoreHistorical && !historicalHasMoreOlder) {
    return true;
  }

  statusText.textContent = shouldLoadMoreHistorical
    ? "Loading older historical matches..."
    : (gamesMode === "historical" ? "Loading historical matches..." : "Loading games...");
  try {
    const query = new URLSearchParams();
    query.set("mode", gamesMode);
    if (shouldLoadMoreHistorical) {
      query.set("load_more", "1");
    }
    const res = await fetch(`/api/games?${query.toString()}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load games");
    rawDays = payload.days || [];
    if (Array.isArray(payload?.saved_market_ids)) {
      setSavedMarketIds(payload.saved_market_ids);
    }
    fillMissingDays = payload.fill_missing_days !== false;
    historicalHasMoreOlder = gamesMode === "historical" ? Boolean(payload.has_more_older) : false;
    updateLeagueFilterOptions();
    updateTierFilterOptions(payload.tiers || []);
    applyGameFilters();
    renderCurrentDay();
    if (gamesMode === "historical") {
      const loadedTotal = Number(payload.total_games) || 0;
      if (shouldLoadMoreHistorical) {
        const addedGames = Number(payload.added_games) || 0;
        statusText.textContent = `Loaded ${addedGames} older matches (${loadedTotal} total)`;
      } else if (loadedTotal === 0 && historicalHasMoreOlder) {
        statusText.textContent = "No matches in the latest window. Click Previous Day to load older matches.";
      } else {
        statusText.textContent = `Loaded ${loadedTotal} historical matches`;
      }
    } else {
      statusText.textContent = `Loaded ${payload.total_games || 0} games`;
    }
    if (activeTab === "mapping") {
      loadManualMappings();
    } else if (activeTab === "rankings" && teamHcRankingsLoaded) {
      loadTeamHcRankings({ silent: true });
    } else if (activeTab === "saved" && savedGamesLoaded) {
      loadSavedGames({ silent: true });
    }

    if (selectedMarketId && !gamesById.has(selectedMarketId)) {
      selectedMarketId = null;
      detailsPanel.classList.add("hidden");
    }
    return true;
  } catch (err) {
    statusText.textContent = String(err.message || err);
    return false;
  }
}

async function rescanClosingPrices(triggerButton = null) {
  if (hcPerfRescanInFlight) return;
  const previousText = statusText.textContent;
  const buttonToDisable = triggerButton instanceof HTMLButtonElement ? triggerButton : null;
  hcPerfRescanInFlight = true;
  if (detailsMainTab === "hcperf" && lastXgdPayload) {
    renderXgd(lastXgdPayload);
  }
  if (buttonToDisable) buttonToDisable.disabled = true;
  statusText.textContent = "Rescanning historical closing prices...";
  try {
    const res = await fetch("/api/rescan-closing-prices", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to rescan closing prices");
    hcPerfPayloadByMarket.clear();
    await loadGames();
    if (selectedMarketId) {
      await loadGameHcPerf(selectedMarketId, true);
    }
    const updated = Number(payload?.updated_games) || 0;
    const changed = Number(payload?.changed_games) || 0;
    statusText.textContent = `Rescanned closing prices for ${updated} games (${changed} changed)`;
  } catch (err) {
    statusText.textContent = String(err.message || err || previousText || "Failed to rescan closing prices");
  } finally {
    hcPerfRescanInFlight = false;
    if (detailsMainTab === "hcperf" && lastXgdPayload) {
      renderXgd(lastXgdPayload);
    }
    if (buttonToDisable) buttonToDisable.disabled = false;
  }
}

function updateSavedTabLabel() {
  if (!(savedGamesTabBtn instanceof HTMLButtonElement)) return;
  savedGamesTabBtn.textContent = savedGamesCount > 0 ? `Saved Games (${savedGamesCount})` : "Saved Games";
}

function setSavedMarketIds(rawIds) {
  const next = new Set();
  if (Array.isArray(rawIds)) {
    for (const raw of rawIds) {
      const marketId = String(raw || "").trim();
      if (marketId) next.add(marketId);
    }
  }
  savedMarketIds = next;
  savedGamesCount = savedMarketIds.size;
  updateSavedTabLabel();
  updateDetailsSaveButton();
}

function updateDetailsSaveButton() {
  if (!(saveGameBtn instanceof HTMLButtonElement)) return;
  const marketIdText = String(selectedMarketId || "").trim();
  if (!marketIdText) {
    saveGameBtn.disabled = true;
    saveGameBtn.classList.remove("is-saved");
    saveGameBtn.textContent = "Save";
    saveGameBtn.title = "Open a game to save it";
    return;
  }
  const isSaved = savedMarketIds.has(marketIdText);
  saveGameBtn.disabled = false;
  saveGameBtn.classList.toggle("is-saved", isSaved);
  saveGameBtn.textContent = isSaved ? "Saved" : "Save";
  saveGameBtn.title = isSaved ? "Remove from Saved Games" : "Add to Saved Games";
}

async function setGameSavedState(marketId, shouldSave) {
  const marketIdText = String(marketId || "").trim();
  if (!marketIdText) return false;
  const endpoint = shouldSave ? "/api/saved-games" : "/api/saved-games/delete";
  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ market_id: marketIdText }),
  });
  const payload = await parseApiResponse(res);
  if (!res.ok) throw new Error(payload.error || "Failed to update saved game");
  if (Array.isArray(payload?.saved_market_ids)) {
    setSavedMarketIds(payload.saved_market_ids);
  } else if (shouldSave) {
    savedMarketIds.add(marketIdText);
    savedGamesCount = savedMarketIds.size;
    updateSavedTabLabel();
  } else {
    savedMarketIds.delete(marketIdText);
    savedGamesCount = savedMarketIds.size;
    updateSavedTabLabel();
  }
  savedGamesLoaded = false;
  updateDetailsSaveButton();
  return true;
}

async function toggleSelectedGameSaved() {
  const marketId = String(selectedMarketId || "").trim();
  if (!marketId) return;
  const targetSavedState = !savedMarketIds.has(marketId);
  const previousStatus = statusText.textContent;
  statusText.textContent = targetSavedState ? "Saving game..." : "Removing saved game...";
  try {
    await setGameSavedState(marketId, targetSavedState);
    statusText.textContent = targetSavedState ? "Game saved" : "Game removed from saved";
    if (activeTab === "saved") {
      await loadSavedGames({ silent: true });
      renderSavedGames();
    } else if (activeTab === "games") {
      renderCurrentDay();
    }
  } catch (err) {
    statusText.textContent = String(err.message || err || previousStatus || "Failed to update saved game");
  }
}

function createGamesTable(sortedGames, isHistorical, options = {}) {
  const emptyMessage = String(options?.emptyMessage || "No games");
  const table = document.createElement("table");
  table.className = "games-table";
  if (isHistorical) {
    table.innerHTML = `
      <thead>
        <tr>
          <th>Kickoff (UTC)</th>
          <th>League</th>
          <th>Game</th>
          <th class="home-price-col">Home</th>
          <th class="line-col handicap-line-col">HC</th>
          <th class="away-price-col">Away</th>
          <th class="goal-under-price-col">Under</th>
          <th class="goal-line-col">Goals</th>
          <th class="goal-over-price-col">Over</th>
          <th class="xg-home-col">xG H</th>
          <th class="line-col score-col">Score</th>
          <th class="xg-away-col">xG A</th>
          <th>xGD</th>
          <th>xGD Perf</th>
          <th>Min</th>
          <th>Max</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
  } else {
    table.innerHTML = `
      <thead>
        <tr>
          <th>Kickoff (UTC)</th>
          <th>League</th>
          <th>Game</th>
          <th class="home-price-col">Home</th>
          <th class="line-col handicap-line-col">HC</th>
          <th class="away-price-col">Away</th>
          <th>xGD</th>
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
  }

  const tbody = table.querySelector("tbody");
  if (!sortedGames.length) {
    const row = document.createElement("tr");
    row.innerHTML = `<td class="no-games-row" colspan="${isHistorical ? "16" : "13"}">${escapeHtml(emptyMessage)}</td>`;
    tbody.appendChild(row);
    return table;
  }

  for (const game of sortedGames) {
    gamesById.set(game.market_id, game);
    const row = document.createElement("tr");
    row.dataset.marketId = game.market_id;
    if (selectedMarketId === game.market_id) row.classList.add("selected");
    const tierClass = tierRowClass(game.tier);
    if (tierClass) row.classList.add(tierClass);

    const xgdMismatch = Boolean(game.xgd_competition_mismatch);
    const noXgMetrics = hasNoXgMetricSignal(game);
    const baseMetricCellClasses = ["metric-stack-cell"];
    const baseMetricCellNotes = [];
    if (xgdMismatch) {
      baseMetricCellClasses.push("xgd-mismatch-cell");
      baseMetricCellNotes.push("xGD derived from a different competition than this fixture.");
    }
    if (noXgMetrics) {
      baseMetricCellClasses.push("no-xg-cell");
      baseMetricCellNotes.push("No xG data available for this league; xGD/xGD Perf shown as -.");
    }
    const seasonStrengthValue = noXgMetrics ? "-" : game.season_strength;
    const last5StrengthValue = noXgMetrics ? "-" : game.last5_strength;
    const last3StrengthValue = noXgMetrics ? "-" : game.last3_strength;
    const seasonXgdValue = noXgMetrics ? "-" : game.season_xgd;
    const last5XgdValue = noXgMetrics ? "-" : game.last5_xgd;
    const last3XgdValue = noXgMetrics ? "-" : game.last3_xgd;
    const xgdCellClasses = [...baseMetricCellClasses];
    const xgdCellNotes = [...baseMetricCellNotes];
    const hcXgdConsensusDirection = noXgMetrics ? null : getHcXgdConsensusDirection(game);
    if (hcXgdConsensusDirection === "positive") {
      xgdCellClasses.push("hc-xgd-consensus-positive");
      xgdCellNotes.push("HC + xGD is > 0.1 for S/5/3.");
    } else if (hcXgdConsensusDirection === "negative") {
      xgdCellClasses.push("hc-xgd-consensus-negative");
      xgdCellNotes.push("HC + xGD is < -0.1 for S/5/3.");
    }
    const xgdCellClass = xgdCellClasses.join(" ");
    const xgdCellTitleAttr = xgdCellNotes.length ? ` title="${escapeHtml(xgdCellNotes.join(" | "))}"` : "";

    const xgdPerfCellClasses = [...baseMetricCellClasses];
    const xgdPerfCellNotes = [...baseMetricCellNotes];
    const hcXgdPerfConsensusDirection = noXgMetrics ? null : getHcXgdPerfConsensusDirection(game);
    if (hcXgdPerfConsensusDirection === "positive") {
      xgdPerfCellClasses.push("hc-xgd-consensus-positive");
      xgdPerfCellNotes.push("HC + xGD Perf is > 0.1 for S/5/3.");
    } else if (hcXgdPerfConsensusDirection === "negative") {
      xgdPerfCellClasses.push("hc-xgd-consensus-negative");
      xgdPerfCellNotes.push("HC + xGD Perf is < -0.1 for S/5/3.");
    }
    const xgdPerfCellClass = xgdPerfCellClasses.join(" ");
    const xgdPerfCellTitleAttr = xgdPerfCellNotes.length ? ` title="${escapeHtml(xgdPerfCellNotes.join(" | "))}"` : "";

    if (isHistorical) {
      row.innerHTML = `
        <td>${escapeHtml(game.kickoff_utc)}</td>
        <td>${escapeHtml(game.competition)}</td>
        <td>${escapeHtml(game.event_name)}</td>
        <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
        <td class="line-col handicap-line-col">${escapeHtml(game.mainline || "-")}</td>
        <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
        <td class="goal-under-price-col">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="goal-line-col">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="goal-over-price-col">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="xg-home-col">${escapeHtml(game.home_xg_actual || "-")}</td>
        <td class="line-col score-col">${escapeHtml(game.scoreline || "-")}</td>
        <td class="xg-away-col">${escapeHtml(game.away_xg_actual || "-")}</td>
        <td class="${xgdCellClass}"${xgdCellTitleAttr}>${buildPeriodMetricStackCell(seasonStrengthValue, last5StrengthValue, last3StrengthValue, xgdMismatch)}</td>
        <td class="${xgdPerfCellClass}"${xgdPerfCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue, xgdMismatch)}</td>
        <td class="metric-stack-cell"${xgdMismatch ? ` title="xGD derived from a different competition than this fixture."` : ""}>${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg, xgdMismatch)}</td>
        <td class="metric-stack-cell"${xgdMismatch ? ` title="xGD derived from a different competition than this fixture."` : ""}>${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg, xgdMismatch)}</td>
      `;
    } else {
      row.innerHTML = `
        <td>${escapeHtml(game.kickoff_utc)}</td>
        <td>${escapeHtml(game.competition)}</td>
        <td>${escapeHtml(game.event_name)}</td>
        <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
        <td class="line-col handicap-line-col">${escapeHtml(game.mainline || "-")}</td>
        <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
        <td class="${xgdCellClass}"${xgdCellTitleAttr}>${buildPeriodMetricStackCell(seasonStrengthValue, last5StrengthValue, last3StrengthValue, xgdMismatch)}</td>
        <td class="${xgdPerfCellClass}"${xgdPerfCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue, xgdMismatch)}</td>
        <td class="goal-under-price-col">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="goal-line-col">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="goal-over-price-col">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="metric-stack-cell"${xgdMismatch ? ` title="xGD derived from a different competition than this fixture."` : ""}>${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg, xgdMismatch)}</td>
        <td class="metric-stack-cell"${xgdMismatch ? ` title="xGD derived from a different competition than this fixture."` : ""}>${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg, xgdMismatch)}</td>
      `;
    }
    row.addEventListener("click", () => loadGameXgd(game.market_id));
    tbody.appendChild(row);
  }
  return table;
}

function renderSavedGames() {
  if (!savedGamesView) return;
  savedGamesView.innerHTML = "";

  if (savedGamesLoading) {
    savedGamesView.innerHTML = "<p>Loading saved games...</p>";
    return;
  }
  if (savedGamesErrorText) {
    savedGamesView.innerHTML = `<p>${escapeHtml(savedGamesErrorText)}</p>`;
    return;
  }
  if (!savedDays.length) {
    savedGamesView.innerHTML = "<p>No saved games yet.</p>";
    return;
  }

  gamesById = new Map();
  for (const day of savedDays) {
    const dayGames = Array.isArray(day?.games) ? day.games : [];
    const sortedDayGames = sortGamesForDay(dayGames);
    const useHistoricalLayout = sortedDayGames.some((game) => Boolean(game?.is_historical));

    const block = document.createElement("section");
    block.className = "day-block";
    const header = document.createElement("div");
    header.className = "day-header";
    const heading = document.createElement("h2");
    heading.className = "day-header-title";
    heading.textContent = `${String(day?.date_label || day?.date || "Saved")} (${sortedDayGames.length})`;
    header.appendChild(heading);

    const table = createGamesTable(sortedDayGames, useHistoricalLayout, { emptyMessage: "No saved games" });
    block.appendChild(header);
    block.appendChild(table);
    savedGamesView.appendChild(block);
  }
}

function getRankingSortCounts(row) {
  const venueKey = teamHcRankingsVenueMode === "home" || teamHcRankingsVenueMode === "away"
    ? teamHcRankingsVenueMode
    : "overall";
  const metricKey = teamHcRankingsSortMetric === "xg" ? "xg" : "result";
  const counts = row && typeof row === "object" && row[metricKey] && typeof row[metricKey] === "object"
    ? row[metricKey][venueKey]
    : null;
  if (!counts || typeof counts !== "object") {
    return { win: -1, push: -1, loss: Number.POSITIVE_INFINITY };
  }
  const win = Number(counts.win) || 0;
  const push = Number(counts.push) || 0;
  const loss = Number(counts.loss) || 0;
  if (metricKey === "result") {
    const halfWin = Number(counts.half_win) || 0;
    const halfLoss = Number(counts.half_loss) || 0;
    return {
      win: win + (0.5 * halfWin),
      push: push + (0.5 * halfWin) + (0.5 * halfLoss),
      loss: loss + (0.5 * halfLoss),
    };
  }
  return { win, push, loss };
}

function getRankingSortPnl(row) {
  const venueKey = teamHcRankingsVenueMode === "home" || teamHcRankingsVenueMode === "away"
    ? teamHcRankingsVenueMode
    : "overall";
  const pnlRaw = row && typeof row === "object" && row.pnl && typeof row.pnl === "object"
    ? row.pnl[venueKey]
    : null;
  const pnlNum = Number(pnlRaw);
  return Number.isFinite(pnlNum) ? pnlNum : Number.NEGATIVE_INFINITY;
}

function getRankingSortPnlAgainst(row) {
  const venueKey = teamHcRankingsVenueMode === "home" || teamHcRankingsVenueMode === "away"
    ? teamHcRankingsVenueMode
    : "overall";
  const pnlRaw = row && typeof row === "object" && row.pnl_against && typeof row.pnl_against === "object"
    ? row.pnl_against[venueKey]
    : null;
  const pnlNum = Number(pnlRaw);
  return Number.isFinite(pnlNum) ? pnlNum : Number.NEGATIVE_INFINITY;
}

function getRankingPnlAgainst(row, venueKey) {
  const pnlRaw = row && typeof row === "object" && row.pnl_against && typeof row.pnl_against === "object"
    ? row.pnl_against[venueKey]
    : null;
  const pnlNum = Number(pnlRaw);
  return Number.isFinite(pnlNum) ? pnlNum : null;
}

function getRankingGamesPlayed(row, venueKey) {
  const raw = row && typeof row === "object" && row.games_played && typeof row.games_played === "object"
    ? row.games_played[venueKey]
    : null;
  const value = Number(raw);
  return Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : 0;
}

function getRankingGamesWithHandicap(row, venueKey) {
  const fromPayload = row && typeof row === "object" && row.games_with_handicap && typeof row.games_with_handicap === "object"
    ? row.games_with_handicap[venueKey]
    : null;
  const fromPayloadNum = Number(fromPayload);
  if (Number.isFinite(fromPayloadNum)) {
    return Math.max(0, Math.trunc(fromPayloadNum));
  }
  const fromResultTotal = row && typeof row === "object"
    && row.result && typeof row.result === "object"
    && row.result[venueKey] && typeof row.result[venueKey] === "object"
    ? row.result[venueKey].total
    : null;
  const fromResultTotalNum = Number(fromResultTotal);
  return Number.isFinite(fromResultTotalNum) ? Math.max(0, Math.trunc(fromResultTotalNum)) : 0;
}

function setSelectedTeamHcRankingsLeague(competitionName) {
  const next = String(competitionName || "").trim();
  selectedTeamHcRankingsLeague = next;
}

function setTeamHcRankingsLeagueFilterOpen(isOpen) {
  if (!teamHcRankingsLeagueFilterMenu || !teamHcRankingsLeagueFilterBtn) return;
  teamHcRankingsLeagueFilterMenu.classList.toggle("hidden", !isOpen);
  teamHcRankingsLeagueFilterBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
}

function updateTeamHcRankingsLeagueFilterButtonLabel() {
  if (!teamHcRankingsLeagueFilterBtn) return;
  const selectedText = String(selectedTeamHcRankingsLeague || "").trim();
  teamHcRankingsLeagueFilterBtn.textContent = selectedText || "Select League";
}

function updateTeamHcRankingsLeagueFilterMenu() {
  if (!teamHcRankingsLeagueFilterMenu) return;
  teamHcRankingsLeagueFilterMenu.innerHTML = "";
  const leagueNames = (Array.isArray(teamHcRankingsLeagues) ? teamHcRankingsLeagues : [])
    .map((row) => String(row?.competition || "").trim())
    .filter((name) => !!name);
  if (!leagueNames.length) {
    const empty = document.createElement("p");
    empty.className = "league-filter-empty";
    empty.textContent = "No leagues available.";
    teamHcRankingsLeagueFilterMenu.appendChild(empty);
    return;
  }

  const searchWrap = document.createElement("div");
  searchWrap.className = "league-filter-search";
  const searchInput = document.createElement("input");
  searchInput.type = "search";
  searchInput.className = "league-filter-search-input";
  searchInput.placeholder = "Search leagues...";
  searchInput.value = teamHcRankingsLeagueSearch;
  searchInput.addEventListener("input", () => {
    teamHcRankingsLeagueSearch = String(searchInput.value || "");
    updateTeamHcRankingsLeagueFilterMenu();
    const nextInput = teamHcRankingsLeagueFilterMenu.querySelector(".league-filter-search-input");
    if (nextInput instanceof HTMLInputElement) {
      nextInput.focus();
    }
  });
  searchWrap.appendChild(searchInput);
  teamHcRankingsLeagueFilterMenu.appendChild(searchWrap);

  const query = teamHcRankingsLeagueSearch.trim().toLowerCase();
  const visibleLeagues = !query
    ? leagueNames
    : leagueNames.filter((league) => league.toLowerCase().includes(query));
  if (!visibleLeagues.length) {
    const empty = document.createElement("p");
    empty.className = "league-filter-empty";
    empty.textContent = "No leagues match search.";
    teamHcRankingsLeagueFilterMenu.appendChild(empty);
    return;
  }

  for (const leagueName of visibleLeagues) {
    const row = document.createElement("label");
    row.className = "league-option";
    const input = document.createElement("input");
    input.type = "radio";
    input.name = "teamHcRankingsLeague";
    input.value = leagueName;
    input.checked = leagueName === selectedTeamHcRankingsLeague;
    const text = document.createElement("span");
    text.textContent = leagueName;
    row.appendChild(input);
    row.appendChild(text);
    teamHcRankingsLeagueFilterMenu.appendChild(row);
  }
}

function sortTeamHcRankingRows(rows) {
  const safeRows = Array.isArray(rows) ? [...rows] : [];
  safeRows.sort((a, b) => {
    if (teamHcRankingsSortMetric === "pnl") {
      const pnlDiff = getRankingSortPnl(b) - getRankingSortPnl(a);
      if (Math.abs(pnlDiff) > 1e-12) return pnlDiff;
      return String(a?.team || "").localeCompare(String(b?.team || ""));
    }
    if (teamHcRankingsSortMetric === "pnl_against") {
      const pnlAgainstDiff = getRankingSortPnlAgainst(b) - getRankingSortPnlAgainst(a);
      if (Math.abs(pnlAgainstDiff) > 1e-12) return pnlAgainstDiff;
      return String(a?.team || "").localeCompare(String(b?.team || ""));
    }
    const aCounts = getRankingSortCounts(a);
    const bCounts = getRankingSortCounts(b);
    const winDiff = bCounts.win - aCounts.win;
    if (Math.abs(winDiff) > 1e-12) return winDiff;
    const pushDiff = bCounts.push - aCounts.push;
    if (Math.abs(pushDiff) > 1e-12) return pushDiff;
    const lossDiff = aCounts.loss - bCounts.loss;
    if (Math.abs(lossDiff) > 1e-12) return lossDiff;
    return String(a?.team || "").localeCompare(String(b?.team || ""));
  });
  return safeRows;
}

function closeTeamHcPerfPanel() {
  if (teamHcPerfPanel) {
    teamHcPerfPanel.classList.add("hidden");
  }
}

async function loadTeamHcRankingTeamDetails(teamName, competitionName) {
  if (!teamHcPerfPanel || !teamHcPerfContent || !teamHcPerfTitle || !teamHcPerfMeta) return;
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  if (!teamText || !competitionText) return;
  const requestKey = `${competitionText}::${teamText}`;
  teamHcPerfDetailLoadingKey = requestKey;

  teamHcPerfTitle.textContent = `${teamText} - HC Perf`;
  teamHcPerfMeta.textContent = `${competitionText} | Loading...`;
  teamHcPerfContent.innerHTML = `
    <div class="hcperf-loading">
      <span class="hcperf-loading-dot" aria-hidden="true"></span>
      <span>Loading team handicap performance...</span>
    </div>
  `;
  teamHcPerfPanel.classList.remove("hidden");

  try {
    const query = new URLSearchParams();
    query.set("team", teamText);
    query.set("competition", competitionText);
    const res = await fetch(`/api/team-hc-rankings/details?${query.toString()}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load team handicap performance");
    if (teamHcPerfDetailLoadingKey !== requestKey) return;

    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    const homeRows = rows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "home");
    const awayRows = rows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "away");

    teamHcPerfTitle.textContent = `${teamText} - HC Perf`;
    teamHcPerfMeta.textContent = `${competitionText} | ${rows.length} games`;
    if (!rows.length) {
      teamHcPerfContent.innerHTML = "<p>No games found for this team in this league.</p>";
      return;
    }

    const summaryHtml = buildHcPerfSummaryTableHtml(teamText, rows);
    const generalGamesHtml = buildSeasonHandicapPerformanceTableHtml(teamText, rows, {
      title: `${teamText} - General`,
      relevantTeam: teamText,
    });
    const homeGamesHtml = buildSeasonHandicapPerformanceTableHtml(teamText, homeRows, {
      title: `${teamText} - Home`,
      relevantTeam: teamText,
    });
    const awayGamesHtml = buildSeasonHandicapPerformanceTableHtml(teamText, awayRows, {
      title: `${teamText} - Away`,
      relevantTeam: teamText,
    });
    teamHcPerfContent.innerHTML = `${summaryHtml}${generalGamesHtml}${homeGamesHtml}${awayGamesHtml}`;
  } catch (err) {
    if (teamHcPerfDetailLoadingKey !== requestKey) return;
    teamHcPerfMeta.textContent = `${competitionText} | Error`;
    teamHcPerfContent.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
  }
}

function renderTeamHcRankings() {
  if (!teamHcRankingsView) return;
  teamHcRankingsView.innerHTML = "";
  updateTeamHcRankingsLeagueFilterButtonLabel();
  updateTeamHcRankingsLeagueFilterMenu();
  if (teamHcRankingsLoading) {
    teamHcRankingsView.innerHTML = "<p>Loading HC rankings...</p>";
    return;
  }
  if (teamHcRankingsErrorText) {
    teamHcRankingsView.innerHTML = `<p>${escapeHtml(teamHcRankingsErrorText)}</p>`;
    return;
  }
  const selectedLeague = teamHcRankingsLeagues.find(
    (row) => String(row?.competition || "").trim() === selectedTeamHcRankingsLeague
  );
  const leagueRows = Array.isArray(selectedLeague?.rows) ? selectedLeague.rows : [];
  const sortedRows = sortTeamHcRankingRows(leagueRows);
  if (!sortedRows.length) {
    teamHcRankingsView.innerHTML = "<p>No ranked teams found for this league.</p>";
    return;
  }
  const venueKey = teamHcRankingsVenueMode === "home" || teamHcRankingsVenueMode === "away"
    ? teamHcRankingsVenueMode
    : "overall";
  const venueLabel = venueKey === "overall" ? "General" : (venueKey === "home" ? "Home" : "Away");
  const showGamesMissingColumn = sortedRows.some((row) => {
    const gamesPlayed = getRankingGamesPlayed(row, venueKey);
    const gamesWithHandicap = getRankingGamesWithHandicap(row, venueKey);
    return gamesWithHandicap !== gamesPlayed;
  });

  const table = document.createElement("table");
  table.className = "games-table recent-lines-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>#</th>
        <th>Team</th>
        <th>Games Played (${venueLabel})</th>
        <th>
          <button
            type="button"
            class="team-hc-sort-head-btn ${teamHcRankingsSortMetric === "result" ? "active" : ""}"
            data-rank-sort="result"
          >
            Result (${venueLabel})
          </button>
        </th>
        <th>
          <button
            type="button"
            class="team-hc-sort-head-btn ${teamHcRankingsSortMetric === "xg" ? "active" : ""}"
            data-rank-sort="xg"
          >
            xG (${venueLabel})
          </button>
        </th>
        <th>
          <button
            type="button"
            class="team-hc-sort-head-btn ${teamHcRankingsSortMetric === "pnl" ? "active" : ""}"
            data-rank-sort="pnl"
          >
            PnL (For)
          </button>
        </th>
        <th>
          <button
            type="button"
            class="team-hc-sort-head-btn ${teamHcRankingsSortMetric === "pnl_against" ? "active" : ""}"
            data-rank-sort="pnl_against"
          >
            PnL (Against)
          </button>
        </th>
        ${showGamesMissingColumn ? "<th>Games Missing</th>" : ""}
      </tr>
    </thead>
    <tbody></tbody>
  `;
  const tbody = table.querySelector("tbody");
  sortedRows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    const tierClass = tierRowClass(row?.tier);
    if (tierClass) tr.classList.add(tierClass);
    const teamToken = encodeURIComponent(String(row?.team || ""));
    const leagueToken = encodeURIComponent(String(row?.competition || ""));
    const gamesPlayed = getRankingGamesPlayed(row, venueKey);
    const gamesWithHandicap = getRankingGamesWithHandicap(row, venueKey);
    const gamesMissing = Math.max(0, gamesPlayed - gamesWithHandicap);
    const pnlAgainst = getRankingPnlAgainst(row, venueKey);
    tr.innerHTML = `
      <td>${idx + 1}</td>
      <td><button type="button" class="team-hc-rank-team-btn" data-team="${teamToken}" data-league="${leagueToken}">${escapeHtml(String(row?.team || "-"))}</button></td>
      <td>${gamesPlayed}</td>
      <td class="hcperf-summary-cell">${formatHcPerfSummaryCell(row?.result?.[venueKey])}</td>
      <td class="hcperf-summary-cell">${formatHcPerfSummaryCellBasic(row?.xg?.[venueKey])}</td>
      <td>${formatSignedMetricValue(row?.pnl?.[venueKey], 2)}</td>
      <td>${formatSignedMetricValue(pnlAgainst, 2)}</td>
      ${showGamesMissingColumn ? `<td>${gamesMissing}</td>` : ""}
    `;
    const teamBtn = tr.querySelector(".team-hc-rank-team-btn");
    if (teamBtn instanceof HTMLButtonElement) {
      teamBtn.addEventListener("click", () => {
        const teamName = decodeURIComponent(String(teamBtn.dataset.team || ""));
        const leagueName = decodeURIComponent(String(teamBtn.dataset.league || ""));
        loadTeamHcRankingTeamDetails(teamName, leagueName);
      });
    }
    tbody.appendChild(tr);
  });
  const sortHeaderButtons = table.querySelectorAll(".team-hc-sort-head-btn");
  for (const button of sortHeaderButtons) {
    if (!(button instanceof HTMLButtonElement)) continue;
    button.addEventListener("click", () => {
      const mode = String(button.dataset.rankSort || "").trim().toLowerCase();
      const nextMetric = mode === "xg" || mode === "pnl" || mode === "pnl_against" ? mode : "result";
      if (nextMetric === teamHcRankingsSortMetric) return;
      teamHcRankingsSortMetric = nextMetric;
      if (teamHcRankingsSortSelect instanceof HTMLSelectElement) {
        teamHcRankingsSortSelect.value = teamHcRankingsSortMetric;
      }
      renderTeamHcRankings();
    });
  }
  teamHcRankingsView.appendChild(table);
}

async function loadTeamHcRankings(options = {}) {
  if (teamHcRankingsLoading) return false;
  const silent = Boolean(options?.silent);
  teamHcRankingsLoading = true;
  if (!silent) {
    statusText.textContent = "Loading team HC rankings...";
  }
  renderTeamHcRankings();
  try {
    const res = await fetch("/api/team-hc-rankings");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load team HC rankings");
    teamHcRankingsLeagues = Array.isArray(payload?.leagues) ? payload.leagues : [];
    teamHcRankingsRows = Array.isArray(payload?.rows) ? payload.rows : [];
    if (
      !selectedTeamHcRankingsLeague
      || !teamHcRankingsLeagues.some((row) => String(row?.competition || "").trim() === selectedTeamHcRankingsLeague)
    ) {
      const premierLeague = teamHcRankingsLeagues.find((row) => {
        const name = String(row?.competition || "").trim().toLowerCase();
        return name === "premier league" || name.includes("premier league");
      });
      const defaultLeague = premierLeague || teamHcRankingsLeagues[0];
      selectedTeamHcRankingsLeague = String(defaultLeague?.competition || "").trim();
    }
    teamHcRankingsLoaded = true;
    teamHcRankingsErrorText = "";
    if (activeTab === "rankings" && !silent) {
      statusText.textContent = `Loaded HC rankings for ${Number(payload?.total_teams) || 0} teams across ${Number(payload?.total_leagues) || 0} leagues`;
    }
    renderTeamHcRankings();
    return true;
  } catch (err) {
    teamHcRankingsErrorText = String(err.message || err);
    if (!silent) {
      statusText.textContent = teamHcRankingsErrorText;
    }
    return false;
  } finally {
    teamHcRankingsLoading = false;
    renderTeamHcRankings();
  }
}

async function loadSavedGames(options = {}) {
  if (savedGamesLoading) return false;
  const silent = Boolean(options?.silent);
  savedGamesLoading = true;
  if (!silent) {
    statusText.textContent = "Loading saved games...";
  }
  renderSavedGames();
  try {
    const res = await fetch("/api/saved-games");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load saved games");
    savedDays = Array.isArray(payload?.days) ? payload.days : [];
    savedGamesLoaded = true;
    savedGamesErrorText = "";
    setSavedMarketIds(payload?.saved_market_ids || []);
    if (activeTab === "saved" && !silent) {
      statusText.textContent = `Loaded ${Number(payload?.total_games) || 0} saved games`;
    }
    renderSavedGames();
    return true;
  } catch (err) {
    savedGamesErrorText = String(err.message || err);
    if (!silent) {
      statusText.textContent = savedGamesErrorText;
    }
    return false;
  } finally {
    savedGamesLoading = false;
    renderSavedGames();
  }
}

function renderCurrentDay() {
  gamesById = new Map();
  calendarView.innerHTML = "";

  if (!allDays.length) {
    dayLabel.textContent = "No games";
    const canLoadOlderHistorical = gamesMode === "historical" && historicalHasMoreOlder;
    prevDayBtn.disabled = !canLoadOlderHistorical;
    todayBtn.disabled = true;
    nextDayBtn.disabled = true;
    calendarView.innerHTML = canLoadOlderHistorical
      ? "<p>No games in the current historical window. Click Previous Day to load older matches.</p>"
      : "<p>No games found for selected filters.</p>";
    return;
  }

  currentDayIndex = clampDayIndex(currentDayIndex);
  const day = allDays[currentDayIndex];
  const sortedDayGames = sortGamesForDay(day.games || []);
  const isHistorical = gamesMode === "historical";
  const todayIndex = allDays.findIndex((entry) => String(entry.date || "") === getTodayIsoUtc());
  const jumpTargetIndex = (todayIndex >= 0)
    ? todayIndex
    : (gamesMode === "historical" ? allDays.length - 1 : -1);
  dayLabel.textContent = `${day.date_label} (${sortedDayGames.length})`;
  const atOldestLoadedDay = currentDayIndex === 0;
  const canLoadOlderHistorical = isHistorical && atOldestLoadedDay && historicalHasMoreOlder;
  prevDayBtn.disabled = atOldestLoadedDay && !canLoadOlderHistorical;
  todayBtn.disabled = jumpTargetIndex < 0 || currentDayIndex === jumpTargetIndex;
  nextDayBtn.disabled = currentDayIndex === allDays.length - 1;

  const block = document.createElement("section");
  block.className = "day-block";

  const header = document.createElement("div");
  header.className = "day-header day-header-bar";
  const headerTitle = document.createElement("h2");
  headerTitle.className = "day-header-title";
  headerTitle.textContent = day.date_label;
  header.appendChild(headerTitle);

  const headerActions = document.createElement("div");
  headerActions.className = "day-header-actions";
  if (tableControls) {
    headerActions.appendChild(tableControls);
  }
  if (isHistorical) {
    const dayIso = String(day.date || "").trim();
    const isComputingDay = historicalDayCalcInFlight.has(dayIso);
    const hasComputedXgd = dayHasComputedXgdMetrics(sortedDayGames);
    const calculateBtn = document.createElement("button");
    calculateBtn.type = "button";
    calculateBtn.className = "day-calc-btn";
    calculateBtn.disabled = isComputingDay || !dayIso;
    calculateBtn.textContent = isComputingDay
      ? "Calculating xGD..."
      : (hasComputedXgd ? "Recalculate Day xGD" : "Calculate Day xGD");
    calculateBtn.addEventListener("click", () => {
      calculateHistoricalDayXgd(dayIso);
    });
    headerActions.appendChild(calculateBtn);
  }
  if (headerActions.childElementCount > 0) {
    header.appendChild(headerActions);
  }

  const table = createGamesTable(sortedDayGames, isHistorical, { emptyMessage: "No games" });
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
  const payloadSeasonHandicapRows =
    payload.season_handicap_rows && typeof payload.season_handicap_rows === "object"
      ? payload.season_handicap_rows
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
    season_handicap_rows: payloadSeasonHandicapRows,
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
    season_handicap_rows:
      view?.season_handicap_rows && typeof view.season_handicap_rows === "object"
        ? view.season_handicap_rows
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
  const normalizeSeasonHandicapRows = (rowsObj) => ({
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
  const cachedHcPerfPayload = selectedMarketId ? hcPerfPayloadByMarket.get(selectedMarketId) : null;
  const cachedHcPerfRows = normalizeSeasonHandicapRows(cachedHcPerfPayload?.season_handicap_rows);
  const activeSeasonRowsRaw = normalizeSeasonHandicapRows(activeView.season_handicap_rows);
  const fallbackSeasonRows = normalizeSeasonHandicapRows(payloadSeasonHandicapRows);
  const seasonHandicapRows = (cachedHcPerfRows.home.length || cachedHcPerfRows.away.length)
    ? cachedHcPerfRows
    : ((activeSeasonRowsRaw.home.length || activeSeasonRowsRaw.away.length)
    ? activeSeasonRowsRaw
    : fallbackSeasonRows);
  const mappingHead = mappingRows[0] || {};
  const homeLabel = String(cachedHcPerfPayload?.home_label || mappingHead.home_sofa || mappingHead.home_raw || "Home team");
  const awayLabel = String(cachedHcPerfPayload?.away_label || mappingHead.away_sofa || mappingHead.away_raw || "Away team");
  const homeSeasonHandicapRows = seasonHandicapRows.home;
  const awaySeasonHandicapRows = seasonHandicapRows.away;
  const hcPerfIsAway = hcPerfTeamView === "away";
  const hcPerfLabel = hcPerfIsAway ? awayLabel : homeLabel;
  const hcPerfRows = hcPerfIsAway ? awaySeasonHandicapRows : homeSeasonHandicapRows;
  const hcPerfVenueKey = hcPerfIsAway ? "away" : "home";
  const hcPerfVenueRows = hcPerfRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === hcPerfVenueKey);
  const hcPerfLoading = Boolean(selectedMarketId) && hcPerfLoadingMarketId === selectedMarketId;
  const hcPerfRescanLoading = hcPerfRescanInFlight;
  const hcPerfBusy = hcPerfLoading || hcPerfRescanLoading;
  const hcPerfErrorText = String(cachedHcPerfPayload?.error || "").trim();
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
  const historicalResultSection = buildHistoricalResultSection(payload);

  const xgdTabContent = `${warning}${historicalResultSection}${metricHtml}${periodTable}${recentMatchesSection}${mappingTable}`;
  const statsIsAway = statsTeamView === "away";
  const statsActiveLabel = statsIsAway ? awayLabel : homeLabel;
  const statsActiveRows = statsIsAway ? awayRecentRows : homeRecentRows;
  const statsActiveVenueRows = statsIsAway ? awayTeamVenueRows : homeTeamVenueRows;
  const statsAllVenueRows = [
    ...(Array.isArray(statsActiveVenueRows?.home) ? statsActiveVenueRows.home : []),
    ...(Array.isArray(statsActiveVenueRows?.away) ? statsActiveVenueRows.away : []),
  ];
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
    ${buildGamestateTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsGamesShownCount)}
    ${buildCardsCornersVenueTableHtml(
      homeFixtureVenueRows,
      awayFixtureVenueRows,
      homeLabel,
      awayLabel,
      statsGamesShownCount
    )}
    ${buildStatsSwitchHtml(homeLabel, awayLabel)}
    ${buildCardsCornersMatchesTableHtml(statsActiveLabel, statsActiveRows, statsGamesShownCount, "Fixture-side matches")}
    <h3 class="section-title">${escapeHtml(statsActiveLabel)}: Home & Away Matches</h3>
    ${buildCardsCornersMatchesTableHtml(statsActiveLabel, statsAllVenueRows, statsGamesShownCount, "All matches")}
  `;
  const hcPerfTabContent = `
    <h3 class="section-title">Season Handicap Performance</h3>
    ${hcPerfBusy ? `
      <div class="hcperf-loading">
        <span class="hcperf-loading-dot" aria-hidden="true"></span>
        <span>Loading historical closing prices...</span>
      </div>
    ` : ""}
    ${hcPerfErrorText ? `<div class="warning">${escapeHtml(hcPerfErrorText)}</div>` : ""}
    <div class="hcperf-controls">
      <button type="button" class="hcperf-rescan-btn" ${hcPerfBusy ? "disabled" : ""}>
        ${hcPerfRescanLoading ? "Rescanning..." : "Rescan Closing Prices"}
      </button>
    </div>
    ${buildHcPerfSwitchHtml(homeLabel, awayLabel)}
    ${buildHcPerfSummaryTableHtml(hcPerfLabel, hcPerfRows, { focusVenue: hcPerfVenueKey })}
    ${buildSeasonHandicapPerformanceTableHtml(hcPerfLabel, hcPerfVenueRows, {
      title: `${hcPerfLabel} - Venue Only Games`,
      relevantTeam: hcPerfLabel,
    })}
    ${buildSeasonHandicapPerformanceTableHtml(hcPerfLabel, hcPerfRows, {
      title: `${hcPerfLabel} - All Games`,
      relevantTeam: hcPerfLabel,
    })}
  `;
  if (detailsMainTab !== "cards" && detailsMainTab !== "hcperf") {
    detailsMainTab = "xgd";
  }
  const detailsTabNav = `
    <section class="page-tabs details-main-tabs">
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "xgd" ? "active" : ""}" data-details-tab="xgd">xGD</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "cards" ? "active" : ""}" data-details-tab="cards">Stats</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "hcperf" ? "active" : ""}" data-details-tab="hcperf">HC Perf</button>
    </section>
  `;
  const activeTabContent = detailsMainTab === "cards"
    ? cardsTabContent
    : (detailsMainTab === "hcperf" ? hcPerfTabContent : xgdTabContent);
  linesContainer.innerHTML = `${detailsTabNav}${viewTabsHtml}${activeTabContent}`;

  if (detailsMainTab === "hcperf" && selectedMarketId && !cachedHcPerfPayload && hcPerfLoadingMarketId !== selectedMarketId) {
    loadGameHcPerf(selectedMarketId);
  }

  const detailsTabButtons = linesContainer.querySelectorAll(".details-main-tab");
  for (const button of detailsTabButtons) {
    button.addEventListener("click", () => {
      const rawTargetTab = String(button.dataset.detailsTab || "").trim().toLowerCase();
      const targetTab = rawTargetTab === "cards" || rawTargetTab === "hcperf" ? rawTargetTab : "xgd";
      if (targetTab === detailsMainTab) return;
      detailsMainTab = targetTab;
      if (targetTab === "hcperf" && selectedMarketId) {
        loadGameHcPerf(selectedMarketId);
      }
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

  const hcPerfSwitchButtons = linesContainer.querySelectorAll(".hcperf-switch-btn");
  for (const button of hcPerfSwitchButtons) {
    button.addEventListener("click", () => {
      const targetView = button.dataset.hcperfTeamView === "away" ? "away" : "home";
      if (targetView === hcPerfTeamView) return;
      hcPerfTeamView = targetView;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const hcPerfRescanButton = linesContainer.querySelector(".hcperf-rescan-btn");
  if (hcPerfRescanButton instanceof HTMLButtonElement) {
    hcPerfRescanButton.addEventListener("click", () => {
      rescanClosingPrices(hcPerfRescanButton);
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

async function loadGameHcPerf(marketId, force = false) {
  const key = String(marketId || "").trim();
  if (!key) return;
  if (!force && hcPerfPayloadByMarket.has(key)) return;
  if (hcPerfLoadingMarketId === key) return;

  hcPerfLoadingMarketId = key;
  if (selectedMarketId === key && detailsMainTab === "hcperf" && lastXgdPayload) {
    renderXgd(lastXgdPayload);
  }
  try {
    const res = await fetch(`/api/game-hcperf?market_id=${encodeURIComponent(key)}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load HC performance");
    hcPerfPayloadByMarket.set(key, payload || {});
  } catch (err) {
    hcPerfPayloadByMarket.set(key, {
      season_handicap_rows: { home: [], away: [] },
      error: String(err.message || err),
    });
  } finally {
    if (hcPerfLoadingMarketId === key) {
      hcPerfLoadingMarketId = null;
    }
    if (selectedMarketId === key && detailsMainTab === "hcperf" && lastXgdPayload) {
      renderXgd(lastXgdPayload);
    }
  }
}

async function loadGameXgd(marketId) {
  const game = findGameByMarketId(marketId);
  if (!game) return;

  selectedMarketId = marketId;
  if (activeTab === "saved") {
    renderSavedGames();
  } else {
    renderCurrentDay();
  }
  updateDetailsSaveButton();

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
  hcPerfTeamView = "home";
  activeXgdViewId = null;
  lastXgdPayload = null;
  hcPerfLoadingMarketId = null;
  const scoreMeta = game.is_historical ? ` | FT ${String(game.scoreline || "-")}` : "";
  detailsMeta.textContent = `${game.competition} | ${game.kickoff_raw}${scoreMeta}`;
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

function findGameByMarketId(marketId) {
  const key = String(marketId || "").trim();
  if (!key) return null;
  const cached = gamesById.get(key);
  if (cached) return cached;

  const findInDays = (days) => {
    if (!Array.isArray(days)) return null;
    for (const day of days) {
      const dayGames = Array.isArray(day?.games) ? day.games : [];
      for (const game of dayGames) {
        if (String(game?.market_id || "").trim() === key) {
          return game;
        }
      }
    }
    return null;
  };

  const found = findInDays(allDays) || findInDays(savedDays) || findInDays(rawDays);
  if (found) {
    gamesById.set(key, found);
  }
  return found;
}

refreshBtn.addEventListener("click", () => loadGames());
gamesTabBtn.addEventListener("click", () => {
  setActiveTab("games");
});
if (savedGamesTabBtn instanceof HTMLButtonElement) {
  savedGamesTabBtn.addEventListener("click", () => {
    setActiveTab("saved");
  });
}
if (teamHcRankingsTabBtn instanceof HTMLButtonElement) {
  teamHcRankingsTabBtn.addEventListener("click", () => {
    setActiveTab("rankings");
  });
}
manualMappingTabBtn.addEventListener("click", () => {
  setActiveTab("mapping");
  loadManualMappings();
});
upcomingModeBtn.addEventListener("click", () => {
  setGamesMode("upcoming");
});
historicalModeBtn.addEventListener("click", () => {
  setGamesMode("historical");
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
if (teamHcRankingsGeneralTabBtn instanceof HTMLButtonElement) {
  teamHcRankingsGeneralTabBtn.addEventListener("click", () => {
    setTeamHcRankingsVenueMode("overall");
    renderTeamHcRankings();
  });
}
if (teamHcRankingsHomeTabBtn instanceof HTMLButtonElement) {
  teamHcRankingsHomeTabBtn.addEventListener("click", () => {
    setTeamHcRankingsVenueMode("home");
    renderTeamHcRankings();
  });
}
if (teamHcRankingsAwayTabBtn instanceof HTMLButtonElement) {
  teamHcRankingsAwayTabBtn.addEventListener("click", () => {
    setTeamHcRankingsVenueMode("away");
    renderTeamHcRankings();
  });
}
if (teamHcRankingsSortSelect instanceof HTMLSelectElement) {
  teamHcRankingsSortSelect.addEventListener("change", () => {
    const mode = String(teamHcRankingsSortSelect.value || "").trim();
    if (mode === "xg" || mode === "pnl" || mode === "pnl_against") {
      teamHcRankingsSortMetric = mode;
    } else {
      teamHcRankingsSortMetric = "result";
    }
    renderTeamHcRankings();
  });
}
if (teamHcRankingsLeagueFilterBtn instanceof HTMLButtonElement) {
  teamHcRankingsLeagueFilterBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    const willOpen = teamHcRankingsLeagueFilterMenu?.classList.contains("hidden");
    setTeamHcRankingsLeagueFilterOpen(Boolean(willOpen));
    if (willOpen) {
      setLeagueFilterOpen(false);
      setTierFilterOpen(false);
      window.setTimeout(() => {
        const searchInput = teamHcRankingsLeagueFilterMenu?.querySelector(".league-filter-search-input");
        if (searchInput instanceof HTMLInputElement) {
          searchInput.focus();
        }
      }, 0);
    }
  });
}
if (teamHcRankingsLeagueFilterMenu instanceof HTMLDivElement) {
  teamHcRankingsLeagueFilterMenu.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || target.type !== "radio") return;
    const selectedLeague = String(target.value || "").trim();
    if (!selectedLeague) return;
    if (selectedLeague !== selectedTeamHcRankingsLeague) {
      setSelectedTeamHcRankingsLeague(selectedLeague);
      closeTeamHcPerfPanel();
    }
    setTeamHcRankingsLeagueFilterOpen(false);
    renderTeamHcRankings();
  });
}
if (teamHcRankingsRefreshBtn instanceof HTMLButtonElement) {
  teamHcRankingsRefreshBtn.addEventListener("click", () => {
    loadTeamHcRankings();
  });
}
if (teamHcPerfCloseBtn instanceof HTMLButtonElement) {
  teamHcPerfCloseBtn.addEventListener("click", () => {
    closeTeamHcPerfPanel();
  });
}
leagueFilterBtn.addEventListener("click", (event) => {
  event.stopPropagation();
  const willOpen = leagueFilterMenu.classList.contains("hidden");
  setLeagueFilterOpen(willOpen);
  if (willOpen) {
    setTierFilterOpen(false);
    window.setTimeout(() => {
      const searchInput = leagueFilterMenu.querySelector(".league-filter-search-input");
      if (searchInput instanceof HTMLInputElement) {
        searchInput.focus();
      }
    }, 0);
  }
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
  if (teamHcRankingsLeagueFilter && !teamHcRankingsLeagueFilter.contains(event.target)) {
    setTeamHcRankingsLeagueFilterOpen(false);
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    setLeagueFilterOpen(false);
    setTierFilterOpen(false);
    setTeamHcRankingsLeagueFilterOpen(false);
  }
});
sortModeBtn.addEventListener("change", () => {
  const selectedSortMode = String(sortModeBtn.value || "").trim().toLowerCase();
  if (selectedSortMode === "league" || selectedSortMode === "tier" || selectedSortMode === "kickoff") {
    sortMode = selectedSortMode;
  } else {
    sortMode = "kickoff";
  }
  updateSortButtonLabel();
  renderCurrentDay();
});
if (teamSearchInput instanceof HTMLInputElement) {
  teamSearchInput.addEventListener("input", (event) => {
    const inputEl = event.currentTarget instanceof HTMLInputElement ? event.currentTarget : teamSearchInput;
    const caretStart = inputEl.selectionStart;
    const caretEnd = inputEl.selectionEnd;
    teamSearchQuery = String(inputEl.value || "");
    applyGameFilters();
    renderCurrentDay();
    if (document.activeElement !== inputEl) {
      inputEl.focus({ preventScroll: true });
    }
    if (caretStart != null && caretEnd != null) {
      const valueLength = String(inputEl.value || "").length;
      const nextStart = Math.max(0, Math.min(caretStart, valueLength));
      const nextEnd = Math.max(0, Math.min(caretEnd, valueLength));
      inputEl.setSelectionRange(nextStart, nextEnd);
    }
  });
}
prevDayBtn.addEventListener("click", async () => {
  if (gamesMode !== "historical") {
    currentDayIndex = clampDayIndex(currentDayIndex - 1);
    renderCurrentDay();
    return;
  }

  if (currentDayIndex > 0) {
    currentDayIndex = clampDayIndex(currentDayIndex - 1);
    renderCurrentDay();
    return;
  }

  if (!historicalHasMoreOlder) {
    return;
  }

  const currentDayIso = allDays[currentDayIndex] && allDays[currentDayIndex].date
    ? String(allDays[currentDayIndex].date)
    : "";
  const ok = await loadGames({ loadMoreHistorical: true });
  if (!ok || !allDays.length) {
    return;
  }
  const currentDayAfterReloadIndex = allDays.findIndex((entry) => String(entry.date || "") === currentDayIso);
  if (currentDayAfterReloadIndex > 0) {
    currentDayIndex = currentDayAfterReloadIndex - 1;
  } else {
    currentDayIndex = clampDayIndex(currentDayIndex - 1);
  }
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
if (saveGameBtn instanceof HTMLButtonElement) {
  saveGameBtn.addEventListener("click", () => {
    toggleSelectedGameSaved();
  });
}

updateSortButtonLabel();
updateLeagueFilterButtonLabel();
updateTierFilterButtonLabel();
if (teamHcRankingsSortSelect instanceof HTMLSelectElement) {
  teamHcRankingsSortSelect.value = teamHcRankingsSortMetric;
}
setTeamHcRankingsVenueMode("overall");
updateSavedTabLabel();
updateDetailsSaveButton();
setMappingSubTab("teams");
setActiveTab("games");
setGamesMode("upcoming", false);
loadGames();
setInterval(() => {
  loadGames();
}, AUTO_REFRESH_MS);
