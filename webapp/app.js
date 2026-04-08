const calendarView = document.getElementById("calendarView");
const statusText = document.getElementById("statusText");
const refreshBtn = document.getElementById("refreshBtn");
const exitAppBtn = document.getElementById("exitAppBtn");
const leagueFilter = document.getElementById("leagueFilter");
const leagueFilterBtn = document.getElementById("leagueFilterBtn");
const leagueFilterMenu = document.getElementById("leagueFilterMenu");
const tierFilter = document.getElementById("tierFilter");
const tierFilterBtn = document.getElementById("tierFilterBtn");
const tierFilterMenu = document.getElementById("tierFilterMenu");
const sortModeBtn = document.getElementById("sortModeBtn");
const savedSortModeBtn = document.getElementById("savedSortModeBtn");
const teamSearchInput = document.getElementById("teamSearchInput");
const tableControls = document.querySelector(".table-controls");
const gamesTabBtn = document.getElementById("gamesTabBtn");
const savedGamesTabBtn = document.getElementById("savedGamesTabBtn");
const teamHcRankingsTabBtn = document.getElementById("teamHcRankingsTabBtn");
const teamsTabBtn = document.getElementById("teamsTabBtn");
const manualMappingTabBtn = document.getElementById("manualMappingTabBtn");
const gamesTabPane = document.getElementById("gamesTabPane");
const savedGamesTabPane = document.getElementById("savedGamesTabPane");
const savedGamesView = document.getElementById("savedGamesView");
const teamHcRankingsTabPane = document.getElementById("teamHcRankingsTabPane");
const teamHcRankingsView = document.getElementById("teamHcRankingsView");
const teamsTabPane = document.getElementById("teamsTabPane");
const teamsView = document.getElementById("teamsView");
const teamsSearchInput = document.getElementById("teamsSearchInput");
const teamsRefreshBtn = document.getElementById("teamsRefreshBtn");
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
const mappingSaveSelectedBtn = document.getElementById("mappingSaveSelectedBtn");
const mappingClearSelectedBtn = document.getElementById("mappingClearSelectedBtn");
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
const detailsTitle = document.getElementById("detailsTitle");
const detailsMeta = document.getElementById("detailsMeta");
const linesContainer = document.getElementById("linesContainer");
const teamDetailsPanel = document.getElementById("teamDetailsPanel");
const teamDetailsTitle = document.getElementById("teamDetailsTitle");
const teamDetailsMeta = document.getElementById("teamDetailsMeta");
const teamDetailsContent = document.getElementById("teamDetailsContent");
const teamDetailsCloseBtn = document.getElementById("teamDetailsCloseBtn");
const teamDetailsCompetitionSelect = document.getElementById("teamDetailsCompetitionSelect");
const prevDayBtn = document.getElementById("prevDayBtn");
const todayBtn = document.getElementById("todayBtn");
const nextDayBtn = document.getElementById("nextDayBtn");
const dayLabel = document.getElementById("dayLabel");
const dayPickerInput = document.getElementById("dayPickerInput");
const dayPickerCount = document.getElementById("dayPickerCount");
const upcomingModeBtn = document.getElementById("upcomingModeBtn");
const historicalModeBtn = document.getElementById("historicalModeBtn");
const xgThresholdInput = document.getElementById("xgThresholdInput");
const xgdHcHighlightToggleBtn = document.getElementById("xgdHcHighlightToggleBtn");
const noHandicapGamesToggleBtn = document.getElementById("noHandicapGamesToggleBtn");

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
let sortMode = "tier";
let savedSortMode = "kickoff";
let leagueFilterSearch = "";
let teamSearchQuery = "";
let gamesShownCount = 0;
let gamesShownAuto = true;
let rollingWindowCount = 10;
let showTrendCharts = false;
let recentTeamView = "home";
let statsVenueGamesShownCount = 0;
let statsVenueGamesShownAuto = true;
let statsAllGamesShownCount = 0;
let statsAllGamesShownAuto = true;
let gamestateStatsMode = "per90";
let statsTeamView = "home";
let statsGeneralTeamView = "home";
let hcPerfTeamView = "home";
const hcPerfPayloadByMarket = new Map();
const xgdPayloadByMarket = new Map();
const teamPagePayloadByKey = new Map();
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
let teamsDirectoryRows = [];
let teamsDirectoryLoaded = false;
let teamsDirectoryLoading = false;
let teamsDirectoryErrorText = "";
let teamsDirectorySearchQuery = "";
let mappingSubTab = "teams";
let lastManualMappingPayload = null;
let teamMappingSearchBetfair = "";
let teamMappingSearchSavedTeams = "";
const teamMappingBatchSelections = new Set();
const teamMappingBatchDrafts = new Map();
const historicalDayCalcInFlight = new Set();
const historicalDayAutoCalcAttempted = new Set();
let teamHcPerfDetailLoadingKey = "";
let teamHcPerfDetailTeam = "";
let teamHcPerfDetailCompetition = "";
let teamHcPerfDetailRows = [];
let teamDetailsLoadingKey = "";
let teamDetailsTeam = "";
let teamDetailsCompetition = "";
let teamDetailsMainTab = "xg";
let teamDetailsPayload = null;
const TEAM_DETAILS_XG_GAMES_DEFAULT = 9999; // effectively "all games"
const TEAM_DETAILS_XG_ROLLING_DEFAULT = 10;
let teamDetailsXgGamesShownCount = TEAM_DETAILS_XG_GAMES_DEFAULT;
let teamDetailsXgRollingWindowCount = TEAM_DETAILS_XG_ROLLING_DEFAULT;
const DEFAULT_XG_PUSH_THRESHOLD = 0.1;
const MIN_XG_PUSH_THRESHOLD = 0.0;
const MAX_XG_PUSH_THRESHOLD = 5.0;
const XG_PUSH_THRESHOLD_STORAGE_KEY = "xgd_hc_xg_threshold";
const XGD_HC_HIGHLIGHT_ENABLED_STORAGE_KEY = "xgd_hc_highlight_enabled";
const SHOW_GAMES_WITHOUT_HC_STORAGE_KEY = "show_games_without_hc_pricing";
let xgPushThreshold = DEFAULT_XG_PUSH_THRESHOLD;
let xgdHcHighlightEnabled = false;
let showGamesWithoutHandicap = true;
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

function formatUtcOffsetLabel(dateValue = new Date()) {
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absMinutes = Math.abs(offsetMinutes);
  const hours = Math.floor(absMinutes / 60);
  const minutes = absMinutes % 60;
  if (minutes === 0) {
    return `UTC${sign}${hours}`;
  }
  return `UTC${sign}${hours}:${String(minutes).padStart(2, "0")}`;
}

function getKickoffColumnLabel() {
  return `Kickoff (${formatUtcOffsetLabel()})`;
}

function getGameKickoffDate(game) {
  const candidates = [
    game?.kickoff_raw,
    game?.kickoff_time,
    game?.date_time,
  ];
  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (!text) continue;
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  return null;
}

function formatGameKickoffLocalTime(game) {
  const kickoffDate = getGameKickoffDate(game);
  if (kickoffDate) {
    return new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(kickoffDate);
  }
  const fallbackUtc = String(game?.kickoff_utc || "").trim();
  return fallbackUtc || "-";
}

function formatGameKickoffLocalDateTime(game) {
  const kickoffDate = getGameKickoffDate(game);
  if (kickoffDate) {
    return new Intl.DateTimeFormat(undefined, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(kickoffDate);
  }
  const fallbackRaw = String(game?.kickoff_raw || "").trim();
  if (fallbackRaw) return fallbackRaw;
  const fallbackUtc = String(game?.kickoff_utc || "").trim();
  return fallbackUtc || "-";
}

function decodeURIComponentSafe(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (_err) {
    return String(value || "");
  }
}

function splitEventTeamsLabel(eventName) {
  const eventText = String(eventName || "").trim();
  if (!eventText) return { home: "", away: "" };
  const separators = [" v ", " vs ", " @ "];
  const lowered = eventText.toLowerCase();
  for (const separator of separators) {
    const idx = lowered.indexOf(separator);
    if (idx < 0) continue;
    const home = eventText.slice(0, idx).trim();
    const away = eventText.slice(idx + separator.length).trim();
    if (home && away) return { home, away };
  }
  return { home: "", away: "" };
}

function resolveGameTeams(game) {
  const homeRaw = String(game?.home_team || "").trim();
  const awayRaw = String(game?.away_team || "").trim();
  if (homeRaw && awayRaw) {
    return { home: homeRaw, away: awayRaw };
  }
  const parsed = splitEventTeamsLabel(game?.event_name);
  return {
    home: parsed.home || homeRaw,
    away: parsed.away || awayRaw,
  };
}

function buildTeamLinkHtml(teamName, competitionName = "", options = {}) {
  const teamText = String(teamName || "").trim();
  if (!teamText) return "-";
  const competitionText = String(competitionName || "").trim();
  const isStrong = Boolean(options?.strong);
  const className = isStrong ? "team-link-btn team-link-btn-strong" : "team-link-btn";
  return `
    <button
      type="button"
      class="${className}"
      data-team-link="1"
      data-team="${encodeURIComponent(teamText)}"
      data-competition="${encodeURIComponent(competitionText)}"
    >${escapeHtml(teamText)}</button>
  `;
}

function bindTeamLinkButtons(rootNode = null) {
  const root = rootNode || document;
  if (!root || typeof root.querySelectorAll !== "function") return;
  const teamButtons = root.querySelectorAll("button[data-team-link='1']");
  teamButtons.forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) return;
    if (button.dataset.teamLinkBound === "1") return;
    button.dataset.teamLinkBound = "1";
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const teamText = decodeURIComponentSafe(button.dataset.team || "").trim();
      const competitionText = decodeURIComponentSafe(button.dataset.competition || "").trim();
      if (!teamText) return;
      openTeamPage(teamText, competitionText || null);
    });
  });
}

function buildGameNameCellHtml(game) {
  const teams = resolveGameTeams(game);
  const competition = String(game?.competition || "").trim();
  if (!teams.home && !teams.away) {
    return escapeHtml(String(game?.event_name || "-"));
  }
  if (!teams.home || !teams.away) {
    return escapeHtml(String(game?.event_name || `${teams.home || teams.away || "-"}`));
  }
  return `
    <span class="game-team-pair">
      ${buildTeamLinkHtml(teams.home, competition)}
      <span>v</span>
      ${buildTeamLinkHtml(teams.away, competition)}
    </span>
  `;
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

function normalizeXgPushThreshold(value, fallback = DEFAULT_XG_PUSH_THRESHOLD) {
  const text = String(value ?? "").trim().replace(",", ".");
  const parsed = Number(text);
  const fallbackNum = Number(fallback);
  const fallbackSafe = Number.isFinite(fallbackNum)
    ? Math.max(MIN_XG_PUSH_THRESHOLD, Math.min(MAX_XG_PUSH_THRESHOLD, fallbackNum))
    : DEFAULT_XG_PUSH_THRESHOLD;
  if (!Number.isFinite(parsed)) return fallbackSafe;
  return Math.max(MIN_XG_PUSH_THRESHOLD, Math.min(MAX_XG_PUSH_THRESHOLD, parsed));
}

function formatXgPushThresholdForInput(value) {
  const normalized = normalizeXgPushThreshold(value, DEFAULT_XG_PUSH_THRESHOLD);
  const rounded = Math.round(normalized * 1000) / 1000;
  return String(rounded);
}

function formatXgPushThresholdForLabel(value) {
  const normalized = normalizeXgPushThreshold(value, DEFAULT_XG_PUSH_THRESHOLD);
  return normalized.toFixed(2);
}

function getStoredXgPushThreshold() {
  try {
    const raw = window.localStorage.getItem(XG_PUSH_THRESHOLD_STORAGE_KEY);
    if (raw == null) return DEFAULT_XG_PUSH_THRESHOLD;
    return normalizeXgPushThreshold(raw, DEFAULT_XG_PUSH_THRESHOLD);
  } catch (_err) {
    return DEFAULT_XG_PUSH_THRESHOLD;
  }
}

function persistXgPushThreshold(value) {
  try {
    window.localStorage.setItem(
      XG_PUSH_THRESHOLD_STORAGE_KEY,
      formatXgPushThresholdForInput(value)
    );
  } catch (_err) {
    // Ignore storage failures.
  }
}

function getCurrentXgPushThreshold() {
  return normalizeXgPushThreshold(xgPushThreshold, DEFAULT_XG_PUSH_THRESHOLD);
}

function persistXgdHcHighlightEnabled(value) {
  try {
    window.localStorage.setItem(
      XGD_HC_HIGHLIGHT_ENABLED_STORAGE_KEY,
      value ? "1" : "0"
    );
  } catch (_err) {
    // Ignore storage failures.
  }
}

function getStoredShowGamesWithoutHandicap() {
  try {
    const raw = window.localStorage.getItem(SHOW_GAMES_WITHOUT_HC_STORAGE_KEY);
    if (raw == null) return true;
    const text = String(raw).trim().toLowerCase();
    if (!text) return true;
    return text === "1" || text === "true" || text === "yes" || text === "on";
  } catch (_err) {
    return true;
  }
}

function persistShowGamesWithoutHandicap(value) {
  try {
    window.localStorage.setItem(
      SHOW_GAMES_WITHOUT_HC_STORAGE_KEY,
      value ? "1" : "0"
    );
  } catch (_err) {
    // Ignore storage failures.
  }
}

function updateXgdHcHighlightToggleButton() {
  if (!(xgdHcHighlightToggleBtn instanceof HTMLButtonElement)) return;
  xgdHcHighlightToggleBtn.textContent = xgdHcHighlightEnabled ? "Highlight: On" : "Highlight: Off";
  xgdHcHighlightToggleBtn.classList.toggle("is-off", !xgdHcHighlightEnabled);
  xgdHcHighlightToggleBtn.setAttribute("aria-pressed", xgdHcHighlightEnabled ? "true" : "false");
  xgdHcHighlightToggleBtn.title = xgdHcHighlightEnabled ? "Highlight is On" : "Highlight is Off";
}

function updateNoHandicapGamesToggleButton() {
  if (!(noHandicapGamesToggleBtn instanceof HTMLButtonElement)) return;
  noHandicapGamesToggleBtn.textContent = showGamesWithoutHandicap
    ? "No-HC Games: Show"
    : "No-HC Games: Hide";
  noHandicapGamesToggleBtn.classList.toggle("is-off", !showGamesWithoutHandicap);
  noHandicapGamesToggleBtn.setAttribute("aria-pressed", showGamesWithoutHandicap ? "true" : "false");
  noHandicapGamesToggleBtn.title = showGamesWithoutHandicap
    ? "Showing games without handicap prices"
    : "Hiding games without handicap prices";
}

function hasVisibleHandicapPricing(game) {
  const hasTextValue = (value) => {
    const text = String(value ?? "").trim();
    return text !== "" && text !== "-";
  };
  return hasTextValue(game?.mainline) && hasTextValue(game?.home_price) && hasTextValue(game?.away_price);
}

function setShowGamesWithoutHandicap(value, options = {}) {
  const silent = Boolean(options?.silent);
  const nextValue = Boolean(value);
  const changed = nextValue !== showGamesWithoutHandicap;
  showGamesWithoutHandicap = nextValue;
  persistShowGamesWithoutHandicap(nextValue);
  updateNoHandicapGamesToggleButton();
  if (!changed) return;

  applyGameFilters();
  if (activeTab === "games") {
    renderCurrentDay();
  } else if (activeTab === "saved") {
    renderSavedGames();
  }
  if (!silent) {
    statusText.textContent = showGamesWithoutHandicap
      ? "Showing games without handicap pricing"
      : "Hiding games without handicap pricing";
  }
}

function setXgdHcHighlightEnabled(value, options = {}) {
  const silent = Boolean(options?.silent);
  const nextValue = Boolean(value);
  const changed = nextValue !== xgdHcHighlightEnabled;
  xgdHcHighlightEnabled = nextValue;
  persistXgdHcHighlightEnabled(nextValue);
  updateXgdHcHighlightToggleButton();
  if (!changed) return;

  if (activeTab === "games") {
    renderCurrentDay();
  } else if (activeTab === "saved") {
    renderSavedGames();
  }

  if (!silent) {
    statusText.textContent = `xGD+HC highlighting ${xgdHcHighlightEnabled ? "enabled" : "disabled"}`;
  }
}

function applyGlobalXgPushThreshold(nextValue) {
  const normalized = normalizeXgPushThreshold(nextValue, getCurrentXgPushThreshold());
  const changed = Math.abs(normalized - getCurrentXgPushThreshold()) > 1e-9;
  xgPushThreshold = normalized;
  persistXgPushThreshold(normalized);
  if (xgThresholdInput instanceof HTMLInputElement) {
    xgThresholdInput.value = formatXgPushThresholdForInput(normalized);
  }
  if (!changed) return;

  if (activeTab === "games") {
    renderCurrentDay();
  } else if (activeTab === "saved") {
    renderSavedGames();
  }
  if (!detailsPanel.classList.contains("hidden") && lastXgdPayload) {
    renderXgd(lastXgdPayload);
  }
  if (
    teamHcPerfPanel
    && !teamHcPerfPanel.classList.contains("hidden")
    && teamHcPerfDetailTeam
    && teamHcPerfDetailCompetition
    && Array.isArray(teamHcPerfDetailRows)
    && teamHcPerfDetailRows.length > 0
  ) {
    renderTeamHcPerfDetailFromRows(
      teamHcPerfDetailTeam,
      teamHcPerfDetailCompetition,
      teamHcPerfDetailRows
    );
  }
  if (teamHcRankingsLoaded || activeTab === "rankings") {
    loadTeamHcRankings({ silent: true });
  }
  statusText.textContent = `xG threshold set to ${formatXgPushThresholdForLabel(normalized)}`;
}

function setActiveTab(tabName) {
  const tabRaw = String(tabName || "").trim().toLowerCase();
  if (tabRaw === "saved") {
    activeTab = "saved";
  } else if (tabRaw === "rankings") {
    activeTab = "rankings";
  } else if (tabRaw === "teams") {
    activeTab = "teams";
  } else if (tabRaw === "mapping") {
    activeTab = "mapping";
  } else {
    activeTab = "games";
  }
  const gamesActive = activeTab === "games";
  const savedActive = activeTab === "saved";
  const rankingsActive = activeTab === "rankings";
  const teamsActive = activeTab === "teams";
  const mappingActive = activeTab === "mapping";
  gamesTabBtn.classList.toggle("active", gamesActive);
  if (savedGamesTabBtn) {
    savedGamesTabBtn.classList.toggle("active", savedActive);
  }
  if (teamHcRankingsTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsTabBtn.classList.toggle("active", rankingsActive);
  }
  if (teamsTabBtn instanceof HTMLButtonElement) {
    teamsTabBtn.classList.toggle("active", teamsActive);
  }
  manualMappingTabBtn.classList.toggle("active", mappingActive);
  gamesTabPane.classList.toggle("hidden", !gamesActive);
  if (savedGamesTabPane) {
    savedGamesTabPane.classList.toggle("hidden", !savedActive);
  }
  if (teamHcRankingsTabPane) {
    teamHcRankingsTabPane.classList.toggle("hidden", !rankingsActive);
  }
  if (teamsTabPane) {
    teamsTabPane.classList.toggle("hidden", !teamsActive);
  }
  manualMappingTabPane.classList.toggle("hidden", !mappingActive);
  if (activeTab !== "games") {
    closeGameDetailsPanel(true);
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
  if (teamsActive && !teamsDirectoryLoading) {
    loadTeamsDirectory({ silent: teamsDirectoryLoaded });
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

  closeGameDetailsPanel(true);
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

function normalizeTeamMappingRawName(rawName) {
  return String(rawName || "").trim();
}

function setTeamMappingDraft(rawName, sofaName) {
  const rawKey = normalizeTeamMappingRawName(rawName);
  if (!rawKey) return;
  const sofaValue = String(sofaName || "").trim();
  if (!sofaValue) {
    teamMappingBatchDrafts.delete(rawKey);
    return;
  }
  teamMappingBatchDrafts.set(rawKey, sofaValue);
}

function clearTeamMappingSelection(rawName) {
  const rawKey = normalizeTeamMappingRawName(rawName);
  if (!rawKey) return;
  teamMappingBatchSelections.delete(rawKey);
  teamMappingBatchDrafts.delete(rawKey);
}

function clearAllTeamMappingSelections() {
  teamMappingBatchSelections.clear();
  teamMappingBatchDrafts.clear();
}

function updateTeamMappingBatchButtons() {
  const selectedCount = teamMappingBatchSelections.size;
  if (mappingSaveSelectedBtn instanceof HTMLButtonElement) {
    mappingSaveSelectedBtn.disabled = selectedCount <= 0;
    mappingSaveSelectedBtn.textContent = selectedCount > 0
      ? `Save Selected Team Mappings (${selectedCount})`
      : "Save Selected Team Mappings";
  }
  if (mappingClearSelectedBtn instanceof HTMLButtonElement) {
    mappingClearSelectedBtn.disabled = selectedCount <= 0;
  }
}

function getTeamMappingLookupByRaw(payload) {
  const map = new Map();
  const mappings = Array.isArray(payload?.mappings) ? payload.mappings : [];
  for (const row of mappings) {
    const rawName = normalizeTeamMappingRawName(row?.raw_name);
    const sofaName = String(row?.sofa_name || "").trim();
    if (!rawName || !sofaName || map.has(rawName)) continue;
    map.set(rawName, sofaName);
  }
  return map;
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
  const activeRawNames = new Set(
    [...teamMappings, ...unmatchedTeams]
      .map((row) => normalizeTeamMappingRawName(row?.raw_name))
      .filter((rawName) => !!rawName)
  );
  for (const rawName of Array.from(teamMappingBatchSelections)) {
    if (!activeRawNames.has(rawName)) {
      teamMappingBatchSelections.delete(rawName);
    }
  }
  for (const rawName of Array.from(teamMappingBatchDrafts.keys())) {
    if (!activeRawNames.has(rawName)) {
      teamMappingBatchDrafts.delete(rawName);
    }
  }
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
  const mappedManualSofaCompetitionNames = new Set(
    manualCompetitionMappings
      .map((row) => String(row?.sofa_name || "").trim().toLowerCase())
      .filter((name) => !!name)
  );
  const normalizedSofaCompetitions = Array.from(
    new Set(
      sofaCompetitions
        .map((competition) => String(competition || "").trim())
        .filter((competition) => !!competition)
    )
  );
  const availableSofaCompetitions = normalizedSofaCompetitions.filter(
    (competition) => !mappedManualSofaCompetitionNames.has(competition.toLowerCase())
  );
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
      const rawKey = normalizeTeamMappingRawName(rawName);
      const draftValue = String(teamMappingBatchDrafts.get(rawKey) || "").trim();
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
      if (input && draftValue) {
        input.value = draftValue;
      }
      const syncBatchSelectionForRow = (autoSelect = false) => {
        if (!(input instanceof HTMLInputElement)) return;
        const resolvedSofaName = resolveSofaTeam(input.value);
        if (resolvedSofaName) {
          setTeamMappingDraft(rawKey, resolvedSofaName);
          if (autoSelect) {
            teamMappingBatchSelections.add(rawKey);
          }
        } else if (!teamMappingBatchSelections.has(rawKey)) {
          teamMappingBatchDrafts.delete(rawKey);
        }
        if (!resolvedSofaName && teamMappingBatchSelections.has(rawKey)) {
          teamMappingBatchSelections.delete(rawKey);
        }
      };
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
          syncBatchSelectionForRow(true);
          updateTeamMappingBatchButtons();
          dropdown.classList.add("hidden");
          input.focus();
        });
      }
      if (input) {
        input.addEventListener("input", () => {
          syncBatchSelectionForRow(true);
          updateTeamMappingBatchButtons();
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
          clearTeamMappingSelection(rawKey);
          updateTeamMappingBatchButtons();
          await upsertManualTeamMapping(rawName, sofaName);
        });
      }
      syncBatchSelectionForRow();
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
        const rawKey = normalizeTeamMappingRawName(rawName);
        const sofaName = String(row.sofa_name || "");
        const sofaNameLower = sofaName.toLowerCase();
        const draftValue = String(teamMappingBatchDrafts.get(rawKey) || "").trim();
        const isManual = row?.is_manual !== false;
        const method = String(row.match_method || "").trim().toLowerCase();
        const methodLabel = method ? `${method.charAt(0).toUpperCase()}${method.slice(1)}` : "Auto";
        const typeLabel = isManual ? "Manual" : `Auto (${methodLabel})`;
        const actionHtml = isManual
          ? `
              <button type="button" class="mapping-save-btn">Save</button>
              <button type="button" class="mapping-delete-btn">Delete</button>
            `
          : `
              <button type="button" class="mapping-save-btn">Override</button>
              <button type="button" class="mapping-delete-btn">Delete Auto</button>
            `;
        tr.innerHTML = `
          <td>${escapeHtml(rawName)}</td>
          <td>
            <input type="text" class="mapping-team-input" list="${teamDatalistId}" value="${escapeHtml(sofaName)}" />
          </td>
          <td>${escapeHtml(typeLabel)}</td>
          <td>${actionHtml}</td>
        `;
        const teamInput = tr.querySelector(".mapping-team-input");
        const resolveSavedSofaName = () => {
          const selectedRaw = String(teamInput?.value || "").trim();
          const selectedLower = selectedRaw.toLowerCase();
          return availableSofaTeamLookup.get(selectedLower)
            || (selectedLower && selectedLower === sofaNameLower ? sofaName : "");
        };
        if (teamInput && draftValue) {
          teamInput.value = draftValue;
        }
        const syncSavedBatchSelectionForRow = () => {
          const resolvedSofaName = resolveSavedSofaName();
          if (resolvedSofaName) {
            setTeamMappingDraft(rawKey, resolvedSofaName);
          } else if (!teamMappingBatchSelections.has(rawKey)) {
            teamMappingBatchDrafts.delete(rawKey);
          }
          if (!resolvedSofaName && teamMappingBatchSelections.has(rawKey)) {
            teamMappingBatchSelections.delete(rawKey);
          }
        };
        if (teamInput) {
          teamInput.addEventListener("input", () => {
            syncSavedBatchSelectionForRow();
            updateTeamMappingBatchButtons();
          });
        }
        const saveBtn = tr.querySelector(".mapping-save-btn");
        if (saveBtn && teamInput) {
          saveBtn.addEventListener("click", async () => {
            const selectedSofaName = resolveSavedSofaName();
            if (!selectedSofaName) {
              mappingStatus.textContent = "Select a valid SofaScore team before saving.";
              return;
            }
            clearTeamMappingSelection(rawKey);
            updateTeamMappingBatchButtons();
            await upsertManualTeamMapping(rawName, selectedSofaName);
          });
        }
        const deleteBtn = tr.querySelector(".mapping-delete-btn");
        if (deleteBtn) {
          deleteBtn.addEventListener("click", async () => {
            clearTeamMappingSelection(rawKey);
            updateTeamMappingBatchButtons();
            await deleteManualTeamMapping(rawName);
          });
        }
        syncSavedBatchSelectionForRow();
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
    unmatchedCompetitionsContainer.innerHTML = `<p class="mapping-empty">No unmatched competitions in selected leagues.</p>`;
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

  updateTeamMappingBatchButtons();
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

async function saveSelectedManualTeamMappings() {
  const selectedRawNames = Array.from(teamMappingBatchSelections)
    .map((rawName) => normalizeTeamMappingRawName(rawName))
    .filter((rawName) => !!rawName);
  if (!selectedRawNames.length) {
    mappingStatus.textContent = "Select at least one team mapping row first.";
    return;
  }

  const currentSofaByRaw = getTeamMappingLookupByRaw(lastManualMappingPayload);
  const bulkMappings = [];
  const missingMappings = [];

  for (const rawName of selectedRawNames) {
    let sofaName = String(teamMappingBatchDrafts.get(rawName) || "").trim();
    if (!sofaName) {
      sofaName = String(currentSofaByRaw.get(rawName) || "").trim();
    }
    if (!sofaName) {
      missingMappings.push(rawName);
      continue;
    }
    bulkMappings.push({ raw_name: rawName, sofa_name: sofaName });
  }

  if (!bulkMappings.length) {
    mappingStatus.textContent = "No valid mappings were selected to save.";
    return;
  }
  if (missingMappings.length) {
    mappingStatus.textContent = "Some selected rows do not have a valid SofaScore team.";
    return;
  }

  try {
    mappingStatus.textContent = `Saving ${bulkMappings.length} team mappings...`;
    const res = await fetch("/api/manual-mappings/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mappings: bulkMappings }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to save selected mappings");
    clearAllTeamMappingSelections();
    updateTeamMappingBatchButtons();
    await Promise.all([loadGames(), loadManualMappings()]);
  } catch (err) {
    mappingStatus.textContent = String(err.message || err);
  }
}

async function upsertManualTeamMapping(rawName, sofaName) {
  clearTeamMappingSelection(rawName);
  updateTeamMappingBatchButtons();
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
  clearTeamMappingSelection(rawName);
  updateTeamMappingBatchButtons();
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

function updateDayPickerCount(dayIso = "") {
  if (!(dayPickerCount instanceof HTMLElement)) return;
  const targetIso = String(dayIso || "").trim();
  if (!targetIso) {
    dayPickerCount.textContent = "";
    return;
  }
  const dayMatch = allDays.find((day) => String(day?.date || "").trim() === targetIso);
  const gameCount = dayMatch && Array.isArray(dayMatch.games) ? dayMatch.games.length : 0;
  const label = gameCount === 1 ? "game" : "games";
  dayPickerCount.textContent = `(${gameCount} ${label})`;
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

function closestDayIndex(dayIso) {
  const targetTs = Date.parse(`${String(dayIso || "").trim()}T00:00:00Z`);
  if (!Number.isFinite(targetTs) || !allDays.length) return -1;
  let bestIndex = -1;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (let idx = 0; idx < allDays.length; idx += 1) {
    const iso = String(allDays[idx]?.date || "").trim();
    if (!iso) continue;
    const dayTs = Date.parse(`${iso}T00:00:00Z`);
    if (!Number.isFinite(dayTs)) continue;
    const delta = Math.abs(dayTs - targetTs);
    if (delta < bestDelta) {
      bestDelta = delta;
      bestIndex = idx;
    }
  }
  return bestIndex;
}

async function jumpToDayIso(dayIso) {
  const targetIso = String(dayIso || "").trim();
  if (!targetIso || !/^\d{4}-\d{2}-\d{2}$/.test(targetIso)) return;
  if (!allDays.length) return;

  let targetIndex = allDays.findIndex((day) => String(day?.date || "") === targetIso);
  if (targetIndex >= 0) {
    currentDayIndex = targetIndex;
    renderCurrentDay();
    return;
  }

  if (gamesMode === "historical") {
    let guard = 0;
    let previousOldestIso = String(allDays[0]?.date || "");
    while (historicalHasMoreOlder && guard < 800) {
      guard += 1;
      statusText.textContent = `Loading historical days to reach ${targetIso}...`;
      const ok = await loadGames({ loadMoreHistorical: true });
      if (!ok || !allDays.length) break;
      targetIndex = allDays.findIndex((day) => String(day?.date || "") === targetIso);
      if (targetIndex >= 0) {
        currentDayIndex = targetIndex;
        renderCurrentDay();
        statusText.textContent = `Jumped to ${targetIso}`;
        return;
      }
      const currentOldestIso = String(allDays[0]?.date || "");
      if (!currentOldestIso || currentOldestIso === previousOldestIso) break;
      if (currentOldestIso <= targetIso) break;
      previousOldestIso = currentOldestIso;
    }
  }

  const nearestIdx = closestDayIndex(targetIso);
  if (nearestIdx >= 0) {
    currentDayIndex = nearestIdx;
    renderCurrentDay();
    const nearestIso = String(allDays[nearestIdx]?.date || "").trim();
    if (nearestIso && nearestIso !== targetIso) {
      statusText.textContent = `No games loaded for ${targetIso}. Jumped to nearest day: ${nearestIso}`;
    }
  }
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

function normalizePeriodForMainTable(periodValue) {
  const text = String(periodValue ?? "").trim().toLowerCase();
  if (!text) return "";
  if (text === "season") return "season";
  const numeric = Number(text);
  if (!Number.isFinite(numeric)) return "";
  const asInt = Math.trunc(numeric);
  if (asInt === 5) return "last5";
  if (asInt === 3) return "last3";
  return "";
}

function metricNumberToTableText(value, decimals = 2) {
  if (value == null || value === "") return "-";
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(decimals) : "-";
}

function extractMainTableMetricsFromXgdPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const xgdViews = Array.isArray(payload.xgd_views) ? payload.xgd_views : [];
  const viewHasAnyNumericMetrics = (view) => {
    const rows = Array.isArray(view?.period_rows) ? view.period_rows : [];
    if (!rows.length) return false;
    for (const row of rows) {
      const metricValues = [
        row?.strength,
        row?.xgd,
        row?.xgd_perf,
        row?.total_min_xg,
        row?.total_max_xg,
        row?.home_xg,
        row?.away_xg,
      ];
      for (const value of metricValues) {
        const num = Number(value);
        if (Number.isFinite(num)) return true;
      }
    }
    return false;
  };

  let preferredView = xgdViews.length ? xgdViews[0] : payload;
  let usedDomesticFallback = false;
  if (xgdViews.length) {
    const fixtureView = xgdViews.find((view) => String(view?.id || "").trim().toLowerCase() === "fixture")
      || xgdViews[0];
    const fixtureWarning = String(fixtureView?.warning || payload?.warning || "").trim().toLowerCase();
    const fixtureMissingCompetition = fixtureWarning.includes("fixture competition not found in database");
    const fixtureHasMetrics = viewHasAnyNumericMetrics(fixtureView);
    if (fixtureMissingCompetition || !fixtureHasMetrics) {
      const domesticView = xgdViews.find((view) => String(view?.id || "").trim().toLowerCase() === "domestic");
      if (domesticView && viewHasAnyNumericMetrics(domesticView)) {
        preferredView = domesticView;
        usedDomesticFallback = true;
      } else {
        preferredView = fixtureView;
      }
    } else {
      preferredView = fixtureView;
    }
  }
  const periodRows = Array.isArray(preferredView?.period_rows)
    ? preferredView.period_rows
    : (Array.isArray(payload.period_rows) ? payload.period_rows : []);
  if (!periodRows.length) return null;

  const rowsByPeriod = { season: null, last5: null, last3: null };
  for (const row of periodRows) {
    const periodKey = normalizePeriodForMainTable(row?.period);
    if (!periodKey || rowsByPeriod[periodKey]) continue;
    rowsByPeriod[periodKey] = row;
  }

  const pickMetric = (periodKey, metricKey) => {
    const row = rowsByPeriod[periodKey];
    if (!row || typeof row !== "object") return null;
    const value = row[metricKey];
    if (value == null || value === "") return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };

  return {
    season_strength: pickMetric("season", "strength"),
    last5_strength: pickMetric("last5", "strength"),
    last3_strength: pickMetric("last3", "strength"),
    season_xgd: pickMetric("season", "xgd"),
    last5_xgd: pickMetric("last5", "xgd"),
    last3_xgd: pickMetric("last3", "xgd"),
    season_xgd_perf: pickMetric("season", "xgd_perf"),
    last5_xgd_perf: pickMetric("last5", "xgd_perf"),
    last3_xgd_perf: pickMetric("last3", "xgd_perf"),
    season_min_xg: pickMetric("season", "total_min_xg"),
    last5_min_xg: pickMetric("last5", "total_min_xg"),
    last3_min_xg: pickMetric("last3", "total_min_xg"),
    season_max_xg: pickMetric("season", "total_max_xg"),
    last5_max_xg: pickMetric("last5", "total_max_xg"),
    last3_max_xg: pickMetric("last3", "total_max_xg"),
    xgd_competition_mismatch: String(preferredView?.id || "").trim().toLowerCase() === "cup" ? false : null,
    xgd_domestic_fallback: usedDomesticFallback,
  };
}

function applyMainTableMetricsForMarket(marketId, metrics) {
  const key = String(marketId || "").trim();
  if (!key || !metrics || typeof metrics !== "object") return false;
  const metricKeys = [
    "season_strength",
    "last5_strength",
    "last3_strength",
    "season_xgd",
    "last5_xgd",
    "last3_xgd",
    "season_xgd_perf",
    "last5_xgd_perf",
    "last3_xgd_perf",
    "season_min_xg",
    "last5_min_xg",
    "last3_min_xg",
    "season_max_xg",
    "last5_max_xg",
    "last3_max_xg",
  ];

  let updated = false;
  const applyToGame = (game) => {
    if (!game || String(game.market_id || "").trim() !== key) return;
    for (const metricKey of metricKeys) {
      const metricValue = metrics[metricKey];
      if (metricValue == null) continue;
      game[metricKey] = metricNumberToTableText(metricValue, 2);
    }
    if (metrics.xgd_competition_mismatch != null) {
      game.xgd_competition_mismatch = Boolean(metrics.xgd_competition_mismatch);
    }
    if (metrics.xgd_domestic_fallback != null) {
      game.xgd_domestic_fallback = Boolean(metrics.xgd_domestic_fallback);
    }
    updated = true;
  };

  const applyToDays = (days) => {
    if (!Array.isArray(days)) return;
    for (const day of days) {
      const dayGames = Array.isArray(day?.games) ? day.games : [];
      for (const game of dayGames) applyToGame(game);
    }
  };

  applyToGame(gamesById.get(key));
  applyToDays(allDays);
  applyToDays(rawDays);
  applyToDays(savedDays);
  return updated;
}

function applyCalculatedXgdToMainTable(marketId, payload) {
  const metrics = extractMainTableMetricsFromXgdPayload(payload);
  if (!metrics) return false;
  return applyMainTableMetricsForMarket(marketId, metrics);
}

function buildXgdPeriodTableHtml(periodRows, title) {
  const rows = Array.isArray(periodRows) ? periodRows : [];
  if (!rows.length) return "";

  const periodMap = {
    season: null,
    last5: null,
    last3: null,
  };
  for (const row of rows) {
    const periodKey = normalizePeriodForMainTable(row?.period);
    if (!periodKey || periodMap[periodKey]) continue;
    periodMap[periodKey] = row;
  }

  const heading = String(title || "").trim();
  const valueFor = (periodKey, metricKey, decimals = 2) => {
    const row = periodMap[periodKey];
    return formatMetricValue(row ? row[metricKey] : null, decimals);
  };

  const metricRows = [
    { label: "xGD", key: "strength" },
    { label: "xGD Perf", key: "xgd_perf" },
    { label: "Min Total xG", key: "total_min_xg" },
    { label: "Max total xG", key: "total_max_xg" },
    { label: "Home xG", key: "home_xg" },
    { label: "Away xG", key: "away_xg" },
  ];

  return `
    ${heading ? `<h3 class="section-title">${escapeHtml(heading)}</h3>` : ""}
    <table class="lines-table">
      <thead>
        <tr>
          <th></th>
          <th>Season</th>
          <th>Last 5</th>
          <th>Last 3</th>
        </tr>
      </thead>
      <tbody>
        ${metricRows
          .map(
            (metric) => `
          <tr>
            <td>${escapeHtml(metric.label)}</td>
            <td>${valueFor("season", metric.key)}</td>
            <td>${valueFor("last5", metric.key)}</td>
            <td>${valueFor("last3", metric.key)}</td>
          </tr>
        `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function buildTeamXgdSummaryTableHtml(teamLabel, rows) {
  const safeRows = Array.isArray(rows) ? [...rows] : [];
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

  safeRows.sort((a, b) => {
    const aTs = parseSortTs(a?.date_time);
    const bTs = parseSortTs(b?.date_time);
    const aValid = Number.isFinite(aTs);
    const bValid = Number.isFinite(bTs);
    if (aValid && bValid) return bTs - aTs;
    if (aValid) return -1;
    if (bValid) return 1;
    return 0;
  });

  const computeMetrics = (sampleRows) => {
    const sourceRows = Array.isArray(sampleRows) ? sampleRows : [];
    const averageOf = (reader) => {
      let total = 0;
      let count = 0;
      for (const row of sourceRows) {
        const value = reader(row);
        if (!Number.isFinite(value)) continue;
        total += value;
        count += 1;
      }
      return count ? (total / count) : null;
    };
    const hasXgotData = sourceRows.some((row) => {
      const xgot = toMetricNumber(row?.xGoT);
      const xgota = toMetricNumber(row?.xGoTA);
      return (
        (xgot != null && Math.abs(xgot) > 1e-9)
        || (xgota != null && Math.abs(xgota) > 1e-9)
      );
    });
    const hasXgData = sourceRows.some((row) => {
      const xg = toMetricNumber(row?.xG);
      const xga = toMetricNumber(row?.xGA);
      return (
        (xg != null && Math.abs(xg) > 1e-9)
        || (xga != null && Math.abs(xga) > 1e-9)
      );
    });

    return {
      xgd: hasXgData ? averageOf((row) => {
        const xg = toMetricNumber(row?.xG);
        const xga = toMetricNumber(row?.xGA);
        if (xg == null || xga == null) return null;
        return xg - xga;
      }) : null,
      xg_for: hasXgData ? averageOf((row) => toMetricNumber(row?.xG)) : null,
      xg_against: hasXgData ? averageOf((row) => toMetricNumber(row?.xGA)) : null,
      xgd_perf: (hasXgData && hasXgotData) ? averageOf((row) => {
        const gf = toMetricNumber(row?.GF);
        const ga = toMetricNumber(row?.GA);
        const xg = toMetricNumber(row?.xG);
        const xga = toMetricNumber(row?.xGA);
        if (gf == null || ga == null || xg == null || xga == null) return null;
        return (gf - ga) - (xg - xga);
      }) : null,
      xgot_minus_xg: hasXgotData ? averageOf((row) => {
        const xgot = toMetricNumber(row?.xGoT);
        const xg = toMetricNumber(row?.xG);
        if (xgot == null || xg == null) return null;
        return xgot - xg;
      }) : null,
      ga_minus_xgota: hasXgotData ? averageOf((row) => {
        const ga = toMetricNumber(row?.GA);
        const xgotAgainst = toMetricNumber(row?.xGoTA);
        if (ga == null || xgotAgainst == null) return null;
        return ga - xgotAgainst;
      }) : null,
    };
  };

  const seasonMetrics = computeMetrics(safeRows);
  const last5Metrics = computeMetrics(safeRows.slice(0, 5));
  const last3Metrics = computeMetrics(safeRows.slice(0, 3));
  const rowSpecs = [
    { label: "xGD", key: "xgd" },
    { label: "xG For", key: "xg_for" },
    { label: "xG Against", key: "xg_against" },
    { label: "xGD Perf", key: "xgd_perf" },
    { label: "xGoT-xG (Shooting)", key: "xgot_minus_xg" },
    { label: "GA-xGoTA (Keeping)", key: "ga_minus_xgota" },
  ];

  return `
    <section class="xgd-team-summary-panel">
      <h4>${escapeHtml(teamLabel || "Team")}</h4>
      <table class="lines-table">
        <thead>
          <tr>
            <th></th>
            <th>Season</th>
            <th>Last 5</th>
            <th>Last 3</th>
          </tr>
        </thead>
        <tbody>
          ${rowSpecs
            .map(
              (metric) => `
            <tr>
              <td>${escapeHtml(metric.label)}</td>
              <td>${formatMetricValue(seasonMetrics[metric.key], 2)}</td>
              <td>${formatMetricValue(last5Metrics[metric.key], 2)}</td>
              <td>${formatMetricValue(last3Metrics[metric.key], 2)}</td>
            </tr>
          `
            )
            .join("")}
        </tbody>
      </table>
    </section>
  `;
}

function buildTeamXgdSummaryTablesHtml(homeLabel, awayLabel, homeRows, awayRows) {
  return `
    <h3 class="section-title">Team xGD Summaries</h3>
    <section class="xgd-team-summary-grid">
      ${buildTeamXgdSummaryTableHtml(homeLabel || "Home team", homeRows)}
      ${buildTeamXgdSummaryTableHtml(awayLabel || "Away team", awayRows)}
    </section>
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

function buildPeriodMetricStackCell(seasonValue, last5Value, last3Value) {
  const isMissing = (value) => {
    const text = String(value ?? "").trim();
    return !text || text === "-";
  };
  const stackClass = "metric-stack";

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
    game?.season_xgd_perf,
    game?.last5_xgd_perf,
    game?.last3_xgd_perf,
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

function getMainTableXgdHcHighlightClass(periodValues, handicap, threshold) {
  const values = Array.isArray(periodValues) ? periodValues : [];
  if (values.length !== 3 || values.some((value) => value == null)) return "";
  if (handicap == null || threshold == null) return "";

  const combined = values.map((value) => Number(value) + Number(handicap));
  const allAbove = combined.every((value) => value > threshold);
  const allBelow = combined.every((value) => value < (-threshold));
  if (allAbove) return "xgd-hc-highlight-green";
  if (allBelow) return "xgd-hc-highlight-red";
  return "";
}

function getMainTableGoalsBandHighlight(goalLine, minPeriodValues, maxPeriodValues, threshold) {
  const mins = Array.isArray(minPeriodValues) ? minPeriodValues : [];
  const maxes = Array.isArray(maxPeriodValues) ? maxPeriodValues : [];
  if (goalLine == null) {
    return { goalLineClass: "", goalUnderClass: "", goalOverClass: "" };
  }
  if (mins.length !== 3 || maxes.length !== 3) {
    return { goalLineClass: "", goalUnderClass: "", goalOverClass: "" };
  }
  if (mins.some((value) => value == null) || maxes.some((value) => value == null)) {
    return { goalLineClass: "", goalUnderClass: "", goalOverClass: "" };
  }

  const goalValue = Number(goalLine);
  const thresholdValue = Number.isFinite(Number(threshold)) ? Math.max(0, Number(threshold)) : 0;
  const belowAllMin = mins.every((value) => (goalValue + thresholdValue) < Number(value));
  const aboveAllMax = maxes.every((value) => (goalValue - thresholdValue) > Number(value));
  if (belowAllMin && !aboveAllMax) {
    return {
      goalLineClass: "xgd-hc-highlight-green",
      goalUnderClass: "",
      goalOverClass: "xgd-hc-highlight-green",
    };
  }
  if (aboveAllMax && !belowAllMin) {
    return {
      goalLineClass: "xgd-hc-highlight-red",
      goalUnderClass: "xgd-hc-highlight-red",
      goalOverClass: "",
    };
  }
  return { goalLineClass: "", goalUnderClass: "", goalOverClass: "" };
}

function buildRecentMatchesTableHtml(title, rows, relevantTeamName = "") {
  const headingPrefix = relevantTeamName
    ? `<strong>${escapeHtml(relevantTeamName)}</strong> - `
    : "";
  const hasXgData = rows.some((r) => {
    const xg = toMetricNumber(r?.xG);
    const xga = toMetricNumber(r?.xGA);
    return (
      (xg != null && Math.abs(xg) > 1e-9)
      || (xga != null && Math.abs(xga) > 1e-9)
    );
  });
  const hasXgotData = rows.some((r) => {
    const xgot = toMetricNumber(r?.xGoT);
    const xgota = toMetricNumber(r?.xGoTA);
    return (
      (xgot != null && Math.abs(xgot) > 1e-9)
      || (xgota != null && Math.abs(xgota) > 1e-9)
    );
  });
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
                  const competitionName = String(r.competition_name || "").trim();
                  const homeTeamCell = homeTeam
                    ? buildTeamLinkHtml(homeTeam, competitionName, { strong: isHome })
                    : "-";
                  const awayTeamCell = awayTeam
                    ? buildTeamLinkHtml(awayTeam, competitionName, { strong: !isHome })
                    : "-";
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
                <td>${hasXgData ? formatMetricValue(xgHome, 2) : "-"}</td>
                <td>${hasXgData ? formatMetricValue(xgAway, 2) : "-"}</td>
                <td>${hasXgData ? formatMetricValue(xgd, 2) : "-"}</td>
                <td>${hasXgotData ? formatMetricValue(xgotHome, 2) : "-"}</td>
                <td>${hasXgotData ? formatMetricValue(xgotAway, 2) : "-"}</td>
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
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function buildMetricTrendPlotHtml(rows, title, relevantTeamName = "", rollingWindowGames = 10) {
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

  const windowSize = Math.max(1, Math.min(points.length, clampRecentMatchesCount(rollingWindowGames)));
  const buildSmoothedSeries = (metricKey) => {
    const rawValues = points.map((point) => {
      const value = point[metricKey];
      return Number.isFinite(value) ? value : null;
    });
    const validCount = rawValues.reduce((count, value) => (value == null ? count : count + 1), 0);
    if (!validCount) return [];

    return rawValues.map((_, index) => {
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
  };

  const xgSmoothed = buildSmoothedSeries("xg");
  const xgaSmoothed = buildSmoothedSeries("xga");
  const plottedValues = [...xgSmoothed, ...xgaSmoothed].filter((value) => Number.isFinite(value));
  if (!plottedValues.length) {
    return `
      <section class="recent-team-block">
        <h4>${headingPrefix}${escapeHtml(title)}</h4>
        <p class="recent-empty">No trend data available.</p>
      </section>
    `;
  }

  let yMin = Math.min(...plottedValues);
  let yMax = Math.max(...plottedValues);
  if (yMin === yMax) {
    const delta = Math.max(0.01, Math.abs(yMin || 0) * 0.05);
    yMin -= delta;
    yMax += delta;
  }

  const width = 860;
  const height = 260;
  const padLeft = 44;
  const padRight = 14;
  const padTop = 8;
  const padBottom = 24;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;

  const xForIndex = (index) => {
    if (points.length <= 1) return padLeft + (plotWidth / 2);
    return padLeft + ((index / (points.length - 1)) * plotWidth);
  };
  const yRange = yMax - yMin;
  const yForValue = (value) => padTop + (((yMax - value) / yRange) * plotHeight);

  const buildTrendPathFromSeries = (series) => {
    const smoothed = Array.isArray(series) ? series : [];
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

  const quarterStep = 0.25;
  const quarterScale = 1 / quarterStep;
  const gridYMin = Math.ceil(yMin * quarterScale) / quarterScale;
  const gridYMax = Math.floor(yMax * quarterScale) / quarterScale;
  const horizontalGridTicks = [];
  if (gridYMax >= gridYMin) {
    for (let tick = gridYMin; tick <= (gridYMax + 1e-9); tick += quarterStep) {
      horizontalGridTicks.push(Number(tick.toFixed(4)));
    }
  }
  const yLabelTicks = [yMax, (yMax + yMin) / 2, yMin];
  const verticalGridXs = points.map((_, index) => xForIndex(index));
  const firstLabel = points[0]?.dateText ? String(points[0].dateText).slice(0, 10) : "-";
  const lastLabel = points[points.length - 1]?.dateText ? String(points[points.length - 1].dateText).slice(0, 10) : "-";

  const xgColor = "#0f766e";
  const xgaColor = "#b91c1c";
  const xgTrendPath = buildTrendPathFromSeries(xgSmoothed);
  const xgaTrendPath = buildTrendPathFromSeries(xgaSmoothed);

  return `
    <section class="recent-team-block">
      <h4>${headingPrefix}${escapeHtml(title)}</h4>
      <div class="recent-table-wrap">
        <div class="trend-plot-wrap">
          <svg class="trend-plot" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(
            `${relevantTeamName || "Team"} trendlines for xG and xGA`
          )}">
            ${horizontalGridTicks
              .map(
                (tick) => `
              <line x1="${padLeft}" y1="${yForValue(tick).toFixed(2)}" x2="${width - padRight}" y2="${yForValue(tick).toFixed(
                2
              )}" class="trend-grid-line" />
            `
              )
              .join("")}
            ${verticalGridXs
              .map(
                (x) => `
              <line x1="${x.toFixed(2)}" y1="${padTop}" x2="${x.toFixed(2)}" y2="${height - padBottom}" class="trend-grid-line trend-grid-line-vertical" />
            `
              )
              .join("")}
            ${yLabelTicks
              .map(
                (tick) => `
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
  const activeMode = normalizeGamestateStatsMode(gamestateStatsMode);
  const modeLabel = activeMode === "total"
    ? "Total Stats"
    : (activeMode === "per90" ? "Stats / 90" : "Min / Stat");
  const entries = [
    { label: homeLabel || "Home team", rows: limitStatsRows(homeRows || [], sampleSize) },
    { label: awayLabel || "Away team", rows: limitStatsRows(awayRows || [], sampleSize) },
  ];
  const states = [
    { key: "drawing", label: "Draw" },
    { key: "winning", label: "Win" },
    { key: "losing", label: "Lose" },
  ];
  const metricCols = [
    { key: "corners_for", label: "Corners For" },
    { key: "corners_against", label: "Corners Against" },
    { key: "cards_for", label: "Cards For" },
    { key: "cards_against", label: "Cards Against" },
  ];

  const toSafeNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const formatMetricByMode = (total, minutes) => {
    if (activeMode === "per90") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || minutes <= 0) return "-";
      return formatMetricValue((total / minutes) * 90, 2);
    }
    if (activeMode === "minperstat") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || total <= 0 || minutes <= 0) return "-";
      return formatMetricValue(minutes / total, 2);
    }
    return formatMetricValue(total, 0);
  };

  const renderTeamTable = (entry) => `
    <section class="recent-team-block">
      <h4>${escapeHtml(entry.label)} - Gamestate Stats (${escapeHtml(sampleLabel)} | ${escapeHtml(modeLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Gamestate</th>
              <th>Time %</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
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
                  const metricValues = metricCols.map((metricCol) => {
                    const metricKey = `${metricCol.key}_${state.key}`;
                    const metricTotal = toSafeNumber(sumMetric(entry.rows, metricKey));
                    return formatMetricByMode(metricTotal, minutes);
                  });
                  return `
                    <tr>
                      <td>${escapeHtml(state.label)}</td>
                      <td>${pct == null ? "-" : `${formatMetricValue(pct, 1)}%`}</td>
                      <td>${metricValues[0]}</td>
                      <td>${metricValues[1]}</td>
                      <td>${metricValues[2]}</td>
                      <td>${metricValues[3]}</td>
                    </tr>
                  `;
                })
                .join("");
            })()}
          </tbody>
        </table>
      </div>
    </section>
  `;

  return `
    ${buildGamestateModeSwitchHtml()}
    ${entries.map((entry) => renderTeamTable(entry)).join("")}
  `;
}

function buildSingleTeamGamestateTableHtml(rows, teamLabel, sampleSize = null) {
  const sampleLabel = sampleSize == null ? "All Previous Games" : `Last ${clampRecentMatchesCount(sampleSize)} Games`;
  const activeMode = normalizeGamestateStatsMode(gamestateStatsMode);
  const modeLabel = activeMode === "total"
    ? "Total Stats"
    : (activeMode === "per90" ? "Stats / 90" : "Min / Stat");
  const safeRows = limitStatsRows(rows || [], sampleSize);
  const states = [
    { key: "drawing", label: "Draw" },
    { key: "winning", label: "Win" },
    { key: "losing", label: "Lose" },
  ];
  const metricCols = [
    { key: "corners_for", label: "Corners For" },
    { key: "corners_against", label: "Corners Against" },
    { key: "cards_for", label: "Cards For" },
    { key: "cards_against", label: "Cards Against" },
  ];

  const toSafeNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const formatMetricByMode = (total, minutes) => {
    if (activeMode === "per90") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || minutes <= 0) return "-";
      return formatMetricValue((total / minutes) * 90, 2);
    }
    if (activeMode === "minperstat") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || total <= 0 || minutes <= 0) return "-";
      return formatMetricValue(minutes / total, 2);
    }
    return formatMetricValue(total, 0);
  };

  const minutesByState = {};
  for (const state of states) {
    const key = `minutes_${state.key}`;
    minutesByState[state.key] = toSafeNumber(sumMetric(safeRows, key));
  }
  const totalMinutes = states.reduce((sum, state) => sum + minutesByState[state.key], 0);

  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} - Gamestate Stats (${escapeHtml(sampleLabel)} | ${escapeHtml(modeLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Gamestate</th>
              <th>Time %</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
            </tr>
          </thead>
          <tbody>
            ${states
              .map((state) => {
                const minutes = minutesByState[state.key];
                const pct = totalMinutes > 0 ? (minutes / totalMinutes) * 100 : null;
                const metricValues = metricCols.map((metricCol) => {
                  const metricKey = `${metricCol.key}_${state.key}`;
                  const metricTotal = toSafeNumber(sumMetric(safeRows, metricKey));
                  return formatMetricByMode(metricTotal, minutes);
                });
                return `
                  <tr>
                    <td>${escapeHtml(state.label)}</td>
                    <td>${pct == null ? "-" : `${formatMetricValue(pct, 1)}%`}</td>
                    <td>${metricValues[0]}</td>
                    <td>${metricValues[1]}</td>
                    <td>${metricValues[2]}</td>
                    <td>${metricValues[3]}</td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
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
                  const competitionName = String(row?.competition_name || "").trim();
                  const homeTeamCell = homeTeam
                    ? buildTeamLinkHtml(homeTeam, competitionName, { strong: isHome })
                    : "-";
                  const awayTeamCell = awayTeam
                    ? buildTeamLinkHtml(awayTeam, competitionName, { strong: !isHome })
                    : "-";
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
  const xgThreshold = getCurrentXgPushThreshold();
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
    const xgVerdict = classifyDelta(xgDelta, xgThreshold);
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
  const thresholdLabel = formatXgPushThresholdForLabel(getCurrentXgPushThreshold());
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
              <td class="hcperf-summary-metric">xG (threshold = ${thresholdLabel})</td>
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
                  const competitionName = String(row?.competition_name || "").trim();
                  const homeCell = homeTeam && homeTeam !== "-"
                    ? buildTeamLinkHtml(homeTeam, competitionName, { strong: homeIsRelevant })
                    : "-";
                  const awayCell = awayTeam && awayTeam !== "-"
                    ? buildTeamLinkHtml(awayTeam, competitionName, { strong: awayIsRelevant })
                    : "-";
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
                  const xgThreshold = getCurrentXgPushThreshold();
                  const xgNoBet = Number.isFinite(xgDelta) && Math.abs(xgDelta) <= xgThreshold;
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

function buildStatsGeneralSwitchHtml(homeLabel, awayLabel) {
  const homeActive = statsGeneralTeamView !== "away";
  const awayActive = statsGeneralTeamView === "away";
  return `
    <div class="recent-switch" role="tablist" aria-label="General matches team view">
      <button
        type="button"
        class="recent-switch-btn stats-general-switch-btn${homeActive ? " active" : ""}"
        data-stats-general-team-view="home"
        role="tab"
        aria-selected="${homeActive ? "true" : "false"}"
      >
        Home: ${escapeHtml(homeLabel)}
      </button>
      <button
        type="button"
        class="recent-switch-btn stats-general-switch-btn${awayActive ? " active" : ""}"
        data-stats-general-team-view="away"
        role="tab"
        aria-selected="${awayActive ? "true" : "false"}"
      >
        Away: ${escapeHtml(awayLabel)}
      </button>
    </div>
  `;
}

function normalizeGamestateStatsMode(value) {
  const mode = String(value || "").trim().toLowerCase();
  if (mode === "per90" || mode === "minperstat" || mode === "total") {
    return mode;
  }
  return "total";
}

function buildGamestateModeSwitchHtml() {
  const activeMode = normalizeGamestateStatsMode(gamestateStatsMode);
  const options = [
    { mode: "total", label: "Total Stats" },
    { mode: "per90", label: "Stats/90" },
    { mode: "minperstat", label: "Min/Stat" },
  ];
  return `
    <div class="recent-switch" role="tablist" aria-label="Gamestate stats mode">
      ${options
        .map((option) => {
          const isActive = option.mode === activeMode;
          return `
            <button
              type="button"
              class="recent-switch-btn gamestate-mode-btn${isActive ? " active" : ""}"
              data-gamestate-mode="${option.mode}"
              role="tab"
              aria-selected="${isActive ? "true" : "false"}"
            >
              ${escapeHtml(option.label)}
            </button>
          `;
        })
        .join("")}
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

function buildVenueSplitSectionHtml(teamLabel, homeRows, awayRows, rollingWindowGames = 10, includeChart = true) {
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

function buildStatsGamesShownControlHtml(value, maxValue, options = {}) {
  const safeMax = Number.isFinite(maxValue) && maxValue > 0 ? Math.floor(maxValue) : 1;
  const safeValue = Math.max(1, Math.min(safeMax, clampRecentMatchesCount(value)));
  const inputIdRaw = String(options?.inputId || "statsGamesShownInput").trim();
  const inputId = inputIdRaw || "statsGamesShownInput";
  const labelText = String(options?.label || "Last X games");
  return `
    <div class="details-options">
      <label for="${escapeHtml(inputId)}">${escapeHtml(labelText)}</label>
      <input id="${escapeHtml(inputId)}" type="number" min="1" max="${safeMax}" step="1" value="${safeValue}" />
    </div>
  `;
}

function updateSortButtonLabel() {
  sortModeBtn.value = sortMode;
  if (savedSortModeBtn instanceof HTMLSelectElement) {
    savedSortModeBtn.value = savedSortMode;
  }
}

function normalizeGameSortMode(value) {
  const mode = String(value || "").trim().toLowerCase();
  if (mode === "league" || mode === "tier" || mode === "kickoff") {
    return mode;
  }
  return "kickoff";
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
        const handicapPass = showGamesWithoutHandicap || hasVisibleHandicapPricing(game);
        return leaguePass && tierPass && teamPass && handicapPass;
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

function sortGamesForDay(games, mode = sortMode) {
  const out = [...games];
  const resolvedSortMode = normalizeGameSortMode(mode);
  const tierSortRank = (tierValue) => {
    const tierText = String(tierValue || "").trim().toLowerCase();
    if (!tierText) return Number.MAX_SAFE_INTEGER;
    const match = tierText.match(/(?:^|\s)tier\s*([0-9]+)(?:\s|$)/i) || tierText.match(/^([0-9]+)$/);
    if (!match) return Number.MAX_SAFE_INTEGER;
    const tierNum = Number.parseInt(match[1], 10);
    return Number.isFinite(tierNum) ? tierNum : Number.MAX_SAFE_INTEGER;
  };
  if (resolvedSortMode === "tier") {
    out.sort((a, b) => {
      const tierCmp = tierSortRank(a.tier) - tierSortRank(b.tier);
      if (tierCmp !== 0) return tierCmp;
      const kickoffCmp = kickoffSortKey(a) - kickoffSortKey(b);
      if (kickoffCmp !== 0) return kickoffCmp;
      return String(a.competition || "").localeCompare(String(b.competition || ""));
    });
    return out;
  }
  if (resolvedSortMode === "league") {
    const leagueTierRank = new Map();
    for (const game of out) {
      const leagueName = String(game?.competition || "").trim();
      if (!leagueName) continue;
      const rank = tierSortRank(game?.tier);
      const existingRank = leagueTierRank.get(leagueName);
      if (existingRank === undefined || rank < existingRank) {
        leagueTierRank.set(leagueName, rank);
      }
    }
    out.sort((a, b) => {
      const aLeague = String(a.competition || "");
      const bLeague = String(b.competition || "");
      const aTierRank = leagueTierRank.get(String(aLeague).trim()) ?? tierSortRank(a.tier);
      const bTierRank = leagueTierRank.get(String(bLeague).trim()) ?? tierSortRank(b.tier);
      const tierCmp = aTierRank - bTierRank;
      if (tierCmp !== 0) return tierCmp;
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
  if (tierNum === 4) return "tier-row-4";
  return "";
}

function dayHasComputedXgdMetrics(dayGames) {
  const metricKeys = [
    "season_xgd",
    "last5_xgd",
    "last3_xgd",
    "season_xgd_perf",
    "last5_xgd_perf",
    "last3_xgd_perf",
    "season_strength",
    "last5_strength",
    "last3_strength",
  ];
  return (dayGames || []).some((game) => {
    return metricKeys.some((metricKey) => {
      return toMetricNumberOrNull(game?.[metricKey]) != null;
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
    xgdPayloadByMarket.clear();
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
    // Keep mapping tab edits stable during game refreshes (manual + auto-refresh).
    // Mapping data now refreshes only when the user opens Mapping, clicks
    // "Refresh Mapping Data", or after an explicit mapping save/delete action.
    if (activeTab === "rankings" && teamHcRankingsLoaded) {
      loadTeamHcRankings({ silent: true });
    } else if (activeTab === "teams" && teamsDirectoryLoaded) {
      loadTeamsDirectory({ silent: true });
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
  const showSavedContour = options?.showSavedContour !== false;
  const kickoffHeaderLabel = escapeHtml(getKickoffColumnLabel());
  const table = document.createElement("table");
  table.className = "games-table main-games-table";
  if (isHistorical) {
    table.innerHTML = `
      <thead>
        <tr>
          <th>${kickoffHeaderLabel}</th>
          <th class="league-col">League</th>
          <th class="home-team-col">Home</th>
          <th class="vs-team-col">v</th>
          <th class="away-team-col">Away</th>
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
          <th>${kickoffHeaderLabel}</th>
          <th class="league-col">League</th>
          <th class="home-team-col">Home</th>
          <th class="vs-team-col">v</th>
          <th class="away-team-col">Away</th>
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
    row.innerHTML = `<td class="no-games-row" colspan="${isHistorical ? "18" : "15"}">${escapeHtml(emptyMessage)}</td>`;
    tbody.appendChild(row);
    return table;
  }

  for (const game of sortedGames) {
    gamesById.set(game.market_id, game);
    const row = document.createElement("tr");
    row.dataset.marketId = game.market_id;
    if (selectedMarketId === game.market_id) row.classList.add("selected");
    if (showSavedContour && savedMarketIds.has(String(game.market_id || "").trim())) {
      row.classList.add("saved-game-row");
    }
    const tierClass = tierRowClass(game.tier);
    if (tierClass) row.classList.add(tierClass);

    const noXgMetrics = hasNoXgMetricSignal(game);
    const handicap = toMetricNumberOrNull(game?.mainline);
    const threshold = getCurrentXgPushThreshold();
    const xgdPeriodValues = [
      toMetricNumberOrNull(game?.season_strength),
      toMetricNumberOrNull(game?.last5_strength),
      toMetricNumberOrNull(game?.last3_strength),
    ];
    const xgdPerfPeriodValues = [
      toMetricNumberOrNull(game?.season_xgd_perf),
      toMetricNumberOrNull(game?.last5_xgd_perf),
      toMetricNumberOrNull(game?.last3_xgd_perf),
    ];
    const minPeriodValues = [
      toMetricNumberOrNull(game?.season_min_xg),
      toMetricNumberOrNull(game?.last5_min_xg),
      toMetricNumberOrNull(game?.last3_min_xg),
    ];
    const maxPeriodValues = [
      toMetricNumberOrNull(game?.season_max_xg),
      toMetricNumberOrNull(game?.last5_max_xg),
      toMetricNumberOrNull(game?.last3_max_xg),
    ];
    const xgdHcHighlightClass = (xgdHcHighlightEnabled && !noXgMetrics)
      ? getMainTableXgdHcHighlightClass(xgdPeriodValues, handicap, threshold)
      : "";
    const xgdPerfHcHighlightClass = (xgdHcHighlightEnabled && !noXgMetrics)
      ? getMainTableXgdHcHighlightClass(xgdPerfPeriodValues, handicap, threshold)
      : "";
    const goalsBandHighlight = (xgdHcHighlightEnabled && !noXgMetrics)
      ? getMainTableGoalsBandHighlight(
        toMetricNumberOrNull(game?.goal_mainline),
        minPeriodValues,
        maxPeriodValues,
        threshold,
      )
      : { goalLineClass: "", goalUnderClass: "", goalOverClass: "" };
    const goalUnderCellClass = ["goal-under-price-col", goalsBandHighlight.goalUnderClass].filter(Boolean).join(" ");
    const goalLineCellClass = ["goal-line-col", goalsBandHighlight.goalLineClass].filter(Boolean).join(" ");
    const goalOverCellClass = ["goal-over-price-col", goalsBandHighlight.goalOverClass].filter(Boolean).join(" ");
    const domesticFallbackClass = Boolean(game?.xgd_domestic_fallback) ? "xgd-domestic-fallback" : "";
    const xgdCellClassParts = ["metric-stack-cell"];
    if (xgdHcHighlightClass) xgdCellClassParts.push(xgdHcHighlightClass);
    if (domesticFallbackClass) xgdCellClassParts.push(domesticFallbackClass);
    const xgdCellClass = xgdCellClassParts.join(" ");
    const xgdPerfCellClassParts = ["metric-stack-cell"];
    if (xgdPerfHcHighlightClass) xgdPerfCellClassParts.push(xgdPerfHcHighlightClass);
    if (domesticFallbackClass) xgdPerfCellClassParts.push(domesticFallbackClass);
    const xgdPerfCellClass = xgdPerfCellClassParts.join(" ");
    const xgdCellTitleAttr = "";
    const xgdPerfCellTitleAttr = "";
    const seasonXgdValue = noXgMetrics ? "-" : game.season_strength;
    const last5XgdValue = noXgMetrics ? "-" : game.last5_strength;
    const last3XgdValue = noXgMetrics ? "-" : game.last3_strength;
    const seasonXgdPerfValue = noXgMetrics ? "-" : game.season_xgd_perf;
    const last5XgdPerfValue = noXgMetrics ? "-" : game.last5_xgd_perf;
    const last3XgdPerfValue = noXgMetrics ? "-" : game.last3_xgd_perf;
    const teams = resolveGameTeams(game);
    const competitionName = String(game?.competition || "").trim();
    const homeTeamCell = teams.home ? buildTeamLinkHtml(teams.home, competitionName) : "-";
    const awayTeamCell = teams.away ? buildTeamLinkHtml(teams.away, competitionName) : "-";
    const kickoffLocalText = formatGameKickoffLocalTime(game);

    if (isHistorical) {
      row.innerHTML = `
        <td>${escapeHtml(kickoffLocalText)}</td>
        <td class="league-col">${escapeHtml(game.competition)}</td>
        <td class="home-team-col">${homeTeamCell}</td>
        <td class="vs-team-col">v</td>
        <td class="away-team-col">${awayTeamCell}</td>
        <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
        <td class="line-col handicap-line-col">${escapeHtml(game.mainline || "-")}</td>
        <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
        <td class="${goalUnderCellClass}">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="${goalLineCellClass}">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="${goalOverCellClass}">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="xg-home-col">${escapeHtml(game.home_xg_actual || "-")}</td>
        <td class="line-col score-col">${escapeHtml(game.scoreline || "-")}</td>
        <td class="xg-away-col">${escapeHtml(game.away_xg_actual || "-")}</td>
        <td class="${xgdCellClass}"${xgdCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue)}</td>
        <td class="${xgdPerfCellClass}"${xgdPerfCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdPerfValue, last5XgdPerfValue, last3XgdPerfValue)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg)}</td>
      `;
    } else {
      row.innerHTML = `
        <td>${escapeHtml(kickoffLocalText)}</td>
        <td class="league-col">${escapeHtml(game.competition)}</td>
        <td class="home-team-col">${homeTeamCell}</td>
        <td class="vs-team-col">v</td>
        <td class="away-team-col">${awayTeamCell}</td>
        <td class="home-price-col">${escapeHtml(game.home_price || "-")}</td>
        <td class="line-col handicap-line-col">${escapeHtml(game.mainline || "-")}</td>
        <td class="away-price-col">${escapeHtml(game.away_price || "-")}</td>
        <td class="${xgdCellClass}"${xgdCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue)}</td>
        <td class="${xgdPerfCellClass}"${xgdPerfCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdPerfValue, last5XgdPerfValue, last3XgdPerfValue)}</td>
        <td class="${goalUnderCellClass}">${escapeHtml(game.goal_under_price || "-")}</td>
        <td class="${goalLineCellClass}">${escapeHtml(game.goal_mainline || "-")}</td>
        <td class="${goalOverCellClass}">${escapeHtml(game.goal_over_price || "-")}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg)}</td>
      `;
    }
    row.addEventListener("click", () => loadGameXgd(game.market_id));
    tbody.appendChild(row);
  }
  bindTeamLinkButtons(table);
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

  const visibleDays = savedDays
    .map((day) => {
      const rawGames = Array.isArray(day?.games) ? day.games : [];
      const visibleGames = showGamesWithoutHandicap
        ? rawGames
        : rawGames.filter((game) => hasVisibleHandicapPricing(game));
      return {
        day,
        games: sortGamesForDay(visibleGames, savedSortMode),
      };
    })
    .filter((entry) => Array.isArray(entry.games) && entry.games.length > 0);

  if (!visibleDays.length) {
    savedGamesView.innerHTML = showGamesWithoutHandicap
      ? "<p>No saved games yet.</p>"
      : "<p>No saved games with handicap pricing.</p>";
    return;
  }

  gamesById = new Map();
  for (const entry of visibleDays) {
    const day = entry.day;
    const sortedDayGames = entry.games;
    const useHistoricalLayout = sortedDayGames.some((game) => Boolean(game?.is_historical));

    const block = document.createElement("section");
    block.className = "day-block";
    const header = document.createElement("div");
    header.className = "day-header";
    const heading = document.createElement("h2");
    heading.className = "day-header-title";
    heading.textContent = `${String(day?.date_label || day?.date || "Saved")} (${sortedDayGames.length})`;
    header.appendChild(heading);

    const table = createGamesTable(sortedDayGames, useHistoricalLayout, {
      emptyMessage: "No saved games",
      showSavedContour: false,
    });
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

function renderTeamHcPerfDetailFromRows(teamName, competitionName, rows) {
  if (!teamHcPerfContent || !teamHcPerfTitle || !teamHcPerfMeta) return;
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  const safeRows = Array.isArray(rows) ? rows : [];
  const homeRows = safeRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "home");
  const awayRows = safeRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "away");

  teamHcPerfTitle.textContent = `${teamText} - HC Perf`;
  teamHcPerfMeta.textContent = `${competitionText} | ${safeRows.length} games`;
  if (!safeRows.length) {
    teamHcPerfContent.innerHTML = "<p>No games found for this team in this league.</p>";
    return;
  }

  const summaryHtml = buildHcPerfSummaryTableHtml(teamText, safeRows);
  const generalGamesHtml = buildSeasonHandicapPerformanceTableHtml(teamText, safeRows, {
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
  bindTeamLinkButtons(teamHcPerfContent);
}

async function loadTeamHcRankingTeamDetails(teamName, competitionName) {
  if (!teamHcPerfPanel || !teamHcPerfContent || !teamHcPerfTitle || !teamHcPerfMeta) return;
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  if (!teamText || !competitionText) return;
  teamHcPerfDetailTeam = teamText;
  teamHcPerfDetailCompetition = competitionText;
  teamHcPerfDetailRows = [];
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
    query.set("xg_threshold", String(getCurrentXgPushThreshold()));
    const res = await fetch(`/api/team-hc-rankings/details?${query.toString()}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load team handicap performance");
    if (teamHcPerfDetailLoadingKey !== requestKey) return;

    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    teamHcPerfDetailRows = rows;
    renderTeamHcPerfDetailFromRows(teamText, competitionText, rows);
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
        openTeamPage(teamName, leagueName || null);
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
    const query = new URLSearchParams();
    query.set("xg_threshold", String(getCurrentXgPushThreshold()));
    const res = await fetch(`/api/team-hc-rankings?${query.toString()}`);
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

function renderTeamsDirectory() {
  if (!teamsView) return;

  if (teamsDirectoryLoading && !teamsDirectoryLoaded) {
    teamsView.innerHTML = "<p>Loading teams...</p>";
    return;
  }
  if (teamsDirectoryErrorText) {
    teamsView.innerHTML = `<p>${escapeHtml(teamsDirectoryErrorText)}</p>`;
    return;
  }
  if (!teamsDirectoryRows.length) {
    teamsView.innerHTML = "<p>No teams available.</p>";
    return;
  }

  const query = String(teamsDirectorySearchQuery || "").trim().toLowerCase();
  const filteredRows = query
    ? teamsDirectoryRows.filter((row) => {
      const teamText = String(row?.team || "").toLowerCase();
      const primaryComp = String(row?.primary_competition || "").toLowerCase();
      const competitions = Array.isArray(row?.competitions) ? row.competitions : [];
      const matchesCompetition = competitions.some((entry) => String(entry?.competition || "").toLowerCase().includes(query));
      return teamText.includes(query) || primaryComp.includes(query) || matchesCompetition;
    })
    : teamsDirectoryRows;

  if (!filteredRows.length) {
    teamsView.innerHTML = "<p>No teams match this search.</p>";
    return;
  }

  const table = document.createElement("table");
  table.className = "games-table recent-lines-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Team</th>
        <th>Primary League</th>
        <th>Games</th>
        <th>Leagues</th>
      </tr>
    </thead>
    <tbody>
      ${filteredRows
        .map((row) => {
          const teamName = String(row?.team || "").trim();
          const primaryCompetition = String(row?.primary_competition || "").trim();
          const gamesCount = Number(row?.games_count) || 0;
          const competitionsCount = Number(row?.competitions_count) || 0;
          return `
            <tr>
              <td>${buildTeamLinkHtml(teamName, primaryCompetition || "")}</td>
              <td>${escapeHtml(primaryCompetition || "-")}</td>
              <td>${gamesCount}</td>
              <td>${competitionsCount}</td>
            </tr>
          `;
        })
        .join("")}
    </tbody>
  `;
  teamsView.innerHTML = "";
  teamsView.appendChild(table);
  bindTeamLinkButtons(teamsView);
}

async function loadTeamsDirectory(options = {}) {
  if (teamsDirectoryLoading) return false;
  const silent = Boolean(options?.silent);
  teamsDirectoryLoading = true;
  if (!silent) {
    statusText.textContent = "Loading teams...";
  }
  if (activeTab === "teams") {
    renderTeamsDirectory();
  }
  try {
    const res = await fetch("/api/teams");
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load teams");
    teamsDirectoryRows = Array.isArray(payload?.teams) ? payload.teams : [];
    teamsDirectoryLoaded = true;
    teamsDirectoryErrorText = "";
    if (activeTab === "teams" && !silent) {
      statusText.textContent = `Loaded ${Number(payload?.total_teams) || teamsDirectoryRows.length} teams`;
    }
    if (activeTab === "teams") {
      renderTeamsDirectory();
    }
    return true;
  } catch (err) {
    teamsDirectoryErrorText = String(err.message || err);
    if (!silent) {
      statusText.textContent = teamsDirectoryErrorText;
    }
    if (activeTab === "teams") {
      renderTeamsDirectory();
    }
    return false;
  } finally {
    teamsDirectoryLoading = false;
    if (activeTab === "teams") {
      renderTeamsDirectory();
    }
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
    if (dayPickerInput instanceof HTMLInputElement) {
      dayPickerInput.disabled = true;
      dayPickerInput.value = "";
      dayPickerInput.min = "";
      dayPickerInput.max = "";
    }
    updateDayPickerCount("");
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
  if (dayPickerInput instanceof HTMLInputElement) {
    const firstIso = String(allDays[0]?.date || "").trim();
    const lastIso = String(allDays[allDays.length - 1]?.date || "").trim();
    const currentIso = String(day?.date || "").trim();
    dayPickerInput.disabled = false;
    dayPickerInput.min = firstIso;
    dayPickerInput.max = lastIso;
    if (currentIso && dayPickerInput.value !== currentIso) {
      dayPickerInput.value = currentIso;
    }
    updateDayPickerCount(currentIso);
  }

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
    const shouldAutoCalcDayXgd = (
      dayIso
      && sortedDayGames.length > 0
      && !isComputingDay
      && !hasComputedXgd
      && !historicalDayAutoCalcAttempted.has(dayIso)
    );
    if (shouldAutoCalcDayXgd) {
      historicalDayAutoCalcAttempted.add(dayIso);
      void calculateHistoricalDayXgd(dayIso);
    }
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
    context_note: String(payload.context_note || ""),
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
    context_note: String(view?.context_note || ""),
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
  const contextNote = activeView.context_note
    ? `<div class="xgd-context-note">${escapeHtml(activeView.context_note)}</div>`
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
            <td>${buildTeamLinkHtml(r.home_sofa, r.fixture_competition || payload.competition)}</td>
            <td>${buildTeamLinkHtml(r.away_sofa, r.fixture_competition || payload.competition)}</td>
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
  const cachedHcPerfPayload = selectedMarketId ? hcPerfPayloadByMarket.get(selectedMarketId) : null;
  const cachedHcPerfRows = normalizeSeasonHandicapRows(cachedHcPerfPayload?.season_handicap_rows);
  const activeSeasonRowsRaw = normalizeSeasonHandicapRows(activeView.season_handicap_rows);
  const fallbackSeasonRows = normalizeSeasonHandicapRows(payloadSeasonHandicapRows);
  const hasCachedSeasonRows = cachedHcPerfRows.home.length > 0 || cachedHcPerfRows.away.length > 0;
  const hasActiveSeasonRows = activeSeasonRowsRaw.home.length > 0 || activeSeasonRowsRaw.away.length > 0;
  const hasMultipleXgdViews = xgdViews.length > 1;
  let seasonHandicapRows = fallbackSeasonRows;
  if (hasMultipleXgdViews && hasActiveSeasonRows) {
    seasonHandicapRows = activeSeasonRowsRaw;
  } else if (hasCachedSeasonRows) {
    seasonHandicapRows = cachedHcPerfRows;
  } else if (hasActiveSeasonRows) {
    seasonHandicapRows = activeSeasonRowsRaw;
  }
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
  const teamSummaryTables = buildTeamXgdSummaryTablesHtml(homeLabel, awayLabel, homeRecentRows, awayRecentRows);
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

  const xgdTabContent = `${warning}${contextNote}${historicalResultSection}${periodTable}${teamSummaryTables}${recentMatchesSection}${mappingTable}`;
  const statsFixtureIsAway = statsTeamView === "away";
  const statsFixtureActiveLabel = statsFixtureIsAway ? awayLabel : homeLabel;
  const statsFixtureActiveRows = statsFixtureIsAway ? awayRecentRows : homeRecentRows;
  const homeGeneralRows = [
    ...(Array.isArray(homeTeamVenueRows?.home) ? homeTeamVenueRows.home : []),
    ...(Array.isArray(homeTeamVenueRows?.away) ? homeTeamVenueRows.away : []),
  ];
  const awayGeneralRows = [
    ...(Array.isArray(awayTeamVenueRows?.home) ? awayTeamVenueRows.home : []),
    ...(Array.isArray(awayTeamVenueRows?.away) ? awayTeamVenueRows.away : []),
  ];
  const statsGeneralIsAway = statsGeneralTeamView === "away";
  const statsGeneralActiveLabel = statsGeneralIsAway ? awayLabel : homeLabel;
  const statsGeneralActiveRows = statsGeneralIsAway ? awayGeneralRows : homeGeneralRows;
  const statsVenueMaxGamesShown = Math.max(
    1,
    homeRecentRows.length,
    awayRecentRows.length,
    statsFixtureActiveRows.length
  );
  if (statsVenueGamesShownAuto) {
    statsVenueGamesShownCount = statsVenueMaxGamesShown;
  } else if (!Number.isFinite(statsVenueGamesShownCount) || statsVenueGamesShownCount < 1) {
    statsVenueGamesShownCount = 1;
  }
  statsVenueGamesShownCount = Math.max(
    1,
    Math.min(statsVenueMaxGamesShown, clampRecentMatchesCount(statsVenueGamesShownCount))
  );
  const statsAllMaxGamesShown = Math.max(1, homeGeneralRows.length, awayGeneralRows.length);
  if (statsAllGamesShownAuto) {
    statsAllGamesShownCount = statsAllMaxGamesShown;
  } else if (!Number.isFinite(statsAllGamesShownCount) || statsAllGamesShownCount < 1) {
    statsAllGamesShownCount = 1;
  }
  statsAllGamesShownCount = Math.max(
    1,
    Math.min(statsAllMaxGamesShown, clampRecentMatchesCount(statsAllGamesShownCount))
  );
  const cardsTabContent = `
    <h3 class="section-title">Cards & Corners</h3>
    ${buildStatsGamesShownControlHtml(statsVenueGamesShownCount, statsVenueMaxGamesShown, {
      inputId: "statsVenueGamesShownInput",
      label: "Venue games shown",
    })}
    ${buildCardsCornersAveragesTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsVenueGamesShownCount)}
    ${buildGamestateTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsVenueGamesShownCount)}
    ${buildStatsSwitchHtml(homeLabel, awayLabel)}
    ${buildCardsCornersMatchesTableHtml(
      statsFixtureActiveLabel,
      statsFixtureActiveRows,
      statsVenueGamesShownCount,
      "Fixture-side matches"
    )}
    <h3 class="section-title">General</h3>
    ${buildStatsGamesShownControlHtml(statsAllGamesShownCount, statsAllMaxGamesShown, {
      inputId: "statsAllGamesShownInput",
      label: "All games shown",
    })}
    ${buildCardsCornersAveragesTableHtml(homeGeneralRows, awayGeneralRows, homeLabel, awayLabel, statsAllGamesShownCount)}
    ${buildGamestateTableHtml(homeGeneralRows, awayGeneralRows, homeLabel, awayLabel, statsAllGamesShownCount)}
    ${buildStatsGeneralSwitchHtml(homeLabel, awayLabel)}
    <h3 class="section-title">${escapeHtml(statsGeneralActiveLabel)}: Home & Away Matches</h3>
    ${buildCardsCornersMatchesTableHtml(
      statsGeneralActiveLabel,
      statsGeneralActiveRows,
      statsAllGamesShownCount,
      "All matches"
    )}
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
  bindTeamLinkButtons(linesContainer);

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

  const statsGeneralSwitchButtons = linesContainer.querySelectorAll(".stats-general-switch-btn");
  for (const button of statsGeneralSwitchButtons) {
    button.addEventListener("click", () => {
      const targetView = button.dataset.statsGeneralTeamView === "away" ? "away" : "home";
      if (targetView === statsGeneralTeamView) return;
      statsGeneralTeamView = targetView;
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const gamestateModeButtons = linesContainer.querySelectorAll(".gamestate-mode-btn");
  for (const button of gamestateModeButtons) {
    button.addEventListener("click", () => {
      const targetMode = normalizeGamestateStatsMode(button.dataset.gamestateMode || "");
      if (targetMode === gamestateStatsMode) return;
      gamestateStatsMode = targetMode;
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

  const statsVenueGamesShownInput = linesContainer.querySelector("#statsVenueGamesShownInput");
  if (statsVenueGamesShownInput) {
    statsVenueGamesShownInput.addEventListener("change", () => {
      statsVenueGamesShownAuto = false;
      statsVenueGamesShownCount = clampRecentMatchesCount(statsVenueGamesShownInput.value);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }

  const statsAllGamesShownInput = linesContainer.querySelector("#statsAllGamesShownInput");
  if (statsAllGamesShownInput) {
    statsAllGamesShownInput.addEventListener("change", () => {
      statsAllGamesShownAuto = false;
      statsAllGamesShownCount = clampRecentMatchesCount(statsAllGamesShownInput.value);
      if (lastXgdPayload) renderXgd(lastXgdPayload);
    });
  }
}

function closeTeamDetailsPanel() {
  if (teamDetailsPanel) {
    teamDetailsPanel.classList.add("hidden");
  }
  teamDetailsLoadingKey = "";
}

function getTeamPageCacheKey(teamName, competitionName = "") {
  return `${String(teamName || "").trim().toLowerCase()}::${String(competitionName || "").trim().toLowerCase()}`;
}

function buildTeamStatsSummaryTableHtml(teamLabel, rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${escapeHtml(teamLabel || "Team")} - Stats Summary</h4>
        <p class="recent-empty">No season stats available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} - Stats Summary</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Games</th>
              <th>Corners For</th>
              <th>Corners Against</th>
              <th>Cards For</th>
              <th>Cards Against</th>
              <th>Yellow For</th>
              <th>Yellow Against</th>
              <th>Red For</th>
              <th>Red Against</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${safeRows.length}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "corners_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "corners_against"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "cards_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "cards_against"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "yellow_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "yellow_against"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "red_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "red_against"), 2)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildTeamXgControlsHtml(gamesShownValue, gamesShownMax, rollingValue, rollingMax) {
  const safeGamesMax = Number.isFinite(gamesShownMax) && gamesShownMax > 0 ? Math.floor(gamesShownMax) : 1;
  const safeGamesValue = Math.max(1, Math.min(safeGamesMax, clampRecentMatchesCount(gamesShownValue)));
  const safeRollingMax = Number.isFinite(rollingMax) && rollingMax > 0 ? Math.floor(rollingMax) : safeGamesMax;
  const safeRollingValue = Math.max(1, Math.min(safeRollingMax, clampRecentMatchesCount(rollingValue)));
  return `
    <div class="details-options">
      <label for="teamXgGamesShownInput">Last X games</label>
      <input id="teamXgGamesShownInput" type="number" min="1" max="${safeGamesMax}" step="1" value="${safeGamesValue}" />
      <label for="teamXgRollingWindowInput">Rolling avg games</label>
      <input id="teamXgRollingWindowInput" type="number" min="1" max="${safeRollingMax}" step="1" value="${safeRollingValue}" />
    </div>
  `;
}

function renderTeamDetailsPanel(payload) {
  if (!teamDetailsContent || !teamDetailsTitle || !teamDetailsMeta) return;
  const teamText = String(payload?.team || teamDetailsTeam || "").trim() || "Team";
  const selectedCompetition = String(payload?.competition || teamDetailsCompetition || "").trim();
  const competitions = Array.isArray(payload?.competitions) ? payload.competitions : [];
  const recentRows = Array.isArray(payload?.recent_rows) ? payload.recent_rows : [];
  const teamVenueRows = payload?.team_venue_rows && typeof payload.team_venue_rows === "object"
    ? payload.team_venue_rows
    : { home: [], away: [] };
  const homeVenueRows = Array.isArray(teamVenueRows?.home) ? teamVenueRows.home : [];
  const awayVenueRows = Array.isArray(teamVenueRows?.away) ? teamVenueRows.away : [];
  const seasonHandicapRows = Array.isArray(payload?.season_handicap_rows) ? payload.season_handicap_rows : [];
  const homeHcRows = seasonHandicapRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "home");
  const awayHcRows = seasonHandicapRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "away");
  teamDetailsTitle.textContent = `${teamText}`;
  teamDetailsMeta.textContent = `${selectedCompetition || "Selected leagues"} | ${recentRows.length} season games`;

  if (teamDetailsCompetitionSelect instanceof HTMLSelectElement) {
    teamDetailsCompetitionSelect.innerHTML = "";
    if (!competitions.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = selectedCompetition || "No league data";
      teamDetailsCompetitionSelect.appendChild(option);
      teamDetailsCompetitionSelect.disabled = true;
    } else {
      competitions.forEach((entry) => {
        const competitionName = String(entry?.competition || "").trim();
        if (!competitionName) return;
        const option = document.createElement("option");
        option.value = competitionName;
        const gamesCount = Number(entry?.games_count) || 0;
        option.textContent = `${competitionName}${gamesCount ? ` (${gamesCount})` : ""}`;
        if (competitionName === selectedCompetition) {
          option.selected = true;
        }
        teamDetailsCompetitionSelect.appendChild(option);
      });
      if (!teamDetailsCompetitionSelect.value && selectedCompetition) {
        teamDetailsCompetitionSelect.value = selectedCompetition;
      }
      teamDetailsCompetitionSelect.disabled = false;
    }
  }

  if (teamDetailsMainTab !== "stats" && teamDetailsMainTab !== "hcperf") {
    teamDetailsMainTab = "xg";
  }
  const maxTeamXgGames = Math.max(1, recentRows.length || 1);
  const xgGamesShown = Math.max(
    1,
    Math.min(maxTeamXgGames, clampRecentMatchesCount(teamDetailsXgGamesShownCount))
  );
  const xgRollingWindow = Math.max(
    1,
    Math.min(xgGamesShown, clampRecentMatchesCount(teamDetailsXgRollingWindowCount))
  );
  teamDetailsXgGamesShownCount = xgGamesShown;
  teamDetailsXgRollingWindowCount = xgRollingWindow;
  const limitedAllRows = recentRows.slice(0, xgGamesShown);
  const limitedHomeRows = homeVenueRows.slice(0, xgGamesShown);
  const limitedAwayRows = awayVenueRows.slice(0, xgGamesShown);
  const buildTeamXgSectionHtml = (sectionTitle, rows, tableTitle) => `
    <h3 class="section-title">${escapeHtml(sectionTitle)}</h3>
    ${buildRecentAveragesTableHtml(rows, Math.max(1, rows.length || 1), teamText)}
    ${buildMetricTrendPlotHtml(rows, `${sectionTitle} trend`, teamText, xgRollingWindow)}
    ${buildRecentMatchesTableHtml(tableTitle, rows, teamText)}
  `;
  const xgTabContent = `
    <h3 class="section-title">Season Games With xG</h3>
    ${buildTeamXgControlsHtml(xgGamesShown, maxTeamXgGames, xgRollingWindow, xgGamesShown)}
    ${buildTeamXgSectionHtml("All Games", limitedAllRows, "All games")}
    ${buildTeamXgSectionHtml("Home Games", limitedHomeRows, "Home games")}
    ${buildTeamXgSectionHtml("Away Games", limitedAwayRows, "Away games")}
  `;
  const statsTabContent = `
    <h3 class="section-title">Season Stats</h3>
    ${buildTeamStatsSummaryTableHtml(teamText, recentRows)}
    ${buildSingleTeamGamestateTableHtml(recentRows, `${teamText} (General)`)}
    <h3 class="section-title">Venue Split</h3>
    ${buildCardsCornersAveragesTableHtml(
      homeVenueRows,
      awayVenueRows,
      `${teamText} (Home)`,
      `${teamText} (Away)`
    )}
    ${buildGamestateTableHtml(
      homeVenueRows,
      awayVenueRows,
      `${teamText} (Home)`,
      `${teamText} (Away)`
    )}
    ${buildCardsCornersMatchesTableHtml(teamText, homeVenueRows, null, "Home games")}
    ${buildCardsCornersMatchesTableHtml(teamText, awayVenueRows, null, "Away games")}
    <h3 class="section-title">All Games</h3>
    ${buildCardsCornersMatchesTableHtml(teamText, recentRows, null, "Season games")}
  `;
  const hcPerfTabContent = `
    <h3 class="section-title">Season Handicap Performance</h3>
    ${buildHcPerfSummaryTableHtml(teamText, seasonHandicapRows)}
    ${buildSeasonHandicapPerformanceTableHtml(teamText, seasonHandicapRows, {
      title: `${teamText} - All Games`,
      relevantTeam: teamText,
    })}
    ${buildSeasonHandicapPerformanceTableHtml(teamText, homeHcRows, {
      title: `${teamText} - Home Games`,
      relevantTeam: teamText,
    })}
    ${buildSeasonHandicapPerformanceTableHtml(teamText, awayHcRows, {
      title: `${teamText} - Away Games`,
      relevantTeam: teamText,
    })}
  `;
  const teamTabNav = `
    <section class="page-tabs details-main-tabs">
      <button type="button" class="tab-btn team-details-main-tab ${teamDetailsMainTab === "xg" ? "active" : ""}" data-team-details-tab="xg">xG Games</button>
      <button type="button" class="tab-btn team-details-main-tab ${teamDetailsMainTab === "stats" ? "active" : ""}" data-team-details-tab="stats">Stats</button>
      <button type="button" class="tab-btn team-details-main-tab ${teamDetailsMainTab === "hcperf" ? "active" : ""}" data-team-details-tab="hcperf">HC Perf</button>
    </section>
  `;
  const activeTabContent = teamDetailsMainTab === "stats"
    ? statsTabContent
    : (teamDetailsMainTab === "hcperf" ? hcPerfTabContent : xgTabContent);
  teamDetailsContent.innerHTML = `${teamTabNav}${activeTabContent}`;
  bindTeamLinkButtons(teamDetailsContent);

  const tabButtons = teamDetailsContent.querySelectorAll(".team-details-main-tab");
  tabButtons.forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) return;
    button.addEventListener("click", () => {
      const nextTabRaw = String(button.dataset.teamDetailsTab || "").trim().toLowerCase();
      const nextTab = nextTabRaw === "stats" || nextTabRaw === "hcperf" ? nextTabRaw : "xg";
      if (nextTab === teamDetailsMainTab) return;
      teamDetailsMainTab = nextTab;
      if (teamDetailsPayload) {
        renderTeamDetailsPanel(teamDetailsPayload);
      }
    });
  });

  const teamXgGamesShownInput = teamDetailsContent.querySelector("#teamXgGamesShownInput");
  if (teamXgGamesShownInput instanceof HTMLInputElement) {
    teamXgGamesShownInput.addEventListener("change", () => {
      teamDetailsXgGamesShownCount = clampRecentMatchesCount(teamXgGamesShownInput.value);
      if (teamDetailsXgRollingWindowCount > teamDetailsXgGamesShownCount) {
        teamDetailsXgRollingWindowCount = teamDetailsXgGamesShownCount;
      }
      if (teamDetailsPayload) {
        renderTeamDetailsPanel(teamDetailsPayload);
      }
    });
  }
  const teamXgRollingWindowInput = teamDetailsContent.querySelector("#teamXgRollingWindowInput");
  if (teamXgRollingWindowInput instanceof HTMLInputElement) {
    teamXgRollingWindowInput.addEventListener("change", () => {
      teamDetailsXgRollingWindowCount = clampRecentMatchesCount(teamXgRollingWindowInput.value);
      if (teamDetailsPayload) {
        renderTeamDetailsPanel(teamDetailsPayload);
      }
    });
  }

  const gamestateModeButtons = teamDetailsContent.querySelectorAll(".gamestate-mode-btn");
  for (const button of gamestateModeButtons) {
    button.addEventListener("click", () => {
      const targetMode = normalizeGamestateStatsMode(button.dataset.gamestateMode || "");
      if (targetMode === gamestateStatsMode) return;
      gamestateStatsMode = targetMode;
      if (teamDetailsPayload) {
        renderTeamDetailsPanel(teamDetailsPayload);
      }
    });
  }
}

async function openTeamPage(teamName, competitionName = null, options = {}) {
  if (!teamDetailsPanel || !teamDetailsContent || !teamDetailsTitle || !teamDetailsMeta) return;
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  const forceRefresh = Boolean(options?.force);
  if (!teamText) return;

  const isDifferentSelection = teamText !== teamDetailsTeam || competitionText !== teamDetailsCompetition;
  if (isDifferentSelection) {
    teamDetailsMainTab = "xg";
    teamDetailsXgGamesShownCount = TEAM_DETAILS_XG_GAMES_DEFAULT;
    teamDetailsXgRollingWindowCount = TEAM_DETAILS_XG_ROLLING_DEFAULT;
  }
  teamDetailsTeam = teamText;
  teamDetailsCompetition = competitionText;
  teamDetailsPanel.classList.remove("hidden");
  teamDetailsTitle.textContent = teamText;
  teamDetailsMeta.textContent = competitionText ? `${competitionText} | Loading...` : "Loading...";
  teamDetailsContent.innerHTML = `
    <div class="hcperf-loading">
      <span class="hcperf-loading-dot" aria-hidden="true"></span>
      <span>Loading team page...</span>
    </div>
  `;

  const cacheKey = getTeamPageCacheKey(teamText, competitionText);
  if (!forceRefresh && teamPagePayloadByKey.has(cacheKey)) {
    const cachedPayload = teamPagePayloadByKey.get(cacheKey);
    if (cachedPayload && typeof cachedPayload === "object") {
      const resolvedTeam = String(cachedPayload?.team || "").trim();
      if (resolvedTeam) {
        teamDetailsTeam = resolvedTeam;
      }
      teamDetailsPayload = cachedPayload;
      renderTeamDetailsPanel(cachedPayload);
      return;
    }
  }

  const requestKey = `${teamText}::${competitionText}`;
  teamDetailsLoadingKey = requestKey;
  try {
    const query = new URLSearchParams();
    query.set("team", teamText);
    if (competitionText) {
      query.set("competition", competitionText);
    }
    const res = await fetch(`/api/team-page?${query.toString()}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load team page");
    if (teamDetailsLoadingKey !== requestKey) return;
    const nextPayload = payload && typeof payload === "object" ? payload : {};
    const resolvedTeam = String(nextPayload?.team || "").trim();
    const resolvedCompetition = String(nextPayload?.competition || "").trim();
    teamPagePayloadByKey.set(cacheKey, nextPayload);
    if (resolvedCompetition) {
      teamPagePayloadByKey.set(getTeamPageCacheKey(teamText, resolvedCompetition), nextPayload);
    }
    if (resolvedTeam) {
      teamDetailsTeam = resolvedTeam;
      teamPagePayloadByKey.set(getTeamPageCacheKey(resolvedTeam, resolvedCompetition || competitionText), nextPayload);
    }
    teamDetailsPayload = nextPayload;
    teamDetailsCompetition = resolvedCompetition || competitionText;
    renderTeamDetailsPanel(nextPayload);
  } catch (err) {
    if (teamDetailsLoadingKey !== requestKey) return;
    teamDetailsMeta.textContent = competitionText ? `${competitionText} | Error` : "Error";
    teamDetailsContent.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
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
  const key = String(marketId || "").trim();

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
  rollingWindowCount = 10;
  showTrendCharts = false;
  detailsMainTab = "xgd";
  recentTeamView = "home";
  statsVenueGamesShownCount = 0;
  statsVenueGamesShownAuto = true;
  statsAllGamesShownCount = 0;
  statsAllGamesShownAuto = true;
  statsTeamView = "home";
  statsGeneralTeamView = "home";
  hcPerfTeamView = "home";
  activeXgdViewId = null;
  lastXgdPayload = null;
  hcPerfLoadingMarketId = null;
  const scoreMeta = game.is_historical ? ` | FT ${String(game.scoreline || "-")}` : "";
  detailsMeta.textContent = `${game.competition} | ${formatGameKickoffLocalDateTime(game)}${scoreMeta}`;

  const cachedPayload = xgdPayloadByMarket.get(key);
  if (cachedPayload && typeof cachedPayload === "object") {
    renderXgd(cachedPayload);
    const updatedMainTableMetrics = applyCalculatedXgdToMainTable(marketId, cachedPayload);
    if (updatedMainTableMetrics) {
      if (activeTab === "saved") {
        renderSavedGames();
      } else {
        renderCurrentDay();
      }
    }
    return;
  }

  linesContainer.innerHTML = "<p>Calculating xGD...</p>";

  try {
    const res = await fetch(`/api/game-xgd?market_id=${encodeURIComponent(marketId)}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load xGD");
    xgdPayloadByMarket.set(key, payload || {});
    renderXgd(payload);
    const updatedMainTableMetrics = applyCalculatedXgdToMainTable(marketId, payload);
    if (updatedMainTableMetrics) {
      if (activeTab === "saved") {
        renderSavedGames();
      } else {
        renderCurrentDay();
      }
    }
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

function closeGameDetailsPanel(clearSelection = true) {
  detailsPanel.classList.add("hidden");
  if (!clearSelection) return;
  if (!selectedMarketId) return;
  selectedMarketId = null;
  if (activeTab === "saved") {
    renderSavedGames();
  } else if (activeTab === "games") {
    renderCurrentDay();
  }
}

async function requestAppExit() {
  if (!(exitAppBtn instanceof HTMLButtonElement)) return;
  const confirmed = window.confirm("Exit the app now? This will stop the local server.");
  if (!confirmed) return;

  const originalText = exitAppBtn.textContent;
  exitAppBtn.disabled = true;
  exitAppBtn.textContent = "Exiting...";
  statusText.textContent = "Stopping app...";

  try {
    const res = await fetch("/api/exit-app", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to exit app");
    statusText.textContent = "App shutdown requested. This page will disconnect shortly.";
  } catch (err) {
    exitAppBtn.disabled = false;
    exitAppBtn.textContent = originalText || "Exit App";
    statusText.textContent = String(err.message || err);
  }
}

refreshBtn.addEventListener("click", () => loadGames());
if (exitAppBtn instanceof HTMLButtonElement) {
  exitAppBtn.addEventListener("click", () => {
    requestAppExit();
  });
}
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
if (teamsTabBtn instanceof HTMLButtonElement) {
  teamsTabBtn.addEventListener("click", () => {
    setActiveTab("teams");
  });
}
manualMappingTabBtn.addEventListener("click", () => {
  setActiveTab("mapping");
  loadManualMappings();
});
if (teamsRefreshBtn instanceof HTMLButtonElement) {
  teamsRefreshBtn.addEventListener("click", () => {
    loadTeamsDirectory();
  });
}
if (teamsSearchInput instanceof HTMLInputElement) {
  teamsSearchInput.addEventListener("input", () => {
    teamsDirectorySearchQuery = String(teamsSearchInput.value || "");
    if (activeTab === "teams") {
      renderTeamsDirectory();
    }
  });
}
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
if (mappingSaveSelectedBtn instanceof HTMLButtonElement) {
  mappingSaveSelectedBtn.addEventListener("click", () => {
    saveSelectedManualTeamMappings();
  });
}
if (mappingClearSelectedBtn instanceof HTMLButtonElement) {
  mappingClearSelectedBtn.addEventListener("click", () => {
    clearAllTeamMappingSelections();
    updateTeamMappingBatchButtons();
    if (lastManualMappingPayload) {
      renderManualMappingSections(lastManualMappingPayload);
    }
  });
}
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
    closeTeamDetailsPanel();
  }
});
sortModeBtn.addEventListener("change", () => {
  sortMode = normalizeGameSortMode(sortModeBtn.value);
  updateSortButtonLabel();
  renderCurrentDay();
});
if (savedSortModeBtn instanceof HTMLSelectElement) {
  savedSortModeBtn.addEventListener("change", () => {
    savedSortMode = normalizeGameSortMode(savedSortModeBtn.value);
    updateSortButtonLabel();
    if (activeTab === "saved") {
      renderSavedGames();
    }
  });
}
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
if (dayPickerInput instanceof HTMLInputElement) {
  dayPickerInput.addEventListener("change", () => {
    const targetIso = String(dayPickerInput.value || "").trim();
    updateDayPickerCount(targetIso);
    void jumpToDayIso(targetIso);
  });
}
closeDetails.addEventListener("click", () => {
  closeGameDetailsPanel(true);
});
if (teamDetailsCloseBtn instanceof HTMLButtonElement) {
  teamDetailsCloseBtn.addEventListener("click", () => {
    closeTeamDetailsPanel();
  });
}
if (teamDetailsCompetitionSelect instanceof HTMLSelectElement) {
  teamDetailsCompetitionSelect.addEventListener("change", () => {
    const nextCompetition = String(teamDetailsCompetitionSelect.value || "").trim();
    if (!teamDetailsTeam) return;
    openTeamPage(teamDetailsTeam, nextCompetition || null);
  });
}
if (saveGameBtn instanceof HTMLButtonElement) {
  saveGameBtn.addEventListener("click", () => {
    toggleSelectedGameSaved();
  });
}

xgPushThreshold = getStoredXgPushThreshold();
xgdHcHighlightEnabled = false;
showGamesWithoutHandicap = getStoredShowGamesWithoutHandicap();
if (xgThresholdInput instanceof HTMLInputElement) {
  xgThresholdInput.value = formatXgPushThresholdForInput(xgPushThreshold);
  xgThresholdInput.addEventListener("change", () => {
    applyGlobalXgPushThreshold(xgThresholdInput.value);
  });
  xgThresholdInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    applyGlobalXgPushThreshold(xgThresholdInput.value);
    xgThresholdInput.blur();
  });
}
updateXgdHcHighlightToggleButton();
if (xgdHcHighlightToggleBtn instanceof HTMLButtonElement) {
  xgdHcHighlightToggleBtn.addEventListener("click", () => {
    setXgdHcHighlightEnabled(!xgdHcHighlightEnabled);
  });
}
updateNoHandicapGamesToggleButton();
if (noHandicapGamesToggleBtn instanceof HTMLButtonElement) {
  noHandicapGamesToggleBtn.addEventListener("click", () => {
    setShowGamesWithoutHandicap(!showGamesWithoutHandicap);
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
updateTeamMappingBatchButtons();
setMappingSubTab("teams");
setActiveTab("games");
setGamesMode("upcoming", false);
loadGames();
setInterval(() => {
  loadGames();
}, AUTO_REFRESH_MS);
