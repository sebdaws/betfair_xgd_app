const calendarView = document.getElementById("calendarView");
const statusText = document.getElementById("statusText");
const refreshBtn = document.getElementById("refreshBtn");
const hardRefreshXgdBtn = document.getElementById("hardRefreshXgdBtn");
const exitAppBtn = document.getElementById("exitAppBtn");
const settingsOpenBtn = document.getElementById("settingsOpenBtn");
const settingsCloseBtn = document.getElementById("settingsCloseBtn");
const settingsSidebar = document.getElementById("settingsSidebar");
const settingsOverlay = document.getElementById("settingsOverlay");
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
const modellingPricesTabBtn = document.getElementById("modellingPricesTabBtn");
const teamHcRankingsTabBtn = document.getElementById("teamHcRankingsTabBtn");
const teamsTabBtn = document.getElementById("teamsTabBtn");
const matchupTabBtn = document.getElementById("matchupTabBtn");
const manualMappingTabBtn = document.getElementById("manualMappingTabBtn");
const gamesTabPane = document.getElementById("gamesTabPane");
const gamesPaneTabs = document.getElementById("gamesPaneTabs");
const gamesMainSubTabBtn = document.getElementById("gamesMainSubTabBtn");
const savedGamesSubTabBtn = document.getElementById("savedGamesSubTabBtn");
const gamesDayToolbar = document.getElementById("gamesDayToolbar");
const savedGamesPane = document.getElementById("savedGamesPane");
const savedGamesView = document.getElementById("savedGamesView");
const teamHcRankingsTabPane = document.getElementById("teamHcRankingsTabPane");
const teamHcRankingsView = document.getElementById("teamHcRankingsView");
const teamsTabPane = document.getElementById("teamsTabPane");
const teamsView = document.getElementById("teamsView");
const teamsSearchInput = document.getElementById("teamsSearchInput");
const teamsRefreshBtn = document.getElementById("teamsRefreshBtn");
const matchupTabPane = document.getElementById("matchupTabPane");
const matchupHomeInput = document.getElementById("matchupHomeInput");
const matchupAwayInput = document.getElementById("matchupAwayInput");
const matchupCompetitionSelect = document.getElementById("matchupCompetitionSelect");
const matchupRunBtn = document.getElementById("matchupRunBtn");
const matchupTeamsList = document.getElementById("matchupTeamsList");
const matchupView = document.getElementById("matchupView");
const teamHcRankingsGeneralTabBtn = document.getElementById("teamHcRankingsGeneralTabBtn");
const teamHcRankingsHomeTabBtn = document.getElementById("teamHcRankingsHomeTabBtn");
const teamHcRankingsAwayTabBtn = document.getElementById("teamHcRankingsAwayTabBtn");
const teamHcRankingsSortSelect = document.getElementById("teamHcRankingsSortSelect");
const teamHcRankingsCompetitionSelect = document.getElementById("teamHcRankingsCompetitionSelect");
const teamHcRankingsSeasonSelect = document.getElementById("teamHcRankingsSeasonSelect");
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
const detailsXgMetricModeToggleBtn = document.getElementById("detailsXgMetricModeToggleBtn");
const teamDetailsPanel = document.getElementById("teamDetailsPanel");
const teamDetailsTitle = document.getElementById("teamDetailsTitle");
const teamDetailsMeta = document.getElementById("teamDetailsMeta");
const teamDetailsContent = document.getElementById("teamDetailsContent");
const teamDetailsCloseBtn = document.getElementById("teamDetailsCloseBtn");
const teamDetailsXgMetricModeToggleBtn = document.getElementById("teamDetailsXgMetricModeToggleBtn");
const teamDetailsSeasonSelect = document.getElementById("teamDetailsSeasonSelect");
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
const xgMetricModeToggleBtn = document.getElementById("xgMetricModeToggleBtn");
const xgdHcHighlightToggleBtn = document.getElementById("xgdHcHighlightToggleBtn");
const noHandicapGamesToggleBtn = document.getElementById("noHandicapGamesToggleBtn");
const themeToggleBtn = document.getElementById("themeToggleBtn");
const modelEdgeLabel = document.getElementById("modelEdgeLabel");
const modelEdgeInput = document.getElementById("modelEdgeInput");

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
const TRENDLINE_DEGREE_DEFAULT = 1;
const TRENDLINE_DEGREE_MAX = 10;
let trendlineDegree = TRENDLINE_DEGREE_DEFAULT;
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
let pricingTotalsExpanded = false;
let pricingHandicapExpanded = false;
let pricingBetBuilderLeg1Market = "handicap";
let pricingBetBuilderLeg1HandicapSide = "home";
let pricingBetBuilderLeg1HandicapLine = "-0.5";
let pricingBetBuilderLeg1TotalSide = "over";
let pricingBetBuilderLeg1TotalLine = "2.5";
let pricingBetBuilderLeg1BttsSide = "yes";
let pricingBetBuilderLeg2Market = "total";
let pricingBetBuilderLeg2HandicapSide = "away";
let pricingBetBuilderLeg2HandicapLine = "0.5";
let pricingBetBuilderLeg2TotalSide = "over";
let pricingBetBuilderLeg2TotalLine = "2.5";
let pricingBetBuilderLeg2BttsSide = "yes";
let pricingBetBuilderEdge = 0.1;
let pricingPeriod = "season";
const hcPerfPayloadByMarket = new Map();
const xgdPayloadByMarket = new Map();
const teamPagePayloadByKey = new Map();
const gamesPayloadByModeAndView = new Map();
const gamesPayloadPrefetchInFlight = new Set();
const gameXgdPrefetchInFlight = new Set();
const teamPagePrefetchInFlight = new Set();
let hcPerfLoadingMarketId = null;
let hcPerfRescanInFlight = false;
let lastXgdPayload = null;
let activeXgdViewId = null;
let detailsMainTab = "xgd";
let activeTab = "games";
let activeGamesPaneView = "main";
let savedDays = [];
let savedGamesLoaded = false;
let savedGamesLoading = false;
let savedGamesErrorText = "";
let savedGamesCount = 0;
let savedMarketIds = new Set();
let teamHcRankingsLeagues = [];
let teamHcRankingsRows = [];
let teamHcRankingsSeasons = [];
let teamHcRankingsSeasonsByCompetition = {};
let teamHcRankingsLoading = false;
let teamHcRankingsLoaded = false;
let teamHcRankingsErrorText = "";
let teamHcRankingsVenueMode = "overall";
let teamHcRankingsSortMetric = "result";
let selectedTeamHcRankingsCompetition = "";
let selectedTeamHcRankingsSeason = "";
let teamsDirectoryRows = [];
let teamsDirectoryLoaded = false;
let teamsDirectoryLoading = false;
let teamsDirectoryErrorText = "";
let teamsDirectorySearchQuery = "";
let matchupPayload = null;
let matchupLoading = false;
let matchupErrorText = "";
let mappingSubTab = "teams";
let lastManualMappingPayload = null;
let teamMappingSearchBetfair = "";
let teamMappingSearchSavedTeams = "";
const teamMappingBatchSelections = new Set();
const teamMappingBatchDrafts = new Map();
const competitionMappingBatchSelections = new Set();
const competitionMappingBatchDrafts = new Map();
const historicalDayCalcInFlight = new Set();
const historicalDayAutoCalcAttempted = new Set();
let teamHcPerfDetailLoadingKey = "";
let teamHcPerfDetailTeam = "";
let teamHcPerfDetailCompetition = "";
let teamHcPerfDetailRows = [];
let teamDetailsLoadingKey = "";
let gameXgdLoadingKey = "";
let teamDetailsTeam = "";
let teamDetailsCompetition = "";
let teamDetailsSeason = "";
let teamDetailsMainTab = "xg";
let teamDetailsPayload = null;
const TEAM_DETAILS_XG_GAMES_DEFAULT = 9999; // effectively "all games"
const TEAM_DETAILS_XG_ROLLING_DEFAULT = 10;
let teamDetailsXgGamesShownCount = TEAM_DETAILS_XG_GAMES_DEFAULT;
let teamDetailsXgRollingWindowCount = TEAM_DETAILS_XG_ROLLING_DEFAULT;
let teamDetailsTrendlineDegree = TRENDLINE_DEGREE_DEFAULT;
const DEFAULT_XG_PUSH_THRESHOLD = 0.1;
const MIN_XG_PUSH_THRESHOLD = 0.0;
const MAX_XG_PUSH_THRESHOLD = 5.0;
const XG_METRIC_MODE_STORAGE_KEY = "xgd_metric_mode";
const XG_PUSH_THRESHOLD_STORAGE_KEY = "xgd_hc_xg_threshold";
const XGD_HC_HIGHLIGHT_ENABLED_STORAGE_KEY = "xgd_hc_highlight_enabled";
const SHOW_GAMES_WITHOUT_HC_STORAGE_KEY = "show_games_without_hc_pricing";
const MODELLING_PRICE_EDGE_STORAGE_KEY = "modelling_price_edge";
const THEME_STORAGE_KEY = "xgd_theme";
const PRICING_PERIOD_STORAGE_KEY = "pricing_period";
const DEFAULT_MODELLING_PRICE_EDGE = 0.0;
const MIN_MODELLING_PRICE_EDGE = -0.95;
const MAX_MODELLING_PRICE_EDGE = 5.0;
let xgMetricMode = "xg";
let xgPushThreshold = DEFAULT_XG_PUSH_THRESHOLD;
let xgdHcHighlightEnabled = false;
let showGamesWithoutHandicap = true;
let modellingPriceEdge = DEFAULT_MODELLING_PRICE_EDGE;
let colorTheme = "light";
const AUTO_REFRESH_MS = 2 * 60 * 1000;
const MAPPING_UNMATCHED_TEAM_RENDER_LIMIT = 250;
const MAPPING_SAVED_TEAM_RENDER_LIMIT = 400;
const MAPPING_UNMATCHED_COMPETITION_RENDER_LIMIT = 200;
const MAPPING_SAVED_COMPETITION_RENDER_LIMIT = 400;
const API_REQUEST_TIMEOUT_MS = 45000;

function getStoredColorTheme() {
  const stored = localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
    return "dark";
  }
  return "light";
}

function updateThemeToggleButton() {
  if (!(themeToggleBtn instanceof HTMLButtonElement)) return;
  const isDark = colorTheme === "dark";
  themeToggleBtn.textContent = isDark ? "Theme: Dark" : "Theme: Light";
  themeToggleBtn.setAttribute("aria-pressed", isDark ? "true" : "false");
  themeToggleBtn.title = isDark ? "Switch to light mode" : "Switch to dark mode";
}

function applyColorTheme(nextTheme, persist = true) {
  colorTheme = nextTheme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = colorTheme;
  if (persist) {
    localStorage.setItem(THEME_STORAGE_KEY, colorTheme);
  }
  updateThemeToggleButton();
}

function setSettingsSidebarOpen(isOpen) {
  if (!(settingsSidebar instanceof HTMLElement) || !(settingsOverlay instanceof HTMLElement)) return;
  settingsSidebar.classList.toggle("hidden", !isOpen);
  settingsOverlay.classList.toggle("hidden", !isOpen);
  settingsOverlay.setAttribute("aria-hidden", isOpen ? "false" : "true");
  if (settingsOpenBtn instanceof HTMLButtonElement) {
    settingsOpenBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
  }
  document.body.classList.toggle("settings-sidebar-open", isOpen);
  if (isOpen && settingsCloseBtn instanceof HTMLButtonElement) {
    settingsCloseBtn.focus({ preventScroll: true });
  }
}

function closeSettingsSidebar() {
  setSettingsSidebarOpen(false);
  if (settingsOpenBtn instanceof HTMLButtonElement) {
    settingsOpenBtn.focus({ preventScroll: true });
  }
}

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

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = API_REQUEST_TIMEOUT_MS) {
  const timeout = Math.max(1000, Number(timeoutMs) || API_REQUEST_TIMEOUT_MS);
  const controller = new AbortController();
  const timerId = window.setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    const payload = await parseApiResponse(res);
    return { res, payload };
  } catch (err) {
    if (err && err.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeout / 1000)}s`);
    }
    throw err;
  } finally {
    window.clearTimeout(timerId);
  }
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

function normalizeModellingPriceEdge(value, fallback = DEFAULT_MODELLING_PRICE_EDGE) {
  const text = String(value ?? "").trim().replace(",", ".");
  const parsed = Number(text);
  const fallbackNum = Number(fallback);
  const fallbackSafe = Number.isFinite(fallbackNum)
    ? Math.max(MIN_MODELLING_PRICE_EDGE, Math.min(MAX_MODELLING_PRICE_EDGE, fallbackNum))
    : DEFAULT_MODELLING_PRICE_EDGE;
  if (!Number.isFinite(parsed)) return fallbackSafe;
  return Math.max(MIN_MODELLING_PRICE_EDGE, Math.min(MAX_MODELLING_PRICE_EDGE, parsed));
}

function formatModellingPriceEdgeForInput(value) {
  const normalized = normalizeModellingPriceEdge(value, DEFAULT_MODELLING_PRICE_EDGE);
  const rounded = Math.round(normalized * 10000) / 10000;
  return String(rounded);
}

function formatModellingPriceEdgeForLabel(value) {
  const normalized = normalizeModellingPriceEdge(value, DEFAULT_MODELLING_PRICE_EDGE);
  const pct = (normalized * 100).toFixed(2);
  return `${normalized >= 0 ? "+" : ""}${pct}%`;
}

function normalizeXgMetricMode(value, fallback = "xg") {
  const fallbackMode = String(fallback || "").trim().toLowerCase() === "npxg" ? "npxg" : "xg";
  const text = String(value ?? "").trim().toLowerCase();
  if (!text) return fallbackMode;
  return text === "npxg" ? "npxg" : "xg";
}

function getXgMetricModeLabel(value) {
  return normalizeXgMetricMode(value, "xg") === "npxg" ? "NPxG" : "xG";
}

function getStoredXgMetricMode() {
  try {
    const raw = window.localStorage.getItem(XG_METRIC_MODE_STORAGE_KEY);
    if (raw == null) return "xg";
    return normalizeXgMetricMode(raw, "xg");
  } catch (_err) {
    return "xg";
  }
}

function persistXgMetricMode(value) {
  try {
    window.localStorage.setItem(XG_METRIC_MODE_STORAGE_KEY, normalizeXgMetricMode(value, "xg"));
  } catch (_err) {
    // Ignore storage failures.
  }
}

function getCurrentXgMetricMode() {
  return normalizeXgMetricMode(xgMetricMode, "xg");
}

function getAlternateXgMetricMode(modeValue = getCurrentXgMetricMode()) {
  return normalizeXgMetricMode(modeValue, "xg") === "npxg" ? "xg" : "npxg";
}

function buildModeScopedCacheKey(rawKey, modeValue = getCurrentXgMetricMode()) {
  return `${normalizeXgMetricMode(modeValue, "xg")}::${String(rawKey || "").trim()}`;
}

function getGamesPayloadCacheKey(targetGamesMode = gamesMode, modeValue = getCurrentXgMetricMode()) {
  const modeKey = normalizeXgMetricMode(modeValue, "xg");
  const gamesModeKey = String(targetGamesMode || "").trim().toLowerCase() === "historical"
    ? "historical"
    : "upcoming";
  return `${modeKey}::${gamesModeKey}`;
}

function updateXgMetricModeToggleButton() {
  const mode = getCurrentXgMetricMode();
  const isNpxg = mode === "npxg";
  if (xgMetricModeToggleBtn instanceof HTMLButtonElement) {
    xgMetricModeToggleBtn.textContent = `xG Mode: ${isNpxg ? "NPxG" : "xG"}`;
    xgMetricModeToggleBtn.classList.toggle("is-off", isNpxg);
    xgMetricModeToggleBtn.setAttribute("aria-pressed", isNpxg ? "true" : "false");
    xgMetricModeToggleBtn.title = isNpxg
      ? "Using Non-penalty xG for xGD calculations"
      : "Using xG for xGD calculations";
  }
  const detailToggleButtons = [detailsXgMetricModeToggleBtn, teamDetailsXgMetricModeToggleBtn];
  for (const button of detailToggleButtons) {
    if (!(button instanceof HTMLButtonElement)) continue;
    button.textContent = `xGD Mode: ${isNpxg ? "NPxGD" : "xGD"}`;
    button.classList.toggle("is-off", isNpxg);
    button.setAttribute("aria-pressed", isNpxg ? "true" : "false");
    button.title = isNpxg
      ? "Using Non-penalty xG for xGD calculations"
      : "Using xG for xGD calculations";
  }
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

function getStoredModellingPriceEdge() {
  try {
    const raw = window.localStorage.getItem(MODELLING_PRICE_EDGE_STORAGE_KEY);
    if (raw == null) return DEFAULT_MODELLING_PRICE_EDGE;
    return normalizeModellingPriceEdge(raw, DEFAULT_MODELLING_PRICE_EDGE);
  } catch (_err) {
    return DEFAULT_MODELLING_PRICE_EDGE;
  }
}

function persistModellingPriceEdge(value) {
  try {
    window.localStorage.setItem(
      MODELLING_PRICE_EDGE_STORAGE_KEY,
      formatModellingPriceEdgeForInput(value)
    );
  } catch (_err) {
    // Ignore storage failures.
  }
}

function getCurrentModellingPriceEdge() {
  return normalizeModellingPriceEdge(modellingPriceEdge, DEFAULT_MODELLING_PRICE_EDGE);
}

function shouldHighlightModelPriceVsBetfair(betfairValue, modelValue, edgeValue = getCurrentModellingPriceEdge()) {
  const betfairOdds = toMetricNumberOrNull(betfairValue);
  const modelOdds = toMetricNumberOrNull(modelValue);
  if (betfairOdds == null || modelOdds == null) return false;
  if (betfairOdds <= 0 || modelOdds <= 0) return false;
  const edge = normalizeModellingPriceEdge(edgeValue, DEFAULT_MODELLING_PRICE_EDGE);
  const adjustedModelOdds = modelOdds * (1 + edge);
  if (!Number.isFinite(adjustedModelOdds) || adjustedModelOdds <= 0) return false;
  return adjustedModelOdds < (betfairOdds - 1e-9);
}

function applyGlobalModellingPriceEdge(nextValue, options = {}) {
  const silent = Boolean(options?.silent);
  const normalized = normalizeModellingPriceEdge(nextValue, getCurrentModellingPriceEdge());
  const changed = Math.abs(normalized - getCurrentModellingPriceEdge()) > 1e-9;
  modellingPriceEdge = normalized;
  persistModellingPriceEdge(normalized);
  if (modelEdgeInput instanceof HTMLInputElement) {
    modelEdgeInput.value = formatModellingPriceEdgeForInput(normalized);
  }
  if (!changed) return;
  if (activeTab === "modelling") {
    renderCurrentDay();
  }
  if (!silent) {
    statusText.textContent = `Model pricing edge set to ${formatModellingPriceEdgeForLabel(normalized)} for highlight logic`;
  }
}

function updateModellingEdgeControlsVisibility() {
  const showEdgeControls = activeTab === "modelling";
  if (modelEdgeLabel) {
    modelEdgeLabel.classList.toggle("hidden", !showEdgeControls);
  }
  if (modelEdgeInput instanceof HTMLInputElement) {
    modelEdgeInput.classList.toggle("hidden", !showEdgeControls);
  }
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

const PRICING_PERIOD_OPTIONS = [
  { key: "season", label: "Season", shortLabel: "S" },
  { key: "last5", label: "Last 5", shortLabel: "L5" },
  { key: "last3", label: "Last 3", shortLabel: "L3" },
];

function normalizePricingPeriod(value) {
  const text = String(value ?? "").trim().toLowerCase().replace(/[\s_-]+/g, "");
  if (text === "season" || text === "s") return "season";
  if (text === "last5" || text === "l5" || text === "5") return "last5";
  if (text === "last3" || text === "l3" || text === "3") return "last3";
  return "season";
}

function getPricingPeriodOption(value = pricingPeriod) {
  const key = normalizePricingPeriod(value);
  return PRICING_PERIOD_OPTIONS.find((option) => option.key === key) || PRICING_PERIOD_OPTIONS[0];
}

function getPricingPeriodLabel(value = pricingPeriod) {
  return getPricingPeriodOption(value).label;
}

function getPricingPeriodShortLabel(value = pricingPeriod) {
  return getPricingPeriodOption(value).shortLabel;
}

function loadStoredPricingPeriod() {
  try {
    return normalizePricingPeriod(localStorage.getItem(PRICING_PERIOD_STORAGE_KEY));
  } catch (err) {
    return "season";
  }
}

function persistPricingPeriod(value) {
  try {
    localStorage.setItem(PRICING_PERIOD_STORAGE_KEY, normalizePricingPeriod(value));
  } catch (err) {
    // Storage can be unavailable in private contexts; the in-memory state still works.
  }
}

function buildPricingPeriodControlHtml() {
  return `
    <div class="pricing-period-control" role="group" aria-label="Pricing period">
      ${PRICING_PERIOD_OPTIONS.map((option) => `
        <button
          type="button"
          class="pricing-period-btn ${pricingPeriod === option.key ? "active" : ""}"
          data-pricing-period="${escapeHtml(option.key)}"
          aria-pressed="${pricingPeriod === option.key ? "true" : "false"}"
        >${escapeHtml(option.label)}</button>
      `).join("")}
    </div>
  `;
}

function bindPricingPeriodButtons(container) {
  if (!(container instanceof Element)) return;
  const buttons = container.querySelectorAll(".pricing-period-btn");
  for (const button of buttons) {
    if (!(button instanceof HTMLButtonElement)) continue;
    button.addEventListener("click", () => {
      applyPricingPeriod(button.dataset.pricingPeriod);
    });
  }
}

function createPricingPeriodControlElement() {
  const wrapper = document.createElement("div");
  wrapper.innerHTML = buildPricingPeriodControlHtml().trim();
  const element = wrapper.firstElementChild;
  bindPricingPeriodButtons(element);
  return element;
}

function applyPricingPeriod(value, options = {}) {
  const silent = Boolean(options?.silent);
  const rerender = options?.rerender !== false;
  const nextPeriod = normalizePricingPeriod(value);
  const changed = nextPeriod !== pricingPeriod;
  pricingPeriod = nextPeriod;
  persistPricingPeriod(nextPeriod);
  if (!changed && rerender) return;

  if (rerender) {
    if (isGamesCalendarTab()) {
      renderCurrentDay();
    }
    if (lastXgdPayload && detailsPanel && !detailsPanel.classList.contains("hidden")) {
      renderXgd(lastXgdPayload);
    }
    if (activeTab === "matchup" && matchupPayload) {
      renderMatchupPage();
    }
  }
  if (!silent && changed && statusText) {
    statusText.textContent = `Pricing period set to ${getPricingPeriodLabel(nextPeriod)}`;
  }
}

function hasVisibleHandicapPricing(game) {
  const hasTextValue = (value) => {
    const text = String(value ?? "").trim();
    return text !== "" && text !== "-";
  };
  return hasTextValue(game?.mainline) && hasTextValue(game?.home_price) && hasTextValue(game?.away_price);
}

function isGamesCalendarTab(tabName = activeTab) {
  const normalized = String(tabName || "").trim().toLowerCase();
  return normalized === "games" || normalized === "modelling";
}

function isSavedGamesViewActive() {
  return isGamesCalendarTab() && activeGamesPaneView === "saved";
}

function updateGamesPaneViewVisibility() {
  const gamesPaneActive = isGamesCalendarTab();
  const savedActive = gamesPaneActive && activeGamesPaneView === "saved";
  if (gamesPaneTabs) {
    gamesPaneTabs.classList.toggle("hidden", !gamesPaneActive);
  }
  if (gamesMainSubTabBtn instanceof HTMLButtonElement) {
    gamesMainSubTabBtn.textContent = activeTab === "modelling" ? "Model Pricing" : "Games";
    gamesMainSubTabBtn.classList.toggle("active", gamesPaneActive && !savedActive);
  }
  if (savedGamesSubTabBtn instanceof HTMLButtonElement) {
    savedGamesSubTabBtn.classList.toggle("active", savedActive);
  }
  if (gamesDayToolbar) {
    gamesDayToolbar.classList.toggle("hidden", savedActive);
  }
  if (tableControls) {
    tableControls.classList.toggle("hidden", savedActive);
  }
  if (calendarView) {
    calendarView.classList.toggle("hidden", savedActive);
  }
  if (savedGamesPane) {
    savedGamesPane.classList.toggle("hidden", !savedActive);
  }
}

function setGamesPaneView(viewName) {
  const nextView = String(viewName || "").trim().toLowerCase() === "saved" ? "saved" : "main";
  const changed = nextView !== activeGamesPaneView;
  activeGamesPaneView = nextView;
  updateGamesPaneViewVisibility();
  if (changed) {
    closeGameDetailsPanel(true);
  }
  if (isSavedGamesViewActive()) {
    loadSavedGames({ silent: savedGamesLoaded });
  } else if (isGamesCalendarTab()) {
    renderCurrentDay();
  }
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
  if (isGamesCalendarTab()) {
    renderCurrentDay();
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

  if (isGamesCalendarTab()) {
    renderCurrentDay();
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

  if (isGamesCalendarTab()) {
    renderCurrentDay();
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

async function applyGlobalXgMetricMode(nextValue, options = {}) {
  const silent = Boolean(options?.silent);
  const normalized = normalizeXgMetricMode(nextValue, getCurrentXgMetricMode());
  const changed = normalized !== getCurrentXgMetricMode();
  xgMetricMode = normalized;
  persistXgMetricMode(normalized);
  updateXgMetricModeToggleButton();
  if (!changed) return;

  const loadOk = await loadGames({ useCache: true });
  if (loadOk && selectedMarketId && !detailsPanel.classList.contains("hidden")) {
    void loadGameXgd(selectedMarketId);
  }
  if (teamDetailsTeam && teamDetailsPanel && !teamDetailsPanel.classList.contains("hidden")) {
    void openTeamPage(
      teamDetailsTeam,
      teamDetailsCompetition || null,
      teamDetailsSeason || null
    );
  }
  if (activeTab === "matchup" && matchupPayload) {
    void loadMatchup();
  }
  if (!silent) {
    statusText.textContent = `xGD metric mode set to ${getXgMetricModeLabel(normalized)}`;
  }
}

function setActiveTab(tabName) {
  const tabRaw = String(tabName || "").trim().toLowerCase();
  const previousActiveTab = activeTab;
  if (tabRaw === "modelling" || tabRaw === "modelling-prices" || tabRaw === "modelling_prices") {
    activeTab = "modelling";
  } else if (tabRaw === "rankings") {
    activeTab = "rankings";
  } else if (tabRaw === "teams") {
    activeTab = "teams";
  } else if (tabRaw === "matchup") {
    activeTab = "matchup";
  } else if (tabRaw === "mapping") {
    activeTab = "mapping";
  } else {
    activeTab = "games";
  }
  const gamesActive = activeTab === "games";
  const modellingActive = activeTab === "modelling";
  const gamesCalendarActive = gamesActive || modellingActive;
  const rankingsActive = activeTab === "rankings";
  const teamsActive = activeTab === "teams";
  const matchupActive = activeTab === "matchup";
  const mappingActive = activeTab === "mapping";
  if (gamesCalendarActive && previousActiveTab !== activeTab) {
    activeGamesPaneView = "main";
  }
  gamesTabBtn.classList.toggle("active", gamesActive);
  if (modellingPricesTabBtn instanceof HTMLButtonElement) {
    modellingPricesTabBtn.classList.toggle("active", modellingActive);
  }
  if (teamHcRankingsTabBtn instanceof HTMLButtonElement) {
    teamHcRankingsTabBtn.classList.toggle("active", rankingsActive);
  }
  if (teamsTabBtn instanceof HTMLButtonElement) {
    teamsTabBtn.classList.toggle("active", teamsActive);
  }
  if (matchupTabBtn instanceof HTMLButtonElement) {
    matchupTabBtn.classList.toggle("active", matchupActive);
  }
  manualMappingTabBtn.classList.toggle("active", mappingActive);
  gamesTabPane.classList.toggle("hidden", !gamesCalendarActive);
  updateGamesPaneViewVisibility();
  if (teamHcRankingsTabPane) {
    teamHcRankingsTabPane.classList.toggle("hidden", !rankingsActive);
  }
  if (teamsTabPane) {
    teamsTabPane.classList.toggle("hidden", !teamsActive);
  }
  if (matchupTabPane) {
    matchupTabPane.classList.toggle("hidden", !matchupActive);
  }
  manualMappingTabPane.classList.toggle("hidden", !mappingActive);
  updateModellingEdgeControlsVisibility();
  if (!gamesCalendarActive) {
    closeGameDetailsPanel(true);
  }
  if (activeTab !== "rankings" && teamHcPerfPanel) {
    teamHcPerfPanel.classList.add("hidden");
  }
  if (mappingActive) {
    setMappingSubTab(mappingSubTab);
  }
  if (gamesCalendarActive) {
    renderCurrentDay();
  }
  if (isSavedGamesViewActive() && !savedGamesLoading) {
    loadSavedGames({ silent: savedGamesLoaded });
  }
  if (rankingsActive && !teamHcRankingsLoading) {
    loadTeamHcRankings({ silent: teamHcRankingsLoaded });
  }
  if (teamsActive && !teamsDirectoryLoading) {
    loadTeamsDirectory({ silent: teamsDirectoryLoaded });
  }
  if (matchupActive) {
    populateMatchupOptions();
    renderMatchupPage();
    if (!teamsDirectoryLoaded && !teamsDirectoryLoading) {
      loadTeamsDirectory({ silent: true });
    }
  }
}

function setMappingSubTab(tabName) {
  mappingSubTab = tabName === "competitions" ? "competitions" : "teams";
  const teamsActive = mappingSubTab === "teams";
  teamMappingsSubTabBtn.classList.toggle("active", teamsActive);
  competitionMappingsSubTabBtn.classList.toggle("active", !teamsActive);
  teamMappingsPane.classList.toggle("hidden", !teamsActive);
  competitionMappingsPane.classList.toggle("hidden", teamsActive);
  updateTeamMappingBatchButtons();
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

function normalizeCompetitionMappingRawName(rawName) {
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

function setCompetitionMappingDraft(rawName, sofaName) {
  const rawKey = normalizeCompetitionMappingRawName(rawName);
  if (!rawKey) return;
  const sofaValue = String(sofaName || "").trim();
  if (!sofaValue) {
    competitionMappingBatchDrafts.delete(rawKey);
    return;
  }
  competitionMappingBatchDrafts.set(rawKey, sofaValue);
}

function clearCompetitionMappingSelection(rawName) {
  const rawKey = normalizeCompetitionMappingRawName(rawName);
  if (!rawKey) return;
  competitionMappingBatchSelections.delete(rawKey);
  competitionMappingBatchDrafts.delete(rawKey);
}

function clearAllCompetitionMappingSelections() {
  competitionMappingBatchSelections.clear();
  competitionMappingBatchDrafts.clear();
}

function updateTeamMappingBatchButtons() {
  const competitionMode = mappingSubTab === "competitions";
  const selectedCount = competitionMode
    ? competitionMappingBatchSelections.size
    : teamMappingBatchSelections.size;
  if (mappingSaveSelectedBtn instanceof HTMLButtonElement) {
    mappingSaveSelectedBtn.disabled = selectedCount <= 0;
    if (competitionMode) {
      mappingSaveSelectedBtn.textContent = selectedCount > 0
        ? `Save Selected Competition Mappings (${selectedCount})`
        : "Save Selected Competition Mappings";
    } else {
      mappingSaveSelectedBtn.textContent = selectedCount > 0
        ? `Save Selected Team Mappings (${selectedCount})`
        : "Save Selected Team Mappings";
    }
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

function getCompetitionMappingLookupByRaw(payload) {
  const map = new Map();
  const mappings = Array.isArray(payload?.competition_mappings) ? payload.competition_mappings : [];
  for (const row of mappings) {
    const rawName = normalizeCompetitionMappingRawName(row?.raw_name);
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
  const activeCompetitionRawNames = new Set(
    [...competitionMappings, ...unmatchedCompetitions]
      .map((row) => normalizeCompetitionMappingRawName(row?.raw_name))
      .filter((rawName) => !!rawName)
  );
  for (const rawName of Array.from(competitionMappingBatchSelections)) {
    if (!activeCompetitionRawNames.has(rawName)) {
      competitionMappingBatchSelections.delete(rawName);
    }
  }
  for (const rawName of Array.from(competitionMappingBatchDrafts.keys())) {
    if (!activeCompetitionRawNames.has(rawName)) {
      competitionMappingBatchDrafts.delete(rawName);
    }
  }
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
  const sourceDbPath = String(payload?.source_db_path || payload?.sofascore_db_path || "").trim();
  const sourceDbLabel = sourceDbPath
    ? sourceDbPath.split("/").filter(Boolean).slice(-2).join("/")
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
    (sourceDbLabel ? ` | DB: ${sourceDbLabel}` : "");

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
          <th>Database Team</th>
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
            mappingStatus.textContent = "Pick a Database team from the dropdown suggestions before saving.";
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
        <input type="search" class="mapping-search-input" data-search="saved-team-mappings" placeholder="Type Betfair or Database team..." />
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
            <th>Database Team</th>
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
              mappingStatus.textContent = "Select a valid Database team before saving.";
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
    const competitionDatalistId = "mappingSofaCompetitionOptionsUnmatched";
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
          <th>Database Competition</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const row of visibleUnmatchedCompetitions) {
      const tr = document.createElement("tr");
      const rawName = String(row.raw_name || "");
      const rawKey = normalizeCompetitionMappingRawName(rawName);
      const existing = competitionMappingsByRawName.get(rawName);
      const existingSofaName = String(existing?.sofa_name || "").trim();
      const existingSofaNameLower = existingSofaName.toLowerCase();
      const draftValue = String(competitionMappingBatchDrafts.get(rawKey) || "").trim();
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
      if (input && draftValue) {
        input.value = draftValue;
      }
      const resolveCompetitionName = (value) => {
        const inputValue = String(value || "").trim();
        const inputValueLower = inputValue.toLowerCase();
        return availableSofaCompetitionLookup.get(inputValueLower)
          || (inputValueLower && inputValueLower === existingSofaNameLower ? existingSofaName : "");
      };
      const syncCompetitionBatchSelectionForRow = (autoSelect = false) => {
        const sofaName = resolveCompetitionName(input?.value);
        if (sofaName) {
          setCompetitionMappingDraft(rawKey, sofaName);
          if (autoSelect) {
            competitionMappingBatchSelections.add(rawKey);
          }
        } else if (!competitionMappingBatchSelections.has(rawKey)) {
          competitionMappingBatchDrafts.delete(rawKey);
        }
        if (!sofaName && competitionMappingBatchSelections.has(rawKey)) {
          competitionMappingBatchSelections.delete(rawKey);
        }
      };
      if (input) {
        input.addEventListener("input", () => {
          syncCompetitionBatchSelectionForRow(true);
          updateTeamMappingBatchButtons();
        });
      }
      const saveBtn = tr.querySelector(".mapping-save-btn");
      if (saveBtn && input) {
        saveBtn.addEventListener("click", async () => {
          const sofaName = resolveCompetitionName(input.value);
          if (!sofaName) {
            mappingStatus.textContent = "Select a Database competition before saving.";
            return;
          }
          clearCompetitionMappingSelection(rawKey);
          updateTeamMappingBatchButtons();
          await upsertManualCompetitionMapping(rawName, sofaName);
        });
      }
      syncCompetitionBatchSelectionForRow();
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
    const competitionDatalistId = "mappingSofaCompetitionOptionsSaved";
    const competitionDatalist = document.createElement("datalist");
    competitionDatalist.id = competitionDatalistId;
    competitionDatalist.innerHTML = availableSofaCompetitions
      .map((competition) => `<option value="${escapeHtml(competition)}"></option>`)
      .join("");
    savedCompetitionMappingsContainer.appendChild(competitionDatalist);

    const visibleCompetitionMappings = competitionMappings.slice(0, MAPPING_SAVED_COMPETITION_RENDER_LIMIT);
    const table = document.createElement("table");
    table.className = "mapping-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Betfair Competition</th>
          <th>Database Competition</th>
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
      const rawKey = normalizeCompetitionMappingRawName(rawName);
      const sofaName = String(row.sofa_name || "").trim();
      const sofaNameLower = sofaName.toLowerCase();
      const draftValue = String(competitionMappingBatchDrafts.get(rawKey) || "").trim();
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
          <input type="text" class="mapping-team-input" list="${competitionDatalistId}" value="${escapeHtml(sofaName)}" />
        </td>
        <td>${escapeHtml(typeLabel)}</td>
        <td>${actionHtml}</td>
      `;
      const input = tr.querySelector(".mapping-team-input");
      const resolveSavedCompetitionName = () => {
        const selectedRaw = String(input?.value || "").trim();
        const selectedLower = selectedRaw.toLowerCase();
        return availableSofaCompetitionLookup.get(selectedLower)
          || (selectedLower && selectedLower === sofaNameLower ? sofaName : "");
      };
      if (input && draftValue) {
        input.value = draftValue;
      }
      const syncSavedCompetitionSelectionForRow = () => {
        const resolvedName = resolveSavedCompetitionName();
        if (resolvedName) {
          setCompetitionMappingDraft(rawKey, resolvedName);
        } else if (!competitionMappingBatchSelections.has(rawKey)) {
          competitionMappingBatchDrafts.delete(rawKey);
        }
        if (!resolvedName && competitionMappingBatchSelections.has(rawKey)) {
          competitionMappingBatchSelections.delete(rawKey);
        }
      };
      if (input) {
        input.addEventListener("input", () => {
          syncSavedCompetitionSelectionForRow();
          updateTeamMappingBatchButtons();
        });
      }
      const saveBtn = tr.querySelector(".mapping-save-btn");
      if (saveBtn && input) {
        saveBtn.addEventListener("click", async () => {
          const selectedCompetitionName = resolveSavedCompetitionName();
          if (!selectedCompetitionName) {
            mappingStatus.textContent = "Select a valid Database competition before saving.";
            return;
          }
          clearCompetitionMappingSelection(rawKey);
          updateTeamMappingBatchButtons();
          await upsertManualCompetitionMapping(rawName, selectedCompetitionName);
        });
      }
      const deleteBtn = tr.querySelector(".mapping-delete-btn");
      if (deleteBtn) {
        deleteBtn.addEventListener("click", async () => {
          clearCompetitionMappingSelection(rawKey);
          updateTeamMappingBatchButtons();
          await deleteManualCompetitionMapping(rawName);
        });
      }
      syncSavedCompetitionSelectionForRow();
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
    mappingStatus.textContent = "Some selected rows do not have a valid Database team.";
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

async function saveSelectedManualCompetitionMappings() {
  const selectedRawNames = Array.from(competitionMappingBatchSelections)
    .map((rawName) => normalizeCompetitionMappingRawName(rawName))
    .filter((rawName) => !!rawName);
  if (!selectedRawNames.length) {
    mappingStatus.textContent = "Select at least one competition mapping row first.";
    return;
  }

  const currentSofaByRaw = getCompetitionMappingLookupByRaw(lastManualMappingPayload);
  const bulkMappings = [];
  const missingMappings = [];

  for (const rawName of selectedRawNames) {
    let sofaName = String(competitionMappingBatchDrafts.get(rawName) || "").trim();
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
    mappingStatus.textContent = "No valid competition mappings were selected to save.";
    return;
  }
  if (missingMappings.length) {
    mappingStatus.textContent = "Some selected rows do not have a valid Database competition.";
    return;
  }

  try {
    mappingStatus.textContent = `Saving ${bulkMappings.length} competition mappings...`;
    const res = await fetch("/api/manual-competition-mappings/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mappings: bulkMappings }),
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to save selected competition mappings");
    clearAllCompetitionMappingSelections();
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
  clearCompetitionMappingSelection(rawName);
  updateTeamMappingBatchButtons();
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
  clearCompetitionMappingSelection(rawName);
  updateTeamMappingBatchButtons();
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

function clampTrendlineDegree(value, maxDegree = TRENDLINE_DEGREE_MAX) {
  const parsed = Number.parseInt(String(value || ""), 10);
  const safeMax = Number.isFinite(maxDegree) ? Math.max(1, Math.floor(maxDegree)) : TRENDLINE_DEGREE_MAX;
  if (Number.isNaN(parsed)) return TRENDLINE_DEGREE_DEFAULT;
  return Math.max(1, Math.min(safeMax, parsed));
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

function getPeriodMetricValue(row, periodKey, metricSuffix, options = {}) {
  const key = normalizePricingPeriod(periodKey);
  const rawKey = `${key}_${metricSuffix}_raw`;
  const displayKey = `${key}_${metricSuffix}`;
  if (options?.preferRaw !== false) {
    const rawValue = toMetricNumberOrNull(row?.[rawKey]);
    if (rawValue != null) return rawValue;
  }
  return toMetricNumberOrNull(row?.[displayKey]);
}

function getPeriodRowsSummary(periodRows, periodKey) {
  const key = normalizePricingPeriod(periodKey);
  const rows = Array.isArray(periodRows) ? periodRows : [];
  const row = rows.find((candidate) => normalizePeriodForMainTable(candidate?.period) === key);
  if (!row || typeof row !== "object") return null;
  return {
    home_xg: toMetricNumberOrNull(row.home_xg),
    away_xg: toMetricNumberOrNull(row.away_xg),
    total_xg: toMetricNumberOrNull(row.total_xg),
    xgd: toMetricNumberOrNull(row.xgd),
    xgd_perf: toMetricNumberOrNull(row.xgd_perf),
    strength: toMetricNumberOrNull(row.strength),
  };
}

function getPricingSummaryForPeriod(view, payload, periodKey = pricingPeriod) {
  const key = normalizePricingPeriod(periodKey);
  const viewSummary = getPeriodRowsSummary(view?.period_rows, key);
  if (viewSummary) return viewSummary;
  const payloadSummary = getPeriodRowsSummary(payload?.period_rows, key);
  if (payloadSummary) return payloadSummary;
  if (key === "season") {
    if (view?.summary && typeof view.summary === "object") return view.summary;
    if (payload?.summary && typeof payload.summary === "object") return payload.summary;
  }
  return null;
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
    season_home_xg: pickMetric("season", "home_xg"),
    last5_home_xg: pickMetric("last5", "home_xg"),
    last3_home_xg: pickMetric("last3", "home_xg"),
    season_away_xg: pickMetric("season", "away_xg"),
    last5_away_xg: pickMetric("last5", "away_xg"),
    last3_away_xg: pickMetric("last3", "away_xg"),
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
    "season_home_xg",
    "last5_home_xg",
    "last3_home_xg",
    "season_away_xg",
    "last5_away_xg",
    "last3_away_xg",
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
    for (const periodKey of ["season", "last5", "last3"]) {
      const homeMetricKey = `${periodKey}_home_xg`;
      const awayMetricKey = `${periodKey}_away_xg`;
      if (metrics[homeMetricKey] != null) {
        game[`${homeMetricKey}_raw`] = metrics[homeMetricKey];
      }
      if (metrics[awayMetricKey] != null) {
        game[`${awayMetricKey}_raw`] = metrics[awayMetricKey];
      }
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
      xgota_minus_ga: hasXgotData ? averageOf((row) => {
        const ga = toMetricNumber(row?.GA);
        const xgotAgainst = toMetricNumber(row?.xGoTA);
        if (ga == null || xgotAgainst == null) return null;
        return xgotAgainst - ga;
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
    { label: "xGoTA-GA (Keeping)", key: "xgota_minus_ga" },
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

function getHistoricalHcBetSide(periodValues, handicap, threshold) {
  const cls = getMainTableXgdHcHighlightClass(periodValues, handicap, threshold);
  if (cls === "xgd-hc-highlight-green") return "home";
  if (cls === "xgd-hc-highlight-red") return "away";
  return null;
}

function getHistoricalGoalsBetSide(goalLine, minPeriodValues, maxPeriodValues, threshold) {
  const highlight = getMainTableGoalsBandHighlight(goalLine, minPeriodValues, maxPeriodValues, threshold);
  if (highlight.goalOverClass === "xgd-hc-highlight-green") return "over";
  if (highlight.goalUnderClass === "xgd-hc-highlight-red") return "under";
  return null;
}

function settleAsianReturnFromDelta(delta) {
  const verdict = classifyResultHandicapDelta(delta);
  if (verdict === "win") return 1;
  if (verdict === "half_win") return 0.5;
  if (verdict === "push") return 0;
  if (verdict === "half_loss") return -0.5;
  if (verdict === "loss") return -1;
  return null;
}

function settleThresholdReturnFromDelta(delta, threshold) {
  if (!Number.isFinite(delta)) return null;
  const deltaNum = Number(delta);
  const thresholdNum = Number.isFinite(Number(threshold)) ? Math.max(0, Number(threshold)) : 0;
  if (deltaNum > thresholdNum) return 1;
  if (deltaNum < (-thresholdNum)) return -1;
  return 0;
}

function formatBetReturnValue(value) {
  if (!Number.isFinite(value)) return "-";
  const num = Number(value);
  if (Math.abs(num) <= 1e-9) return "+0";
  if (Math.abs(Math.abs(num) - 0.5) <= 1e-9) return num > 0 ? "+0.5" : "-0.5";
  return num > 0 ? "+1" : "-1";
}

function formatXgBetReturnValue(value) {
  if (!Number.isFinite(value)) return "-";
  const num = Number(value);
  if (Math.abs(num) <= 1e-9) return "0";
  return num > 0 ? "+1" : "-1";
}

function formatBetReturnTotalValue(value) {
  if (!Number.isFinite(value)) return "-";
  const num = Number(value);
  if (Math.abs(num) <= 1e-9) return "+0";
  const rounded = Math.round(num * 10) / 10;
  const isWhole = Math.abs(rounded - Math.round(rounded)) <= 1e-9;
  const text = isWhole ? String(Math.round(rounded)) : rounded.toFixed(1);
  return rounded > 0 ? `+${text}` : text;
}

function getBetReturnClass(value) {
  if (!Number.isFinite(value)) return "";
  const num = Number(value);
  if (Math.abs(num) <= 1e-9) return "hcperf-pick-neutral";
  if (Math.abs(Math.abs(num) - 0.5) <= 1e-9) {
    return num > 0 ? "hcperf-pick-relevant-half" : "hcperf-pick-other-half";
  }
  return num > 0 ? "hcperf-pick-relevant" : "hcperf-pick-other";
}

function buildBetResultStackCell(resultValue, xgValue) {
  if (!Number.isFinite(resultValue) && !Number.isFinite(xgValue)) {
    return "-";
  }
  const resultClass = getBetReturnClass(resultValue);
  const xgClass = getBetReturnClass(xgValue);
  return `
    <div class="metric-stack">
      <div class="metric-stack-row">
        <span class="metric-stack-label">R</span>
        <span class="metric-stack-value ${resultClass}">${escapeHtml(formatBetReturnValue(resultValue))}</span>
      </div>
      <div class="metric-stack-row">
        <span class="metric-stack-label">xG</span>
        <span class="metric-stack-value ${xgClass}">${escapeHtml(formatXgBetReturnValue(xgValue))}</span>
      </div>
    </div>
  `;
}

function buildBetResultSummaryStackCell(resultValue, xgValue) {
  if (!Number.isFinite(resultValue) && !Number.isFinite(xgValue)) {
    return "-";
  }
  const resultClass = getBetReturnClass(resultValue);
  const xgClass = getBetReturnClass(xgValue);
  return `
    <div class="metric-stack bet-summary-stack">
      <div class="metric-stack-row">
        <span class="metric-stack-label">R</span>
        <span class="metric-stack-value ${resultClass}">${escapeHtml(formatBetReturnTotalValue(resultValue))}</span>
      </div>
      <div class="metric-stack-row">
        <span class="metric-stack-label">xG</span>
        <span class="metric-stack-value ${xgClass}">${escapeHtml(formatBetReturnTotalValue(xgValue))}</span>
      </div>
    </div>
  `;
}

function calculateHistoricalBetReturns(game, options = {}) {
  const threshold = Number.isFinite(Number(options?.threshold))
    ? Number(options.threshold)
    : getCurrentXgPushThreshold();
  const handicap = toMetricNumberOrNull(game?.mainline);
  const goalLine = toMetricNumberOrNull(game?.goal_mainline);
  const homeGoals = toMetricNumberOrNull(game?.home_goals);
  const awayGoals = toMetricNumberOrNull(game?.away_goals);
  const homeXgActual = toMetricNumberOrNull(game?.home_xg_actual);
  const awayXgActual = toMetricNumberOrNull(game?.away_xg_actual);
  const xgdPeriodValues = [
    toMetricNumberOrNull(game?.season_strength),
    toMetricNumberOrNull(game?.last5_strength),
    toMetricNumberOrNull(game?.last3_strength),
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
  const hcBetSide = getHistoricalHcBetSide(xgdPeriodValues, handicap, threshold);
  const goalsBetSide = getHistoricalGoalsBetSide(goalLine, minPeriodValues, maxPeriodValues, threshold);
  let hcBetResult = null;
  let hcBetResultXg = null;
  let goalsBetResult = null;
  let goalsBetResultXg = null;

  if (hcBetSide && handicap != null && homeGoals != null && awayGoals != null) {
    const homeHandicapDeltaResult = (homeGoals + handicap) - awayGoals;
    const betDeltaResult = hcBetSide === "home" ? homeHandicapDeltaResult : -homeHandicapDeltaResult;
    hcBetResult = settleAsianReturnFromDelta(betDeltaResult);
  }
  if (hcBetSide && handicap != null && homeXgActual != null && awayXgActual != null) {
    const homeHandicapDeltaXg = (homeXgActual + handicap) - awayXgActual;
    const betDeltaXg = hcBetSide === "home" ? homeHandicapDeltaXg : -homeHandicapDeltaXg;
    hcBetResultXg = settleThresholdReturnFromDelta(betDeltaXg, threshold);
  }

  if (goalsBetSide && goalLine != null && homeGoals != null && awayGoals != null) {
    const totalGoals = homeGoals + awayGoals;
    const betDeltaResult = goalsBetSide === "over" ? (totalGoals - goalLine) : (goalLine - totalGoals);
    goalsBetResult = settleAsianReturnFromDelta(betDeltaResult);
  }
  if (goalsBetSide && goalLine != null && homeXgActual != null && awayXgActual != null) {
    const totalXg = homeXgActual + awayXgActual;
    const betDeltaXg = goalsBetSide === "over" ? (totalXg - goalLine) : (goalLine - totalXg);
    goalsBetResultXg = settleThresholdReturnFromDelta(betDeltaXg, threshold);
  }

  return {
    hcBetResult,
    hcBetResultXg,
    goalsBetResult,
    goalsBetResultXg,
  };
}

function buildHistoricalBetSummaryRowHtml(games) {
  const safeGames = Array.isArray(games) ? games : [];
  const totals = safeGames.reduce(
    (acc, game) => {
      const returns = calculateHistoricalBetReturns(game, { threshold: getCurrentXgPushThreshold() });
      if (Number.isFinite(returns.hcBetResult)) {
        acc.hcResult += Number(returns.hcBetResult);
        acc.hcResultCount += 1;
      }
      if (Number.isFinite(returns.hcBetResultXg)) {
        acc.hcXg += Number(returns.hcBetResultXg);
        acc.hcXgCount += 1;
      }
      if (Number.isFinite(returns.goalsBetResult)) {
        acc.goalsResult += Number(returns.goalsBetResult);
        acc.goalsResultCount += 1;
      }
      if (Number.isFinite(returns.goalsBetResultXg)) {
        acc.goalsXg += Number(returns.goalsBetResultXg);
        acc.goalsXgCount += 1;
      }
      return acc;
    },
    {
      hcResult: 0,
      hcResultCount: 0,
      hcXg: 0,
      hcXgCount: 0,
      goalsResult: 0,
      goalsResultCount: 0,
      goalsXg: 0,
      goalsXgCount: 0,
    },
  );
  return `
    <tr class="historical-bet-summary-row">
      <td colspan="18"></td>
      <td class="metric-stack-cell">${buildBetResultSummaryStackCell(
    totals.hcResultCount ? totals.hcResult : null,
    totals.hcXgCount ? totals.hcXg : null,
  )}</td>
      <td class="metric-stack-cell">${buildBetResultSummaryStackCell(
    totals.goalsResultCount ? totals.goalsResult : null,
    totals.goalsXgCount ? totals.goalsXg : null,
  )}</td>
    </tr>
  `;
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

function normalizeRollingSeedRowsByVenue(rowsObj) {
  return {
    home: Array.isArray(rowsObj?.home_prev_season_rows) ? rowsObj.home_prev_season_rows : [],
    away: Array.isArray(rowsObj?.away_prev_season_rows) ? rowsObj.away_prev_season_rows : [],
  };
}

function mergeRollingSeedRowsByVenue(rowsByVenue) {
  const homeRows = Array.isArray(rowsByVenue?.home) ? rowsByVenue.home : [];
  const awayRows = Array.isArray(rowsByVenue?.away) ? rowsByVenue.away : [];
  return [...homeRows, ...awayRows].sort((a, b) =>
    String(b?.date_time || "").localeCompare(String(a?.date_time || ""))
  );
}

function buildMetricTrendPlotHtml(
  rows,
  title,
  relevantTeamName = "",
  rollingWindowGames = 10,
  options = {}
) {
  const headingPrefix = relevantTeamName ? `<strong>${escapeHtml(relevantTeamName)}</strong> - ` : "";
  const safeRows = Array.isArray(rows) ? rows : [];
  const rollingSeedRows = Array.isArray(options?.rollingSeedRows) ? options.rollingSeedRows : [];
  const requestedTrendlineDegree = clampTrendlineDegree(options?.trendlineDegree);
  const showTrendlineControl = Boolean(options?.showTrendlineControl);
  const trendlineControlClass = String(options?.trendlineControlClass || "").trim();
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
      const xg = toMetricNumber(row?.xG);
      const xga = toMetricNumber(row?.xGA);
      return {
        idx,
        sortTs: parseSortTs(row?.date_time),
        dateText: String(row?.date_time || "").trim(),
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

  const seedPoints = rollingSeedRows
    .map((row, idx) => {
      const xg = toMetricNumber(row?.xG);
      const xga = toMetricNumber(row?.xGA);
      return {
        idx,
        sortTs: parseSortTs(row?.date_time),
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
    const seedValues = seedPoints.map((point) => {
      const value = point[metricKey];
      return Number.isFinite(value) ? value : null;
    });
    const validCount = rawValues.reduce((count, value) => (value == null ? count : count + 1), 0);
    if (!validCount) return [];
    const hasSeedValues = seedValues.some((value) => value != null);

    return rawValues.map((_, index) => {
      const valuesForWindow = [];
      for (const value of seedValues) {
        if (value == null) continue;
        valuesForWindow.push(value);
      }
      for (let pos = 0; pos <= index; pos += 1) {
        const value = rawValues[pos];
        if (value == null) continue;
        valuesForWindow.push(value);
      }
      const windowValues = valuesForWindow.slice(-windowSize);
      if (!windowValues.length) return null;
      if (!hasSeedValues && index === 0) return null;
      let sum = 0;
      for (const value of windowValues) {
        sum += value;
      }
      return sum / windowValues.length;
    });
  };

  const solveLinearSystem = (matrix, vector) => {
    const n = Array.isArray(vector) ? vector.length : 0;
    if (!n || !Array.isArray(matrix) || matrix.length !== n) return null;
    const a = matrix.map((row) => (Array.isArray(row) ? row.slice() : []));
    const b = vector.slice();

    for (let col = 0; col < n; col += 1) {
      let pivotRow = col;
      let pivotAbs = Math.abs(a[col][col] || 0);
      for (let row = col + 1; row < n; row += 1) {
        const nextAbs = Math.abs(a[row][col] || 0);
        if (nextAbs > pivotAbs) {
          pivotAbs = nextAbs;
          pivotRow = row;
        }
      }
      if (!Number.isFinite(pivotAbs) || pivotAbs < 1e-12) {
        return null;
      }
      if (pivotRow !== col) {
        const tmpRow = a[col];
        a[col] = a[pivotRow];
        a[pivotRow] = tmpRow;
        const tmpValue = b[col];
        b[col] = b[pivotRow];
        b[pivotRow] = tmpValue;
      }
      const pivot = a[col][col];
      for (let j = col; j < n; j += 1) {
        a[col][j] /= pivot;
      }
      b[col] /= pivot;

      for (let row = 0; row < n; row += 1) {
        if (row === col) continue;
        const factor = a[row][col];
        if (!Number.isFinite(factor) || Math.abs(factor) < 1e-15) continue;
        for (let j = col; j < n; j += 1) {
          a[row][j] -= factor * a[col][j];
        }
        b[row] -= factor * b[col];
      }
    }
    if (b.some((value) => !Number.isFinite(value))) return null;
    return b;
  };

  const fitPolynomialSeries = (series, degree) => {
    const safeSeries = Array.isArray(series) ? series : [];
    const samples = [];
    for (let idx = 0; idx < safeSeries.length; idx += 1) {
      const value = safeSeries[idx];
      if (!Number.isFinite(value)) continue;
      samples.push({ x: idx, y: value });
    }
    if (samples.length < 2) {
      return { series: [], degreeUsed: 0 };
    }
    const maxDegree = Math.max(1, Math.min(clampTrendlineDegree(degree), samples.length - 1));

    for (let useDegree = maxDegree; useDegree >= 1; useDegree -= 1) {
      const dimension = useDegree + 1;
      const matrix = Array.from({ length: dimension }, () => Array(dimension).fill(0));
      const vector = Array(dimension).fill(0);
      for (let row = 0; row < dimension; row += 1) {
        for (let col = 0; col < dimension; col += 1) {
          let sum = 0;
          for (const sample of samples) {
            sum += sample.x ** (row + col);
          }
          matrix[row][col] = sum;
        }
        let rhs = 0;
        for (const sample of samples) {
          rhs += sample.y * (sample.x ** row);
        }
        vector[row] = rhs;
      }
      const coeffs = solveLinearSystem(matrix, vector);
      if (!Array.isArray(coeffs)) {
        continue;
      }
      const trendSeries = Array(safeSeries.length).fill(null);
      const firstIdx = samples[0].x;
      const lastIdx = samples[samples.length - 1].x;
      for (let idx = firstIdx; idx <= lastIdx; idx += 1) {
        let predicted = 0;
        for (let power = 0; power < coeffs.length; power += 1) {
          predicted += coeffs[power] * (idx ** power);
        }
        trendSeries[idx] = Number.isFinite(predicted) ? predicted : null;
      }
      return { series: trendSeries, degreeUsed: useDegree };
    }

    return { series: [], degreeUsed: 0 };
  };

  const xgSmoothed = buildSmoothedSeries("xg");
  const xgaSmoothed = buildSmoothedSeries("xga");
  const xgTrendFit = fitPolynomialSeries(xgSmoothed, requestedTrendlineDegree);
  const xgaTrendFit = fitPolynomialSeries(xgaSmoothed, requestedTrendlineDegree);
  const xgPolySeries = Array.isArray(xgTrendFit.series) ? xgTrendFit.series : [];
  const xgaPolySeries = Array.isArray(xgaTrendFit.series) ? xgaTrendFit.series : [];

  const plottedValues = [...xgSmoothed, ...xgaSmoothed, ...xgPolySeries, ...xgaPolySeries]
    .filter((value) => Number.isFinite(value));
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
    const safeSeries = Array.isArray(series) ? series : [];
    const parts = [];
    let drawMode = "M";
    for (let i = 0; i < safeSeries.length; i += 1) {
      const value = safeSeries[i];
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
  const xgRollingPath = buildTrendPathFromSeries(xgSmoothed);
  const xgaRollingPath = buildTrendPathFromSeries(xgaSmoothed);
  const xgPolyPath = buildTrendPathFromSeries(xgPolySeries);
  const xgaPolyPath = buildTrendPathFromSeries(xgaPolySeries);

  return `
    <section class="recent-team-block">
      <h4>${headingPrefix}${escapeHtml(title)}</h4>
      <div class="recent-table-wrap">
        <div class="trend-plot-wrap">
          <svg class="trend-plot" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(
            `${relevantTeamName || "Team"} rolling averages and polynomial trendlines for xG and xGA`
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
            ${xgaPolyPath
    ? `<path d="${xgaPolyPath}" fill="none" stroke="${xgaColor}" stroke-width="2" stroke-opacity="0.62" stroke-dasharray="7 5" />`
    : ""}
            ${xgPolyPath
    ? `<path d="${xgPolyPath}" fill="none" stroke="${xgColor}" stroke-width="2" stroke-opacity="0.62" stroke-dasharray="7 5" />`
    : ""}
            ${xgaRollingPath ? `<path d="${xgaRollingPath}" fill="none" stroke="${xgaColor}" stroke-width="2.5" />` : ""}
            ${xgRollingPath ? `<path d="${xgRollingPath}" fill="none" stroke="${xgColor}" stroke-width="2.5" />` : ""}
            <text x="${padLeft}" y="${height - 8}" text-anchor="start" class="trend-axis-text">${escapeHtml(firstLabel)}</text>
            <text x="${width - padRight}" y="${height - 8}" text-anchor="end" class="trend-axis-text">${escapeHtml(lastLabel)}</text>
          </svg>
          <div class="trend-legend">
            <span class="trend-legend-item"><i class="trend-swatch" style="--swatch-color:${xgColor};"></i>xG rolling</span>
            <span class="trend-legend-item"><i class="trend-swatch" style="--swatch-color:${xgaColor};"></i>xGA rolling</span>
            ${xgPolyPath
    ? `<span class="trend-legend-item"><i class="trend-swatch trend-swatch-dashed" style="--swatch-color:${xgColor};"></i>xG poly (d=${xgTrendFit.degreeUsed || requestedTrendlineDegree})</span>`
    : ""}
            ${xgaPolyPath
    ? `<span class="trend-legend-item"><i class="trend-swatch trend-swatch-dashed" style="--swatch-color:${xgaColor};"></i>xGA poly (d=${xgaTrendFit.degreeUsed || requestedTrendlineDegree})</span>`
    : ""}
          </div>
          ${showTrendlineControl
    ? `
          <div class="trend-controls">
            <label class="trend-control-label">
              Trendline degree
              <input class="trendline-degree-input ${escapeHtml(trendlineControlClass)}" type="number" min="1" max="${TRENDLINE_DEGREE_MAX}" step="1" value="${requestedTrendlineDegree}" />
            </label>
          </div>
          `
    : ""}
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

function buildShotsAveragesTableHtml(homeRows, awayRows, homeLabel, awayLabel, sampleSize = null) {
  const sampleLabel = sampleSize == null ? "All Previous Games" : `Last ${clampRecentMatchesCount(sampleSize)} Games`;
  const entries = [
    { label: homeLabel || "Home team", rows: limitStatsRows(homeRows || [], sampleSize) },
    { label: awayLabel || "Away team", rows: limitStatsRows(awayRows || [], sampleSize) },
  ];
  return `
    <section class="recent-team-block">
      <h4>Average Shot Output (${escapeHtml(sampleLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Team</th>
              <th>Shots For</th>
              <th>Shots Against</th>
              <th>On Target For</th>
              <th>On Target Against</th>
              <th>xG For</th>
              <th>xG Against</th>
              <th>xG/Shot</th>
              <th>xGA/ShotA</th>
              <th>xGoT For</th>
              <th>xGoT Against</th>
            </tr>
          </thead>
          <tbody>
            ${entries
              .map(
                (entry) => {
                  const shotsForTotal = sumMetric(entry.rows, "shots_for");
                  const shotsAgainstTotal = sumMetric(entry.rows, "shots_against");
                  const xgForTotal = sumMetric(entry.rows, "xG");
                  const xgAgainstTotal = sumMetric(entry.rows, "xGA");
                  const xgPerShot = (
                    Number.isFinite(xgForTotal)
                    && Number.isFinite(shotsForTotal)
                    && shotsForTotal > 0
                  )
                    ? (xgForTotal / shotsForTotal)
                    : null;
                  const xgaPerShotAgainst = (
                    Number.isFinite(xgAgainstTotal)
                    && Number.isFinite(shotsAgainstTotal)
                    && shotsAgainstTotal > 0
                  )
                    ? (xgAgainstTotal / shotsAgainstTotal)
                    : null;
                  return `
              <tr>
                <td><strong>${escapeHtml(entry.label)}</strong></td>
                <td>${formatMetricValue(averageMetric(entry.rows, "shots_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "shots_against"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "shots_on_target_for"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "shots_on_target_against"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "xG"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "xGA"), 2)}</td>
                <td>${formatMetricValue(xgPerShot, 3)}</td>
                <td>${formatMetricValue(xgaPerShotAgainst, 3)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "xGoT"), 2)}</td>
                <td>${formatMetricValue(averageMetric(entry.rows, "xGoTA"), 2)}</td>
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
              <th>Minutes</th>
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
                      <td>${formatMetricValue(minutes, 1)}</td>
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

function buildShotGamestateTableHtml(homeRows, awayRows, homeLabel, awayLabel, sampleSize = null) {
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
    { key: "shots_for", label: "Shots For", totalDecimals: 0 },
    { key: "shots_against", label: "Shots Against", totalDecimals: 0 },
    { key: "shots_on_target_for", label: "On Target For", totalDecimals: 0 },
    { key: "shots_on_target_against", label: "On Target Against", totalDecimals: 0 },
    { key: "xg_for", label: "xG For", totalDecimals: 2 },
    { key: "xg_against", label: "xG Against", totalDecimals: 2 },
  ];

  const toSafeNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const formatMetricByMode = (total, minutes, totalDecimals = 0) => {
    if (activeMode === "per90") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || minutes <= 0) return "-";
      return formatMetricValue((total / minutes) * 90, 2);
    }
    if (activeMode === "minperstat") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || total <= 0 || minutes <= 0) return "-";
      return formatMetricValue(minutes / total, 2);
    }
    return formatMetricValue(total, totalDecimals);
  };

  const renderTeamTable = (entry) => `
    <section class="recent-team-block">
      <h4>${escapeHtml(entry.label)} - Shot Gamestate Stats (${escapeHtml(sampleLabel)} | ${escapeHtml(modeLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Gamestate</th>
              <th>Time %</th>
              <th>Minutes</th>
              <th>Shots For</th>
              <th>Shots Against</th>
              <th>On Target For</th>
              <th>On Target Against</th>
              <th>xG For</th>
              <th>xG Against</th>
              <th>xG/Shot</th>
              <th>xGA/ShotA</th>
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
                    return formatMetricByMode(metricTotal, minutes, metricCol.totalDecimals);
                  });
                  const shotsForTotal = toSafeNumber(sumMetric(entry.rows, `shots_for_${state.key}`));
                  const shotsAgainstTotal = toSafeNumber(sumMetric(entry.rows, `shots_against_${state.key}`));
                  const xgForTotal = toSafeNumber(sumMetric(entry.rows, `xg_for_${state.key}`));
                  const xgAgainstTotal = toSafeNumber(sumMetric(entry.rows, `xg_against_${state.key}`));
                  const xgPerShot = shotsForTotal > 0 ? formatMetricValue(xgForTotal / shotsForTotal, 3) : "-";
                  const xgaPerShotAgainst = shotsAgainstTotal > 0 ? formatMetricValue(xgAgainstTotal / shotsAgainstTotal, 3) : "-";
                  return `
                    <tr>
                      <td>${escapeHtml(state.label)}</td>
                      <td>${pct == null ? "-" : `${formatMetricValue(pct, 1)}%`}</td>
                      <td>${formatMetricValue(minutes, 1)}</td>
                      <td>${metricValues[0]}</td>
                      <td>${metricValues[1]}</td>
                      <td>${metricValues[2]}</td>
                      <td>${metricValues[3]}</td>
                      <td>${metricValues[4]}</td>
                      <td>${metricValues[5]}</td>
                      <td>${xgPerShot}</td>
                      <td>${xgaPerShotAgainst}</td>
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
              <th>Minutes</th>
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
                    <td>${formatMetricValue(minutes, 1)}</td>
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

function buildSingleTeamShotGamestateTableHtml(rows, teamLabel, sampleSize = null) {
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
    { key: "shots_for", label: "Shots For", totalDecimals: 0 },
    { key: "shots_against", label: "Shots Against", totalDecimals: 0 },
    { key: "shots_on_target_for", label: "On Target For", totalDecimals: 0 },
    { key: "shots_on_target_against", label: "On Target Against", totalDecimals: 0 },
    { key: "xg_for", label: "xG For", totalDecimals: 2 },
    { key: "xg_against", label: "xG Against", totalDecimals: 2 },
  ];

  const toSafeNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const formatMetricByMode = (total, minutes, totalDecimals = 0) => {
    if (activeMode === "per90") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || minutes <= 0) return "-";
      return formatMetricValue((total / minutes) * 90, 2);
    }
    if (activeMode === "minperstat") {
      if (!Number.isFinite(total) || !Number.isFinite(minutes) || total <= 0 || minutes <= 0) return "-";
      return formatMetricValue(minutes / total, 2);
    }
    return formatMetricValue(total, totalDecimals);
  };

  const minutesByState = {};
  for (const state of states) {
    const key = `minutes_${state.key}`;
    minutesByState[state.key] = toSafeNumber(sumMetric(safeRows, key));
  }
  const totalMinutes = states.reduce((sum, state) => sum + minutesByState[state.key], 0);

  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} - Shot Gamestate Stats (${escapeHtml(sampleLabel)} | ${escapeHtml(modeLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Gamestate</th>
              <th>Time %</th>
              <th>Minutes</th>
              <th>Shots For</th>
              <th>Shots Against</th>
              <th>On Target For</th>
              <th>On Target Against</th>
              <th>xG For</th>
              <th>xG Against</th>
              <th>xG/Shot</th>
              <th>xGA/ShotA</th>
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
                  return formatMetricByMode(metricTotal, minutes, metricCol.totalDecimals);
                });
                const shotsForTotal = toSafeNumber(sumMetric(safeRows, `shots_for_${state.key}`));
                const shotsAgainstTotal = toSafeNumber(sumMetric(safeRows, `shots_against_${state.key}`));
                const xgForTotal = toSafeNumber(sumMetric(safeRows, `xg_for_${state.key}`));
                const xgAgainstTotal = toSafeNumber(sumMetric(safeRows, `xg_against_${state.key}`));
                const xgPerShot = shotsForTotal > 0 ? formatMetricValue(xgForTotal / shotsForTotal, 3) : "-";
                const xgaPerShotAgainst = shotsAgainstTotal > 0 ? formatMetricValue(xgAgainstTotal / shotsAgainstTotal, 3) : "-";
                return `
                  <tr>
                    <td>${escapeHtml(state.label)}</td>
                    <td>${pct == null ? "-" : `${formatMetricValue(pct, 1)}%`}</td>
                    <td>${formatMetricValue(minutes, 1)}</td>
                    <td>${metricValues[0]}</td>
                    <td>${metricValues[1]}</td>
                    <td>${metricValues[2]}</td>
                    <td>${metricValues[3]}</td>
                    <td>${metricValues[4]}</td>
                    <td>${metricValues[5]}</td>
                    <td>${xgPerShot}</td>
                    <td>${xgaPerShotAgainst}</td>
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

function buildShotsMatchesTableHtml(teamLabel, rows, sampleSize = null, titleOverride = "") {
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
              <th>Shots<span class="metric-suffix">H</span></th>
              <th>Shots<span class="metric-suffix">A</span></th>
              <th>OnT<span class="metric-suffix">H</span></th>
              <th>OnT<span class="metric-suffix">A</span></th>
              <th>xG<span class="metric-suffix">H</span></th>
              <th>xG<span class="metric-suffix">A</span></th>
              <th>xGoT<span class="metric-suffix">H</span></th>
              <th>xGoT<span class="metric-suffix">A</span></th>
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
                  const shotsHome = isHome ? row?.shots_for : row?.shots_against;
                  const shotsAway = isHome ? row?.shots_against : row?.shots_for;
                  const shotsOnTargetHome = isHome ? row?.shots_on_target_for : row?.shots_on_target_against;
                  const shotsOnTargetAway = isHome ? row?.shots_on_target_against : row?.shots_on_target_for;
                  const xgHome = isHome ? row?.xG : row?.xGA;
                  const xgAway = isHome ? row?.xGA : row?.xG;
                  const xgotHome = isHome ? row?.xGoT : row?.xGoTA;
                  const xgotAway = isHome ? row?.xGoTA : row?.xGoT;
                  return `
              <tr>
                <td>${escapeHtml(row?.date_time || "-")}</td>
                <td>${escapeHtml(row?.competition_name || "-")}</td>
                <td>${homeTeamCell}</td>
                <td>${awayTeamCell}</td>
                <td>${formatMetricValue(shotsHome, 2)}</td>
                <td>${formatMetricValue(shotsAway, 2)}</td>
                <td>${formatMetricValue(shotsOnTargetHome, 2)}</td>
                <td>${formatMetricValue(shotsOnTargetAway, 2)}</td>
                <td>${formatMetricValue(xgHome, 2)}</td>
                <td>${formatMetricValue(xgAway, 2)}</td>
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

function averagePair(valueA, valueB) {
  const a = Number(valueA);
  const b = Number(valueB);
  if (Number.isFinite(a) && Number.isFinite(b)) return (a + b) / 2;
  if (Number.isFinite(a)) return a;
  if (Number.isFinite(b)) return b;
  return null;
}

function clampProbability01(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num <= 0) return 0;
  if (num >= 1) return 1;
  return num;
}

function roundShotsForDistribution(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return 0;
  return Math.max(0, Math.round(num));
}

function buildBinomialGoalPmf(trials, probability) {
  const n = Math.max(0, Math.floor(Number(trials) || 0));
  const pRaw = clampProbability01(probability);
  const p = pRaw == null ? 0 : pRaw;
  const pmf = new Array(n + 1).fill(0);
  if (n === 0) {
    pmf[0] = 1;
    return pmf;
  }
  if (p <= 0) {
    pmf[0] = 1;
    return pmf;
  }
  if (p >= 1) {
    pmf[n] = 1;
    return pmf;
  }
  const q = 1 - p;
  pmf[0] = Math.pow(q, n);
  for (let goals = 0; goals < n; goals += 1) {
    const ratio = ((n - goals) / (goals + 1)) * (p / q);
    pmf[goals + 1] = pmf[goals] * ratio;
  }
  const totalProb = pmf.reduce((sum, value) => sum + value, 0);
  if (totalProb > 0) {
    for (let idx = 0; idx < pmf.length; idx += 1) {
      pmf[idx] = pmf[idx] / totalProb;
    }
  }
  return pmf;
}

function convolveDiscretePmf(leftPmf, rightPmf) {
  const left = Array.isArray(leftPmf) ? leftPmf : [];
  const right = Array.isArray(rightPmf) ? rightPmf : [];
  if (!left.length) return right.length ? [...right] : [1];
  if (!right.length) return [...left];
  const out = new Array(left.length + right.length - 1).fill(0);
  for (let i = 0; i < left.length; i += 1) {
    const lp = Number(left[i]);
    if (!Number.isFinite(lp) || lp <= 0) continue;
    for (let j = 0; j < right.length; j += 1) {
      const rp = Number(right[j]);
      if (!Number.isFinite(rp) || rp <= 0) continue;
      out[i + j] += lp * rp;
    }
  }
  return out;
}

function buildGoalDiffPmf(homeGoalPmf, awayGoalPmf) {
  const home = Array.isArray(homeGoalPmf) ? homeGoalPmf : [1];
  const away = Array.isArray(awayGoalPmf) ? awayGoalPmf : [1];
  const out = new Map();
  for (let homeGoals = 0; homeGoals < home.length; homeGoals += 1) {
    const pHome = Number(home[homeGoals]);
    if (!Number.isFinite(pHome) || pHome <= 0) continue;
    for (let awayGoals = 0; awayGoals < away.length; awayGoals += 1) {
      const pAway = Number(away[awayGoals]);
      if (!Number.isFinite(pAway) || pAway <= 0) continue;
      const diff = homeGoals - awayGoals;
      out.set(diff, (out.get(diff) || 0) + (pHome * pAway));
    }
  }
  return out;
}

function expectedValueFromPmf(pmf) {
  const safePmf = Array.isArray(pmf) ? pmf : [];
  let expected = 0;
  for (let idx = 0; idx < safePmf.length; idx += 1) {
    const prob = Number(safePmf[idx]);
    if (!Number.isFinite(prob) || prob <= 0) continue;
    expected += idx * prob;
  }
  return expected;
}

function expectedDiffFromPmf(diffPmf) {
  if (!(diffPmf instanceof Map)) return 0;
  let expected = 0;
  for (const [diff, probRaw] of diffPmf.entries()) {
    const prob = Number(probRaw);
    if (!Number.isFinite(prob) || prob <= 0) continue;
    expected += Number(diff) * prob;
  }
  return expected;
}

function computeMatchOddsFromGoalDiffPmf(goalDiffPmf) {
  if (!(goalDiffPmf instanceof Map)) {
    return {
      homeWinProb: null,
      drawProb: null,
      awayWinProb: null,
      homeWinOdds: null,
      drawOdds: null,
      awayWinOdds: null,
    };
  }
  let homeWinProb = 0;
  let drawProb = 0;
  let awayWinProb = 0;
  for (const [rawDiff, rawProb] of goalDiffPmf.entries()) {
    const diff = Number(rawDiff);
    const prob = Number(rawProb);
    if (!Number.isFinite(diff) || !Number.isFinite(prob) || prob <= 0) continue;
    if (diff > 0) homeWinProb += prob;
    else if (diff < 0) awayWinProb += prob;
    else drawProb += prob;
  }
  return {
    homeWinProb,
    drawProb,
    awayWinProb,
    homeWinOdds: homeWinProb > 0 ? (1 / homeWinProb) : null,
    drawOdds: drawProb > 0 ? (1 / drawProb) : null,
    awayWinOdds: awayWinProb > 0 ? (1 / awayWinProb) : null,
  };
}

function splitAsianLineComponents(lineValue) {
  const line = Number(lineValue);
  if (!Number.isFinite(line)) return [];
  const quarterUnits = Math.round(line * 4);
  const remainder = ((quarterUnits % 4) + 4) % 4;
  if (remainder === 1 || remainder === 3) {
    return [
      { line: line - 0.25, weight: 0.5 },
      { line: line + 0.25, weight: 0.5 },
    ];
  }
  return [{ line, weight: 1 }];
}

function computeAsianSideFairPrice(components, outcomeFn) {
  let weightedWin = 0;
  let weightedLoss = 0;
  let weightedPush = 0;
  for (const component of components) {
    const line = Number(component?.line);
    const weight = Number(component?.weight);
    if (!Number.isFinite(line) || !Number.isFinite(weight) || weight <= 0) continue;
    const outcomes = outcomeFn(line);
    const win = Number(outcomes?.win);
    const loss = Number(outcomes?.loss);
    const push = Number(outcomes?.push);
    weightedWin += weight * (Number.isFinite(win) ? win : 0);
    weightedLoss += weight * (Number.isFinite(loss) ? loss : 0);
    weightedPush += weight * (Number.isFinite(push) ? push : 0);
  }
  const fairOdds = weightedWin > 0 ? (1 + (weightedLoss / weightedWin)) : null;
  return {
    weightedWin,
    weightedLoss,
    weightedPush,
    fairOdds,
  };
}

function computeTotalSideOutcomes(totalGoalPmf, line, side) {
  const safePmf = Array.isArray(totalGoalPmf) ? totalGoalPmf : [];
  const targetLine = Number(line);
  const targetSide = side === "under" ? "under" : "over";
  let win = 0;
  let loss = 0;
  let push = 0;
  for (let totalGoals = 0; totalGoals < safePmf.length; totalGoals += 1) {
    const prob = Number(safePmf[totalGoals]);
    if (!Number.isFinite(prob) || prob <= 0) continue;
    if (targetSide === "over") {
      if (totalGoals > targetLine) win += prob;
      else if (totalGoals < targetLine) loss += prob;
      else push += prob;
    } else {
      if (totalGoals < targetLine) win += prob;
      else if (totalGoals > targetLine) loss += prob;
      else push += prob;
    }
  }
  return { win, loss, push };
}

function computeHandicapSideOutcomes(goalDiffPmf, line, side) {
  const targetLine = Number(line);
  const targetSide = side === "away" ? "away" : "home";
  let win = 0;
  let loss = 0;
  let push = 0;
  if (!(goalDiffPmf instanceof Map)) return { win, loss, push };
  for (const [rawDiff, rawProb] of goalDiffPmf.entries()) {
    const diff = Number(rawDiff);
    const prob = Number(rawProb);
    if (!Number.isFinite(diff) || !Number.isFinite(prob) || prob <= 0) continue;
    const adjusted = targetSide === "home"
      ? (diff + targetLine)
      : ((-diff) + targetLine);
    if (adjusted > 0) win += prob;
    else if (adjusted < 0) loss += prob;
    else push += prob;
  }
  return { win, loss, push };
}

function buildQuarterLineRange(startLine, endLine) {
  const startUnits = Math.round(Number(startLine) * 4);
  const endUnits = Math.round(Number(endLine) * 4);
  if (!Number.isFinite(startUnits) || !Number.isFinite(endUnits)) return [];
  const lo = Math.min(startUnits, endUnits);
  const hi = Math.max(startUnits, endUnits);
  const out = [];
  for (let units = lo; units <= hi; units += 1) {
    out.push(units / 4);
  }
  return out;
}

function formatQuarterLine(value, signed = false) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  const rounded = Math.round(num * 100) / 100;
  let text = "";
  if (Math.abs(rounded - Math.round(rounded)) < 1e-9) {
    text = `${Math.round(rounded)}.0`;
  } else if (Math.abs((rounded * 2) - Math.round(rounded * 2)) < 1e-9) {
    text = rounded.toFixed(1);
  } else {
    text = rounded.toFixed(2);
  }
  if (signed && rounded > 0) return `+${text}`;
  return text;
}

function isHalfLineValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return false;
  const quarterUnitsAbs = Math.abs(Math.round(num * 4));
  return (quarterUnitsAbs % 4) === 2;
}

function isStrictHalfLineValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return false;
  const doubled = num * 2;
  return (
    Math.abs(doubled - Math.round(doubled)) <= 1e-9
    && Math.abs(num - Math.round(num)) > 1e-9
  );
}

function parsePricingHalfLine(value) {
  const text = String(value ?? "").trim().replace(",", ".");
  if (!text) return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function formatHalfLineInputValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value ?? "");
  return num.toFixed(1);
}

function formatOddsValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return "-";
  if (num > 999) return "999+";
  return formatMetricValue(num, 2);
}

function findMainLineIndex(rows, getLeftOdds, getRightOdds) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) return -1;
  let bestIndex = -1;
  let bestScore = Infinity;
  for (let idx = 0; idx < safeRows.length; idx += 1) {
    const row = safeRows[idx];
    const leftOdds = Number(getLeftOdds(row));
    const rightOdds = Number(getRightOdds(row));
    if (!Number.isFinite(leftOdds) || !Number.isFinite(rightOdds)) continue;
    const score = Math.abs(leftOdds - 2.0) + Math.abs(rightOdds - 2.0);
    if (score < bestScore) {
      bestScore = score;
      bestIndex = idx;
    }
  }
  if (bestIndex >= 0) return bestIndex;
  return Math.floor(safeRows.length / 2);
}

function selectDefaultMainWindow(rows, mainIndex, eachSide = 2) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) return [];
  if (!Number.isFinite(mainIndex) || mainIndex < 0) {
    return safeRows.slice(0, Math.min(5, safeRows.length));
  }
  const targetCount = Math.min(safeRows.length, (eachSide * 2) + 1);
  let start = Math.max(0, mainIndex - eachSide);
  let end = Math.min(safeRows.length - 1, mainIndex + eachSide);
  let currentCount = (end - start) + 1;
  if (currentCount < targetCount) {
    const missing = targetCount - currentCount;
    const roomLeft = start;
    const roomRight = (safeRows.length - 1) - end;
    const shiftLeft = Math.min(roomLeft, missing);
    start -= shiftLeft;
    currentCount += shiftLeft;
    const stillMissing = targetCount - currentCount;
    const shiftRight = Math.min(roomRight, stillMissing);
    end += shiftRight;
  }
  return safeRows.slice(start, end + 1);
}

function calculateTeamSeasonShotProfile(rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const xgForAvg = averageMetric(safeRows, "xG");
  const xgAgainstAvg = averageMetric(safeRows, "xGA");
  const shotsForAvg = averageMetric(safeRows, "shots_for");
  const shotsAgainstAvg = averageMetric(safeRows, "shots_against");
  const xgForTotal = sumMetric(safeRows, "xG");
  const shotsForTotal = sumMetric(safeRows, "shots_for");
  const xgPerShotFor = (
    Number.isFinite(xgForTotal)
    && Number.isFinite(shotsForTotal)
    && shotsForTotal > 0
  )
    ? (xgForTotal / shotsForTotal)
    : null;
  return {
    games: safeRows.length,
    xgForAvg,
    xgAgainstAvg,
    shotsForAvg,
    shotsAgainstAvg,
    xgPerShotFor,
  };
}

function buildGamePricingModel(homeRows, awayRows, options = {}) {
  const homeProfile = calculateTeamSeasonShotProfile(homeRows);
  const awayProfile = calculateTeamSeasonShotProfile(awayRows);
  const overrideHomeXg = toMetricNumber(options?.projectedHomeXg);
  const overrideAwayXg = toMetricNumber(options?.projectedAwayXg);
  const requireProjectedXg = options?.requireProjectedXg === true;
  const avgHomeXg = averagePair(homeProfile.xgForAvg, awayProfile.xgAgainstAvg);
  const avgAwayXg = averagePair(awayProfile.xgForAvg, homeProfile.xgAgainstAvg);
  const predictedHomeXg = Number.isFinite(overrideHomeXg)
    ? overrideHomeXg
    : (requireProjectedXg ? null : avgHomeXg);
  const predictedAwayXg = Number.isFinite(overrideAwayXg)
    ? overrideAwayXg
    : (requireProjectedXg ? null : avgAwayXg);
  const averageProjectedHomeShots = averagePair(homeProfile.shotsForAvg, awayProfile.shotsAgainstAvg);
  const averageProjectedAwayShots = averagePair(awayProfile.shotsForAvg, homeProfile.shotsAgainstAvg);
  const predictedHomeShots = averageProjectedHomeShots;
  const predictedAwayShots = averageProjectedAwayShots;
  const homeGoalProbPerShot = (
    Number.isFinite(predictedHomeXg)
    && Number.isFinite(predictedHomeShots)
    && predictedHomeShots > 0
  )
    ? clampProbability01(predictedHomeXg / predictedHomeShots)
    : null;
  const awayGoalProbPerShot = (
    Number.isFinite(predictedAwayXg)
    && Number.isFinite(predictedAwayShots)
    && predictedAwayShots > 0
  )
    ? clampProbability01(predictedAwayXg / predictedAwayShots)
    : null;
  const homeShotTrials = roundShotsForDistribution(predictedHomeShots);
  const awayShotTrials = roundShotsForDistribution(predictedAwayShots);
  const canBuildDistributions = Number.isFinite(predictedHomeXg) && Number.isFinite(predictedAwayXg);

  if (!canBuildDistributions) {
    return {
      homeProfile,
      awayProfile,
      predictedHomeXg,
      predictedAwayXg,
      predictedHomeShots,
      predictedAwayShots,
      homeGoalProbPerShot,
      awayGoalProbPerShot,
      homeShotTrials,
      awayShotTrials,
      homeGoalPmf: [],
      awayGoalPmf: [],
      totalGoalPmf: [],
      goalDiffPmf: new Map(),
      matchOdds: {
        homeWinProb: null,
        drawProb: null,
        awayWinProb: null,
        homeWinOdds: null,
        drawOdds: null,
        awayWinOdds: null,
      },
      totalLines: [],
      handicapLines: [],
      warnings: [
        "Missing projected xG for one side, so pricing cannot be calculated yet.",
      ],
    };
  }

  const homeGoalPmf = buildPoissonGoalPmf(predictedHomeXg);
  const awayGoalPmf = buildPoissonGoalPmf(predictedAwayXg);
  const pricing = buildPricingLinesFromGoalPmfs(homeGoalPmf, awayGoalPmf, {
    expectedTotalGoals: predictedHomeXg + predictedAwayXg,
    expectedGoalDiff: predictedHomeXg - predictedAwayXg,
  });

  return {
    homeProfile,
    awayProfile,
    predictedHomeXg,
    predictedAwayXg,
    predictedHomeShots,
    predictedAwayShots,
    homeGoalProbPerShot,
    awayGoalProbPerShot,
    homeShotTrials,
    awayShotTrials,
    homeGoalPmf,
    awayGoalPmf,
    totalGoalPmf: pricing.totalGoalPmf,
    goalDiffPmf: pricing.goalDiffPmf,
    matchOdds: pricing.matchOdds,
    totalLines: pricing.totalLines,
    handicapLines: pricing.handicapLines,
    warnings: [],
  };
}

function buildPoissonGoalPmf(expectedGoals, options = {}) {
  const lambda = Number(expectedGoals);
  if (!Number.isFinite(lambda) || lambda < 0) return [];
  const maxGoalsInput = Number(options?.maxGoals);
  const dynamicMaxGoals = Number.isFinite(maxGoalsInput) && maxGoalsInput >= 1
    ? Math.floor(maxGoalsInput)
    : Math.max(12, Math.min(40, Math.ceil(lambda + (8 * Math.sqrt(lambda + 1)) + 4)));
  const pmf = new Array(dynamicMaxGoals + 1).fill(0);
  const p0 = Math.exp(-lambda);
  pmf[0] = p0;
  let running = p0;
  for (let goals = 1; goals <= dynamicMaxGoals; goals += 1) {
    const prev = Number(pmf[goals - 1]) || 0;
    const value = prev * (lambda / goals);
    pmf[goals] = value;
    running += value;
  }
  const tail = Math.max(0, 1 - running);
  pmf[dynamicMaxGoals] = (Number(pmf[dynamicMaxGoals]) || 0) + tail;
  return pmf;
}

function buildPricingLinesFromGoalPmfs(homeGoalPmf, awayGoalPmf, options = {}) {
  const totalGoalPmf = convolveDiscretePmf(homeGoalPmf, awayGoalPmf);
  const goalDiffPmf = buildGoalDiffPmf(homeGoalPmf, awayGoalPmf);
  const matchOdds = computeMatchOddsFromGoalDiffPmf(goalDiffPmf);
  const optionExpectedTotalGoals = Number(options?.expectedTotalGoals);
  const optionExpectedGoalDiff = Number(options?.expectedGoalDiff);
  const expectedTotalGoals = Number.isFinite(optionExpectedTotalGoals)
    ? Math.max(0, optionExpectedTotalGoals)
    : expectedValueFromPmf(totalGoalPmf);
  const expectedGoalDiff = Number.isFinite(optionExpectedGoalDiff)
    ? optionExpectedGoalDiff
    : expectedDiffFromPmf(goalDiffPmf);

  const totalUpperLine = Math.min(
    10.5,
    Math.max(6.5, (Math.ceil(expectedTotalGoals + 4) + 0.5))
  );
  const totalLines = buildQuarterLineRange(0.5, totalUpperLine).map((line) => {
    const components = splitAsianLineComponents(line);
    const over = computeAsianSideFairPrice(
      components,
      (componentLine) => computeTotalSideOutcomes(totalGoalPmf, componentLine, "over")
    );
    const under = computeAsianSideFairPrice(
      components,
      (componentLine) => computeTotalSideOutcomes(totalGoalPmf, componentLine, "under")
    );
    return { line, over, under };
  });

  const handicapAbs = Math.min(
    4.0,
    Math.max(2.0, Math.ceil(Math.abs(expectedGoalDiff) + 2))
  );
  const handicapLines = buildQuarterLineRange(-handicapAbs, handicapAbs).map((homeLine) => {
    const homeComponents = splitAsianLineComponents(homeLine);
    const awayLine = -homeLine;
    const awayComponents = splitAsianLineComponents(awayLine);
    const home = computeAsianSideFairPrice(
      homeComponents,
      (componentLine) => computeHandicapSideOutcomes(goalDiffPmf, componentLine, "home")
    );
    const away = computeAsianSideFairPrice(
      awayComponents,
      (componentLine) => computeHandicapSideOutcomes(goalDiffPmf, componentLine, "away")
    );
    return { homeLine, awayLine, home, away };
  });

  return {
    totalGoalPmf,
    goalDiffPmf,
    matchOdds,
    totalLines,
    handicapLines,
  };
}

function buildMainlinePricingFromProjectedXg(homeXgValue, awayXgValue) {
  const projectedHomeXg = Number(homeXgValue);
  const projectedAwayXg = Number(awayXgValue);
  if (!Number.isFinite(projectedHomeXg) || !Number.isFinite(projectedAwayXg)) {
    return null;
  }

  const homeGoalPmf = buildPoissonGoalPmf(projectedHomeXg);
  const awayGoalPmf = buildPoissonGoalPmf(projectedAwayXg);
  if (!homeGoalPmf.length || !awayGoalPmf.length) {
    return null;
  }

  const pricing = buildPricingLinesFromGoalPmfs(homeGoalPmf, awayGoalPmf, {
    expectedTotalGoals: projectedHomeXg + projectedAwayXg,
    expectedGoalDiff: projectedHomeXg - projectedAwayXg,
  });

  const totalMainIndex = findMainLineIndex(
    pricing.totalLines,
    (row) => row?.under?.fairOdds,
    (row) => row?.over?.fairOdds
  );
  const handicapMainIndex = findMainLineIndex(
    pricing.handicapLines,
    (row) => row?.home?.fairOdds,
    (row) => row?.away?.fairOdds,
  );

  return {
    projectedHomeXg,
    projectedAwayXg,
    matchOdds: pricing.matchOdds,
    totalMainline: totalMainIndex >= 0 ? pricing.totalLines[totalMainIndex] : null,
    handicapMainline: handicapMainIndex >= 0 ? pricing.handicapLines[handicapMainIndex] : null,
  };
}

function buildGameMainlinePricingFromPeriodXg(game, periodKey = pricingPeriod) {
  const period = normalizePricingPeriod(periodKey);
  const projectedHomeXg = getPeriodMetricValue(game, period, "home_xg");
  const projectedAwayXg = getPeriodMetricValue(game, period, "away_xg");
  if (projectedHomeXg == null || projectedAwayXg == null) return null;
  return buildMainlinePricingFromProjectedXg(projectedHomeXg, projectedAwayXg);
}

function buildTeamGoalDistributionTableHtml(teamLabel, trials, probability, pmf) {
  const safePmf = Array.isArray(pmf) ? pmf : [];
  if (!safePmf.length) {
    return `
      <section class="recent-team-block">
        <h4>${escapeHtml(teamLabel || "Team")} Goal Distribution</h4>
        <p class="recent-empty">No distribution available.</p>
      </section>
    `;
  }
  const maxGoalsShown = 20;
  const rowsShown = safePmf.slice(0, maxGoalsShown + 1);
  const tailProbability = safePmf.slice(maxGoalsShown + 1).reduce((sum, value) => sum + Number(value || 0), 0);
  const expectedGoals = expectedValueFromPmf(safePmf);
  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} Goal Distribution</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Shots (n)</th>
              <th>Goal/Shot (p)</th>
              <th>Exp Goals</th>
              <th>Goals</th>
              <th>Prob</th>
              <th>Fair Odds</th>
            </tr>
          </thead>
          <tbody>
            ${rowsShown
              .map((probabilityAtGoals, goals) => {
                const prob = Number(probabilityAtGoals);
                const fairOdds = prob > 0 ? (1 / prob) : null;
                return `
              <tr>
                <td>${goals === 0 ? trials : ""}</td>
                <td>${goals === 0 ? formatMetricValue(probability, 3) : ""}</td>
                <td>${goals === 0 ? formatMetricValue(expectedGoals, 2) : ""}</td>
                <td>${goals}</td>
                <td>${formatMetricValue(prob * 100, 2)}%</td>
                <td>${formatOddsValue(fairOdds)}</td>
              </tr>
            `;
              })
              .join("")}
            ${tailProbability > 1e-8
    ? `
            <tr>
              <td></td>
              <td></td>
              <td></td>
              <td>${maxGoalsShown + 1}+</td>
              <td>${formatMetricValue(tailProbability * 100, 2)}%</td>
              <td>-</td>
            </tr>
            `
    : ""}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function buildTotalPricingTableHtml(totalLines, options = {}) {
  const rows = Array.isArray(totalLines) ? totalLines : [];
  const expanded = Boolean(options?.expanded);
  const mainIndex = findMainLineIndex(rows, (row) => row?.under?.fairOdds, (row) => row?.over?.fairOdds);
  const defaultRows = selectDefaultMainWindow(rows, mainIndex, 2);
  const rowsToRender = expanded ? rows : defaultRows;
  const hasHiddenRows = rows.length > rowsToRender.length;
  if (!rows.length) {
    return `
      <section class="recent-team-block">
        <h4>Total Goals Prices</h4>
        <p class="recent-empty">No total-goals prices available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <div class="pricing-table-head">
        <h4>Total Goals Prices (Fair, No Margin)</h4>
        ${hasHiddenRows || expanded
    ? `
        <button type="button" class="pricing-expand-btn" data-pricing-table="totals">
          ${expanded ? "Show Main Window" : `Show All (${rows.length})`}
        </button>
        `
    : ""}
      </div>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Under</th>
              <th>Line</th>
              <th>Over</th>
            </tr>
          </thead>
          <tbody>
            ${rowsToRender
              .map((row) => {
                const rowIndex = rows.indexOf(row);
                const isMainLine = rowIndex >= 0 && rowIndex === mainIndex;
                return `
              <tr class="${isHalfLineValue(row?.line) ? "pricing-half-line-row" : ""} ${isMainLine ? "pricing-main-line-row" : ""}">
                <td>${formatOddsValue(row?.under?.fairOdds)}</td>
                <td>${formatQuarterLine(row?.line, false)}</td>
                <td>${formatOddsValue(row?.over?.fairOdds)}</td>
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

function buildHandicapPricingTableHtml(handicapLines, options = {}) {
  const rows = Array.isArray(handicapLines) ? handicapLines : [];
  const expanded = Boolean(options?.expanded);
  const mainIndex = findMainLineIndex(rows, (row) => row?.home?.fairOdds, (row) => row?.away?.fairOdds);
  const defaultRows = selectDefaultMainWindow(rows, mainIndex, 2);
  const rowsToRender = expanded ? rows : defaultRows;
  const hasHiddenRows = rows.length > rowsToRender.length;
  if (!rows.length) {
    return `
      <section class="recent-team-block">
        <h4>Asian Handicap Prices</h4>
        <p class="recent-empty">No handicap prices available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <div class="pricing-table-head">
        <h4>Asian Handicap Prices (Fair, No Margin)</h4>
        ${hasHiddenRows || expanded
    ? `
        <button type="button" class="pricing-expand-btn" data-pricing-table="handicap">
          ${expanded ? "Show Main Window" : `Show All (${rows.length})`}
        </button>
        `
    : ""}
      </div>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Home</th>
              <th>Handicap</th>
              <th>Away</th>
            </tr>
          </thead>
          <tbody>
            ${rowsToRender
              .map((row) => {
                const rowIndex = rows.indexOf(row);
                const isMainLine = rowIndex >= 0 && rowIndex === mainIndex;
                return `
              <tr class="${isHalfLineValue(row?.homeLine) ? "pricing-half-line-row" : ""} ${isMainLine ? "pricing-main-line-row" : ""}">
                <td>${formatOddsValue(row?.home?.fairOdds)}</td>
                <td>${formatQuarterLine(row?.homeLine, true)}</td>
                <td>${formatOddsValue(row?.away?.fairOdds)}</td>
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

function buildMatchOddsPricingTableHtml(matchOdds) {
  const safe = matchOdds && typeof matchOdds === "object"
    ? matchOdds
    : {
      homeWinProb: null,
      drawProb: null,
      awayWinProb: null,
      homeWinOdds: null,
      drawOdds: null,
      awayWinOdds: null,
    };
  const hasPrices = (
    Number.isFinite(Number(safe.homeWinOdds))
    || Number.isFinite(Number(safe.drawOdds))
    || Number.isFinite(Number(safe.awayWinOdds))
  );
  if (!hasPrices) {
    return `
      <section class="recent-team-block">
        <h4>Match Odds (1X2)</h4>
        <p class="recent-empty">No match-odds prices available.</p>
      </section>
    `;
  }
  return `
    <section class="recent-team-block">
      <h4>Match Odds (1X2) - Fair, No Margin</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Home Win</th>
              <th>Draw</th>
              <th>Away Win</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${formatOddsValue(safe.homeWinOdds)}</td>
              <td>${formatOddsValue(safe.drawOdds)}</td>
              <td>${formatOddsValue(safe.awayWinOdds)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function normalizeBetBuilderMarket(value) {
  const market = String(value || "").trim().toLowerCase();
  if (market === "handicap" || market === "btts") return market;
  return "total";
}

function getBetBuilderLegState(legNumber) {
  if (legNumber === 2) {
    return {
      market: normalizeBetBuilderMarket(pricingBetBuilderLeg2Market),
      handicapSide: pricingBetBuilderLeg2HandicapSide === "away" ? "away" : "home",
      handicapLine: pricingBetBuilderLeg2HandicapLine,
      totalSide: pricingBetBuilderLeg2TotalSide === "under" ? "under" : "over",
      totalLine: pricingBetBuilderLeg2TotalLine,
      bttsSide: pricingBetBuilderLeg2BttsSide === "no" ? "no" : "yes",
    };
  }
  return {
    market: normalizeBetBuilderMarket(pricingBetBuilderLeg1Market),
    handicapSide: pricingBetBuilderLeg1HandicapSide === "away" ? "away" : "home",
    handicapLine: pricingBetBuilderLeg1HandicapLine,
    totalSide: pricingBetBuilderLeg1TotalSide === "under" ? "under" : "over",
    totalLine: pricingBetBuilderLeg1TotalLine,
    bttsSide: pricingBetBuilderLeg1BttsSide === "no" ? "no" : "yes",
  };
}

function normalizeBetBuilderLeg(rawLeg, label) {
  const market = normalizeBetBuilderMarket(rawLeg?.market);
  const bet = market === "handicap"
    ? (rawLeg?.handicapSide === "away" ? "away" : "home")
    : (market === "btts"
      ? (rawLeg?.bttsSide === "no" ? "no" : "yes")
      : (rawLeg?.totalSide === "under" ? "under" : "over"));
  const line = market === "handicap"
    ? parsePricingHalfLine(rawLeg?.handicapLine)
    : (market === "total" ? parsePricingHalfLine(rawLeg?.totalLine) : null);
  const warnings = [];
  if (market === "handicap" && !isStrictHalfLineValue(line)) {
    warnings.push(`${label} handicap line must be a half line.`);
  }
  if (market === "total" && (!isStrictHalfLineValue(line) || Number(line) < 0.5)) {
    warnings.push(`${label} goal line must be a positive half line.`);
  }
  return { market, bet, line, warnings };
}

function betBuilderLegWins(leg, homeGoals, awayGoals) {
  if (!leg || typeof leg !== "object") return false;
  if (leg.market === "handicap") {
    const homeAdjusted = (homeGoals - awayGoals) + Number(leg.line);
    return leg.bet === "home" ? homeAdjusted > 0 : homeAdjusted < 0;
  }
  if (leg.market === "btts") {
    const bttsHappened = homeGoals > 0 && awayGoals > 0;
    return leg.bet === "yes" ? bttsHappened : !bttsHappened;
  }
  const totalGoals = homeGoals + awayGoals;
  return leg.bet === "over" ? totalGoals > Number(leg.line) : totalGoals < Number(leg.line);
}

function computeBetBuilderFairPrice(homeGoalPmf, awayGoalPmf, options = {}) {
  const home = Array.isArray(homeGoalPmf) ? homeGoalPmf : [];
  const away = Array.isArray(awayGoalPmf) ? awayGoalPmf : [];
  const leg1 = normalizeBetBuilderLeg(options?.leg1, "Leg 1");
  const leg2 = normalizeBetBuilderLeg(options?.leg2, "Leg 2");
  const warnings = [...leg1.warnings, ...leg2.warnings];

  if (!home.length || !away.length) {
    warnings.push("No goal distribution is available for this game.");
  }
  if (warnings.length) {
    return {
      leg1,
      leg2,
      combinedWinProb: null,
      fairOdds: null,
      warnings,
    };
  }

  let combinedWinProb = 0;
  for (let homeGoals = 0; homeGoals < home.length; homeGoals += 1) {
    const homeProb = Number(home[homeGoals]);
    if (!Number.isFinite(homeProb) || homeProb <= 0) continue;
    for (let awayGoals = 0; awayGoals < away.length; awayGoals += 1) {
      const awayProb = Number(away[awayGoals]);
      if (!Number.isFinite(awayProb) || awayProb <= 0) continue;
      if (betBuilderLegWins(leg1, homeGoals, awayGoals) && betBuilderLegWins(leg2, homeGoals, awayGoals)) {
        combinedWinProb += homeProb * awayProb;
      }
    }
  }

  return {
    leg1,
    leg2,
    combinedWinProb,
    fairOdds: combinedWinProb > 0 ? (1 / combinedWinProb) : null,
    warnings: [],
  };
}

function normalizePricingBetBuilderEdge(value) {
  const num = Number(String(value ?? "").trim().replace(",", "."));
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(10, Math.round(num * 10000) / 10000));
}

function formatPricingBetBuilderEdge(value) {
  const normalized = normalizePricingBetBuilderEdge(value);
  return String(Math.round(normalized * 10000) / 10000);
}

function formatBetBuilderLegSelection(leg) {
  if (!leg || typeof leg !== "object") return "-";
  if (leg.market === "handicap") {
    return `Handicap ${leg.bet === "away" ? "Away" : "Home"} ${formatQuarterLine(leg.line, true)}`;
  }
  if (leg.market === "btts") {
    return `BTTS ${leg.bet === "no" ? "No" : "Yes"}`;
  }
  return `Total Goals ${leg.bet === "under" ? "Under" : "Over"} ${formatQuarterLine(leg.line, false)}`;
}

function buildBetBuilderBetOptionsHtml(market, selectedBet) {
  if (market === "handicap") {
    const bet = selectedBet === "away" ? "away" : "home";
    return `
      <option value="home" ${bet === "home" ? "selected" : ""}>Home</option>
      <option value="away" ${bet === "away" ? "selected" : ""}>Away</option>
    `;
  }
  if (market === "btts") {
    const bet = selectedBet === "no" ? "no" : "yes";
    return `
      <option value="yes" ${bet === "yes" ? "selected" : ""}>Yes</option>
      <option value="no" ${bet === "no" ? "selected" : ""}>No</option>
    `;
  }
  const bet = selectedBet === "under" ? "under" : "over";
  return `
    <option value="over" ${bet === "over" ? "selected" : ""}>Over</option>
    <option value="under" ${bet === "under" ? "selected" : ""}>Under</option>
  `;
}

function getBetBuilderLegSelectedBet(legState) {
  const market = normalizeBetBuilderMarket(legState?.market);
  if (market === "handicap") return legState?.handicapSide === "away" ? "away" : "home";
  if (market === "btts") return legState?.bttsSide === "no" ? "no" : "yes";
  return legState?.totalSide === "under" ? "under" : "over";
}

function getBetBuilderLegLineDisplayValue(legState) {
  const market = normalizeBetBuilderMarket(legState?.market);
  if (market === "btts") return "N/A";
  const rawLine = market === "handicap" ? legState?.handicapLine : legState?.totalLine;
  return formatHalfLineInputValue(rawLine);
}

function buildBetBuilderLegControlsHtml(legNumber, legState) {
  const market = normalizeBetBuilderMarket(legState?.market);
  const selectedBet = getBetBuilderLegSelectedBet(legState);
  const lineInputType = market === "btts" ? "text" : "number";
  const disabledAttr = market === "btts" ? "disabled" : "";
  const minAttr = market === "handicap" ? "-10.5" : "0.5";
  const maxAttr = market === "handicap" ? "10.5" : "15.5";
  return `
    <div class="betbuilder-leg-row">
      <label class="betbuilder-control" for="pricingBetBuilderLeg${legNumber}Market">
        <span>Market ${legNumber}</span>
        <select id="pricingBetBuilderLeg${legNumber}Market" data-betbuilder-leg="${legNumber}">
          <option value="handicap" ${market === "handicap" ? "selected" : ""}>Handicap</option>
          <option value="total" ${market === "total" ? "selected" : ""}>Total Goals</option>
          <option value="btts" ${market === "btts" ? "selected" : ""}>BTTS</option>
        </select>
      </label>
      <label class="betbuilder-control" for="pricingBetBuilderLeg${legNumber}Line">
        <span>Line ${legNumber}</span>
        <input
          id="pricingBetBuilderLeg${legNumber}Line"
          data-betbuilder-leg="${legNumber}"
          type="${lineInputType}"
          step="1"
          min="${minAttr}"
          max="${maxAttr}"
          value="${escapeHtml(getBetBuilderLegLineDisplayValue(legState))}"
          ${disabledAttr}
        />
      </label>
      <label class="betbuilder-control" for="pricingBetBuilderLeg${legNumber}Bet">
        <span>Bet ${legNumber}</span>
        <select id="pricingBetBuilderLeg${legNumber}Bet" data-betbuilder-leg="${legNumber}">
          ${buildBetBuilderBetOptionsHtml(market, selectedBet)}
        </select>
      </label>
    </div>
  `;
}

function setBetBuilderLegMarket(legNumber, marketValue) {
  const market = normalizeBetBuilderMarket(marketValue);
  if (legNumber === 2) {
    pricingBetBuilderLeg2Market = market;
  } else {
    pricingBetBuilderLeg1Market = market;
  }
}

function setBetBuilderLegLine(legNumber, lineValue) {
  const legState = getBetBuilderLegState(legNumber);
  const market = normalizeBetBuilderMarket(legState.market);
  if (market === "btts") return;
  const parsed = parsePricingHalfLine(lineValue);
  const validLine = market === "handicap"
    ? isStrictHalfLineValue(parsed)
    : (isStrictHalfLineValue(parsed) && parsed >= 0.5);
  const nextValue = validLine ? formatHalfLineInputValue(parsed) : String(lineValue || "");
  if (legNumber === 2) {
    if (market === "handicap") pricingBetBuilderLeg2HandicapLine = nextValue;
    else pricingBetBuilderLeg2TotalLine = nextValue;
  } else if (market === "handicap") {
    pricingBetBuilderLeg1HandicapLine = nextValue;
  } else {
    pricingBetBuilderLeg1TotalLine = nextValue;
  }
}

function setBetBuilderLegBet(legNumber, betValue) {
  const legState = getBetBuilderLegState(legNumber);
  const market = normalizeBetBuilderMarket(legState.market);
  if (legNumber === 2) {
    if (market === "handicap") pricingBetBuilderLeg2HandicapSide = betValue === "away" ? "away" : "home";
    else if (market === "btts") pricingBetBuilderLeg2BttsSide = betValue === "no" ? "no" : "yes";
    else pricingBetBuilderLeg2TotalSide = betValue === "under" ? "under" : "over";
    return;
  }
  if (market === "handicap") pricingBetBuilderLeg1HandicapSide = betValue === "away" ? "away" : "home";
  else if (market === "btts") pricingBetBuilderLeg1BttsSide = betValue === "no" ? "no" : "yes";
  else pricingBetBuilderLeg1TotalSide = betValue === "under" ? "under" : "over";
}

function buildBetBuilderPricingHtml(model, homeLabel, awayLabel) {
  const safeModel = model && typeof model === "object" ? model : {};
  const result = computeBetBuilderFairPrice(
    safeModel.homeGoalPmf,
    safeModel.awayGoalPmf,
    {
      leg1: getBetBuilderLegState(1),
      leg2: getBetBuilderLegState(2),
    }
  );
  const leg1State = getBetBuilderLegState(1);
  const leg2State = getBetBuilderLegState(2);
  const selectionText = `${formatBetBuilderLegSelection(result.leg1)} + ${formatBetBuilderLegSelection(result.leg2)}`;
  const warningHtml = result.warnings.length
    ? `<div class="warning">${escapeHtml(result.warnings.join(" "))}</div>`
    : "";
  const edge = normalizePricingBetBuilderEdge(pricingBetBuilderEdge);
  const minPrice = Number.isFinite(Number(result.fairOdds))
    ? Number(result.fairOdds) * (1 + edge)
    : null;
  return `
    <section class="recent-team-block betbuilder-calculator">
      <h4>Betbuilder Price Calculator</h4>
      <div class="betbuilder-layout">
        <div class="betbuilder-controls">
          ${buildBetBuilderLegControlsHtml(1, leg1State)}
          ${buildBetBuilderLegControlsHtml(2, leg2State)}
        </div>
        <div class="betbuilder-price-panel">
          <div class="betbuilder-selection">${escapeHtml(selectionText)}</div>
          <div class="betbuilder-price-grid">
            <div class="betbuilder-price-cell">
              <span>Fair Price</span>
              <strong>${formatOddsValue(result.fairOdds)}</strong>
            </div>
            <div class="betbuilder-price-cell">
              <span>Min Price</span>
              <strong>${formatOddsValue(minPrice)}</strong>
            </div>
          </div>
          <label class="betbuilder-edge-control" for="pricingBetBuilderEdge">
            <span>Edge</span>
            <input
              id="pricingBetBuilderEdge"
              type="text"
              inputmode="decimal"
              autocomplete="off"
              value="${escapeHtml(formatPricingBetBuilderEdge(edge))}"
            />
          </label>
        </div>
      </div>
      ${warningHtml}
    </section>
  `;
}

function buildGamePricingTabHtml(homeLabel, awayLabel, homeRows, awayRows, options = {}) {
  const model = buildGamePricingModel(homeRows, awayRows, options);
  const periodLabel = getPricingPeriodLabel();
  const warningHtml = model.warnings.length
    ? `<div class="warning">${escapeHtml(model.warnings.join(" "))}</div>`
    : "";
  return `
    <div class="pricing-toolbar">
      ${buildPricingPeriodControlHtml()}
    </div>
    ${warningHtml}
    ${buildMatchOddsPricingTableHtml(model.matchOdds)}
    <div class="pricing-tables-grid">
      ${buildTotalPricingTableHtml(model.totalLines, { expanded: pricingTotalsExpanded })}
      ${buildHandicapPricingTableHtml(model.handicapLines, { expanded: pricingHandicapExpanded })}
    </div>
    ${buildBetBuilderPricingHtml(model, homeLabel, awayLabel)}
    <h3 class="section-title">Pricing Inputs</h3>
    <section class="recent-team-block">
      <h4>Projected Match Inputs (${escapeHtml(periodLabel)})</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Home Team</th>
              <th>Away Team</th>
              <th>Proj Home xG</th>
              <th>Proj Away xG</th>
              <th>Proj Home Shots</th>
              <th>Proj Away Shots</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${escapeHtml(homeLabel || "Home team")}</td>
              <td>${escapeHtml(awayLabel || "Away team")}</td>
              <td>${formatMetricValue(model.predictedHomeXg, 2)}</td>
              <td>${formatMetricValue(model.predictedAwayXg, 2)}</td>
              <td>${formatMetricValue(model.predictedHomeShots, 2)}</td>
              <td>${formatMetricValue(model.predictedAwayShots, 2)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    <section class="recent-team-block">
      <h4>Source Shot Quality Inputs</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Team</th>
              <th>Games</th>
              <th>xG For Avg</th>
              <th>xG Against Avg</th>
              <th>Shots For Avg</th>
              <th>Shots Against Avg</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${escapeHtml(homeLabel || "Home team")}</td>
              <td>${model.homeProfile.games}</td>
              <td>${formatMetricValue(model.homeProfile.xgForAvg, 2)}</td>
              <td>${formatMetricValue(model.homeProfile.xgAgainstAvg, 2)}</td>
              <td>${formatMetricValue(model.homeProfile.shotsForAvg, 2)}</td>
              <td>${formatMetricValue(model.homeProfile.shotsAgainstAvg, 2)}</td>
            </tr>
            <tr>
              <td>${escapeHtml(awayLabel || "Away team")}</td>
              <td>${model.awayProfile.games}</td>
              <td>${formatMetricValue(model.awayProfile.xgForAvg, 2)}</td>
              <td>${formatMetricValue(model.awayProfile.xgAgainstAvg, 2)}</td>
              <td>${formatMetricValue(model.awayProfile.shotsForAvg, 2)}</td>
              <td>${formatMetricValue(model.awayProfile.shotsAgainstAvg, 2)}</td>
            </tr>
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
                  const rawMainline = String(row?.closing_mainline || row?.mainline || "").trim();
                  const homeHcNum = Number(rawMainline);
                  const homePriceText = String(row?.closing_home_price || row?.home_price || "-");
                  const awayPriceText = String(row?.closing_away_price || row?.away_price || "-");
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
                <td class="hcperf-odds-col hcperf-odds-col-start">${escapeHtml(homePriceText || "-")}</td>
                <td class="hcperf-odds-line-col">${escapeHtml(rawMainline || "-")}</td>
                <td class="hcperf-odds-col hcperf-odds-col-end">${escapeHtml(awayPriceText || "-")}</td>
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

function buildVenueSplitSectionHtml(
  teamLabel,
  homeRows,
  awayRows,
  rollingWindowGames = 10,
  includeChart = true,
  rollingSeedRowsByVenue = null,
  trendlineDegreeValue = TRENDLINE_DEGREE_DEFAULT
) {
  const mergedRows = [...homeRows, ...awayRows].sort((a, b) =>
    String(b.date_time || "").localeCompare(String(a.date_time || ""))
  );
  const mergedSeedRows = mergeRollingSeedRowsByVenue({
    home: Array.isArray(rollingSeedRowsByVenue?.home) ? rollingSeedRowsByVenue.home : [],
    away: Array.isArray(rollingSeedRowsByVenue?.away) ? rollingSeedRowsByVenue.away : [],
  });
  return `
    <h3 class="section-title">${escapeHtml(teamLabel)}: Home & Away Matches</h3>
    ${includeChart
    ? buildMetricTrendPlotHtml(
      mergedRows,
      "Total games trend",
      teamLabel,
      rollingWindowGames,
      {
        rollingSeedRows: mergedSeedRows,
        trendlineDegree: trendlineDegreeValue,
        showTrendlineControl: true,
        trendlineControlClass: "trendline-degree-input-main",
      }
    )
    : ""}
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
  const games = Array.isArray(dayGames) ? dayGames : [];
  if (!games.length) return false;
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
  const pricingMetricKeys = [
    "season_home_xg",
    "season_away_xg",
    "last5_home_xg",
    "last5_away_xg",
    "last3_home_xg",
    "last3_away_xg",
  ];
  const hasDisplayMetrics = games.some((game) => {
    return metricKeys.some((metricKey) => {
      return toMetricNumberOrNull(game?.[metricKey]) != null;
    });
  });
  const hasPricingMetrics = games.every((game) => {
    return pricingMetricKeys.every((metricKey) => {
      return toMetricNumberOrNull(game?.[metricKey]) != null;
    });
  });
  return hasDisplayMetrics && hasPricingMetrics;
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

function applyLoadedGamesPayload(payload, options = {}) {
  const shouldLoadMoreHistorical = Boolean(options?.shouldLoadMoreHistorical);
  const fromCache = Boolean(options?.fromCache);
  rawDays = Array.isArray(payload?.days) ? payload.days : [];
  if (Array.isArray(payload?.saved_market_ids)) {
    setSavedMarketIds(payload.saved_market_ids);
  }
  fillMissingDays = payload?.fill_missing_days !== false;
  historicalHasMoreOlder = gamesMode === "historical" ? Boolean(payload?.has_more_older) : false;
  updateLeagueFilterOptions();
  updateTierFilterOptions(payload?.tiers || []);
  applyGameFilters();
  renderCurrentDay();
  if (gamesMode === "historical") {
    const loadedTotal = Number(payload?.total_games) || 0;
    if (shouldLoadMoreHistorical) {
      const addedGames = Number(payload?.added_games) || 0;
      statusText.textContent = `Loaded ${addedGames} older matches (${loadedTotal} total)`;
    } else if (loadedTotal === 0 && historicalHasMoreOlder) {
      statusText.textContent = "No matches in the latest window. Click Previous Day to load older matches.";
    } else {
      statusText.textContent = fromCache
        ? `Loaded ${loadedTotal} historical matches (cached)`
        : `Loaded ${loadedTotal} historical matches`;
    }
  } else {
    const loadedTotal = Number(payload?.total_games) || 0;
    statusText.textContent = fromCache
      ? `Loaded ${loadedTotal} games (cached)`
      : `Loaded ${loadedTotal} games`;
  }
  // Keep mapping tab edits stable during game refreshes (manual + auto-refresh).
  // Mapping data now refreshes only when the user opens Mapping, clicks
  // "Refresh Mapping Data", or after an explicit mapping save/delete action.
  if (activeTab === "rankings" && teamHcRankingsLoaded) {
    loadTeamHcRankings({ silent: true });
  } else if (activeTab === "teams" && teamsDirectoryLoaded) {
    loadTeamsDirectory({ silent: true });
  } else if (isSavedGamesViewActive() && savedGamesLoaded) {
    loadSavedGames({ silent: true });
  }

  if (selectedMarketId && !gamesById.has(selectedMarketId)) {
    selectedMarketId = null;
    detailsPanel.classList.add("hidden");
  }
}

async function prefetchGamesPayloadForMode(targetGamesMode = gamesMode) {
  const gamesModeKey = String(targetGamesMode || "").trim().toLowerCase() === "historical"
    ? "historical"
    : "upcoming";
  const activeMode = getCurrentXgMetricMode();
  const alternateMode = getAlternateXgMetricMode(activeMode);
  const alternateCacheKey = getGamesPayloadCacheKey(gamesModeKey, alternateMode);
  if (gamesPayloadByModeAndView.has(alternateCacheKey)) return;
  if (gamesPayloadPrefetchInFlight.has(alternateCacheKey)) return;
  gamesPayloadPrefetchInFlight.add(alternateCacheKey);
  try {
    const query = new URLSearchParams();
    query.set("mode", gamesModeKey);
    query.set("xg_mode", alternateMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/games?${query.toString()}`);
    if (!res.ok) return;
    if (alternateMode === "npxg" && payload?.npxg_available === false) {
      gamesPayloadByModeAndView.delete(alternateCacheKey);
      return;
    }
    gamesPayloadByModeAndView.set(alternateCacheKey, payload || {});
  } catch (_err) {
    // Ignore prefetch failures to keep the primary load path resilient.
  } finally {
    gamesPayloadPrefetchInFlight.delete(alternateCacheKey);
  }
}

async function loadGames(options = {}) {
  const loadMoreHistorical = Boolean(options && options.loadMoreHistorical);
  const useCache = Boolean(options?.useCache);
  const force = Boolean(options?.force);
  const shouldLoadMoreHistorical = gamesMode === "historical" && loadMoreHistorical;
  if (shouldLoadMoreHistorical && !historicalHasMoreOlder) {
    return true;
  }

  const requestedMetricMode = getCurrentXgMetricMode();
  const cacheKey = getGamesPayloadCacheKey(gamesMode, requestedMetricMode);
  if (!force && useCache && !shouldLoadMoreHistorical && gamesPayloadByModeAndView.has(cacheKey)) {
    const cachedPayload = gamesPayloadByModeAndView.get(cacheKey);
    applyLoadedGamesPayload(cachedPayload && typeof cachedPayload === "object" ? cachedPayload : {}, {
      shouldLoadMoreHistorical,
      fromCache: true,
    });
    void prefetchGamesPayloadForMode(gamesMode);
    return true;
  }

  statusText.textContent = shouldLoadMoreHistorical
    ? "Loading older historical matches..."
    : (gamesMode === "historical" ? "Loading historical matches..." : "Loading games...");
  try {
    const query = new URLSearchParams();
    query.set("mode", gamesMode);
    query.set("xg_mode", requestedMetricMode);
    if (shouldLoadMoreHistorical) {
      query.set("load_more", "1");
    }
    const { res, payload } = await fetchJsonWithTimeout(`/api/games?${query.toString()}`);
    if (!res.ok) throw new Error(payload.error || "Failed to load games");
    if (payload && payload.npxg_available === false && requestedMetricMode === "npxg") {
      xgMetricMode = "xg";
      persistXgMetricMode("xg");
      updateXgMetricModeToggleButton();
      gamesPayloadByModeAndView.delete(getGamesPayloadCacheKey(gamesMode, "npxg"));
    }
    const effectiveMetricMode = getCurrentXgMetricMode();
    gamesPayloadByModeAndView.set(getGamesPayloadCacheKey(gamesMode, effectiveMetricMode), payload || {});
    if (shouldLoadMoreHistorical) {
      const alternateMode = effectiveMetricMode === "xg" ? "npxg" : "xg";
      gamesPayloadByModeAndView.delete(getGamesPayloadCacheKey(gamesMode, alternateMode));
    }
    applyLoadedGamesPayload(payload, { shouldLoadMoreHistorical, fromCache: false });
    if (!shouldLoadMoreHistorical) {
      void prefetchGamesPayloadForMode(gamesMode);
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
  if (!(savedGamesSubTabBtn instanceof HTMLButtonElement)) return;
  savedGamesSubTabBtn.textContent = savedGamesCount > 0 ? `Saved Games (${savedGamesCount})` : "Saved Games";
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
    if (isSavedGamesViewActive()) {
      await loadSavedGames({ silent: true });
      renderSavedGames();
    } else if (isGamesCalendarTab()) {
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
  table.className = isHistorical
    ? "games-table main-games-table historical-games-table"
    : "games-table main-games-table";
  if (isHistorical) {
    table.innerHTML = `
      <thead>
        <tr>
          <th class="kickoff-col">${kickoffHeaderLabel}</th>
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
          <th>xGD</th>
          <th>xGD Perf</th>
          <th>Min</th>
          <th>Max</th>
          <th class="xg-home-col">xG H</th>
          <th class="line-col score-col">Score</th>
          <th class="xg-away-col">xG A</th>
          <th>HC Bet</th>
          <th>Goals Bet</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
  } else {
    table.innerHTML = `
      <thead>
        <tr>
          <th class="kickoff-col">${kickoffHeaderLabel}</th>
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
    row.innerHTML = `<td class="no-games-row" colspan="${isHistorical ? "20" : "15"}">${escapeHtml(emptyMessage)}</td>`;
    tbody.appendChild(row);
    return table;
  }

  if (isHistorical) {
    tbody.insertAdjacentHTML("beforeend", buildHistoricalBetSummaryRowHtml(sortedGames));
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
    const historicalBetReturns = isHistorical
      ? calculateHistoricalBetReturns(game, { threshold })
      : {
        hcBetResult: null,
        hcBetResultXg: null,
        goalsBetResult: null,
        goalsBetResultXg: null,
      };

    if (isHistorical) {
      row.innerHTML = `
        <td class="kickoff-col">${escapeHtml(kickoffLocalText)}</td>
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
        <td class="${xgdCellClass}"${xgdCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue)}</td>
        <td class="${xgdPerfCellClass}"${xgdPerfCellTitleAttr}>${buildPeriodMetricStackCell(seasonXgdPerfValue, last5XgdPerfValue, last3XgdPerfValue)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_min_xg, game.last5_min_xg, game.last3_min_xg)}</td>
        <td class="metric-stack-cell">${buildPeriodMetricStackCell(game.season_max_xg, game.last5_max_xg, game.last3_max_xg)}</td>
        <td class="xg-home-col">${escapeHtml(game.home_xg_actual || "-")}</td>
        <td class="line-col score-col">${escapeHtml(game.scoreline || "-")}</td>
        <td class="xg-away-col">${escapeHtml(game.away_xg_actual || "-")}</td>
        <td class="metric-stack-cell">${buildBetResultStackCell(historicalBetReturns.hcBetResult, historicalBetReturns.hcBetResultXg)}</td>
        <td class="metric-stack-cell">${buildBetResultStackCell(historicalBetReturns.goalsBetResult, historicalBetReturns.goalsBetResultXg)}</td>
      `;
    } else {
      row.innerHTML = `
        <td class="kickoff-col">${escapeHtml(kickoffLocalText)}</td>
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

function buildModellingPriceStackCellHtml(betfairValue, modelValue, options = {}) {
  const betfairText = String(betfairValue ?? "").trim() || "-";
  const modelText = String(modelValue ?? "").trim() || "-";
  const highlightModel = Boolean(options?.highlightModel);
  const modelRowClass = highlightModel
    ? "modelling-price-row modelling-price-row-model modelling-price-row-model-highlight"
    : "modelling-price-row modelling-price-row-model";
  return `
    <div class="modelling-price-stack">
      <div class="modelling-price-row modelling-price-row-betfair">
        <span class="modelling-price-label">BF</span>
        <span class="modelling-price-value">${escapeHtml(betfairText)}</span>
      </div>
      <div class="${modelRowClass}">
        <span class="modelling-price-label">PR</span>
        <span class="modelling-price-value">${escapeHtml(modelText)}</span>
      </div>
    </div>
  `;
}

function createModellingPricesTable(sortedGames, isHistorical, options = {}) {
  const emptyMessage = String(options?.emptyMessage || "No games");
  const showSavedContour = options?.showSavedContour !== false;
  const kickoffHeaderLabel = escapeHtml(getKickoffColumnLabel());
  const table = document.createElement("table");
  table.className = isHistorical
    ? "games-table main-games-table historical-games-table modelling-prices-table"
    : "games-table main-games-table modelling-prices-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th class="kickoff-col">${kickoffHeaderLabel}</th>
        <th class="league-col">League</th>
        <th class="home-team-col">Home</th>
        <th class="vs-team-col">v</th>
        <th class="away-team-col">Away</th>
        <th class="home-price-col">H (1X2)</th>
        <th class="win-draw-col">D (1X2)</th>
        <th class="away-price-col">A (1X2)</th>
        <th class="home-price-col">Home</th>
        <th class="line-col handicap-line-col">HC</th>
        <th class="away-price-col">Away</th>
        <th class="model-xgd-col">xGD</th>
        <th class="goal-under-price-col">Under</th>
        <th class="goal-line-col">Goals</th>
        <th class="goal-over-price-col">Over</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector("tbody");
  if (!sortedGames.length) {
    const row = document.createElement("tr");
    row.innerHTML = `<td class="no-games-row" colspan="15">${escapeHtml(emptyMessage)}</td>`;
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

    const teams = resolveGameTeams(game);
    const competitionName = String(game?.competition || "").trim();
    const homeTeamCell = teams.home ? buildTeamLinkHtml(teams.home, competitionName) : "-";
    const awayTeamCell = teams.away ? buildTeamLinkHtml(teams.away, competitionName) : "-";
    const kickoffLocalText = formatGameKickoffLocalTime(game);

    const model = buildGameMainlinePricingFromPeriodXg(game, pricingPeriod);
    const modelMatchOdds = model?.matchOdds || null;
    const modelHandicapMainline = model?.handicapMainline || null;
    const modelTotalMainline = model?.totalMainline || null;
    const modelHomeWinOdds = toMetricNumberOrNull(modelMatchOdds?.homeWinOdds);
    const modelDrawOdds = toMetricNumberOrNull(modelMatchOdds?.drawOdds);
    const modelAwayWinOdds = toMetricNumberOrNull(modelMatchOdds?.awayWinOdds);
    const modelHomeWinText = formatOddsValue(modelHomeWinOdds);
    const modelDrawText = formatOddsValue(modelDrawOdds);
    const modelAwayWinText = formatOddsValue(modelAwayWinOdds);
    const highlightModelHomeWin = shouldHighlightModelPriceVsBetfair(game.win_home_price, modelHomeWinOdds);
    const highlightModelDraw = shouldHighlightModelPriceVsBetfair(game.win_draw_price, modelDrawOdds);
    const highlightModelAwayWin = shouldHighlightModelPriceVsBetfair(game.win_away_price, modelAwayWinOdds);
    const modelHomeHcText = formatOddsValue(modelHandicapMainline?.home?.fairOdds);
    const modelHcLineText = formatQuarterLine(modelHandicapMainline?.homeLine, true);
    const modelAwayHcText = formatOddsValue(modelHandicapMainline?.away?.fairOdds);
    const modelUnderText = formatOddsValue(modelTotalMainline?.under?.fairOdds);
    const modelGoalLineText = formatQuarterLine(modelTotalMainline?.line, false);
    const modelOverText = formatOddsValue(modelTotalMainline?.over?.fairOdds);
    const noXgMetrics = hasNoXgMetricSignal(game);
    const handicap = toMetricNumberOrNull(game?.mainline);
    const threshold = getCurrentXgPushThreshold();
    const xgdPeriodValues = [
      toMetricNumberOrNull(game?.season_strength),
      toMetricNumberOrNull(game?.last5_strength),
      toMetricNumberOrNull(game?.last3_strength),
    ];
    const xgdHcHighlightClass = (xgdHcHighlightEnabled && !noXgMetrics)
      ? getMainTableXgdHcHighlightClass(xgdPeriodValues, handicap, threshold)
      : "";
    const domesticFallbackClass = Boolean(game?.xgd_domestic_fallback) ? "xgd-domestic-fallback" : "";
    const modelXgdCellClass = [
      "model-xgd-col",
      "metric-stack-cell",
      xgdHcHighlightClass,
      domesticFallbackClass,
    ].filter(Boolean).join(" ");
    const seasonXgdValue = noXgMetrics ? "-" : game.season_strength;
    const last5XgdValue = noXgMetrics ? "-" : game.last5_strength;
    const last3XgdValue = noXgMetrics ? "-" : game.last3_strength;

    row.innerHTML = `
      <td class="kickoff-col">${escapeHtml(kickoffLocalText)}</td>
      <td class="league-col">${escapeHtml(game.competition)}</td>
      <td class="home-team-col">${homeTeamCell}</td>
      <td class="vs-team-col">v</td>
      <td class="away-team-col">${awayTeamCell}</td>
      <td class="home-price-col">${buildModellingPriceStackCellHtml(game.win_home_price || "-", modelHomeWinText, { highlightModel: highlightModelHomeWin })}</td>
      <td class="win-draw-col">${buildModellingPriceStackCellHtml(game.win_draw_price || "-", modelDrawText, { highlightModel: highlightModelDraw })}</td>
      <td class="away-price-col">${buildModellingPriceStackCellHtml(game.win_away_price || "-", modelAwayWinText, { highlightModel: highlightModelAwayWin })}</td>
      <td class="home-price-col">${buildModellingPriceStackCellHtml(game.home_price || "-", modelHomeHcText)}</td>
      <td class="line-col handicap-line-col">${buildModellingPriceStackCellHtml(game.mainline || "-", modelHcLineText)}</td>
      <td class="away-price-col">${buildModellingPriceStackCellHtml(game.away_price || "-", modelAwayHcText)}</td>
      <td class="${modelXgdCellClass}">${buildPeriodMetricStackCell(seasonXgdValue, last5XgdValue, last3XgdValue)}</td>
      <td class="goal-under-price-col">${buildModellingPriceStackCellHtml(game.goal_under_price || "-", modelUnderText)}</td>
      <td class="goal-line-col">${buildModellingPriceStackCellHtml(game.goal_mainline || "-", modelGoalLineText)}</td>
      <td class="goal-over-price-col">${buildModellingPriceStackCellHtml(game.goal_over_price || "-", modelOverText)}</td>
    `;
    row.addEventListener("click", () => loadGameXgd(game.market_id));
    tbody.appendChild(row);
  }

  bindTeamLinkButtons(table);
  return table;
}

function renderSavedGames() {
  if (!savedGamesView) return;
  updateGamesPaneViewVisibility();
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
  if (activeTab === "modelling") {
    const toolbar = document.createElement("div");
    toolbar.className = "saved-games-toolbar";
    const pricingPeriodControl = createPricingPeriodControlElement();
    if (pricingPeriodControl instanceof HTMLElement) {
      toolbar.appendChild(pricingPeriodControl);
    }
    savedGamesView.appendChild(toolbar);
  }
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

    const table = activeTab === "modelling"
      ? createModellingPricesTable(sortedDayGames, useHistoricalLayout, { emptyMessage: "No saved games" })
      : createGamesTable(sortedDayGames, useHistoricalLayout, {
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

function setSelectedTeamHcRankingsCompetition(competitionName) {
  const next = String(competitionName || "").trim();
  selectedTeamHcRankingsCompetition = next;
}

function getTeamHcRankingsCompetitionNames() {
  const names = [];
  const seen = new Set();
  const seasonsByCompetition = (
    teamHcRankingsSeasonsByCompetition
    && typeof teamHcRankingsSeasonsByCompetition === "object"
  )
    ? teamHcRankingsSeasonsByCompetition
    : {};
  for (const key of Object.keys(seasonsByCompetition)) {
    const name = String(key || "").trim();
    if (!name) continue;
    const norm = name.toLowerCase();
    if (seen.has(norm)) continue;
    seen.add(norm);
    names.push(name);
  }
  for (const row of (Array.isArray(teamHcRankingsLeagues) ? teamHcRankingsLeagues : [])) {
    const name = String(row?.competition || "").trim();
    if (!name) continue;
    const norm = name.toLowerCase();
    if (seen.has(norm)) continue;
    seen.add(norm);
    names.push(name);
  }
  names.sort((a, b) => a.localeCompare(b));
  return names;
}

function findDefaultTeamHcRankingsCompetition(leagues) {
  const candidates = Array.isArray(leagues) ? leagues : [];
  const normalizedCompetitionName = (value) => String(value || "").trim().toLowerCase();
  const candidateNames = candidates.map((row) => String(row?.competition || "").trim()).filter((value) => !!value);
  const pool = candidateNames.length ? candidateNames : getTeamHcRankingsCompetitionNames();
  const findCompetition = (predicate) => pool.find((name) => predicate(normalizedCompetitionName(name)));
  const englishPremierLeague = (
    findCompetition((name) => name === "english premier league")
    || findCompetition((name) => name === "england premier league")
    || findCompetition((name) => name.includes("english") && name.includes("premier league"))
    || findCompetition((name) => name.includes("england") && name.includes("premier league"))
    || findCompetition((name) => name === "premier league")
  );
  return String(englishPremierLeague || pool[0] || "").trim();
}

function updateTeamHcRankingsCompetitionSelectOptions() {
  if (!(teamHcRankingsCompetitionSelect instanceof HTMLSelectElement)) return;
  const competitionNames = getTeamHcRankingsCompetitionNames();

  teamHcRankingsCompetitionSelect.innerHTML = "";
  if (!competitionNames.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No competition data";
    teamHcRankingsCompetitionSelect.appendChild(option);
    teamHcRankingsCompetitionSelect.disabled = true;
    selectedTeamHcRankingsCompetition = "";
    return;
  }

  if (!competitionNames.includes(selectedTeamHcRankingsCompetition)) {
    selectedTeamHcRankingsCompetition = findDefaultTeamHcRankingsCompetition(teamHcRankingsLeagues);
  }

  for (const competitionName of competitionNames) {
    const option = document.createElement("option");
    option.value = competitionName;
    option.textContent = competitionName;
    if (competitionName === selectedTeamHcRankingsCompetition) {
      option.selected = true;
    }
    teamHcRankingsCompetitionSelect.appendChild(option);
  }

  if (!teamHcRankingsCompetitionSelect.value && selectedTeamHcRankingsCompetition) {
    teamHcRankingsCompetitionSelect.value = selectedTeamHcRankingsCompetition;
  }
  teamHcRankingsCompetitionSelect.disabled = false;
}

function getTeamHcRankingsSeasonsForCompetition(competitionName) {
  const competitionText = String(competitionName || "").trim();
  if (!competitionText) return [];
  const seasonsByCompetition = (
    teamHcRankingsSeasonsByCompetition
    && typeof teamHcRankingsSeasonsByCompetition === "object"
  )
    ? teamHcRankingsSeasonsByCompetition
    : {};
  const entries = seasonsByCompetition[competitionText];
  return Array.isArray(entries) ? entries : [];
}

function updateTeamHcRankingsSeasonSelectOptions() {
  if (!(teamHcRankingsSeasonSelect instanceof HTMLSelectElement)) return;
  const selectedCompetition = String(selectedTeamHcRankingsCompetition || "").trim();
  const seasons = getTeamHcRankingsSeasonsForCompetition(selectedCompetition);
  teamHcRankingsSeasonSelect.innerHTML = "";
  if (!selectedCompetition) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Select competition first";
    teamHcRankingsSeasonSelect.appendChild(option);
    teamHcRankingsSeasonSelect.disabled = true;
    return;
  }
  if (!seasons.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No season data";
    teamHcRankingsSeasonSelect.appendChild(option);
    teamHcRankingsSeasonSelect.disabled = true;
    return;
  }
  if (!seasons.some((entry) => String(entry?.season || "").trim() === selectedTeamHcRankingsSeason)) {
    selectedTeamHcRankingsSeason = String(seasons[0]?.season || "").trim();
  }

  for (const entry of seasons) {
    const seasonValue = String(entry?.season || "").trim();
    if (!seasonValue) continue;
    const option = document.createElement("option");
    option.value = seasonValue;
    const seasonLabel = String(entry?.season_label || seasonValue).trim();
    const gamesCount = Number(entry?.games_count) || 0;
    option.textContent = `${seasonLabel}${gamesCount ? ` (${gamesCount})` : ""}`;
    if (seasonValue === selectedTeamHcRankingsSeason) {
      option.selected = true;
    }
    teamHcRankingsSeasonSelect.appendChild(option);
  }
  if (!teamHcRankingsSeasonSelect.value && selectedTeamHcRankingsSeason) {
    teamHcRankingsSeasonSelect.value = selectedTeamHcRankingsSeason;
  }
  teamHcRankingsSeasonSelect.disabled = false;
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
    const seasonText = String(selectedTeamHcRankingsSeason || "").trim();
    if (seasonText) {
      query.set("season", seasonText);
    }
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
  updateTeamHcRankingsCompetitionSelectOptions();
  updateTeamHcRankingsSeasonSelectOptions();
  if (teamHcRankingsLoading) {
    teamHcRankingsView.innerHTML = "<p>Loading HC rankings...</p>";
    return;
  }
  if (teamHcRankingsErrorText) {
    teamHcRankingsView.innerHTML = `<p>${escapeHtml(teamHcRankingsErrorText)}</p>`;
    return;
  }
  const selectedCompetition = teamHcRankingsLeagues.find(
    (row) => String(row?.competition || "").trim() === selectedTeamHcRankingsCompetition
  );
  const competitionRows = Array.isArray(selectedCompetition?.rows) ? selectedCompetition.rows : [];
  const sortedRows = sortTeamHcRankingRows(competitionRows);
  if (!sortedRows.length) {
    teamHcRankingsView.innerHTML = "<p>No ranked teams found for this competition.</p>";
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
        openTeamPage(teamName, leagueName || null, selectedTeamHcRankingsSeason || null);
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
  const skipCompetitionSeasonReload = Boolean(options?.skipCompetitionSeasonReload);
  teamHcRankingsLoading = true;
  if (!silent) {
    statusText.textContent = "Loading team HC rankings...";
  }
  renderTeamHcRankings();
  try {
    const query = new URLSearchParams();
    query.set("xg_threshold", String(getCurrentXgPushThreshold()));
    const requestedCompetition = String(selectedTeamHcRankingsCompetition || "").trim();
    if (requestedCompetition) {
      query.set("competition", requestedCompetition);
    }
    const requestedSeason = String(selectedTeamHcRankingsSeason || "").trim();
    if (requestedSeason) {
      query.set("season", requestedSeason);
    }
    const res = await fetch(`/api/team-hc-rankings?${query.toString()}`);
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to load team HC rankings");
    teamHcRankingsSeasons = Array.isArray(payload?.seasons) ? payload.seasons : [];
    teamHcRankingsSeasonsByCompetition = (
      payload?.seasons_by_competition
      && typeof payload.seasons_by_competition === "object"
    )
      ? payload.seasons_by_competition
      : {};
    const payloadSeason = String(payload?.season || "").trim();
    if (payloadSeason) {
      selectedTeamHcRankingsSeason = payloadSeason;
    } else if (!selectedTeamHcRankingsSeason && teamHcRankingsSeasons.length) {
      selectedTeamHcRankingsSeason = String(teamHcRankingsSeasons[0]?.season || "").trim();
    }
    teamHcRankingsLeagues = Array.isArray(payload?.leagues) ? payload.leagues : [];
    teamHcRankingsRows = Array.isArray(payload?.rows) ? payload.rows : [];
    const availableCompetitionNames = getTeamHcRankingsCompetitionNames();
    if (
      !selectedTeamHcRankingsCompetition
      || !availableCompetitionNames.includes(selectedTeamHcRankingsCompetition)
    ) {
      selectedTeamHcRankingsCompetition = findDefaultTeamHcRankingsCompetition(teamHcRankingsLeagues);
    }
    const competitionSeasons = getTeamHcRankingsSeasonsForCompetition(selectedTeamHcRankingsCompetition);
    if (competitionSeasons.length) {
      const currentSeason = String(selectedTeamHcRankingsSeason || "").trim();
      const hasCurrentSeason = competitionSeasons.some((entry) => String(entry?.season || "").trim() === currentSeason);
      if (!hasCurrentSeason) {
        selectedTeamHcRankingsSeason = String(competitionSeasons[0]?.season || "").trim();
      }
    } else if (!payloadSeason) {
      selectedTeamHcRankingsSeason = "";
    }
    const shouldReloadForCompetitionSeason = (
      !skipCompetitionSeasonReload
      && (
        String(selectedTeamHcRankingsCompetition || "").trim() !== requestedCompetition
        || (
          String(payloadSeason || "").trim() !== ""
          && String(selectedTeamHcRankingsSeason || "").trim() !== String(payloadSeason || "").trim()
        )
      )
    );
    teamHcRankingsLoaded = true;
    teamHcRankingsErrorText = "";
    if (activeTab === "rankings" && !silent) {
      const seasonLabel = (
        getTeamHcRankingsSeasonsForCompetition(selectedTeamHcRankingsCompetition).find(
          (entry) => String(entry?.season || "").trim() === String(selectedTeamHcRankingsSeason || "").trim()
        )?.season_label
      ) || selectedTeamHcRankingsSeason;
      const seasonPrefix = seasonLabel ? `${seasonLabel}: ` : "";
      statusText.textContent = `${seasonPrefix}Loaded HC rankings for ${Number(payload?.total_teams) || 0} teams across ${Number(payload?.total_leagues) || 0} competitions`;
    }
    renderTeamHcRankings();
    if (shouldReloadForCompetitionSeason) {
      window.setTimeout(() => {
        loadTeamHcRankings({ silent: true, skipCompetitionSeasonReload: true });
      }, 0);
    }
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

function populateMatchupOptions() {
  if (matchupTeamsList instanceof HTMLElement) {
    const teamNames = teamsDirectoryRows
      .map((row) => String(row?.team || "").trim())
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    matchupTeamsList.innerHTML = teamNames
      .map((teamName) => `<option value="${escapeHtml(teamName)}"></option>`)
      .join("");
  }
  if (matchupCompetitionSelect instanceof HTMLSelectElement) {
    const currentValue = String(matchupCompetitionSelect.value || "").trim();
    const competitionCounts = new Map();
    for (const row of teamsDirectoryRows) {
      const competitions = Array.isArray(row?.competitions) ? row.competitions : [];
      for (const entry of competitions) {
        const competitionName = String(entry?.competition || "").trim();
        if (!competitionName) continue;
        competitionCounts.set(
          competitionName,
          (competitionCounts.get(competitionName) || 0) + (Number(entry?.games_count) || 0)
        );
      }
    }
    const competitions = Array.from(competitionCounts.entries())
      .sort((left, right) => (right[1] - left[1]) || left[0].localeCompare(right[0]))
      .map(([competitionName]) => competitionName);
    matchupCompetitionSelect.innerHTML = `
      <option value="">Auto</option>
      ${competitions
        .map((competitionName) => `
          <option value="${escapeHtml(competitionName)}">${escapeHtml(competitionName)}</option>
        `)
        .join("")}
    `;
    if (currentValue && competitions.includes(currentValue)) {
      matchupCompetitionSelect.value = currentValue;
    }
  }
}

function renderMatchupPage() {
  if (!matchupView) return;

  if (matchupLoading) {
    matchupView.innerHTML = "<p>Building matchup...</p>";
    return;
  }
  if (matchupErrorText) {
    matchupView.innerHTML = `<p>${escapeHtml(matchupErrorText)}</p>`;
    return;
  }
  if (!matchupPayload) {
    matchupView.innerHTML = "<p>Enter two teams and build a matchup.</p>";
    return;
  }

  const payload = matchupPayload;
  const mappingHead = Array.isArray(payload?.mapping_rows) && payload.mapping_rows.length
    ? payload.mapping_rows[0]
    : {};
  const homeLabel = String(payload?.home_label || mappingHead.home_sofa || mappingHead.home_raw || "Home team");
  const awayLabel = String(payload?.away_label || mappingHead.away_sofa || mappingHead.away_raw || "Away team");
  const xgdViews = Array.isArray(payload?.xgd_views) && payload.xgd_views.length
    ? payload.xgd_views
    : [{
      id: "fixture",
      label: "Fixture",
      summary: payload?.summary || null,
      period_rows: Array.isArray(payload?.period_rows) ? payload.period_rows : [],
      warning: String(payload?.warning || ""),
      home_recent_rows: Array.isArray(payload?.home_recent_rows) ? payload.home_recent_rows : [],
      away_recent_rows: Array.isArray(payload?.away_recent_rows) ? payload.away_recent_rows : [],
      home_team_venue_rows: payload?.home_team_venue_rows || { home: [], away: [] },
      away_team_venue_rows: payload?.away_team_venue_rows || { home: [], away: [] },
    }];
  const activeView = xgdViews[0] || {};
  const homeRecentRows = Array.isArray(activeView.home_recent_rows) ? activeView.home_recent_rows : [];
  const awayRecentRows = Array.isArray(activeView.away_recent_rows) ? activeView.away_recent_rows : [];
  const homeTeamVenueRows = activeView.home_team_venue_rows && typeof activeView.home_team_venue_rows === "object"
    ? activeView.home_team_venue_rows
    : { home: [], away: [] };
  const awayTeamVenueRows = activeView.away_team_venue_rows && typeof activeView.away_team_venue_rows === "object"
    ? activeView.away_team_venue_rows
    : { home: [], away: [] };
  const homeGeneralRows = [
    ...(Array.isArray(homeTeamVenueRows.home) ? homeTeamVenueRows.home : []),
    ...(Array.isArray(homeTeamVenueRows.away) ? homeTeamVenueRows.away : []),
  ];
  const awayGeneralRows = [
    ...(Array.isArray(awayTeamVenueRows.home) ? awayTeamVenueRows.home : []),
    ...(Array.isArray(awayTeamVenueRows.away) ? awayTeamVenueRows.away : []),
  ];
  const pricingSummary = getPricingSummaryForPeriod(activeView, payload, pricingPeriod);
  const competitionText = String(payload?.competition || "").trim();
  const seasonText = String(payload?.season || "").trim();
  const warningText = String(activeView?.warning || payload?.warning || "").trim();
  const warning = warningText ? `<div class="warning">${escapeHtml(warningText)}</div>` : "";

  matchupView.innerHTML = `
    <section class="matchup-result-header">
      <h2>${escapeHtml(homeLabel)} v ${escapeHtml(awayLabel)}</h2>
      <p class="meta">${escapeHtml([competitionText, seasonText ? `Season ${seasonText}` : ""].filter(Boolean).join(" | "))}</p>
    </section>
    ${warning}
    ${buildXgdPeriodTableHtml(Array.isArray(activeView.period_rows) ? activeView.period_rows : [], "xGD Output")}
    ${buildTeamXgdSummaryTablesHtml(homeLabel, awayLabel, homeRecentRows, awayRecentRows)}
    <h3 class="section-title">Model Source Matches</h3>
    ${buildRecentAveragesTableHtml(homeRecentRows, Math.max(1, homeRecentRows.length), homeLabel)}
    ${buildRecentMatchesTableHtml("Home fixture-side matches", homeRecentRows, homeLabel)}
    ${buildRecentAveragesTableHtml(awayRecentRows, Math.max(1, awayRecentRows.length), awayLabel)}
    ${buildRecentMatchesTableHtml("Away fixture-side matches", awayRecentRows, awayLabel)}
    <h3 class="section-title">Cards & Corners</h3>
    ${buildCardsCornersAveragesTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, Math.max(1, homeRecentRows.length, awayRecentRows.length))}
    ${buildGamestateTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, Math.max(1, homeRecentRows.length, awayRecentRows.length))}
    <h3 class="section-title">Shots</h3>
    ${buildShotsAveragesTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, Math.max(1, homeRecentRows.length, awayRecentRows.length))}
    ${buildShotGamestateTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, Math.max(1, homeRecentRows.length, awayRecentRows.length))}
    ${buildGamePricingTabHtml(homeLabel, awayLabel, homeGeneralRows, awayGeneralRows, {
      projectedHomeXg: pricingSummary?.home_xg,
      projectedAwayXg: pricingSummary?.away_xg,
      requireProjectedXg: true,
    })}
  `;
  bindTeamLinkButtons(matchupView);
  bindPricingControls(matchupView, renderMatchupPage);
}

async function loadMatchup() {
  if (matchupLoading) return;
  const homeText = matchupHomeInput instanceof HTMLInputElement ? String(matchupHomeInput.value || "").trim() : "";
  const awayText = matchupAwayInput instanceof HTMLInputElement ? String(matchupAwayInput.value || "").trim() : "";
  const competitionText = matchupCompetitionSelect instanceof HTMLSelectElement
    ? String(matchupCompetitionSelect.value || "").trim()
    : "";
  if (!homeText || !awayText) {
    matchupErrorText = "Enter both teams.";
    renderMatchupPage();
    return;
  }

  matchupLoading = true;
  matchupErrorText = "";
  renderMatchupPage();
  statusText.textContent = "Building matchup...";
  try {
    const query = new URLSearchParams();
    query.set("home", homeText);
    query.set("away", awayText);
    if (competitionText) query.set("competition", competitionText);
    query.set("xg_mode", getCurrentXgMetricMode());
    const { res, payload } = await fetchJsonWithTimeout(`/api/matchup?${query.toString()}`);
    if (!res.ok) throw new Error(payload.error || "Failed to build matchup");
    matchupPayload = payload || null;
    matchupErrorText = "";
    statusText.textContent = `Built matchup: ${String(payload?.event_name || "").trim() || `${homeText} v ${awayText}`}`;
  } catch (err) {
    matchupPayload = null;
    matchupErrorText = String(err.message || err);
    statusText.textContent = matchupErrorText;
  } finally {
    matchupLoading = false;
    renderMatchupPage();
  }
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
    populateMatchupOptions();
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
    if (isSavedGamesViewActive() && !silent) {
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
  updateGamesPaneViewVisibility();
  if (isSavedGamesViewActive()) {
    renderSavedGames();
    return;
  }
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
  if (activeTab === "modelling") {
    const pricingPeriodControl = createPricingPeriodControlElement();
    if (pricingPeriodControl instanceof HTMLElement) {
      headerActions.appendChild(pricingPeriodControl);
    }
  }
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

  const table = (activeTab === "modelling")
    ? createModellingPricesTable(sortedDayGames, isHistorical, { emptyMessage: "No games" })
    : createGamesTable(sortedDayGames, isHistorical, { emptyMessage: "No games" });
  block.appendChild(header);
  block.appendChild(table);
  calendarView.appendChild(block);
}

function bindPricingControls(container, rerender) {
  if (!(container instanceof Element) || typeof rerender !== "function") return;

  bindPricingPeriodButtons(container);

  const pricingExpandButtons = container.querySelectorAll(".pricing-expand-btn");
  for (const button of pricingExpandButtons) {
    if (!(button instanceof HTMLButtonElement)) continue;
    button.addEventListener("click", () => {
      const targetTable = String(button.dataset.pricingTable || "").trim().toLowerCase();
      if (targetTable === "totals") {
        pricingTotalsExpanded = !pricingTotalsExpanded;
      } else if (targetTable === "handicap") {
        pricingHandicapExpanded = !pricingHandicapExpanded;
      } else {
        return;
      }
      rerender();
    });
  }

  const betBuilderMarketSelects = container.querySelectorAll("[id^='pricingBetBuilderLeg'][id$='Market']");
  for (const select of betBuilderMarketSelects) {
    if (!(select instanceof HTMLSelectElement)) continue;
    select.addEventListener("change", () => {
      const legNumber = Number(select.dataset.betbuilderLeg) === 2 ? 2 : 1;
      setBetBuilderLegMarket(legNumber, select.value);
      rerender();
    });
  }
  const betBuilderLineInputs = container.querySelectorAll("[id^='pricingBetBuilderLeg'][id$='Line']");
  for (const input of betBuilderLineInputs) {
    if (!(input instanceof HTMLInputElement)) continue;
    input.addEventListener("change", () => {
      const legNumber = Number(input.dataset.betbuilderLeg) === 2 ? 2 : 1;
      setBetBuilderLegLine(legNumber, input.value);
      rerender();
    });
  }
  const betBuilderBetSelects = container.querySelectorAll("[id^='pricingBetBuilderLeg'][id$='Bet']");
  for (const select of betBuilderBetSelects) {
    if (!(select instanceof HTMLSelectElement)) continue;
    select.addEventListener("change", () => {
      const legNumber = Number(select.dataset.betbuilderLeg) === 2 ? 2 : 1;
      setBetBuilderLegBet(legNumber, select.value);
      rerender();
    });
  }
  const betBuilderEdgeInput = container.querySelector("#pricingBetBuilderEdge");
  if (betBuilderEdgeInput instanceof HTMLInputElement) {
    const applyEdge = () => {
      pricingBetBuilderEdge = normalizePricingBetBuilderEdge(betBuilderEdgeInput.value);
      rerender();
    };
    betBuilderEdgeInput.addEventListener("change", applyEdge);
    betBuilderEdgeInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      pricingBetBuilderEdge = normalizePricingBetBuilderEdge(betBuilderEdgeInput.value);
      betBuilderEdgeInput.blur();
      rerender();
    });
  }
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
    home_prev_season_rows: Array.isArray(rowsObj?.home_prev_season_rows) ? rowsObj.home_prev_season_rows : [],
    away_prev_season_rows: Array.isArray(rowsObj?.away_prev_season_rows) ? rowsObj.away_prev_season_rows : [],
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
  const selectedMarketCacheKey = selectedMarketId ? buildModeScopedCacheKey(selectedMarketId) : "";
  const cachedHcPerfPayload = selectedMarketCacheKey ? hcPerfPayloadByMarket.get(selectedMarketCacheKey) : null;
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
  const hcPerfLoading = Boolean(selectedMarketCacheKey) && hcPerfLoadingMarketId === selectedMarketCacheKey;
  const hcPerfRescanLoading = hcPerfRescanInFlight;
  const hcPerfBusy = hcPerfLoading || hcPerfRescanLoading;
  const hcPerfErrorText = String(cachedHcPerfPayload?.error || "").trim();
  const activeIsAway = recentTeamView === "away";
  const activeLabel = activeIsAway ? awayLabel : homeLabel;
  const activeRows = activeIsAway ? awayRecentRows : homeRecentRows;
  const activeVenueRows = activeIsAway ? awayTeamVenueRows : homeTeamVenueRows;
  const activeVenueSeedRows = normalizeRollingSeedRowsByVenue(activeVenueRows);
  const activeRollingSeedRows = activeIsAway ? activeVenueSeedRows.away : activeVenueSeedRows.home;
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
  trendlineDegree = clampTrendlineDegree(trendlineDegree);
  const shownRows = activeRows.slice(0, gamesShownCount);
  const teamSummaryTables = buildTeamXgdSummaryTablesHtml(homeLabel, awayLabel, homeRecentRows, awayRecentRows);
  const recentMatchesSection = `
    <h3 class="section-title">Model Source Matches</h3>
    ${buildGamesShownControlHtml(gamesShownCount, maxGamesShown, maxRollingWindow)}
    ${buildRecentSwitchHtml(homeLabel, awayLabel)}
    ${buildRecentAveragesTableHtml(activeRows, gamesShownCount, activeLabel)}
    ${showTrendCharts
    ? buildMetricTrendPlotHtml(
      shownRows,
      "Venue games trend",
      activeLabel,
      rollingWindowCount,
      {
        rollingSeedRows: activeRollingSeedRows,
        trendlineDegree,
        showTrendlineControl: true,
        trendlineControlClass: "trendline-degree-input-main",
      }
    )
    : ""}
    ${buildRecentMatchesTableHtml("Fixture-side matches", shownRows, activeLabel)}
    ${buildVenueSplitSectionHtml(
      activeLabel,
      activeHomeVenueRows,
      activeAwayVenueRows,
      rollingWindowCount,
      showTrendCharts,
      activeVenueSeedRows,
      trendlineDegree
    )}
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
  const shotsTabContent = `
    <h3 class="section-title">Shots</h3>
    ${buildStatsGamesShownControlHtml(statsVenueGamesShownCount, statsVenueMaxGamesShown, {
      inputId: "statsVenueGamesShownInput",
      label: "Venue games shown",
    })}
    ${buildShotsAveragesTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsVenueGamesShownCount)}
    ${buildShotGamestateTableHtml(homeRecentRows, awayRecentRows, homeLabel, awayLabel, statsVenueGamesShownCount)}
    ${buildStatsSwitchHtml(homeLabel, awayLabel)}
    ${buildShotsMatchesTableHtml(
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
    ${buildShotsAveragesTableHtml(homeGeneralRows, awayGeneralRows, homeLabel, awayLabel, statsAllGamesShownCount)}
    ${buildShotGamestateTableHtml(homeGeneralRows, awayGeneralRows, homeLabel, awayLabel, statsAllGamesShownCount)}
    ${buildStatsGeneralSwitchHtml(homeLabel, awayLabel)}
    <h3 class="section-title">${escapeHtml(statsGeneralActiveLabel)}: Home & Away Matches</h3>
    ${buildShotsMatchesTableHtml(
      statsGeneralActiveLabel,
      statsGeneralActiveRows,
      statsAllGamesShownCount,
      "All matches"
    )}
  `;
  const pricingSummary = getPricingSummaryForPeriod(activeView, payload, pricingPeriod);
  const pricingTabContent = buildGamePricingTabHtml(
    homeLabel,
    awayLabel,
    homeGeneralRows,
    awayGeneralRows,
    {
      projectedHomeXg: pricingSummary?.home_xg,
      projectedAwayXg: pricingSummary?.away_xg,
      requireProjectedXg: true,
    }
  );
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
  if (
    detailsMainTab !== "cards"
    && detailsMainTab !== "shots"
    && detailsMainTab !== "pricing"
    && detailsMainTab !== "hcperf"
  ) {
    detailsMainTab = "xgd";
  }
  const detailsTabNav = `
    <section class="page-tabs details-main-tabs">
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "xgd" ? "active" : ""}" data-details-tab="xgd">xGD</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "cards" ? "active" : ""}" data-details-tab="cards">Stats</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "shots" ? "active" : ""}" data-details-tab="shots">Shots</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "pricing" ? "active" : ""}" data-details-tab="pricing">Pricing</button>
      <button type="button" class="tab-btn details-main-tab ${detailsMainTab === "hcperf" ? "active" : ""}" data-details-tab="hcperf">HC Perf</button>
    </section>
  `;
  const activeTabContent = detailsMainTab === "cards"
    ? cardsTabContent
    : (detailsMainTab === "shots"
      ? shotsTabContent
      : (detailsMainTab === "pricing"
        ? pricingTabContent
        : (detailsMainTab === "hcperf" ? hcPerfTabContent : xgdTabContent)));
  linesContainer.innerHTML = `${detailsTabNav}${viewTabsHtml}${activeTabContent}`;
  bindTeamLinkButtons(linesContainer);

  if (detailsMainTab === "hcperf" && selectedMarketId && !cachedHcPerfPayload && hcPerfLoadingMarketId !== selectedMarketId) {
    loadGameHcPerf(selectedMarketId);
  }

  const detailsTabButtons = linesContainer.querySelectorAll(".details-main-tab");
  for (const button of detailsTabButtons) {
    button.addEventListener("click", () => {
      const rawTargetTab = String(button.dataset.detailsTab || "").trim().toLowerCase();
      const targetTab = rawTargetTab === "cards"
        || rawTargetTab === "shots"
        || rawTargetTab === "pricing"
        || rawTargetTab === "hcperf"
        ? rawTargetTab
        : "xgd";
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
  const trendlineDegreeInputsInline = linesContainer.querySelectorAll(".trendline-degree-input-main");
  for (const input of trendlineDegreeInputsInline) {
    if (!(input instanceof HTMLInputElement)) continue;
    input.addEventListener("change", () => {
      trendlineDegree = clampTrendlineDegree(input.value);
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

  bindPricingControls(linesContainer, () => {
    if (lastXgdPayload) renderXgd(lastXgdPayload);
  });
}

function closeTeamDetailsPanel() {
  if (teamDetailsPanel) {
    teamDetailsPanel.classList.add("hidden");
  }
  teamDetailsLoadingKey = "";
}

function getTeamPageCacheKey(teamName, competitionName = "", seasonKey = "", xgMode = getCurrentXgMetricMode()) {
  return [
    normalizeXgMetricMode(xgMode, "xg"),
    String(teamName || "").trim().toLowerCase(),
    String(competitionName || "").trim().toLowerCase(),
    String(seasonKey || "").trim().toLowerCase(),
  ].join("::");
}

function cacheTeamPagePayloadForMode(teamName, competitionName, seasonKey, metricMode, payload) {
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  const seasonText = String(seasonKey || "").trim();
  const modeKey = normalizeXgMetricMode(metricMode, "xg");
  const nextPayload = payload && typeof payload === "object" ? payload : {};
  const resolvedTeam = String(nextPayload?.team || "").trim();
  const resolvedCompetition = String(nextPayload?.competition || "").trim();
  const resolvedSeason = String(nextPayload?.season || "").trim();
  teamPagePayloadByKey.set(
    getTeamPageCacheKey(teamText, competitionText, seasonText, modeKey),
    nextPayload
  );
  if (resolvedCompetition || resolvedSeason) {
    teamPagePayloadByKey.set(
      getTeamPageCacheKey(
        teamText,
        resolvedCompetition || competitionText,
        resolvedSeason || seasonText,
        modeKey
      ),
      nextPayload
    );
  }
  if (resolvedTeam) {
    teamPagePayloadByKey.set(
      getTeamPageCacheKey(
        resolvedTeam,
        resolvedCompetition || competitionText,
        resolvedSeason || seasonText,
        modeKey
      ),
      nextPayload
    );
  }
  return {
    payload: nextPayload,
    resolvedTeam,
    resolvedCompetition,
    resolvedSeason,
  };
}

async function prefetchAlternateTeamPage(teamName, competitionName = "", seasonKey = "", currentMode = getCurrentXgMetricMode()) {
  const teamText = String(teamName || "").trim();
  if (!teamText) return;
  const competitionText = String(competitionName || "").trim();
  const seasonText = String(seasonKey || "").trim();
  const alternateMode = getAlternateXgMetricMode(currentMode);
  const cacheKey = getTeamPageCacheKey(teamText, competitionText, seasonText, alternateMode);
  if (teamPagePayloadByKey.has(cacheKey)) return;
  if (teamPagePrefetchInFlight.has(cacheKey)) return;
  teamPagePrefetchInFlight.add(cacheKey);
  try {
    const query = new URLSearchParams();
    query.set("team", teamText);
    if (competitionText) {
      query.set("competition", competitionText);
    }
    if (seasonText) {
      query.set("season", seasonText);
    }
    query.set("xg_mode", alternateMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/team-page?${query.toString()}`);
    if (!res.ok) return;
    cacheTeamPagePayloadForMode(teamText, competitionText, seasonText, alternateMode, payload);
  } catch (_err) {
    // Ignore prefetch failures to avoid disrupting active UI paths.
  } finally {
    teamPagePrefetchInFlight.delete(cacheKey);
  }
}

async function prefetchAlternateGameXgd(marketId, currentMode = getCurrentXgMetricMode()) {
  const marketKey = String(marketId || "").trim();
  if (!marketKey) return;
  const alternateMode = getAlternateXgMetricMode(currentMode);
  const cacheKey = buildModeScopedCacheKey(marketKey, alternateMode);
  if (xgdPayloadByMarket.has(cacheKey)) return;
  if (gameXgdPrefetchInFlight.has(cacheKey)) return;
  gameXgdPrefetchInFlight.add(cacheKey);
  try {
    const query = new URLSearchParams();
    query.set("market_id", marketKey);
    query.set("xg_mode", alternateMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/game-xgd?${query.toString()}`);
    if (!res.ok) return;
    xgdPayloadByMarket.set(cacheKey, payload || {});
  } catch (_err) {
    // Ignore prefetch failures to avoid disrupting active UI paths.
  } finally {
    gameXgdPrefetchInFlight.delete(cacheKey);
  }
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

function buildTeamShotsSummaryTableHtml(teamLabel, rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    return `
      <section class="recent-team-block">
        <h4>${escapeHtml(teamLabel || "Team")} - Shots Summary</h4>
        <p class="recent-empty">No season shot data available.</p>
      </section>
    `;
  }
  const shotsForTotal = sumMetric(safeRows, "shots_for");
  const shotsAgainstTotal = sumMetric(safeRows, "shots_against");
  const xgForTotal = sumMetric(safeRows, "xG");
  const xgAgainstTotal = sumMetric(safeRows, "xGA");
  const xgPerShot = (
    Number.isFinite(xgForTotal)
    && Number.isFinite(shotsForTotal)
    && shotsForTotal > 0
  )
    ? (xgForTotal / shotsForTotal)
    : null;
  const xgaPerShotAgainst = (
    Number.isFinite(xgAgainstTotal)
    && Number.isFinite(shotsAgainstTotal)
    && shotsAgainstTotal > 0
  )
    ? (xgAgainstTotal / shotsAgainstTotal)
    : null;
  return `
    <section class="recent-team-block">
      <h4>${escapeHtml(teamLabel || "Team")} - Shots Summary</h4>
      <div class="recent-table-wrap">
        <table class="lines-table recent-lines-table">
          <thead>
            <tr>
              <th>Games</th>
              <th>Shots For</th>
              <th>Shots Against</th>
              <th>On Target For</th>
              <th>On Target Against</th>
              <th>xG For</th>
              <th>xG Against</th>
              <th>xG/Shot</th>
              <th>xGA/ShotA</th>
              <th>xGoT For</th>
              <th>xGoT Against</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${safeRows.length}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "shots_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "shots_against"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "shots_on_target_for"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "shots_on_target_against"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "xG"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "xGA"), 2)}</td>
              <td>${formatMetricValue(xgPerShot, 3)}</td>
              <td>${formatMetricValue(xgaPerShotAgainst, 3)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "xGoT"), 2)}</td>
              <td>${formatMetricValue(averageMetric(safeRows, "xGoTA"), 2)}</td>
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
  const selectedSeason = String(payload?.season || teamDetailsSeason || "").trim();
  const competitions = Array.isArray(payload?.competitions) ? payload.competitions : [];
  const seasons = Array.isArray(payload?.seasons) ? payload.seasons : [];
  const recentRows = Array.isArray(payload?.recent_rows) ? payload.recent_rows : [];
  const teamVenueRows = payload?.team_venue_rows && typeof payload.team_venue_rows === "object"
    ? payload.team_venue_rows
    : { home: [], away: [] };
  const homeVenueRows = Array.isArray(teamVenueRows?.home) ? teamVenueRows.home : [];
  const awayVenueRows = Array.isArray(teamVenueRows?.away) ? teamVenueRows.away : [];
  const teamVenueSeedRows = normalizeRollingSeedRowsByVenue(teamVenueRows);
  const allVenueSeedRows = mergeRollingSeedRowsByVenue(teamVenueSeedRows);
  const seasonHandicapRows = Array.isArray(payload?.season_handicap_rows) ? payload.season_handicap_rows : [];
  const homeHcRows = seasonHandicapRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "home");
  const awayHcRows = seasonHandicapRows.filter((row) => String(row?.venue || "").trim().toLowerCase() === "away");
  const selectedSeasonEntry = seasons.find((entry) => String(entry?.season || "").trim() === selectedSeason) || null;
  const selectedSeasonLabel = String(selectedSeasonEntry?.season_label || selectedSeason || "").trim();
  teamDetailsCompetition = selectedCompetition;
  teamDetailsSeason = selectedSeason;
  teamDetailsTitle.textContent = `${teamText}`;
  teamDetailsMeta.textContent = `${selectedCompetition || "Selected leagues"} | ${selectedSeasonLabel || "All seasons"} | ${recentRows.length} games`;

  if (teamDetailsSeasonSelect instanceof HTMLSelectElement) {
    teamDetailsSeasonSelect.innerHTML = "";
    if (!seasons.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = selectedSeasonLabel || "No season data";
      teamDetailsSeasonSelect.appendChild(option);
      teamDetailsSeasonSelect.disabled = true;
    } else {
      seasons.forEach((entry) => {
        const seasonValue = String(entry?.season || "").trim();
        if (!seasonValue) return;
        const option = document.createElement("option");
        option.value = seasonValue;
        const seasonLabel = String(entry?.season_label || seasonValue).trim();
        const gamesCount = Number(entry?.games_count) || 0;
        option.textContent = `${seasonLabel}${gamesCount ? ` (${gamesCount})` : ""}`;
        if (seasonValue === selectedSeason) {
          option.selected = true;
        }
        teamDetailsSeasonSelect.appendChild(option);
      });
      if (!teamDetailsSeasonSelect.value && selectedSeason) {
        teamDetailsSeasonSelect.value = selectedSeason;
      }
      teamDetailsSeasonSelect.disabled = false;
    }
  }

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

  if (teamDetailsMainTab !== "stats" && teamDetailsMainTab !== "shots" && teamDetailsMainTab !== "hcperf") {
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
  const xgTrendlineDegree = clampTrendlineDegree(teamDetailsTrendlineDegree);
  teamDetailsXgGamesShownCount = xgGamesShown;
  teamDetailsXgRollingWindowCount = xgRollingWindow;
  teamDetailsTrendlineDegree = xgTrendlineDegree;
  const limitedAllRows = recentRows.slice(0, xgGamesShown);
  const limitedHomeRows = homeVenueRows.slice(0, xgGamesShown);
  const limitedAwayRows = awayVenueRows.slice(0, xgGamesShown);
  const buildTeamXgSectionHtml = (sectionTitle, rows, tableTitle, rollingSeedRows = []) => `
    <h3 class="section-title">${escapeHtml(sectionTitle)}</h3>
    ${buildRecentAveragesTableHtml(rows, Math.max(1, rows.length || 1), teamText)}
    ${buildMetricTrendPlotHtml(
    rows,
    `${sectionTitle} trend`,
    teamText,
    xgRollingWindow,
    {
      rollingSeedRows,
      trendlineDegree: xgTrendlineDegree,
      showTrendlineControl: true,
      trendlineControlClass: "trendline-degree-input-team",
    }
  )}
    ${buildRecentMatchesTableHtml(tableTitle, rows, teamText)}
  `;
  const xgTabContent = `
    <h3 class="section-title">Season Games With xG</h3>
    ${buildTeamXgControlsHtml(xgGamesShown, maxTeamXgGames, xgRollingWindow, xgGamesShown)}
    ${buildTeamXgSectionHtml("All Games", limitedAllRows, "All games", allVenueSeedRows)}
    ${buildTeamXgSectionHtml("Home Games", limitedHomeRows, "Home games", teamVenueSeedRows.home)}
    ${buildTeamXgSectionHtml("Away Games", limitedAwayRows, "Away games", teamVenueSeedRows.away)}
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
  const shotsTabContent = `
    <h3 class="section-title">Season Shots</h3>
    ${buildTeamShotsSummaryTableHtml(teamText, recentRows)}
    ${buildSingleTeamShotGamestateTableHtml(recentRows, `${teamText} (General)`)}
    <h3 class="section-title">Venue Split</h3>
    ${buildShotsAveragesTableHtml(
      homeVenueRows,
      awayVenueRows,
      `${teamText} (Home)`,
      `${teamText} (Away)`
    )}
    ${buildShotGamestateTableHtml(
      homeVenueRows,
      awayVenueRows,
      `${teamText} (Home)`,
      `${teamText} (Away)`
    )}
    ${buildShotsMatchesTableHtml(teamText, homeVenueRows, null, "Home games")}
    ${buildShotsMatchesTableHtml(teamText, awayVenueRows, null, "Away games")}
    <h3 class="section-title">All Games</h3>
    ${buildShotsMatchesTableHtml(teamText, recentRows, null, "Season games")}
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
      <button type="button" class="tab-btn team-details-main-tab ${teamDetailsMainTab === "shots" ? "active" : ""}" data-team-details-tab="shots">Shots</button>
      <button type="button" class="tab-btn team-details-main-tab ${teamDetailsMainTab === "hcperf" ? "active" : ""}" data-team-details-tab="hcperf">HC Perf</button>
    </section>
  `;
  const activeTabContent = teamDetailsMainTab === "stats"
    ? statsTabContent
    : (teamDetailsMainTab === "shots"
      ? shotsTabContent
      : (teamDetailsMainTab === "hcperf" ? hcPerfTabContent : xgTabContent));
  teamDetailsContent.innerHTML = `${teamTabNav}${activeTabContent}`;
  bindTeamLinkButtons(teamDetailsContent);

  const tabButtons = teamDetailsContent.querySelectorAll(".team-details-main-tab");
  tabButtons.forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) return;
    button.addEventListener("click", () => {
      const nextTabRaw = String(button.dataset.teamDetailsTab || "").trim().toLowerCase();
      const nextTab = nextTabRaw === "stats" || nextTabRaw === "shots" || nextTabRaw === "hcperf"
        ? nextTabRaw
        : "xg";
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
  const teamXgTrendlineDegreeInputs = teamDetailsContent.querySelectorAll(".trendline-degree-input-team");
  for (const input of teamXgTrendlineDegreeInputs) {
    if (!(input instanceof HTMLInputElement)) continue;
    input.addEventListener("change", () => {
      teamDetailsTrendlineDegree = clampTrendlineDegree(input.value);
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

async function openTeamPage(teamName, competitionName = null, seasonKey = null, options = {}) {
  if (!teamDetailsPanel || !teamDetailsContent || !teamDetailsTitle || !teamDetailsMeta) return;
  const teamText = String(teamName || "").trim();
  const competitionText = String(competitionName || "").trim();
  const seasonText = String(seasonKey || "").trim();
  const metricMode = getCurrentXgMetricMode();
  const forceRefresh = Boolean(options?.force);
  if (!teamText) return;

  const isDifferentSelection = (
    teamText !== teamDetailsTeam
    || competitionText !== teamDetailsCompetition
    || seasonText !== teamDetailsSeason
  );
  if (isDifferentSelection) {
    teamDetailsMainTab = "xg";
    teamDetailsXgGamesShownCount = TEAM_DETAILS_XG_GAMES_DEFAULT;
    teamDetailsXgRollingWindowCount = TEAM_DETAILS_XG_ROLLING_DEFAULT;
    teamDetailsTrendlineDegree = TRENDLINE_DEGREE_DEFAULT;
  }
  teamDetailsTeam = teamText;
  teamDetailsCompetition = competitionText;
  teamDetailsSeason = seasonText;
  teamDetailsPanel.classList.remove("hidden");
  teamDetailsTitle.textContent = teamText;
  const loadingParts = [];
  if (competitionText) loadingParts.push(competitionText);
  if (seasonText) loadingParts.push(seasonText);
  loadingParts.push("Loading...");
  teamDetailsMeta.textContent = loadingParts.join(" | ");
  teamDetailsContent.innerHTML = `
    <div class="hcperf-loading">
      <span class="hcperf-loading-dot" aria-hidden="true"></span>
      <span>Loading team page...</span>
    </div>
  `;

  const cacheKey = getTeamPageCacheKey(teamText, competitionText, seasonText, metricMode);
  if (!forceRefresh && teamPagePayloadByKey.has(cacheKey)) {
    const cachedPayload = teamPagePayloadByKey.get(cacheKey);
    if (cachedPayload && typeof cachedPayload === "object") {
      const resolvedTeam = String(cachedPayload?.team || "").trim();
      const resolvedCompetition = String(cachedPayload?.competition || "").trim();
      const resolvedSeason = String(cachedPayload?.season || "").trim();
      if (resolvedTeam) {
        teamDetailsTeam = resolvedTeam;
      }
      teamDetailsCompetition = resolvedCompetition || competitionText;
      teamDetailsSeason = resolvedSeason || seasonText;
      teamDetailsPayload = cachedPayload;
      renderTeamDetailsPanel(cachedPayload);
      void prefetchAlternateTeamPage(
        resolvedTeam || teamText,
        resolvedCompetition || competitionText,
        resolvedSeason || seasonText,
        metricMode
      );
      return;
    }
  }

  const requestKey = `${metricMode}::${teamText}::${competitionText}::${seasonText}`;
  teamDetailsLoadingKey = requestKey;
  try {
    const query = new URLSearchParams();
    query.set("team", teamText);
    if (competitionText) {
      query.set("competition", competitionText);
    }
    if (seasonText) {
      query.set("season", seasonText);
    }
    query.set("xg_mode", metricMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/team-page?${query.toString()}`);
    if (!res.ok) throw new Error(payload.error || "Failed to load team page");
    if (teamDetailsLoadingKey !== requestKey) return;
    const cached = cacheTeamPagePayloadForMode(teamText, competitionText, seasonText, metricMode, payload);
    const nextPayload = cached.payload;
    const resolvedTeam = cached.resolvedTeam;
    const resolvedCompetition = cached.resolvedCompetition;
    const resolvedSeason = cached.resolvedSeason;
    if (resolvedTeam) {
      teamDetailsTeam = resolvedTeam;
    }
    teamDetailsPayload = nextPayload;
    teamDetailsCompetition = resolvedCompetition || competitionText;
    teamDetailsSeason = resolvedSeason || seasonText;
    renderTeamDetailsPanel(nextPayload);
    void prefetchAlternateTeamPage(
      resolvedTeam || teamText,
      resolvedCompetition || competitionText,
      resolvedSeason || seasonText,
      metricMode
    );
  } catch (err) {
    if (teamDetailsLoadingKey !== requestKey) return;
    const errorParts = [];
    if (competitionText) errorParts.push(competitionText);
    if (seasonText) errorParts.push(seasonText);
    errorParts.push("Error");
    teamDetailsMeta.textContent = errorParts.join(" | ");
    teamDetailsContent.innerHTML = `<p>${escapeHtml(String(err.message || err))}</p>`;
  }
}

async function loadGameHcPerf(marketId, force = false) {
  const key = String(marketId || "").trim();
  const metricMode = getCurrentXgMetricMode();
  const cacheKey = buildModeScopedCacheKey(key, metricMode);
  if (!key) return;
  if (!force && hcPerfPayloadByMarket.has(cacheKey)) return;
  if (hcPerfLoadingMarketId === cacheKey) return;

  hcPerfLoadingMarketId = cacheKey;
  if (selectedMarketId === key && detailsMainTab === "hcperf" && lastXgdPayload) {
    renderXgd(lastXgdPayload);
  }
  try {
    const query = new URLSearchParams();
    query.set("market_id", key);
    query.set("xg_mode", metricMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/game-hcperf?${query.toString()}`);
    if (!res.ok) throw new Error(payload.error || "Failed to load HC performance");
    hcPerfPayloadByMarket.set(cacheKey, payload || {});
  } catch (err) {
    hcPerfPayloadByMarket.set(cacheKey, {
      season_handicap_rows: { home: [], away: [] },
      error: String(err.message || err),
    });
  } finally {
    if (hcPerfLoadingMarketId === cacheKey) {
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
  const metricMode = getCurrentXgMetricMode();
  const cacheKey = buildModeScopedCacheKey(key, metricMode);
  const requestKey = `${cacheKey}`;

  selectedMarketId = marketId;
  if (isSavedGamesViewActive()) {
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
  trendlineDegree = TRENDLINE_DEGREE_DEFAULT;
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
  pricingTotalsExpanded = false;
  pricingHandicapExpanded = false;
  activeXgdViewId = null;
  lastXgdPayload = null;
  hcPerfLoadingMarketId = null;
  gameXgdLoadingKey = requestKey;
  const scoreMeta = game.is_historical ? ` | FT ${String(game.scoreline || "-")}` : "";
  detailsMeta.textContent = `${game.competition} | ${formatGameKickoffLocalDateTime(game)}${scoreMeta}`;

  const cachedPayload = xgdPayloadByMarket.get(cacheKey);
  if (cachedPayload && typeof cachedPayload === "object") {
    renderXgd(cachedPayload);
    const updatedMainTableMetrics = applyCalculatedXgdToMainTable(marketId, cachedPayload);
    if (updatedMainTableMetrics) {
      if (isSavedGamesViewActive()) {
        renderSavedGames();
      } else {
        renderCurrentDay();
      }
    }
    void prefetchAlternateGameXgd(marketId, metricMode);
    return;
  }

  linesContainer.innerHTML = "<p>Calculating xGD...</p>";

  try {
    const query = new URLSearchParams();
    query.set("market_id", String(marketId || ""));
    query.set("xg_mode", metricMode);
    const { res, payload } = await fetchJsonWithTimeout(`/api/game-xgd?${query.toString()}`);
    if (!res.ok) throw new Error(payload.error || "Failed to load xGD");
    if (gameXgdLoadingKey !== requestKey) return;
    xgdPayloadByMarket.set(cacheKey, payload || {});
    renderXgd(payload);
    const updatedMainTableMetrics = applyCalculatedXgdToMainTable(marketId, payload);
    if (updatedMainTableMetrics) {
      if (isSavedGamesViewActive()) {
        renderSavedGames();
      } else {
        renderCurrentDay();
      }
    }
    void prefetchAlternateGameXgd(marketId, metricMode);
  } catch (err) {
    if (gameXgdLoadingKey !== requestKey) return;
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
  gameXgdLoadingKey = "";
  if (!clearSelection) return;
  if (!selectedMarketId) return;
  selectedMarketId = null;
  if (isSavedGamesViewActive()) {
    renderSavedGames();
  } else if (isGamesCalendarTab()) {
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

async function requestHardRefreshXgd() {
  if (!(hardRefreshXgdBtn instanceof HTMLButtonElement)) return;

  const originalHardRefreshText = hardRefreshXgdBtn.textContent || "Hard Refresh xGD";
  const disableOddsRefresh = refreshBtn instanceof HTMLButtonElement;
  hardRefreshXgdBtn.disabled = true;
  hardRefreshXgdBtn.textContent = "Refreshing...";
  if (disableOddsRefresh) {
    refreshBtn.disabled = true;
  }
  statusText.textContent = "Hard refreshing xGD data from source database...";

  try {
    const res = await fetch("/api/hard-refresh-xgd", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const payload = await parseApiResponse(res);
    if (!res.ok) throw new Error(payload.error || "Failed to hard refresh xGD data");

    // Clear client-side caches so all detail panels and rankings re-query fresh values.
    xgdPayloadByMarket.clear();
    hcPerfPayloadByMarket.clear();
    teamPagePayloadByKey.clear();
    gamesPayloadByModeAndView.clear();
    gamesPayloadPrefetchInFlight.clear();
    gameXgdPrefetchInFlight.clear();
    teamPagePrefetchInFlight.clear();
    matchupPayload = null;
    matchupErrorText = "";
    teamDetailsPayload = null;
    teamHcPerfDetailRows = [];
    teamHcRankingsLoaded = false;
    teamsDirectoryLoaded = false;
    historicalDayAutoCalcAttempted.clear();

    await loadGames();

    const upcomingCount = Number(payload?.upcoming_games_count) || 0;
    const historicalCount = Number(payload?.historical_games_count) || 0;
    statusText.textContent = `Hard refresh complete: ${upcomingCount} upcoming and ${historicalCount} historical games recalculated`;
  } catch (err) {
    statusText.textContent = String(err.message || err);
  } finally {
    hardRefreshXgdBtn.disabled = false;
    hardRefreshXgdBtn.textContent = originalHardRefreshText;
    if (disableOddsRefresh) {
      refreshBtn.disabled = false;
    }
  }
}

refreshBtn.addEventListener("click", () => loadGames());
if (hardRefreshXgdBtn instanceof HTMLButtonElement) {
  hardRefreshXgdBtn.addEventListener("click", () => {
    requestHardRefreshXgd();
  });
}
if (exitAppBtn instanceof HTMLButtonElement) {
  exitAppBtn.addEventListener("click", () => {
    requestAppExit();
  });
}
gamesTabBtn.addEventListener("click", () => {
  activeGamesPaneView = "main";
  setActiveTab("games");
});
if (modellingPricesTabBtn instanceof HTMLButtonElement) {
  modellingPricesTabBtn.addEventListener("click", () => {
    activeGamesPaneView = "main";
    setActiveTab("modelling");
  });
}
if (gamesMainSubTabBtn instanceof HTMLButtonElement) {
  gamesMainSubTabBtn.addEventListener("click", () => {
    setGamesPaneView("main");
  });
}
if (savedGamesSubTabBtn instanceof HTMLButtonElement) {
  savedGamesSubTabBtn.addEventListener("click", () => {
    setGamesPaneView("saved");
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
if (matchupTabBtn instanceof HTMLButtonElement) {
  matchupTabBtn.addEventListener("click", () => {
    setActiveTab("matchup");
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
if (matchupRunBtn instanceof HTMLButtonElement) {
  matchupRunBtn.addEventListener("click", () => {
    loadMatchup();
  });
}
for (const input of [matchupHomeInput, matchupAwayInput]) {
  if (!(input instanceof HTMLInputElement)) continue;
  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    loadMatchup();
  });
}
if (matchupCompetitionSelect instanceof HTMLSelectElement) {
  matchupCompetitionSelect.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    loadMatchup();
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
    if (mappingSubTab === "competitions") {
      saveSelectedManualCompetitionMappings();
      return;
    }
    saveSelectedManualTeamMappings();
  });
}
if (mappingClearSelectedBtn instanceof HTMLButtonElement) {
  mappingClearSelectedBtn.addEventListener("click", () => {
    if (mappingSubTab === "competitions") {
      clearAllCompetitionMappingSelections();
    } else {
      clearAllTeamMappingSelections();
    }
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
if (teamHcRankingsSeasonSelect instanceof HTMLSelectElement) {
  teamHcRankingsSeasonSelect.addEventListener("change", () => {
    const nextSeason = String(teamHcRankingsSeasonSelect.value || "").trim();
    if (nextSeason === selectedTeamHcRankingsSeason) return;
    selectedTeamHcRankingsSeason = nextSeason;
    closeTeamHcPerfPanel();
    loadTeamHcRankings();
  });
}
if (teamHcRankingsCompetitionSelect instanceof HTMLSelectElement) {
  teamHcRankingsCompetitionSelect.addEventListener("change", () => {
    const nextCompetition = String(teamHcRankingsCompetitionSelect.value || "").trim();
    if (!nextCompetition || nextCompetition === selectedTeamHcRankingsCompetition) return;
    setSelectedTeamHcRankingsCompetition(nextCompetition);
    const competitionSeasons = getTeamHcRankingsSeasonsForCompetition(nextCompetition);
    selectedTeamHcRankingsSeason = String(competitionSeasons[0]?.season || "").trim();
    closeTeamHcPerfPanel();
    loadTeamHcRankings();
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
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeSettingsSidebar();
    setLeagueFilterOpen(false);
    setTierFilterOpen(false);
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
    if (isSavedGamesViewActive()) {
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
    openTeamPage(teamDetailsTeam, nextCompetition || null, teamDetailsSeason || null);
  });
}
if (teamDetailsSeasonSelect instanceof HTMLSelectElement) {
  teamDetailsSeasonSelect.addEventListener("change", () => {
    const nextSeason = String(teamDetailsSeasonSelect.value || "").trim();
    if (!teamDetailsTeam) return;
    openTeamPage(teamDetailsTeam, teamDetailsCompetition || null, nextSeason || null);
  });
}
if (saveGameBtn instanceof HTMLButtonElement) {
  saveGameBtn.addEventListener("click", () => {
    toggleSelectedGameSaved();
  });
}

xgMetricMode = getStoredXgMetricMode();
xgPushThreshold = getStoredXgPushThreshold();
xgdHcHighlightEnabled = false;
showGamesWithoutHandicap = getStoredShowGamesWithoutHandicap();
modellingPriceEdge = getStoredModellingPriceEdge();
applyPricingPeriod(loadStoredPricingPeriod(), { silent: true, rerender: false });
updateXgMetricModeToggleButton();
if (xgMetricModeToggleBtn instanceof HTMLButtonElement) {
  xgMetricModeToggleBtn.addEventListener("click", () => {
    const nextMode = getCurrentXgMetricMode() === "xg" ? "npxg" : "xg";
    void applyGlobalXgMetricMode(nextMode);
  });
}
if (detailsXgMetricModeToggleBtn instanceof HTMLButtonElement) {
  detailsXgMetricModeToggleBtn.addEventListener("click", () => {
    const nextMode = getCurrentXgMetricMode() === "xg" ? "npxg" : "xg";
    void applyGlobalXgMetricMode(nextMode);
  });
}
if (teamDetailsXgMetricModeToggleBtn instanceof HTMLButtonElement) {
  teamDetailsXgMetricModeToggleBtn.addEventListener("click", () => {
    const nextMode = getCurrentXgMetricMode() === "xg" ? "npxg" : "xg";
    void applyGlobalXgMetricMode(nextMode);
  });
}
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
if (modelEdgeInput instanceof HTMLInputElement) {
  modelEdgeInput.value = formatModellingPriceEdgeForInput(modellingPriceEdge);
  modelEdgeInput.addEventListener("change", () => {
    applyGlobalModellingPriceEdge(modelEdgeInput.value);
  });
  modelEdgeInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    applyGlobalModellingPriceEdge(modelEdgeInput.value);
    modelEdgeInput.blur();
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
applyColorTheme(getStoredColorTheme(), false);
if (themeToggleBtn instanceof HTMLButtonElement) {
  themeToggleBtn.addEventListener("click", () => {
    applyColorTheme(colorTheme === "dark" ? "light" : "dark");
  });
}
if (settingsOpenBtn instanceof HTMLButtonElement) {
  settingsOpenBtn.addEventListener("click", () => {
    setSettingsSidebarOpen(true);
  });
}
if (settingsCloseBtn instanceof HTMLButtonElement) {
  settingsCloseBtn.addEventListener("click", () => {
    closeSettingsSidebar();
  });
}
if (settingsOverlay instanceof HTMLElement) {
  settingsOverlay.addEventListener("click", () => {
    closeSettingsSidebar();
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
