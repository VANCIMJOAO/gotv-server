package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MatchPhase representa a fase atual da partida
type MatchPhase string

const (
	PhaseIdle     MatchPhase = "idle"
	PhaseWarmup   MatchPhase = "warmup"
	PhaseKnife    MatchPhase = "knife"
	PhaseLive     MatchPhase = "live"
	PhaseHalftime MatchPhase = "halftime"
	PhaseOvertime MatchPhase = "overtime"
	PhasePaused   MatchPhase = "paused"
	PhaseFinished MatchPhase = "finished"
)

// MatchZyEventType tipos de eventos do MatchZy
type MatchZyEventType string

const (
	EventSeriesStart        MatchZyEventType = "series_start"
	EventGoingLive          MatchZyEventType = "going_live"
	EventRoundEnd           MatchZyEventType = "round_end"
	EventMapResult          MatchZyEventType = "map_result"
	EventSeriesEnd          MatchZyEventType = "series_end"
	EventSidePicked         MatchZyEventType = "side_picked"
	EventMapPicked          MatchZyEventType = "map_picked"
	EventMapVetoed          MatchZyEventType = "map_vetoed"
	EventPlayerDisconnected MatchZyEventType = "player_disconnect"
	EventDemoUploadEnded    MatchZyEventType = "demo_upload_ended"
	EventPlayerDeath        MatchZyEventType = "player_death"
	EventBombPlanted        MatchZyEventType = "bomb_planted"
	EventBombDefused        MatchZyEventType = "bomb_defused"
)

// FlexibleString aceita tanto string quanto número no JSON unmarshal
type FlexibleString string

func (f *FlexibleString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*f = FlexibleString(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		*f = FlexibleString(n.String())
		return nil
	}
	return fmt.Errorf("matchid must be string or number, got: %s", string(data))
}

// --- Structs que correspondem ao formato REAL do MatchZy ---

// MatchZyEvent evento genérico do MatchZy (usado no histórico)
type MatchZyEvent struct {
	Event     MatchZyEventType       `json:"event"`
	MatchID   FlexibleString         `json:"matchid"`
	MapNumber int                    `json:"map_number,omitempty"`
	Timestamp string                 `json:"timestamp,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// MatchZyPlayerStats stats de um jogador enviados pelo MatchZy no round_end e map_result
type MatchZyPlayerStats struct {
	Kills             int `json:"kills"`
	Deaths            int `json:"deaths"`
	Assists           int `json:"assists"`
	FlashAssists      int `json:"flash_assists"`
	TeamKills         int `json:"team_kills"`
	Suicides          int `json:"suicides"`
	Damage            int `json:"damage"`
	UtilityDamage     int `json:"utility_damage"`
	EnemiesFlashed    int `json:"enemies_flashed"`
	FriendliesFlashed int `json:"friendlies_flashed"`
	KnifeKills        int `json:"knife_kills"`
	HeadshotKills     int `json:"headshot_kills"`
	RoundsPlayed      int `json:"rounds_played"`
	BombDefuses       int `json:"bomb_defuses"`
	BombPlants        int `json:"bomb_plants"`
	OneK              int `json:"1k"`
	TwoK              int `json:"2k"`
	ThreeK            int `json:"3k"`
	FourK             int `json:"4k"`
	FiveK             int `json:"5k"`
	OneV1             int `json:"1v1"`
	OneV2             int `json:"1v2"`
	OneV3             int `json:"1v3"`
	OneV4             int `json:"1v4"`
	OneV5             int `json:"1v5"`
	FirstKillsT       int `json:"first_kills_t"`
	FirstKillsCT      int `json:"first_kills_ct"`
	FirstDeathsT      int `json:"first_deaths_t"`
	FirstDeathsCT     int `json:"first_deaths_ct"`
	TradeKills        int `json:"trade_kills"`
	Kast              int `json:"kast"`
	Score             int `json:"score"`
	Mvp               int `json:"mvp"`
}

// MatchZyStatsPlayer um jogador com suas stats
type MatchZyStatsPlayer struct {
	SteamID string             `json:"steamid"`
	Name    string             `json:"name"`
	Stats   MatchZyPlayerStats `json:"stats"`
}

// MatchZyStatsTeam um time com seus jogadores e stats
type MatchZyStatsTeam struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	SeriesScore int                  `json:"series_score"`
	Score       int                  `json:"score"`
	ScoreCT     int                  `json:"score_ct"`
	ScoreT      int                  `json:"score_t"`
	Players     []MatchZyStatsPlayer `json:"players"`
}

// MatchZyWinner informacao do vencedor
type MatchZyWinner struct {
	Side string `json:"side"` // "ct" ou "t"
	Team string `json:"team"` // "team1" ou "team2"
}

// MatchZyGoingLive evento quando a partida comeca
type MatchZyGoingLive struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   FlexibleString   `json:"matchid"`
	MapNumber int              `json:"map_number"`
}

// MatchZyRoundEnd evento de fim de round
type MatchZyRoundEnd struct {
	Event       MatchZyEventType `json:"event"`
	MatchID     FlexibleString   `json:"matchid"`
	MapNumber   int              `json:"map_number"`
	RoundNumber int              `json:"round_number"`
	RoundTime   int              `json:"round_time"`
	Reason      int              `json:"reason"`
	Winner      MatchZyWinner    `json:"winner"`
	Team1       MatchZyStatsTeam `json:"team1"`
	Team2       MatchZyStatsTeam `json:"team2"`
}

// MatchZyMapResult evento de fim de mapa
type MatchZyMapResult struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   FlexibleString   `json:"matchid"`
	MapNumber int              `json:"map_number"`
	Winner    MatchZyWinner    `json:"winner"`
	Team1     MatchZyStatsTeam `json:"team1"`
	Team2     MatchZyStatsTeam `json:"team2"`
}

// MatchZySeriesEnd evento de fim de serie
type MatchZySeriesEnd struct {
	Event            MatchZyEventType `json:"event"`
	MatchID          FlexibleString   `json:"matchid"`
	Winner           MatchZyWinner    `json:"winner"`
	Team1SeriesScore int              `json:"team1_series_score"`
	Team2SeriesScore int              `json:"team2_series_score"`
	TimeUntilRestore int              `json:"time_until_restore"`
}

// MatchZySidePicked evento de escolha de lado
type MatchZySidePicked struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   FlexibleString   `json:"matchid"`
	MapNumber int              `json:"map_number"`
	Team      string           `json:"team"`
	Side      string           `json:"side"`
}

// MatchZyMapPicked evento de pick de mapa
type MatchZyMapPicked struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   FlexibleString   `json:"matchid"`
	Team      string           `json:"team"`
	MapName   string           `json:"map_name"`
	MapNumber int              `json:"map_number"`
}

// MatchZyMapVetoed evento de ban de mapa
type MatchZyMapVetoed struct {
	Event   MatchZyEventType `json:"event"`
	MatchID FlexibleString   `json:"matchid"`
	Team    string           `json:"team"`
	MapName string           `json:"map_name"`
}

// MatchZySeriesStart evento de inicio de serie
type MatchZySeriesStart struct {
	Event   MatchZyEventType `json:"event"`
	MatchID FlexibleString   `json:"matchid"`
	NumMaps int              `json:"num_maps"`
	Team1   struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"team1"`
	Team2 struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"team2"`
}

// MatchZyPlayerDeath evento de morte de jogador (formato MatchZy)
type MatchZyPlayerDeath struct {
	Event       MatchZyEventType `json:"event"`
	MatchID     FlexibleString   `json:"matchid"`
	MapNumber   int              `json:"map_number"`
	RoundNumber int              `json:"round_number"`
	Attacker    *struct {
		SteamID string `json:"steamid"`
		Name    string `json:"name"`
		Side    string `json:"side"`
	} `json:"attacker"`
	Player struct { // victim
		SteamID string `json:"steamid"`
		Name    string `json:"name"`
		Side    string `json:"side"`
	} `json:"player"`
	Weapon struct {
		Name string `json:"name"`
	} `json:"weapon"`
	Headshot      bool `json:"headshot"`
	Penetrated    bool `json:"penetrated"`
	Thrusmoke     bool `json:"thrusmoke"`
	Noscope       bool `json:"noscope"`
	Attackerblind bool `json:"attackerblind"`
	Assister      *struct {
		SteamID      string `json:"steamid"`
		Name         string `json:"name"`
		FriendlyFire bool   `json:"friendly_fire"`
	} `json:"assister"`
}

// MatchZyBombEvent evento de bomba (planted/defused) do MatchZy
type MatchZyBombEvent struct {
	Event       MatchZyEventType `json:"event"`
	MatchID     FlexibleString   `json:"matchid"`
	MapNumber   int              `json:"map_number"`
	RoundNumber int              `json:"round_number"`
	Player      struct {
		SteamID string `json:"steamid"`
		Name    string `json:"name"`
		Side    string `json:"side"`
	} `json:"player"`
	Site string `json:"site"`
}

// --- WebSocket Game Event structs (formato que o frontend espera) ---

// WSGameEvent evento de jogo formatado para o frontend (GOTVEvent format)
type WSGameEvent struct {
	Type  string      `json:"type"`
	Tick  int         `json:"tick"`
	Round int         `json:"round"`
	Data  interface{} `json:"data"`
}

// WSPlayerRef referencia a um jogador para eventos WebSocket
type WSPlayerRef struct {
	Name    string `json:"name"`
	Team    string `json:"team"`
	SteamID string `json:"steamId"`
}

// --- Estado do Servidor ---

// MatchTeamInfo informacoes do time na partida
type MatchTeamInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Tag         string `json:"tag"`
	LogoURL     string `json:"logoUrl,omitempty"`
	CurrentSide string `json:"currentSide"`
	Score       int    `json:"score"`
	MapsWon     int    `json:"mapsWon"`
}

// MaxGameEvents numero maximo de game events mantidos para replay
const MaxGameEvents = 200

// MatchZyState estado completo da partida com dados do MatchZy
type MatchZyState struct {
	MatchID        string            `json:"matchId"`
	Phase          MatchPhase        `json:"phase"`
	IsCapturing    bool              `json:"isCapturing"`
	BestOf         int               `json:"bestOf"`
	CurrentMap     int               `json:"currentMap"`
	CurrentRound   int               `json:"currentRound"`
	CurrentHalf    int               `json:"currentHalf"`
	Team1          *MatchTeamInfo    `json:"team1,omitempty"`
	Team2          *MatchTeamInfo    `json:"team2,omitempty"`
	Team1StartSide string            `json:"team1StartSide"`
	MapName        string            `json:"mapName"`
	Events         []MatchZyEvent    `json:"events,omitempty"`
	LastEvent      *MatchZyEvent     `json:"lastEvent,omitempty"`
	GameEvents     []WSGameEvent     `json:"-"` // Eventos formatados para replay via WS
	LastRoundEnd   *MatchZyRoundEnd  `json:"-"`
	LastMapResult  *MatchZyMapResult `json:"-"`
	UpdatedAt      time.Time         `json:"updatedAt"`
	mu             sync.RWMutex
}

// NewMatchZyState cria um novo estado de partida
func NewMatchZyState(matchID string) *MatchZyState {
	return &MatchZyState{
		MatchID:      matchID,
		Phase:        PhaseIdle,
		IsCapturing:  false,
		BestOf:       1,
		CurrentMap:   0,
		CurrentRound: 0,
		CurrentHalf:  1,
		Events:       make([]MatchZyEvent, 0),
		GameEvents:   make([]WSGameEvent, 0),
		UpdatedAt:    time.Now(),
	}
}

// SetTeams define os times da partida
func (s *MatchZyState) SetTeams(team1, team2 *MatchTeamInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Team1 = team1
	s.Team2 = team2
	s.UpdatedAt = time.Now()
}

// UpdatePhase atualiza a fase da partida
func (s *MatchZyState) UpdatePhase(phase MatchPhase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Phase = phase
	s.IsCapturing = phase == PhaseLive || phase == PhaseOvertime
	s.UpdatedAt = time.Now()
	log.Printf("[MatchZy] Match %s phase -> %s (capturing: %v)", s.MatchID, phase, s.IsCapturing)
}

// AddEvent adiciona um evento raw do MatchZy ao historico
func (s *MatchZyState) AddEvent(event MatchZyEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, event)
	if len(s.Events) > MaxMatchZyEvents {
		copy(s.Events, s.Events[len(s.Events)-MaxMatchZyEvents:])
		s.Events = s.Events[:MaxMatchZyEvents]
	}
	s.LastEvent = &event
	s.UpdatedAt = time.Now()
}

// AddGameEvent adiciona um evento de jogo formatado para replay via WebSocket
func (s *MatchZyState) AddGameEvent(event WSGameEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.GameEvents = append(s.GameEvents, event)
	if len(s.GameEvents) > MaxGameEvents {
		copy(s.GameEvents, s.GameEvents[len(s.GameEvents)-MaxGameEvents:])
		s.GameEvents = s.GameEvents[:MaxGameEvents]
	}
}

// GetTeamBySide retorna o time pelo lado atual
func (s *MatchZyState) GetTeamBySide(side string) *MatchTeamInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Team1 != nil && strings.EqualFold(s.Team1.CurrentSide, side) {
		return s.Team1
	}
	if s.Team2 != nil && strings.EqualFold(s.Team2.CurrentSide, side) {
		return s.Team2
	}
	return nil
}

// --- Handler ---

// MatchZyHandler gerencia eventos do MatchZy
type MatchZyHandler struct {
	states        map[string]*MatchZyState
	statesMu      sync.RWMutex
	gotvServer    *GOTVServer
	authToken     string
	supabase      *SupabaseClient
	persister     *StatsPersister // Apenas para CallFinishAPI (safety net)
	forwardURL    string
	forwardSecret string
	uuidCache     map[string]string
	uuidMu        sync.RWMutex
}

// NewMatchZyHandler cria um novo handler de eventos MatchZy
func NewMatchZyHandler(gotvServer *GOTVServer, authToken string, supabase *SupabaseClient, persister *StatsPersister, forwardURL, forwardSecret string) *MatchZyHandler {
	return &MatchZyHandler{
		states:        make(map[string]*MatchZyState),
		gotvServer:    gotvServer,
		authToken:     authToken,
		supabase:      supabase,
		persister:     persister,
		forwardURL:    forwardURL,
		forwardSecret: forwardSecret,
		uuidCache:     make(map[string]string),
	}
}

// CleanupExpiredStates remove estados de partidas expiradas
func (h *MatchZyHandler) CleanupExpiredStates() {
	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	now := time.Now()
	for id, state := range h.states {
		state.mu.RLock()
		lastUpdate := state.UpdatedAt
		state.mu.RUnlock()

		if now.Sub(lastUpdate) > MatchExpirationTime {
			delete(h.states, id)
			log.Printf("[MatchZy] Expired state removed: %s", id)
		}
	}
}

// resolveMatchID resolve um matchID numerico (do MatchZy) para UUID real do banco
func (h *MatchZyHandler) resolveMatchID(matchID string) string {
	if strings.Contains(matchID, "-") {
		return matchID
	}

	h.uuidMu.RLock()
	if uuid, ok := h.uuidCache[matchID]; ok {
		h.uuidMu.RUnlock()
		return uuid
	}
	h.uuidMu.RUnlock()

	if h.supabase != nil {
		uuid, err := h.supabase.ResolveMatchUUID(matchID)
		if err != nil {
			log.Printf("[MatchZy] Failed to resolve matchID %s to UUID: %v", matchID, err)
			return matchID
		}
		h.uuidMu.Lock()
		h.uuidCache[matchID] = uuid
		h.uuidMu.Unlock()
		return uuid
	}

	return matchID
}

// GetOrCreateState obtem ou cria um estado de partida
func (h *MatchZyHandler) GetOrCreateState(matchID string) *MatchZyState {
	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	state, exists := h.states[matchID]
	if !exists {
		state = NewMatchZyState(matchID)
		h.states[matchID] = state
		log.Printf("[MatchZy] Created new state for match: %s", matchID)
	}
	return state
}

// GetState obtem o estado de uma partida
func (h *MatchZyHandler) GetState(matchID string) *MatchZyState {
	h.statesMu.RLock()
	defer h.statesMu.RUnlock()
	return h.states[matchID]
}

// forwardToNextJS encaminha o evento raw para o webhook Next.js para persistencia
func (h *MatchZyHandler) forwardToNextJS(body []byte) {
	if h.forwardURL == "" {
		return
	}
	go func() {
		req, err := http.NewRequest("POST", h.forwardURL, bytes.NewReader(body))
		if err != nil {
			log.Printf("[Forward] Failed to create request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if h.forwardSecret != "" {
			req.Header.Set("Authorization", "Bearer "+h.forwardSecret)
		}

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[Forward] Failed to forward event: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			log.Printf("[Forward] Next.js returned status %d: %s", resp.StatusCode, string(respBody))
		}
	}()
}

// HandleEvent processa um evento do MatchZy
func (h *MatchZyHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Verificar autenticacao
	authHeader := r.Header.Get("Authorization")
	expectedAuth := "Bearer " + h.authToken
	if authHeader != expectedAuth {
		log.Printf("[MatchZy] Unauthorized request from %s", r.RemoteAddr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("[MatchZy] Received event: %s", string(body))

	// Forward para Next.js ANTES de processar (fire-and-forget)
	h.forwardToNextJS(body)

	// Parse evento generico para identificar tipo
	var genericEvent MatchZyEvent
	if err := json.Unmarshal(body, &genericEvent); err != nil {
		log.Printf("[MatchZy] Failed to parse event: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Resolver matchID numerico -> UUID
	rawMatchID := string(genericEvent.MatchID)
	resolvedMatchID := h.resolveMatchID(rawMatchID)
	if resolvedMatchID != rawMatchID {
		log.Printf("[MatchZy] Resolved matchID: %s -> %s", rawMatchID, resolvedMatchID)
	}

	// Obter ou criar estado e match room
	state := h.GetOrCreateState(resolvedMatchID)
	h.gotvServer.GetOrCreateMatch(resolvedMatchID)

	// Processar evento
	switch genericEvent.Event {
	case EventSeriesStart:
		h.handleSeriesStart(state, body)
	case EventGoingLive:
		h.handleGoingLive(state, body)
	case EventSidePicked:
		h.handleSidePicked(state, body)
	case EventMapPicked:
		h.handleMapPicked(state, body)
	case EventMapVetoed:
		h.handleMapVetoed(state, body)
	case EventRoundEnd:
		h.handleRoundEnd(state, body)
	case EventMapResult:
		h.handleMapResult(state, body)
	case EventSeriesEnd:
		h.handleSeriesEnd(state, body)
	case EventPlayerDeath:
		h.handlePlayerDeath(state, body)
	case EventBombPlanted:
		h.handleBombPlanted(state, body)
	case EventBombDefused:
		h.handleBombDefused(state, body)
	case EventPlayerDisconnected:
		log.Printf("[MatchZy] Player disconnected in match %s", state.MatchID)
	case EventDemoUploadEnded:
		log.Printf("[MatchZy] Demo upload ended for match %s", state.MatchID)
	default:
		log.Printf("[MatchZy] Unknown event type: %s", genericEvent.Event)
	}

	// Adicionar evento raw ao historico
	state.AddEvent(genericEvent)

	// Broadcast estado completo para clientes WebSocket
	h.broadcastStateUpdate(state)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// normalizeSide converte side do MatchZy ("ct"/"t") para formato do frontend ("CT"/"T")
func normalizeSide(side string) string {
	return strings.ToUpper(side)
}

// --- Handlers de eventos novos (kill, bomb) ---

// handlePlayerDeath processa morte de jogador e broadcast kill event via WebSocket
func (h *MatchZyHandler) handlePlayerDeath(state *MatchZyState, body []byte) {
	var event MatchZyPlayerDeath
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse player_death: %v", err)
		return
	}

	// Construir evento no formato que o frontend espera (GOTVEvent)
	killData := map[string]interface{}{
		"victim": WSPlayerRef{
			Name:    event.Player.Name,
			Team:    normalizeSide(event.Player.Side),
			SteamID: event.Player.SteamID,
		},
		"weapon":       event.Weapon.Name,
		"headshot":     event.Headshot,
		"wallbang":     event.Penetrated,
		"throughSmoke": event.Thrusmoke,
		"noScope":      event.Noscope,
	}

	if event.Attacker != nil {
		killData["attacker"] = WSPlayerRef{
			Name:    event.Attacker.Name,
			Team:    normalizeSide(event.Attacker.Side),
			SteamID: event.Attacker.SteamID,
		}
	}

	gameEvent := WSGameEvent{
		Type:  "kill",
		Tick:  int(time.Now().UnixMilli()),
		Round: event.RoundNumber,
		Data:  killData,
	}

	// Broadcast imediatamente + armazenar para replay
	h.broadcastGameEvent(state, gameEvent)
	state.AddGameEvent(gameEvent)

	attackerName := "world"
	if event.Attacker != nil {
		attackerName = event.Attacker.Name
	}
	log.Printf("[MatchZy] Kill: %s -> %s (%s) round %d",
		attackerName, event.Player.Name, event.Weapon.Name, event.RoundNumber)
}

// handleBombPlanted processa bomba plantada e broadcast via WebSocket
func (h *MatchZyHandler) handleBombPlanted(state *MatchZyState, body []byte) {
	var event MatchZyBombEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse bomb_planted: %v", err)
		return
	}

	bombData := map[string]interface{}{
		"planter": WSPlayerRef{
			Name:    event.Player.Name,
			Team:    normalizeSide(event.Player.Side),
			SteamID: event.Player.SteamID,
		},
		"site": event.Site,
	}

	gameEvent := WSGameEvent{
		Type:  "bomb_planted",
		Tick:  int(time.Now().UnixMilli()),
		Round: event.RoundNumber,
		Data:  bombData,
	}

	h.broadcastGameEvent(state, gameEvent)
	state.AddGameEvent(gameEvent)

	log.Printf("[MatchZy] Bomb planted by %s at site %s (round %d)",
		event.Player.Name, event.Site, event.RoundNumber)
}

// handleBombDefused processa bomba desarmada e broadcast via WebSocket
func (h *MatchZyHandler) handleBombDefused(state *MatchZyState, body []byte) {
	var event MatchZyBombEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse bomb_defused: %v", err)
		return
	}

	bombData := map[string]interface{}{
		"defuser": WSPlayerRef{
			Name:    event.Player.Name,
			Team:    normalizeSide(event.Player.Side),
			SteamID: event.Player.SteamID,
		},
		"site": event.Site,
	}

	gameEvent := WSGameEvent{
		Type:  "bomb_defused",
		Tick:  int(time.Now().UnixMilli()),
		Round: event.RoundNumber,
		Data:  bombData,
	}

	h.broadcastGameEvent(state, gameEvent)
	state.AddGameEvent(gameEvent)

	log.Printf("[MatchZy] Bomb defused by %s at site %s (round %d)",
		event.Player.Name, event.Site, event.RoundNumber)
}

// --- Handlers de eventos existentes ---

// handleSeriesStart processa inicio de serie
func (h *MatchZyHandler) handleSeriesStart(state *MatchZyState, body []byte) {
	var event MatchZySeriesStart
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse series_start: %v", err)
		return
	}

	state.mu.Lock()
	state.BestOf = event.NumMaps
	state.Phase = PhaseWarmup
	state.mu.Unlock()

	log.Printf("[MatchZy] Series started: match %s, BO%d, %s vs %s",
		state.MatchID, event.NumMaps, event.Team1.Name, event.Team2.Name)

	// Popular times do Supabase
	if h.supabase != nil {
		go h.populateTeamsFromDB(state)
	}
}

// handleGoingLive processa evento de partida ao vivo
func (h *MatchZyHandler) handleGoingLive(state *MatchZyState, body []byte) {
	var event MatchZyGoingLive
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse going_live: %v", err)
		return
	}

	state.UpdatePhase(PhaseLive)
	state.mu.Lock()
	state.CurrentMap = event.MapNumber
	state.CurrentRound = 1
	state.CurrentHalf = 1
	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s is now LIVE! Map: %d", state.MatchID, event.MapNumber)

	// Popular times do Supabase (caso series_start nao tenha sido recebido)
	if h.supabase != nil {
		go h.populateTeamsFromDB(state)
	}

	// NOTA: UpdateMatchLive removido - Next.js webhook cuida da persistencia via forwarding
}

// populateTeamsFromDB busca os dados dos times no Supabase e popula o state
func (h *MatchZyHandler) populateTeamsFromDB(state *MatchZyState) {
	state.mu.RLock()
	hasTeams := state.Team1 != nil && state.Team1.ID != ""
	state.mu.RUnlock()
	if hasTeams {
		return
	}

	matchDetails, err := h.supabase.FetchMatchDetails(state.MatchID)
	if err != nil {
		log.Printf("[MatchZy] Failed to fetch match details from DB: %v", err)
		return
	}

	if matchDetails.Team1 == nil || matchDetails.Team2 == nil {
		log.Printf("[MatchZy] No team data found in DB for match %s", state.MatchID)
		return
	}

	logoURL1 := ""
	if matchDetails.Team1.LogoURL != nil {
		logoURL1 = *matchDetails.Team1.LogoURL
	}
	logoURL2 := ""
	if matchDetails.Team2.LogoURL != nil {
		logoURL2 = *matchDetails.Team2.LogoURL
	}

	team1 := &MatchTeamInfo{
		ID:          matchDetails.Team1.ID,
		Name:        matchDetails.Team1.Name,
		Tag:         matchDetails.Team1.Tag,
		LogoURL:     logoURL1,
		CurrentSide: "CT",
		Score:       0,
	}
	team2 := &MatchTeamInfo{
		ID:          matchDetails.Team2.ID,
		Name:        matchDetails.Team2.Name,
		Tag:         matchDetails.Team2.Tag,
		LogoURL:     logoURL2,
		CurrentSide: "T",
		Score:       0,
	}

	state.SetTeams(team1, team2)
	state.mu.Lock()
	state.BestOf = matchDetails.BestOf
	if matchDetails.MapName != nil {
		state.MapName = *matchDetails.MapName
	}
	state.mu.Unlock()

	log.Printf("[MatchZy] Teams populated: %s (%s) vs %s (%s) - BO%d",
		team1.Name, team1.Tag, team2.Name, team2.Tag, matchDetails.BestOf)

	h.broadcastStateUpdate(state)
}

// handleSidePicked processa escolha de lado
func (h *MatchZyHandler) handleSidePicked(state *MatchZyState, body []byte) {
	var event MatchZySidePicked
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse side_picked: %v", err)
		return
	}

	state.mu.Lock()
	side := strings.ToUpper(event.Side)
	oppositeSide := "T"
	if side == "T" {
		oppositeSide = "CT"
	}

	if event.Team == "team1" {
		if state.Team1 != nil {
			state.Team1.CurrentSide = side
			state.Team1StartSide = side
		}
		if state.Team2 != nil {
			state.Team2.CurrentSide = oppositeSide
		}
	} else {
		if state.Team2 != nil {
			state.Team2.CurrentSide = side
		}
		if state.Team1 != nil {
			state.Team1.CurrentSide = oppositeSide
			state.Team1StartSide = oppositeSide
		}
	}
	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s: %s picked %s side", state.MatchID, event.Team, event.Side)
}

// handleMapPicked processa pick de mapa
func (h *MatchZyHandler) handleMapPicked(state *MatchZyState, body []byte) {
	var event MatchZyMapPicked
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse map_picked: %v", err)
		return
	}
	log.Printf("[MatchZy] Match %s: %s picked %s (map #%d)", state.MatchID, event.Team, event.MapName, event.MapNumber)
}

// handleMapVetoed processa ban de mapa
func (h *MatchZyHandler) handleMapVetoed(state *MatchZyState, body []byte) {
	var event MatchZyMapVetoed
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse map_vetoed: %v", err)
		return
	}
	log.Printf("[MatchZy] Match %s: %s banned %s", state.MatchID, event.Team, event.MapName)
}

// handleRoundEnd processa fim de round
func (h *MatchZyHandler) handleRoundEnd(state *MatchZyState, body []byte) {
	var event MatchZyRoundEnd
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse round_end: %v", err)
		return
	}

	state.mu.Lock()
	state.CurrentRound = event.RoundNumber
	state.LastRoundEnd = &event

	// Atualizar scores
	if state.Team1 != nil {
		state.Team1.Score = event.Team1.Score
	}
	if state.Team2 != nil {
		state.Team2.Score = event.Team2.Score
	}

	// Verificar halftime (round 12 em MR12)
	if event.RoundNumber == 12 && event.Team1.Score+event.Team2.Score == 12 {
		state.mu.Unlock()
		state.UpdatePhase(PhaseHalftime)
		state.mu.Lock()
		if state.Team1 != nil && state.Team2 != nil {
			if state.Team1.CurrentSide == "CT" {
				state.Team1.CurrentSide = "T"
				state.Team2.CurrentSide = "CT"
			} else {
				state.Team1.CurrentSide = "CT"
				state.Team2.CurrentSide = "T"
			}
			state.CurrentHalf = 2
		}
		state.mu.Unlock()
		log.Printf("[MatchZy] Match %s reached halftime - sides swapped", state.MatchID)
	} else if event.Team1.Score == 12 && event.Team2.Score == 12 {
		state.mu.Unlock()
		state.UpdatePhase(PhaseOvertime)
		log.Printf("[MatchZy] Match %s entered overtime", state.MatchID)
	} else {
		state.mu.Unlock()
	}

	log.Printf("[MatchZy] Match %s round %d ended. Score: %d-%d. Winner: %s (%s)",
		state.MatchID, event.RoundNumber, event.Team1.Score, event.Team2.Score,
		event.Winner.Team, event.Winner.Side)

	// Broadcast round_end como game event para o frontend
	state.mu.RLock()
	var scoreCT, scoreT int
	if state.Team1 != nil && state.Team1.CurrentSide == "CT" {
		scoreCT = event.Team1.Score
		scoreT = event.Team2.Score
	} else {
		scoreCT = event.Team2.Score
		scoreT = event.Team1.Score
	}
	state.mu.RUnlock()

	roundEndEvent := WSGameEvent{
		Type:  "round_end",
		Tick:  int(time.Now().UnixMilli()),
		Round: event.RoundNumber,
		Data: map[string]interface{}{
			"round":   event.RoundNumber,
			"winner":  normalizeSide(event.Winner.Side),
			"reason":  matchzyRoundReason(event.Reason),
			"scoreCT": scoreCT,
			"scoreT":  scoreT,
		},
	}

	h.broadcastGameEvent(state, roundEndEvent)
	state.AddGameEvent(roundEndEvent)

	// NOTA: Persistencia removida - Next.js webhook cuida via forwarding
}

// matchzyRoundReason mapeia o codigo de razao para string legivel
func matchzyRoundReason(reason int) string {
	switch reason {
	case 1:
		return "target_bombed"
	case 7:
		return "bomb_defused"
	case 8:
		return "ct_elimination"
	case 9:
		return "t_elimination"
	case 12:
		return "time_expired"
	case 17:
		return "hostages_rescued"
	default:
		return fmt.Sprintf("reason_%d", reason)
	}
}

// handleMapResult processa fim de mapa
func (h *MatchZyHandler) handleMapResult(state *MatchZyState, body []byte) {
	var event MatchZyMapResult
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse map_result: %v", err)
		return
	}

	state.mu.Lock()
	state.LastMapResult = &event
	if event.Winner.Team == "team1" && state.Team1 != nil {
		state.Team1.MapsWon++
	} else if event.Winner.Team == "team2" && state.Team2 != nil {
		state.Team2.MapsWon++
	}
	if state.Team1 != nil {
		state.Team1.Score = event.Team1.Score
	}
	if state.Team2 != nil {
		state.Team2.Score = event.Team2.Score
	}
	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s map %d finished. Winner: %s (%s), Score: %d-%d",
		state.MatchID, event.MapNumber, event.Winner.Team, event.Winner.Side,
		event.Team1.Score, event.Team2.Score)

	// Se for BO1, a partida terminou - chamar finish API como safety net
	if state.BestOf <= 1 {
		state.UpdatePhase(PhaseFinished)
		if h.persister != nil {
			go func() {
				if err := h.persister.CallFinishAPI(state.MatchID, event.Team1.Score, event.Team2.Score); err != nil {
					log.Printf("[MatchZy] Failed to call finish API: %v", err)
				}
			}()
		}
	}
}

// handleSeriesEnd processa fim de serie
func (h *MatchZyHandler) handleSeriesEnd(state *MatchZyState, body []byte) {
	var event MatchZySeriesEnd
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse series_end: %v", err)
		return
	}

	state.UpdatePhase(PhaseFinished)

	log.Printf("[MatchZy] Match %s series finished! Winner: %s (%s), Series: %d-%d",
		state.MatchID, event.Winner.Team, event.Winner.Side,
		event.Team1SeriesScore, event.Team2SeriesScore)

	// Chamar finish API como safety net (Next.js webhook faz a persistencia principal)
	if h.persister != nil {
		go func() {
			team1Score := 0
			team2Score := 0
			state.mu.RLock()
			if state.Team1 != nil {
				team1Score = state.Team1.Score
			}
			if state.Team2 != nil {
				team2Score = state.Team2.Score
			}
			state.mu.RUnlock()
			if err := h.persister.CallFinishAPI(state.MatchID, team1Score, team2Score); err != nil {
				log.Printf("[MatchZy] Failed to call finish API: %v", err)
			}
		}()
	}
}

// --- Broadcast ---

// broadcastGameEvent envia um game event formatado via WebSocket
func (h *MatchZyHandler) broadcastGameEvent(state *MatchZyState, event WSGameEvent) {
	if h.gotvServer == nil {
		return
	}

	msg := WebSocketMessage{
		Type:      "event",
		MatchID:   state.MatchID,
		Data:      event,
		Timestamp: time.Now().UnixMilli(),
	}

	h.gotvServer.broadcastToMatch(state.MatchID, msg)
}

// broadcastStateUpdate envia atualizacao de estado para clientes WebSocket
func (h *MatchZyHandler) broadcastStateUpdate(state *MatchZyState) {
	if h.gotvServer == nil {
		return
	}

	msg := WebSocketMessage{
		Type:      "matchzy_state",
		MatchID:   state.MatchID,
		Data:      state,
		Timestamp: time.Now().UnixMilli(),
	}

	h.gotvServer.broadcastToMatch(state.MatchID, msg)

	h.gotvServer.matchesMu.RLock()
	match, exists := h.gotvServer.matches[state.MatchID]
	h.gotvServer.matchesMu.RUnlock()
	if exists {
		match.UpdatedAt = time.Now()
	}
}

// --- Rotas ---

// RegisterRoutes registra as rotas do MatchZy no servidor
func (h *MatchZyHandler) RegisterRoutes() {
	http.HandleFunc("/api/matchzy/events", loggingMiddleware(h.HandleEvent))
	http.HandleFunc("/api/matchzy/state/", loggingMiddleware(h.handleGetState))
	log.Printf("[MatchZy] Routes registered:")
	log.Printf("  - POST /api/matchzy/events       - Receive MatchZy webhooks")
	log.Printf("  - GET  /api/matchzy/state/{id}    - Get match state")
}

// handleGetState retorna o estado atual de uma partida
func (h *MatchZyHandler) handleGetState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/api/matchzy/state/")
	matchID := strings.TrimSuffix(path, "/")

	if matchID == "" {
		http.Error(w, "Match ID required", http.StatusBadRequest)
		return
	}

	state := h.GetState(matchID)
	if state == nil {
		resolved := h.resolveMatchID(matchID)
		if resolved != matchID {
			state = h.GetState(resolved)
		}
	}
	if state == nil {
		http.Error(w, "Match not found", http.StatusNotFound)
		return
	}

	state.mu.RLock()
	defer state.mu.RUnlock()
	json.NewEncoder(w).Encode(state)
}
