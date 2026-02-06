package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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
	EventGoingLive          MatchZyEventType = "going_live"
	EventRoundStart         MatchZyEventType = "round_start"
	EventRoundEnd           MatchZyEventType = "round_end"
	EventMapResult          MatchZyEventType = "map_result"
	EventSeriesResult       MatchZyEventType = "series_result"
	EventSidePicked         MatchZyEventType = "side_picked"
	EventMapPicked          MatchZyEventType = "map_picked"
	EventMapVetoed          MatchZyEventType = "map_vetoed"
	EventPlayerDisconnected MatchZyEventType = "player_disconnect"
	EventMatchPaused        MatchZyEventType = "match_paused"
	EventMatchUnpaused      MatchZyEventType = "match_unpaused"
	EventKnifeRoundStarted  MatchZyEventType = "knife_start"
	EventKnifeRoundWon      MatchZyEventType = "knife_won"
)

// MatchZyEvent evento genérico do MatchZy
type MatchZyEvent struct {
	Event     MatchZyEventType       `json:"event"`
	MatchID   string                 `json:"matchid"`
	MapNumber int                    `json:"map_number,omitempty"`
	Timestamp string                 `json:"timestamp,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// MatchZyGoingLive evento quando a partida começa
type MatchZyGoingLive struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   string           `json:"matchid"`
	MapNumber int              `json:"map_number"`
}

// MatchZyRoundEnd evento de fim de round
type MatchZyRoundEnd struct {
	Event       MatchZyEventType `json:"event"`
	MatchID     string           `json:"matchid"`
	MapNumber   int              `json:"map_number"`
	RoundNumber int              `json:"round_number"`
	RoundTime   int              `json:"round_time"`
	Reason      int              `json:"reason"`
	Winner      string           `json:"winner"` // "team1" ou "team2"
	Team1Score  int              `json:"team1_score"`
	Team2Score  int              `json:"team2_score"`
}

// MatchZyMapResult evento de fim de mapa
type MatchZyMapResult struct {
	Event      MatchZyEventType `json:"event"`
	MatchID    string           `json:"matchid"`
	MapNumber  int              `json:"map_number"`
	Winner     string           `json:"winner"` // "team1", "team2" ou "none"
	Team1Score int              `json:"team1_score"`
	Team2Score int              `json:"team2_score"`
}

// MatchZySeriesResult evento de fim de série
type MatchZySeriesResult struct {
	Event            MatchZyEventType `json:"event"`
	MatchID          string           `json:"matchid"`
	Winner           string           `json:"winner"`
	Team1SeriesScore int              `json:"team1_series_score"`
	Team2SeriesScore int              `json:"team2_series_score"`
	TimeUntilRestore int              `json:"time_until_restore"`
}

// MatchZySidePicked evento de escolha de lado
type MatchZySidePicked struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   string           `json:"matchid"`
	MapNumber int              `json:"map_number"`
	Team      string           `json:"team"` // "team1" ou "team2"
	Side      string           `json:"side"` // "CT" ou "T"
}

// MatchZyKnifeWon evento de vitória no knife
type MatchZyKnifeWon struct {
	Event     MatchZyEventType `json:"event"`
	MatchID   string           `json:"matchid"`
	MapNumber int              `json:"map_number"`
	Winner    string           `json:"winner"` // "team1" ou "team2"
}

// MatchTeamInfo informações do time na partida
type MatchTeamInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Tag         string `json:"tag"`
	LogoURL     string `json:"logoUrl,omitempty"`
	CurrentSide string `json:"currentSide"` // "CT" ou "T"
	Score       int    `json:"score"`
	MapsWon     int    `json:"mapsWon"`
}

// MatchZyState estado completo da partida com dados do MatchZy
type MatchZyState struct {
	MatchID        string          `json:"matchId"`
	Phase          MatchPhase      `json:"phase"`
	IsCapturing    bool            `json:"isCapturing"` // true em LIVE ou OVERTIME
	BestOf         int             `json:"bestOf"`
	CurrentMap     int             `json:"currentMap"`
	CurrentRound   int             `json:"currentRound"`
	CurrentHalf    int             `json:"currentHalf"` // 1 ou 2
	Team1          *MatchTeamInfo  `json:"team1,omitempty"`
	Team2          *MatchTeamInfo  `json:"team2,omitempty"`
	Team1StartSide string          `json:"team1StartSide"` // Lado que team1 começou
	KnifeWinner    string          `json:"knifeWinner,omitempty"`
	MapName        string          `json:"mapName"`
	Events         []MatchZyEvent  `json:"events,omitempty"`
	LastEvent      *MatchZyEvent   `json:"lastEvent,omitempty"`
	UpdatedAt      time.Time       `json:"updatedAt"`
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
	log.Printf("[MatchZy] Match %s phase changed to: %s (capturing: %v)", s.MatchID, phase, s.IsCapturing)
}

// SwapSides troca os lados dos times (halftime/overtime)
func (s *MatchZyState) SwapSides() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Team1 != nil && s.Team2 != nil {
		if s.Team1.CurrentSide == "CT" {
			s.Team1.CurrentSide = "T"
			s.Team2.CurrentSide = "CT"
		} else {
			s.Team1.CurrentSide = "CT"
			s.Team2.CurrentSide = "T"
		}
		s.CurrentHalf++
		log.Printf("[MatchZy] Match %s sides swapped. Half: %d, Team1 now: %s",
			s.MatchID, s.CurrentHalf, s.Team1.CurrentSide)
	}
	s.UpdatedAt = time.Now()
}

// GetTeamBySide retorna o time pelo lado atual
func (s *MatchZyState) GetTeamBySide(side string) *MatchTeamInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.Team1 != nil && s.Team1.CurrentSide == side {
		return s.Team1
	}
	if s.Team2 != nil && s.Team2.CurrentSide == side {
		return s.Team2
	}
	return nil
}

// AddEvent adiciona um evento ao histórico com cap de MaxMatchZyEvents
func (s *MatchZyState) AddEvent(event MatchZyEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, event)
	// Manter apenas os últimos MaxMatchZyEvents eventos
	if len(s.Events) > MaxMatchZyEvents {
		copy(s.Events, s.Events[len(s.Events)-MaxMatchZyEvents:])
		s.Events = s.Events[:MaxMatchZyEvents]
	}
	s.LastEvent = &event
	s.UpdatedAt = time.Now()
}

// MatchZyHandler gerencia eventos do MatchZy
type MatchZyHandler struct {
	states       map[string]*MatchZyState
	statesMu     sync.RWMutex
	gotvServer   *GOTVServer
	authToken    string
	gotvMatchMap map[string]string // dbMatchID → gotvMatchID
	mapMu        sync.RWMutex
	supabase     *SupabaseClient
	persister    *StatsPersister
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
			log.Printf("[MatchZy] 🧹 Expired state removed: %s", id)
		}
	}
}

// NewMatchZyHandler cria um novo handler de eventos MatchZy
func NewMatchZyHandler(gotvServer *GOTVServer, authToken string, supabase *SupabaseClient, persister *StatsPersister) *MatchZyHandler {
	return &MatchZyHandler{
		states:       make(map[string]*MatchZyState),
		gotvServer:   gotvServer,
		authToken:    authToken,
		gotvMatchMap: make(map[string]string),
		supabase:     supabase,
		persister:    persister,
	}
}

// GetOrCreateState obtém ou cria um estado de partida
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

// GetState obtém o estado de uma partida
func (h *MatchZyHandler) GetState(matchID string) *MatchZyState {
	h.statesMu.RLock()
	defer h.statesMu.RUnlock()
	return h.states[matchID]
}

// HandleEvent processa um evento do MatchZy
func (h *MatchZyHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	// CORS
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

	// Verificar autenticação
	authHeader := r.Header.Get("Authorization")
	expectedAuth := "Bearer " + h.authToken
	if authHeader != expectedAuth {
		log.Printf("[MatchZy] Unauthorized request. Expected: %s, Got: %s", expectedAuth, authHeader)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Ler body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("[MatchZy] Received event: %s", string(body))

	// Parse evento genérico primeiro para identificar o tipo
	var genericEvent MatchZyEvent
	if err := json.Unmarshal(body, &genericEvent); err != nil {
		log.Printf("[MatchZy] Failed to parse event: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Obter ou criar estado da partida
	state := h.GetOrCreateState(genericEvent.MatchID)

	// Processar evento baseado no tipo
	switch genericEvent.Event {
	case EventGoingLive:
		h.handleGoingLive(state, body)
	case EventKnifeRoundStarted:
		h.handleKnifeStart(state, body)
	case EventKnifeRoundWon:
		h.handleKnifeWon(state, body)
	case EventSidePicked:
		h.handleSidePicked(state, body)
	case EventRoundStart:
		h.handleRoundStart(state, body)
	case EventRoundEnd:
		h.handleRoundEnd(state, body)
	case EventMapResult:
		h.handleMapResult(state, body)
	case EventSeriesResult:
		h.handleSeriesResult(state, body)
	case EventMatchPaused:
		state.UpdatePhase(PhasePaused)
	case EventMatchUnpaused:
		state.UpdatePhase(PhaseLive)
	case EventPlayerDisconnected:
		h.handlePlayerDisconnected(state, body)
	default:
		log.Printf("[MatchZy] Unknown event type: %s", genericEvent.Event)
	}

	// Adicionar evento ao histórico
	state.AddEvent(genericEvent)

	// Broadcast para clientes WebSocket
	h.broadcastStateUpdate(state)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleGoingLive processa evento de partida ao vivo
func (h *MatchZyHandler) handleGoingLive(state *MatchZyState, body []byte) {
	var event MatchZyGoingLive
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse going_live: %v", err)
		return
	}

	state.UpdatePhase(PhaseLive)
	state.CurrentMap = event.MapNumber
	state.CurrentRound = 1
	state.CurrentHalf = 1
	log.Printf("[MatchZy] Match %s is now LIVE! Map: %d", event.MatchID, event.MapNumber)

	// Vincular DB matchID → GOTV matchID
	// Encontrar a partida GOTV ativa (normalmente só 1 no servidor)
	h.gotvServer.matchesMu.RLock()
	for gotvID, match := range h.gotvServer.matches {
		if match.ParserStarted {
			h.mapMu.Lock()
			h.gotvMatchMap[state.MatchID] = gotvID
			h.mapMu.Unlock()
			log.Printf("[MatchZy] Linked DB match %s → GOTV match %s", state.MatchID, gotvID)
			break
		}
	}
	h.gotvServer.matchesMu.RUnlock()

	// Popular times do Supabase
	if h.supabase != nil {
		go h.populateTeamsFromDB(state)
	}

	// Marcar partida como live no Supabase
	if h.supabase != nil {
		go func() {
			if err := h.supabase.UpdateMatchLive(state.MatchID); err != nil {
				log.Printf("[MatchZy] Failed to update match live in DB: %v", err)
			}
		}()
	}
}

// populateTeamsFromDB busca os dados dos times no Supabase e popula o state
func (h *MatchZyHandler) populateTeamsFromDB(state *MatchZyState) {
	matchDetails, err := h.supabase.FetchMatchDetails(state.MatchID)
	if err != nil {
		log.Printf("[MatchZy] Failed to fetch match details from DB: %v", err)
		return
	}

	// Popular Team1
	if matchDetails.Team1 != nil {
		logoURL := ""
		if matchDetails.Team1.LogoURL != nil {
			logoURL = *matchDetails.Team1.LogoURL
		}
		team1 := &MatchTeamInfo{
			ID:          matchDetails.Team1.ID,
			Name:        matchDetails.Team1.Name,
			Tag:         matchDetails.Team1.Tag,
			LogoURL:     logoURL,
			CurrentSide: "CT", // Default, será corrigido pelo side_picked
			Score:       0,
		}

		team2LogoURL := ""
		if matchDetails.Team2 != nil && matchDetails.Team2.LogoURL != nil {
			team2LogoURL = *matchDetails.Team2.LogoURL
		}
		team2 := &MatchTeamInfo{
			ID:          matchDetails.Team2.ID,
			Name:        matchDetails.Team2.Name,
			Tag:         matchDetails.Team2.Tag,
			LogoURL:     team2LogoURL,
			CurrentSide: "T", // Default
			Score:       0,
		}

		state.SetTeams(team1, team2)
		state.BestOf = matchDetails.BestOf

		log.Printf("[MatchZy] Teams populated from DB: %s (%s) vs %s (%s) - BO%d",
			team1.Name, team1.Tag, team2.Name, team2.Tag, matchDetails.BestOf)
	} else {
		log.Printf("[MatchZy] No team data found in DB for match %s", state.MatchID)
	}

	// Broadcast atualização com dados dos times
	h.broadcastStateUpdate(state)
}

// handleKnifeStart processa início do knife round
func (h *MatchZyHandler) handleKnifeStart(state *MatchZyState, body []byte) {
	state.UpdatePhase(PhaseKnife)
	log.Printf("[MatchZy] Match %s knife round started", state.MatchID)
}

// handleKnifeWon processa vitória no knife round
func (h *MatchZyHandler) handleKnifeWon(state *MatchZyState, body []byte) {
	var event MatchZyKnifeWon
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse knife_won: %v", err)
		return
	}

	state.mu.Lock()
	state.KnifeWinner = event.Winner
	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s knife won by: %s", state.MatchID, event.Winner)
}

// handleSidePicked processa escolha de lado
func (h *MatchZyHandler) handleSidePicked(state *MatchZyState, body []byte) {
	var event MatchZySidePicked
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse side_picked: %v", err)
		return
	}

	state.mu.Lock()
	// Se team1 escolheu, configurar os lados
	if event.Team == "team1" {
		if state.Team1 != nil {
			state.Team1.CurrentSide = event.Side
			state.Team1StartSide = event.Side
		}
		if state.Team2 != nil {
			if event.Side == "CT" {
				state.Team2.CurrentSide = "T"
			} else {
				state.Team2.CurrentSide = "CT"
			}
		}
	} else { // team2 escolheu
		if state.Team2 != nil {
			state.Team2.CurrentSide = event.Side
		}
		if state.Team1 != nil {
			if event.Side == "CT" {
				state.Team1.CurrentSide = "T"
				state.Team1StartSide = "T"
			} else {
				state.Team1.CurrentSide = "CT"
				state.Team1StartSide = "CT"
			}
		}
	}
	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s: %s picked %s side", state.MatchID, event.Team, event.Side)
}

// handleRoundStart processa início de round
func (h *MatchZyHandler) handleRoundStart(state *MatchZyState, body []byte) {
	// Round start apenas atualiza o contador
	state.mu.Lock()
	state.CurrentRound++
	state.mu.Unlock()
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

	// Atualizar scores
	if state.Team1 != nil {
		state.Team1.Score = event.Team1Score
	}
	if state.Team2 != nil {
		state.Team2.Score = event.Team2Score
	}

	// Verificar halftime (round 12 em MR12)
	if event.RoundNumber == 12 {
		state.mu.Unlock()
		state.UpdatePhase(PhaseHalftime)
		// Swap após um pequeno delay seria feito pelo evento de round_start do half 2
		log.Printf("[MatchZy] Match %s reached halftime", state.MatchID)
		return
	}

	// Verificar overtime (empate 12-12)
	if event.Team1Score == 12 && event.Team2Score == 12 {
		state.mu.Unlock()
		state.UpdatePhase(PhaseOvertime)
		log.Printf("[MatchZy] Match %s entered overtime", state.MatchID)
		return
	}

	state.mu.Unlock()

	log.Printf("[MatchZy] Match %s round %d ended. Score: %d-%d. Winner: %s",
		state.MatchID, event.RoundNumber, event.Team1Score, event.Team2Score, event.Winner)

	// Atualizar scores no Supabase em tempo real
	if h.supabase != nil {
		go func() {
			if err := h.supabase.UpdateMatchScores(state.MatchID, event.Team1Score, event.Team2Score); err != nil {
				log.Printf("[MatchZy] Failed to update scores in DB: %v", err)
			}
		}()
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
	// Atualizar maps ganhos
	if event.Winner == "team1" && state.Team1 != nil {
		state.Team1.MapsWon++
	} else if event.Winner == "team2" && state.Team2 != nil {
		state.Team2.MapsWon++
	}
	state.mu.Unlock()

	// Se for BO1, a partida terminou
	if state.BestOf <= 1 {
		state.UpdatePhase(PhaseFinished)

		// Persistir stats e chamar finish API
		if h.persister != nil {
			go h.persistMatchData(state)
		}
	}

	log.Printf("[MatchZy] Match %s map %d finished. Winner: %s, Score: %d-%d",
		state.MatchID, event.MapNumber, event.Winner, event.Team1Score, event.Team2Score)
}

// handleSeriesResult processa fim de série (BO3, etc)
func (h *MatchZyHandler) handleSeriesResult(state *MatchZyState, body []byte) {
	var event MatchZySeriesResult
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[MatchZy] Failed to parse series_result: %v", err)
		return
	}

	state.UpdatePhase(PhaseFinished)

	log.Printf("[MatchZy] Match %s series finished! Winner: %s, Series score: %d-%d",
		state.MatchID, event.Winner, event.Team1SeriesScore, event.Team2SeriesScore)

	// Persistir stats e chamar finish API
	if h.persister != nil {
		go h.persistMatchData(state)
	}
}

// persistMatchData persiste stats dos jogadores e chama finish API
func (h *MatchZyHandler) persistMatchData(state *MatchZyState) {
	dbMatchID := state.MatchID

	// Encontrar o GOTV match vinculado
	h.mapMu.RLock()
	gotvID, linked := h.gotvMatchMap[dbMatchID]
	h.mapMu.RUnlock()

	if !linked {
		log.Printf("[MatchZy] No GOTV match linked for DB match %s - trying to find active match", dbMatchID)
		// Fallback: tentar encontrar qualquer match ativa
		h.gotvServer.matchesMu.RLock()
		for id, match := range h.gotvServer.matches {
			if match.ParserStarted {
				gotvID = id
				linked = true
				break
			}
		}
		h.gotvServer.matchesMu.RUnlock()
	}

	if !linked {
		log.Printf("[MatchZy] Cannot persist - no GOTV match found for DB match %s", dbMatchID)
		// Ainda chamar finish API mesmo sem stats do parser
		if h.persister != nil {
			team1Score := 0
			team2Score := 0
			if state.Team1 != nil {
				team1Score = state.Team1.Score
			}
			if state.Team2 != nil {
				team2Score = state.Team2.Score
			}
			if err := h.persister.CallFinishAPI(dbMatchID, team1Score, team2Score); err != nil {
				log.Printf("[MatchZy] Failed to call finish API: %v", err)
			}
		}
		return
	}

	h.gotvServer.matchesMu.RLock()
	match := h.gotvServer.matches[gotvID]
	h.gotvServer.matchesMu.RUnlock()

	if match == nil {
		log.Printf("[MatchZy] GOTV match %s not found in server matches", gotvID)
		return
	}

	if err := h.persister.PersistMatchStats(dbMatchID, match, state); err != nil {
		log.Printf("[MatchZy] Failed to persist match stats: %v", err)
	}
}

// handlePlayerDisconnected processa desconexão de jogador
func (h *MatchZyHandler) handlePlayerDisconnected(state *MatchZyState, body []byte) {
	log.Printf("[MatchZy] Player disconnected in match %s", state.MatchID)
}

// broadcastStateUpdate envia atualização de estado para clientes WebSocket
func (h *MatchZyHandler) broadcastStateUpdate(state *MatchZyState) {
	if h.gotvServer == nil {
		return
	}

	// Tentar encontrar o match pelo mapa dbMatchID → gotvMatchID
	h.mapMu.RLock()
	gotvID, linked := h.gotvMatchMap[state.MatchID]
	h.mapMu.RUnlock()

	var match *ActiveMatch
	var exists bool

	if linked {
		h.gotvServer.matchesMu.RLock()
		match, exists = h.gotvServer.matches[gotvID]
		h.gotvServer.matchesMu.RUnlock()
	}

	if !exists {
		// Fallback: tentar pelo matchID direto (caso não tenha sido vinculado ainda)
		h.gotvServer.matchesMu.RLock()
		match, exists = h.gotvServer.matches[state.MatchID]
		if !exists {
			// Último fallback: broadcast para qualquer match ativa
			for _, m := range h.gotvServer.matches {
				match = m
				exists = true
				break
			}
		}
		h.gotvServer.matchesMu.RUnlock()
	}

	if !exists || match == nil {
		return
	}

	// Criar mensagem com estado do MatchZy
	msg := WebSocketMessage{
		Type:      "matchzy_state",
		MatchID:   state.MatchID,
		Data:      state,
		Timestamp: time.Now().UnixMilli(),
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[MatchZy] Failed to marshal state: %v", err)
		return
	}

	// Broadcast para todos os clientes
	match.ClientsMu.RLock()
	for client := range match.Clients {
		if err := client.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
			log.Printf("[MatchZy] Failed to send to client: %v", err)
		}
	}
	match.ClientsMu.RUnlock()
}

// RegisterRoutes registra as rotas do MatchZy no servidor
func (h *MatchZyHandler) RegisterRoutes() {
	http.HandleFunc("/api/matchzy/events", loggingMiddleware(h.HandleEvent))
	http.HandleFunc("/api/matchzy/state/", loggingMiddleware(h.handleGetState))
	log.Printf("[MatchZy] Routes registered:")
	log.Printf("  - POST /api/matchzy/events - Receive MatchZy webhooks")
	log.Printf("  - GET  /api/matchzy/state/{matchId} - Get match state")
}

// handleGetState retorna o estado atual de uma partida
func (h *MatchZyHandler) handleGetState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	// Parse matchId from URL
	path := strings.TrimPrefix(r.URL.Path, "/api/matchzy/state/")
	matchID := strings.TrimSuffix(path, "/")

	if matchID == "" {
		http.Error(w, "Match ID required", http.StatusBadRequest)
		return
	}

	state := h.GetState(matchID)
	if state == nil {
		http.Error(w, "Match not found", http.StatusNotFound)
		return
	}

	state.mu.RLock()
	defer state.mu.RUnlock()
	json.NewEncoder(w).Encode(state)
}
