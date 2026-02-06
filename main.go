package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// MaxFragments é o número máximo de fragmentos mantidos em memória por tipo
	MaxFragments = 30
	// MaxEvents é o número máximo de eventos mantidos em memória por partida
	MaxEvents = 500
	// MaxMatchZyEvents é o número máximo de eventos MatchZy mantidos
	MaxMatchZyEvents = 200
	// MatchExpirationTime é o tempo para expirar uma partida sem atividade
	MatchExpirationTime = 30 * time.Minute
	// WSReadTimeout é o timeout de leitura do WebSocket
	WSReadTimeout = 5 * time.Minute
	// WSWriteTimeout é o timeout de escrita do WebSocket
	WSWriteTimeout = 10 * time.Second
	// CleanupInterval é o intervalo entre limpezas de partidas expiradas
	CleanupInterval = 5 * time.Minute
	// MemoryLogInterval é o intervalo entre logs de memória
	MemoryLogInterval = 2 * time.Minute
)

// MatchState representa o estado atual da partida
type MatchState struct {
	MatchID       string          `json:"matchId"`
	Status        string          `json:"status"`
	MapName       string          `json:"mapName"`
	ScoreCT       int             `json:"scoreCT"`
	ScoreT        int             `json:"scoreT"`
	CurrentRound  int             `json:"currentRound"`
	RoundPhase    string          `json:"roundPhase"`
	Players       []PlayerState   `json:"players"`
	Bomb          *BombState      `json:"bomb,omitempty"`
	TeamCT        *IdentifiedTeam `json:"teamCT,omitempty"`
	TeamT         *IdentifiedTeam `json:"teamT,omitempty"`
	LastTick      int             `json:"lastTick"`
	TotalBytes    int64           `json:"totalBytes"`
	FragmentCount int             `json:"fragmentCount"`
	UpdatedAt     time.Time       `json:"updatedAt"`
}

// PlayerState representa o estado de um jogador
type PlayerState struct {
	SteamID      string   `json:"steamId"`
	Name         string   `json:"name"`
	Team         string   `json:"team"`
	Health       int      `json:"health"`
	Armor        int      `json:"armor"`
	HasHelmet    bool     `json:"hasHelmet"`
	HasDefuser   bool     `json:"hasDefuser"`
	Money        int      `json:"money"`
	IsAlive      bool     `json:"isAlive"`
	Position     Position `json:"position"`
	ViewAngle    float32  `json:"viewAngle"`
	ActiveWeapon string   `json:"activeWeapon"`
	Weapons      []string `json:"weapons"`
	Kills        int      `json:"kills"`
	Deaths       int      `json:"deaths"`
	Assists      int      `json:"assists"`
	Headshots    int      `json:"headshots"`
	Damage       int      `json:"damage"`
	RoundKills   int      `json:"roundKills"`
	RoundDamage  int      `json:"roundDamage"`
}

// Position representa uma posição 3D
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

// BombState representa o estado da bomba
type BombState struct {
	State         string   `json:"state"`
	Position      Position `json:"position,omitempty"`
	TimeRemaining float64  `json:"timeRemaining,omitempty"`
	Site          string   `json:"site,omitempty"`
}

// Fragment representa um fragmento GOTV+
type Fragment struct {
	Number    int
	Type      string
	Data      []byte
	Timestamp time.Time
}

// WebSocketMessage mensagem enviada via WebSocket
type WebSocketMessage struct {
	Type      string      `json:"type"`
	MatchID   string      `json:"matchId"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
}

// WSClient representa um cliente WebSocket com mutex para escrita thread-safe
type WSClient struct {
	Conn  *websocket.Conn
	Mu    sync.Mutex
}

// WriteMessage escreve uma mensagem de forma thread-safe
func (c *WSClient) WriteMessage(messageType int, data []byte) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

// ActiveMatch representa uma partida ativa
type ActiveMatch struct {
	MatchID       string
	State         *MatchState
	Clients       map[*WSClient]bool
	Fragments     map[int]*Fragment
	DeltaFragments map[int]*Fragment
	StartFragment *Fragment
	Parser        *BroadcastParser
	ParserStarted bool
	TPS           float64
	Protocol      int
	Mu            sync.RWMutex
	ClientsMu     sync.RWMutex
}

// GOTVServer servidor GOTV+
type GOTVServer struct {
	matches        map[string]*ActiveMatch
	matchesMu      sync.RWMutex
	upgrader       websocket.Upgrader
	port           int
	authToken      string
	supabaseClient *SupabaseClient
	teamRegistry   *TeamRegistryCache
	teamIdentifier *TeamIdentifier
	mapExtractor   *MapExtractor
	matchzyHandler *MatchZyHandler
}

// NewGOTVServer cria um novo servidor GOTV+
func NewGOTVServer(port int, authToken string) *GOTVServer {
	// Inicializar cliente Supabase e registry de times
	supabaseClient := NewSupabaseClient()
	var teamRegistry *TeamRegistryCache
	var teamIdentifier *TeamIdentifier

	if supabaseClient != nil {
		teamRegistry = NewTeamRegistryCache(supabaseClient)
		teamIdentifier = NewTeamIdentifier(teamRegistry)
		// Carregar times iniciais
		if err := teamRegistry.Refresh(); err != nil {
			log.Printf("[GOTV] Warning: Could not load teams from Supabase: %v", err)
		}
	} else {
		log.Printf("[GOTV] Warning: Supabase not configured - team identification disabled")
	}

	return &GOTVServer{
		matches:        make(map[string]*ActiveMatch),
		port:           port,
		authToken:      authToken,
		supabaseClient: supabaseClient,
		teamRegistry:   teamRegistry,
		teamIdentifier: teamIdentifier,
		mapExtractor:   NewMapExtractor(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// loggingMiddleware logs all incoming HTTP requests
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[HTTP] %s %s from %s", r.Method, r.URL.String(), r.RemoteAddr)
		next(w, r)
	}
}

// Start inicia o servidor
func (s *GOTVServer) Start() {
	// Iniciar goroutines de manutenção
	go s.cleanupExpiredMatches()
	go s.logMemoryStats()

	// Rotas HTTP com logging
	http.HandleFunc("/gotv/", loggingMiddleware(s.handleGOTV))
	http.HandleFunc("/api/matches", loggingMiddleware(s.handleListMatches))
	http.HandleFunc("/api/match/", loggingMiddleware(s.handleGetMatch))
	http.HandleFunc("/api/events/", loggingMiddleware(s.handleGetEvents))
	http.HandleFunc("/api/teams/refresh", loggingMiddleware(s.handleRefreshTeams))
	http.HandleFunc("/api/setmap/", loggingMiddleware(s.handleSetMap))
	http.HandleFunc("/ws", loggingMiddleware(s.handleWebSocket))

	// Inicializar e registrar handler do MatchZy
	matchzyAuthToken := os.Getenv("MATCHZY_AUTH_TOKEN")
	if matchzyAuthToken == "" {
		matchzyAuthToken = "orbital_secret_token"
	}

	// Configurar persistência de stats
	finishAPIURL := os.Getenv("FINISH_API_URL")
	if finishAPIURL == "" {
		finishAPIURL = "https://orbital-store.vercel.app"
	}

	var persister *StatsPersister
	if s.supabaseClient != nil {
		persister = NewStatsPersister(s.supabaseClient, s.teamRegistry, finishAPIURL)
		log.Printf("[GOTV] Stats persister configured - Finish API: %s", finishAPIURL)
	}

	s.matchzyHandler = NewMatchZyHandler(s, matchzyAuthToken, s.supabaseClient, persister)
	s.matchzyHandler.RegisterRoutes()

	log.Printf("=================================")
	log.Printf("   ArenaCS GOTV+ Server (Go)")
	log.Printf("=================================")
	log.Printf("")
	log.Printf("[GOTV] Server running on port %d", s.port)
	log.Printf("[GOTV] HTTP endpoints:")
	log.Printf("  - POST /gotv/{matchId}/{fragment}/{type} - Receive fragments from CS2")
	log.Printf("  - GET  /gotv/{matchId}/sync - Fragment sync")
	log.Printf("  - GET  /api/matches - List active matches")
	log.Printf("  - GET  /api/match/{matchId} - Get match state")
	log.Printf("[GOTV] WebSocket: ws://localhost:%d/ws?match={matchId}", s.port)
	log.Printf("")
	log.Printf("CS2 Server Configuration:")
	log.Printf("-------------------------")
	log.Printf("  tv_enable 1")
	log.Printf("  tv_broadcast_url \"http://YOUR_IP:%d/gotv\"", s.port)
	log.Printf("  tv_broadcast_origin_auth \"%s\"", s.authToken)
	log.Printf("  tv_broadcast 1")
	log.Printf("")
	log.Printf("MatchZy Configuration:")
	log.Printf("----------------------")
	log.Printf("  matchzy_remote_log_url \"http://YOUR_IP:%d/api/matchzy/events\"", s.port)
	log.Printf("  matchzy_remote_log_header_key \"Authorization\"")
	log.Printf("  matchzy_remote_log_header_value \"Bearer %s\"", matchzyAuthToken)
	log.Printf("")

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil))
}

// handleGOTV processa todas as requisições GOTV+
func (s *GOTVServer) handleGOTV(w http.ResponseWriter, r *http.Request) {
	// CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Origin-Auth")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Parse URL: /gotv/{matchId}/{fragment}/{type} ou /gotv/{matchId}/sync
	path := strings.TrimPrefix(r.URL.Path, "/gotv/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	matchID := parts[0]

	// Sync request
	if parts[1] == "sync" {
		s.handleSync(w, r, matchID)
		return
	}

	// Fragment request
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	fragmentNum, err := strconv.Atoi(parts[1])
	if err != nil {
		http.Error(w, "Invalid fragment number", http.StatusBadRequest)
		return
	}

	fragmentType := parts[2]

	if r.Method == "POST" {
		// Verificar autenticação
		authHeader := r.Header.Get("X-Origin-Auth")
		if authHeader != s.authToken {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		s.handleReceiveFragment(w, r, matchID, fragmentNum, fragmentType)
	} else if r.Method == "GET" {
		s.handleServeFragment(w, matchID, fragmentNum, fragmentType)
	}
}

// handleSync responde a requisições de sincronização
func (s *GOTVServer) handleSync(w http.ResponseWriter, r *http.Request, matchID string) {
	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		http.Error(w, "Match not found", http.StatusNotFound)
		return
	}

	match.Mu.RLock()
	defer match.Mu.RUnlock()

	// Encontrar o último fragmento
	lastFrag := 0
	for f := range match.Fragments {
		if f > lastFrag {
			lastFrag = f
		}
	}

	// Se o cliente pediu um fragmento específico, usar esse
	requestedFragment := r.URL.Query().Get("fragment")
	fragment := lastFrag
	if requestedFragment != "" {
		fmt.Sscanf(requestedFragment, "%d", &fragment)
		// Garantir que não seja maior que o último disponível
		if fragment > lastFrag {
			fragment = lastFrag
		}
	}

	// Determinar o signup_fragment (start) real
	signupFragment := 0
	if match.StartFragment != nil {
		signupFragment = match.StartFragment.Number
	}

	response := map[string]interface{}{
		"tick":             match.State.LastTick,
		"rtdelay":          0.0,
		"rcvage":           0.0,
		"fragment":         fragment,
		"signup_fragment":  signupFragment,
		"tps":              int(match.TPS),
		"keyframe_interval": 3.0,
		"protocol":         match.Protocol,
		"map":              match.State.MapName,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleReceiveFragment recebe um fragmento do CS2
func (s *GOTVServer) handleReceiveFragment(w http.ResponseWriter, r *http.Request, matchID string, fragmentNum int, fragmentType string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	s.matchesMu.Lock()
	match, exists := s.matches[matchID]
	if !exists {
		match = &ActiveMatch{
			MatchID: matchID,
			State: &MatchState{
				MatchID:      matchID,
				Status:       "live",
				RoundPhase:   "warmup",
				Players:      []PlayerState{},
				UpdatedAt:    time.Now(),
			},
			Clients:        make(map[*WSClient]bool),
			Fragments:      make(map[int]*Fragment),
			DeltaFragments: make(map[int]*Fragment),
			TPS:            128,
			Protocol:       4,
		}
		s.matches[matchID] = match
		log.Printf("[GOTV] ✓ Match created: %s", matchID)
	}
	s.matchesMu.Unlock()

	fragment := &Fragment{
		Number:    fragmentNum,
		Type:      fragmentType,
		Data:      data,
		Timestamp: time.Now(),
	}

	match.Mu.Lock()

	// Armazenar fragmento no mapa correto e limpar antigos
	if fragmentType == "delta" {
		match.DeltaFragments[fragmentNum] = fragment
		// Rotação: manter apenas os últimos MaxFragments
		if len(match.DeltaFragments) > MaxFragments {
			pruneFragmentMap(match.DeltaFragments, MaxFragments)
		}
	} else {
		match.Fragments[fragmentNum] = fragment
		if len(match.Fragments) > MaxFragments {
			pruneFragmentMap(match.Fragments, MaxFragments)
		}
	}

	// Tentar extrair mapa se ainda não temos
	if match.State.MapName == "" && s.mapExtractor != nil {
		mapName, err := s.mapExtractor.ExtractMapFromStartFragment(data)
		if err == nil && mapName != "" {
			match.State.MapName = mapName
			log.Printf("[GOTV] ✓ Map extracted from %s fragment #%d: %s", fragmentType, fragmentNum, mapName)
		}
	}

	if fragmentType == "start" {
		// Se já tinha um parser rodando, parar e resetar para recomeçar limpo
		if match.ParserStarted && match.Parser != nil {
			log.Printf("[GOTV] 🔄 New start fragment received - resetting parser for clean sync")
			match.Parser.Stop()
			match.Parser = nil
			match.ParserStarted = false
			// Limpar fragmentos antigos
			match.Fragments = make(map[int]*Fragment)
			match.DeltaFragments = make(map[int]*Fragment)
			// Resetar estado
			match.State.ScoreCT = 0
			match.State.ScoreT = 0
			match.State.CurrentRound = 0
			match.State.RoundPhase = "warmup"
			match.State.Players = []PlayerState{}
			match.State.Bomb = nil
		}

		match.StartFragment = fragment
		log.Printf("[GOTV] Start fragment received - %d bytes", len(data))

		// Usar MapExtractor para extrair o nome do mapa do start fragment
		if s.mapExtractor != nil {
			mapName, err := s.mapExtractor.ExtractMapFromStartFragment(data)
			if err == nil && mapName != "" {
				match.State.MapName = mapName
				log.Printf("[GOTV] ✓ Map extracted from start fragment: %s", mapName)
			} else {
				log.Printf("[GOTV] ⚠ Could not extract map from start fragment: %v", err)
			}
		}

		// Fallback: capturar da query string se disponível
		if match.State.MapName == "" {
			mapName := r.URL.Query().Get("map")
			if mapName != "" {
				match.State.MapName = mapName
				log.Printf("[GOTV] Map name captured from query: %s", mapName)
			}
		}
		if tps := r.URL.Query().Get("tps"); tps != "" {
			fmt.Sscanf(tps, "%f", &match.TPS)
			log.Printf("[GOTV] TPS captured: %f", match.TPS)
		}
		if protocol := r.URL.Query().Get("protocol"); protocol != "" {
			fmt.Sscanf(protocol, "%d", &match.Protocol)
			log.Printf("[GOTV] Protocol captured: %d", match.Protocol)
		}
	}
	match.State.TotalBytes += int64(len(data))
	match.State.FragmentCount = len(match.Fragments)
	match.State.LastTick = fragmentNum * 128 // Aproximação
	match.State.UpdatedAt = time.Now()

	// O CS2 só envia o start fragment uma vez quando o broadcast começa.
	// Se o GOTV server reiniciar, o start NÃO é reenviado.
	// Sem o start fragment, o demoinfocs parser não consegue inicializar as tabelas de entidades.
	// Nesse caso, NÃO iniciamos o parser - ele precisa do start real.
	// Log quando estamos recebendo fragments sem start (para diagnóstico)
	if match.StartFragment == nil && fragmentType == "full" && len(match.Fragments) == 1 {
		log.Printf("[GOTV] ⚠ Receiving fragments but no start fragment yet - parser will start when CS2 sends start (requires match restart or tv_broadcast 1)")
	}

	// Iniciar parser APENAS quando temos o start fragment real + alguns full fragments
	shouldStartParser := !match.ParserStarted && match.StartFragment != nil && match.StartFragment.Type == "start" && len(match.Fragments) >= 3
	match.Mu.Unlock()

	// Log apenas a cada 10 fragmentos para reduzir spam
	if fragmentNum%10 == 0 {
		hasStart := "no-start"
		if match.StartFragment != nil && match.StartFragment.Type == "start" {
			hasStart = "has-start"
		}
		parserStatus := "off"
		if match.ParserStarted {
			parserStatus = "running"
		}
		log.Printf("[GOTV] ← Fragment #%d (%s) - %d bytes - Match: %s - Frags: %d+%d delta [%s|parser:%s]",
			fragmentNum, fragmentType, len(data), matchID, len(match.Fragments), len(match.DeltaFragments), hasStart, parserStatus)
	}

	// Iniciar o parser de broadcast
	if shouldStartParser {
		match.Mu.Lock()
		match.ParserStarted = true
		match.Parser = NewBroadcastParser(matchID)
		match.Mu.Unlock()

		// Configurar callbacks do parser
		match.Parser.SetCallbacks(
			func(state *MatchState) {
				// Identificar times baseado nos jogadores
				var teamCT, teamT *IdentifiedTeam
				if s.teamIdentifier != nil && len(state.Players) >= 6 {
					teamCT, teamT = s.teamIdentifier.IdentifyTeams(state.Players)
				}

				// Atualizar estado da partida
				match.Mu.Lock()
				match.State.Status = state.Status
				match.State.MapName = state.MapName
				match.State.ScoreCT = state.ScoreCT
				match.State.ScoreT = state.ScoreT
				match.State.CurrentRound = state.CurrentRound
				match.State.RoundPhase = state.RoundPhase
				match.State.Players = state.Players
				match.State.Bomb = state.Bomb
				// Atualizar times identificados
				if teamCT != nil {
					match.State.TeamCT = teamCT
					state.TeamCT = teamCT
				}
				if teamT != nil {
					match.State.TeamT = teamT
					state.TeamT = teamT
				}
				match.Mu.Unlock()

				// Broadcast estado para clientes
				s.broadcastToMatch(matchID, WebSocketMessage{
					Type:      "match_state",
					MatchID:   matchID,
					Data:      state,
					Timestamp: time.Now().UnixMilli(),
				})
			},
			func(event GameEvent) {
				// Broadcast evento para clientes
				s.broadcastToMatch(matchID, WebSocketMessage{
					Type:      "event",
					MatchID:   matchID,
					Data:      event,
					Timestamp: time.Now().UnixMilli(),
				})
			},
		)

		// Iniciar parser conectando ao próprio servidor de broadcast
		go func() {
			broadcastURL := fmt.Sprintf("http://127.0.0.1:%d/gotv/%s", s.port, matchID)
			log.Printf("[GOTV] 🎮 Starting broadcast parser for match %s at %s", matchID, broadcastURL)

			if err := match.Parser.ParseBroadcast(broadcastURL); err != nil {
				log.Printf("[GOTV] ❌ Parser error: %v", err)
			}
		}()
	}

	// Broadcast para clientes WebSocket
	s.broadcastToMatch(matchID, WebSocketMessage{
		Type:    "fragment",
		MatchID: matchID,
		Data: map[string]interface{}{
			"fragment": fragmentNum,
			"type":     fragmentType,
			"size":     len(data),
		},
		Timestamp: time.Now().UnixMilli(),
	})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleServeFragment serve um fragmento para clientes
func (s *GOTVServer) handleServeFragment(w http.ResponseWriter, matchID string, fragmentNum int, fragmentType string) {
	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		http.Error(w, "Match not found", http.StatusNotFound)
		return
	}

	match.Mu.RLock()
	var fragment *Fragment
	switch fragmentType {
	case "start":
		fragment = match.StartFragment
	case "delta":
		fragment = match.DeltaFragments[fragmentNum]
	case "full":
		fragment = match.Fragments[fragmentNum]
	default:
		fragment = match.Fragments[fragmentNum]
	}
	match.Mu.RUnlock()

	if fragment == nil {
		http.Error(w, "Fragment not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment.Data)))
	w.Write(fragment.Data)
}

// broadcastToMatch envia mensagem para todos os clientes de uma partida
func (s *GOTVServer) broadcastToMatch(matchID string, msg WebSocketMessage) {
	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[GOTV] Error marshaling message: %v", err)
		return
	}

	match.ClientsMu.RLock()
	clients := make([]*WSClient, 0, len(match.Clients))
	for client := range match.Clients {
		clients = append(clients, client)
	}
	match.ClientsMu.RUnlock()

	var deadClients []*WSClient
	for _, client := range clients {
		client.Conn.SetWriteDeadline(time.Now().Add(WSWriteTimeout))
		err := client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			deadClients = append(deadClients, client)
		}
	}

	// Remover clientes mortos
	if len(deadClients) > 0 {
		match.ClientsMu.Lock()
		for _, client := range deadClients {
			delete(match.Clients, client)
			client.Conn.Close()
		}
		match.ClientsMu.Unlock()
	}
}

// handleListMatches lista todas as partidas ativas
func (s *GOTVServer) handleListMatches(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	s.matchesMu.RLock()
	var matches []map[string]interface{}
	for id, match := range s.matches {
		match.Mu.RLock()
		matches = append(matches, map[string]interface{}{
			"matchId":       id,
			"status":        match.State.Status,
			"mapName":       match.State.MapName,
			"scoreCT":       match.State.ScoreCT,
			"scoreT":        match.State.ScoreT,
			"currentRound":  match.State.CurrentRound,
			"fragmentCount": match.State.FragmentCount,
			"totalBytes":    match.State.TotalBytes,
			"clients":       len(match.Clients),
			"updatedAt":     match.State.UpdatedAt,
		})
		match.Mu.RUnlock()
	}
	s.matchesMu.RUnlock()

	if matches == nil {
		matches = []map[string]interface{}{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"matches": matches})
}

// handleGetMatch retorna o estado de uma partida específica
func (s *GOTVServer) handleGetMatch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	matchID := strings.TrimPrefix(r.URL.Path, "/api/match/")

	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Match not found"})
		return
	}

	match.Mu.RLock()
	defer match.Mu.RUnlock()

	json.NewEncoder(w).Encode(match.State)
}

// handleGetEvents retorna os eventos de uma partida
func (s *GOTVServer) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	matchID := strings.TrimPrefix(r.URL.Path, "/api/events/")

	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Match not found"})
		return
	}

	var events []GameEvent
	if match.Parser != nil {
		events = match.Parser.GetEvents()
	}

	if events == nil {
		events = []GameEvent{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"matchId": matchID,
		"events":  events,
		"count":   len(events),
	})
}

// handleRefreshTeams força atualização do cache de times
func (s *GOTVServer) handleRefreshTeams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if s.teamRegistry == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "Team registry not available - Supabase not configured"})
		return
	}

	if err := s.teamRegistry.Refresh(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	teams := s.teamRegistry.GetAllTeams()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Teams refreshed successfully",
		"count":   len(teams),
	})
}

// handleSetMap define o nome do mapa manualmente para uma partida
// POST /api/setmap/{matchId}?map=de_dust2
func (s *GOTVServer) handleSetMap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	matchID := strings.TrimPrefix(r.URL.Path, "/api/setmap/")
	mapName := r.URL.Query().Get("map")

	if mapName == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "map parameter required"})
		return
	}

	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Match not found"})
		return
	}

	match.Mu.Lock()
	match.State.MapName = mapName
	match.Mu.Unlock()

	log.Printf("[GOTV] ✓ Map manually set for match %s: %s", matchID, mapName)

	// Também atualizar no parser se existir
	if match.Parser != nil {
		match.Parser.mu.Lock()
		match.Parser.state.MapName = mapName
		match.Parser.mu.Unlock()
	}

	// Broadcast para clientes
	s.broadcastToMatch(matchID, WebSocketMessage{
		Type:      "match_state",
		MatchID:   matchID,
		Data:      match.State,
		Timestamp: time.Now().UnixMilli(),
	})

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"matchId": matchID,
		"map":     mapName,
	})
}

// handleWebSocket gerencia conexões WebSocket
func (s *GOTVServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	matchID := r.URL.Query().Get("match")
	if matchID == "" {
		http.Error(w, "Match ID required", http.StatusBadRequest)
		return
	}

	s.matchesMu.RLock()
	match, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if !exists {
		http.Error(w, "Match not found", http.StatusNotFound)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[GOTV] WebSocket upgrade error: %v", err)
		return
	}

	// Criar cliente com mutex para escrita thread-safe
	client := &WSClient{Conn: conn}

	// Configurar timeouts no WebSocket
	conn.SetReadDeadline(time.Now().Add(WSReadTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(WSReadTimeout))
		return nil
	})

	match.ClientsMu.Lock()
	match.Clients[client] = true
	match.ClientsMu.Unlock()

	log.Printf("[GOTV] → Client connected to match %s (total: %d)", matchID, len(match.Clients))

	// Enviar estado inicial
	match.Mu.RLock()
	initialMsg := WebSocketMessage{
		Type:      "connected",
		MatchID:   matchID,
		Data:      match.State,
		Timestamp: time.Now().UnixMilli(),
	}
	match.Mu.RUnlock()

	data, _ := json.Marshal(initialMsg)
	conn.SetWriteDeadline(time.Now().Add(WSWriteTimeout))
	client.WriteMessage(websocket.TextMessage, data)

	// Enviar eventos existentes para o novo cliente (para sincronizar game log)
	if match.Parser != nil {
		existingEvents := match.Parser.GetEvents()
		for _, event := range existingEvents {
			eventMsg := WebSocketMessage{
				Type:      "event",
				MatchID:   matchID,
				Data:      event,
				Timestamp: time.Now().UnixMilli(),
			}
			eventData, _ := json.Marshal(eventMsg)
			conn.SetWriteDeadline(time.Now().Add(WSWriteTimeout))
			client.WriteMessage(websocket.TextMessage, eventData)
		}
		log.Printf("[GOTV] Sent %d existing events to new client", len(existingEvents))
	}

	// Gerenciar mensagens recebidas
	go func() {
		defer func() {
			match.ClientsMu.Lock()
			delete(match.Clients, client)
			match.ClientsMu.Unlock()
			conn.Close()
			log.Printf("[GOTV] ← Client disconnected from match %s", matchID)
		}()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
			// Reset read deadline on any message
			conn.SetReadDeadline(time.Now().Add(WSReadTimeout))

			var msg map[string]interface{}
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}

			if msg["type"] == "ping" {
				pong, _ := json.Marshal(map[string]interface{}{
					"type":      "pong",
					"timestamp": time.Now().UnixMilli(),
				})
				conn.SetWriteDeadline(time.Now().Add(WSWriteTimeout))
				conn.WriteMessage(websocket.TextMessage, pong)
			}
		}
	}()
}

// pruneFragmentMap remove os fragmentos mais antigos mantendo apenas os últimos maxKeep
func pruneFragmentMap(fragments map[int]*Fragment, maxKeep int) {
	if len(fragments) <= maxKeep {
		return
	}

	// Encontrar o fragmento mais recente
	maxNum := 0
	for num := range fragments {
		if num > maxNum {
			maxNum = num
		}
	}

	// Remover fragmentos antigos (manter apenas os que estão dentro do range)
	minKeep := maxNum - maxKeep
	for num := range fragments {
		if num < minKeep {
			delete(fragments, num)
		}
	}
}

// cleanupExpiredMatches remove partidas sem atividade
func (s *GOTVServer) cleanupExpiredMatches() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.matchesMu.Lock()
		now := time.Now()
		var toDelete []string

		for id, match := range s.matches {
			match.Mu.RLock()
			lastUpdate := match.State.UpdatedAt
			match.Mu.RUnlock()

			if now.Sub(lastUpdate) > MatchExpirationTime {
				// Parar parser se estiver rodando
				if match.Parser != nil {
					match.Parser.Stop()
				}
				// Fechar todos os clientes WebSocket
				match.ClientsMu.Lock()
				for client := range match.Clients {
					client.Conn.Close()
				}
				match.ClientsMu.Unlock()

				toDelete = append(toDelete, id)
			}
		}

		for _, id := range toDelete {
			delete(s.matches, id)
			log.Printf("[GOTV] 🧹 Expired match removed: %s", id)
		}
		s.matchesMu.Unlock()

		if len(toDelete) > 0 {
			runtime.GC()
			log.Printf("[GOTV] 🧹 Cleanup: removed %d expired matches", len(toDelete))
		}

		// Limpar estados MatchZy expirados também
		if s.matchzyHandler != nil {
			s.matchzyHandler.CleanupExpiredStates()
		}
	}
}

// logMemoryStats loga estatísticas de memória periodicamente
func (s *GOTVServer) logMemoryStats() {
	ticker := time.NewTicker(MemoryLogInterval)
	defer ticker.Stop()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		s.matchesMu.RLock()
		matchCount := len(s.matches)
		totalFrags := 0
		totalClients := 0
		for _, match := range s.matches {
			match.Mu.RLock()
			totalFrags += len(match.Fragments) + len(match.DeltaFragments)
			match.Mu.RUnlock()
			match.ClientsMu.RLock()
			totalClients += len(match.Clients)
			match.ClientsMu.RUnlock()
		}
		s.matchesMu.RUnlock()

		log.Printf("[MEM] Alloc: %dMB | Sys: %dMB | GC: %d | Goroutines: %d | Matches: %d | Fragments: %d | Clients: %d",
			m.Alloc/1024/1024,
			m.Sys/1024/1024,
			m.NumGC,
			runtime.NumGoroutine(),
			matchCount,
			totalFrags,
			totalClients,
		)
	}
}

func main() {
	port := 8080
	authToken := "orbital_gotv_secret"

	// Railway usa PORT, mas também suportamos GOTV_PORT
	if p := os.Getenv("PORT"); p != "" {
		fmt.Sscanf(p, "%d", &port)
	} else if p := os.Getenv("GOTV_PORT"); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}
	if t := os.Getenv("GOTV_AUTH_TOKEN"); t != "" {
		authToken = t
	}

	server := NewGOTVServer(port, authToken)
	server.Start()
}
