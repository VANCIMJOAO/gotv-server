package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// MaxMatchZyEvents é o número máximo de eventos MatchZy mantidos por partida
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

// GameEvent representa um evento de jogo (kill, bomb, round, etc.)
// Mantido aqui porque é usado na persistência e no WebSocket broadcast
type GameEvent struct {
	Type      string                 `json:"type"`
	Tick      int                    `json:"tick"`
	Round     int                    `json:"round"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
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
	Conn *websocket.Conn
	Mu   sync.Mutex
}

// WriteMessage escreve uma mensagem de forma thread-safe
func (c *WSClient) WriteMessage(messageType int, data []byte) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

// ActiveMatch representa uma partida ativa no servidor
// Simplificado: apenas contém clientes WebSocket e referência ao estado MatchZy
type ActiveMatch struct {
	MatchID   string // UUID do banco de dados
	Clients   map[*WSClient]bool
	ClientsMu sync.RWMutex
	UpdatedAt time.Time
}

// GOTVServer servidor principal (agora apenas relay de MatchZy + WebSocket)
type GOTVServer struct {
	matches        map[string]*ActiveMatch // matchUUID → ActiveMatch
	matchesMu      sync.RWMutex
	upgrader       websocket.Upgrader
	port           int
	authToken      string
	supabaseClient *SupabaseClient
	teamRegistry   *TeamRegistryCache
	matchzyHandler *MatchZyHandler
}

// NewGOTVServer cria um novo servidor
func NewGOTVServer(port int, authToken string) *GOTVServer {
	supabaseClient := NewSupabaseClient()
	var teamRegistry *TeamRegistryCache

	if supabaseClient != nil {
		teamRegistry = NewTeamRegistryCache(supabaseClient)
		if err := teamRegistry.Refresh(); err != nil {
			log.Printf("[Server] Warning: Could not load teams from Supabase: %v", err)
		}
	} else {
		log.Printf("[Server] Warning: Supabase not configured - persistence disabled")
	}

	return &GOTVServer{
		matches:        make(map[string]*ActiveMatch),
		port:           port,
		authToken:      authToken,
		supabaseClient: supabaseClient,
		teamRegistry:   teamRegistry,
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
	go s.cleanupExpiredMatches()
	go s.logMemoryStats()

	// Rotas REST
	http.HandleFunc("/api/matches", loggingMiddleware(s.handleListMatches))
	http.HandleFunc("/api/match/", loggingMiddleware(s.handleGetMatch))
	http.HandleFunc("/api/teams/refresh", loggingMiddleware(s.handleRefreshTeams))
	http.HandleFunc("/ws", loggingMiddleware(s.handleWebSocket))

	// Inicializar MatchZy handler
	matchzyAuthToken := os.Getenv("MATCHZY_AUTH_TOKEN")
	if matchzyAuthToken == "" {
		matchzyAuthToken = "orbital_secret_token"
	}

	finishAPIURL := os.Getenv("FINISH_API_URL")
	if finishAPIURL == "" {
		finishAPIURL = "https://orbital-store.vercel.app"
	}

	// Forward URL para encaminhar eventos para Next.js webhook (persistencia)
	forwardWebhookURL := os.Getenv("FORWARD_WEBHOOK_URL")
	forwardWebhookSecret := os.Getenv("FORWARD_WEBHOOK_SECRET")

	persister := NewStatsPersister(finishAPIURL)
	log.Printf("[Server] Finish API configured: %s", finishAPIURL)
	if forwardWebhookURL != "" {
		log.Printf("[Server] Event forwarding enabled: %s", forwardWebhookURL)
	}

	s.matchzyHandler = NewMatchZyHandler(s, matchzyAuthToken, s.supabaseClient, persister, forwardWebhookURL, forwardWebhookSecret)
	s.matchzyHandler.RegisterRoutes()

	log.Printf("=================================")
	log.Printf("  Orbital Roxa - Match Server")
	log.Printf("=================================")
	log.Printf("")
	log.Printf("[Server] Running on port %d", s.port)
	log.Printf("[Server] HTTP endpoints:")
	log.Printf("  - GET  /api/matches            - List active matches")
	log.Printf("  - GET  /api/match/{matchId}     - Get match state")
	log.Printf("  - POST /api/teams/refresh       - Refresh team cache")
	log.Printf("  - POST /api/matchzy/events      - MatchZy webhook receiver")
	log.Printf("  - GET  /api/matchzy/state/{id}  - Get MatchZy state")
	log.Printf("[Server] WebSocket: ws://localhost:%d/ws?match={matchId}", s.port)
	log.Printf("")
	log.Printf("MatchZy Configuration (no CS2 server):")
	log.Printf("---------------------------------------")
	log.Printf("  matchzy_remote_log_url \"http://YOUR_IP:%d/api/matchzy/events\"", s.port)
	log.Printf("  matchzy_remote_log_header_key \"Authorization\"")
	log.Printf("  matchzy_remote_log_header_value \"Bearer %s\"", matchzyAuthToken)
	log.Printf("")

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil))
}

// GetOrCreateMatch obtém ou cria uma ActiveMatch pelo UUID
func (s *GOTVServer) GetOrCreateMatch(matchID string) *ActiveMatch {
	s.matchesMu.Lock()
	defer s.matchesMu.Unlock()

	match, exists := s.matches[matchID]
	if !exists {
		match = &ActiveMatch{
			MatchID:   matchID,
			Clients:   make(map[*WSClient]bool),
			UpdatedAt: time.Now(),
		}
		s.matches[matchID] = match
		log.Printf("[Server] Created match room: %s", matchID)
	}
	return match
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
		log.Printf("[Server] Error marshaling message: %v", err)
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
		match.ClientsMu.RLock()
		clientCount := len(match.Clients)
		match.ClientsMu.RUnlock()

		entry := map[string]interface{}{
			"matchId":   id,
			"clients":   clientCount,
			"updatedAt": match.UpdatedAt,
		}

		// Enriquecer com dados do MatchZy state se disponível
		if s.matchzyHandler != nil {
			state := s.matchzyHandler.GetState(id)
			if state != nil {
				state.mu.RLock()
				entry["phase"] = state.Phase
				entry["mapName"] = state.MapName
				entry["currentRound"] = state.CurrentRound
				if state.Team1 != nil {
					entry["team1Name"] = state.Team1.Name
					entry["team1Score"] = state.Team1.Score
				}
				if state.Team2 != nil {
					entry["team2Name"] = state.Team2.Name
					entry["team2Score"] = state.Team2.Score
				}
				state.mu.RUnlock()
			}
		}

		matches = append(matches, entry)
	}
	s.matchesMu.RUnlock()

	if matches == nil {
		matches = []map[string]interface{}{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"matches": matches,
		"version": "2026-02-11-v4-matchzy-only",
	})
}

// handleGetMatch retorna o estado de uma partida específica
func (s *GOTVServer) handleGetMatch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	matchID := strings.TrimPrefix(r.URL.Path, "/api/match/")

	// Buscar estado MatchZy pelo UUID direto
	if s.matchzyHandler != nil {
		state := s.matchzyHandler.GetState(matchID)
		if state != nil {
			state.mu.RLock()
			defer state.mu.RUnlock()
			json.NewEncoder(w).Encode(state)
			return
		}
	}

	// Fallback: verificar se a match existe (pode não ter recebido eventos ainda)
	s.matchesMu.RLock()
	_, exists := s.matches[matchID]
	s.matchesMu.RUnlock()

	if exists {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"matchId": matchID,
			"phase":   "waiting",
			"message": "Match room exists but no MatchZy events received yet",
		})
		return
	}

	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(map[string]string{"error": "Match not found"})
}

// handleRefreshTeams força atualização do cache de times
func (s *GOTVServer) handleRefreshTeams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if s.teamRegistry == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "Team registry not available"})
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
		"count":   len(teams),
	})
}

// handleWebSocket gerencia conexões WebSocket
func (s *GOTVServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	matchID := r.URL.Query().Get("match")
	if matchID == "" {
		http.Error(w, "Match ID required (?match=UUID)", http.StatusBadRequest)
		return
	}

	// Criar/obter match room automaticamente (o frontend pode conectar antes do going_live)
	match := s.GetOrCreateMatch(matchID)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v", err)
		return
	}

	client := &WSClient{Conn: conn}

	conn.SetReadDeadline(time.Now().Add(WSReadTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(WSReadTimeout))
		return nil
	})

	match.ClientsMu.Lock()
	match.Clients[client] = true
	match.ClientsMu.Unlock()

	log.Printf("[WS] Client connected to match %s (total: %d)", matchID, len(match.Clients))

	// Enviar estado inicial (connected + MatchZy state se disponível)
	initialData := map[string]interface{}{
		"matchId": matchID,
	}
	if s.matchzyHandler != nil {
		state := s.matchzyHandler.GetState(matchID)
		if state != nil {
			state.mu.RLock()
			initialData["matchzyState"] = state
			state.mu.RUnlock()
		}
	}

	connMsg := WebSocketMessage{
		Type:      "connected",
		MatchID:   matchID,
		Data:      initialData,
		Timestamp: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(connMsg)
	conn.SetWriteDeadline(time.Now().Add(WSWriteTimeout))
	client.WriteMessage(websocket.TextMessage, data)

	// Enviar game events formatados para sincronizar kill feed, game log
	if s.matchzyHandler != nil {
		state := s.matchzyHandler.GetState(matchID)
		if state != nil {
			state.mu.RLock()
			gameEvents := make([]WSGameEvent, len(state.GameEvents))
			copy(gameEvents, state.GameEvents)
			state.mu.RUnlock()

			for _, event := range gameEvents {
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
			if len(gameEvents) > 0 {
				log.Printf("[WS] Sent %d game events to new client", len(gameEvents))
			}
		}
	}

	// Loop de leitura (pings e futuras mensagens)
	go func() {
		defer func() {
			match.ClientsMu.Lock()
			delete(match.Clients, client)
			match.ClientsMu.Unlock()
			conn.Close()
			log.Printf("[WS] Client disconnected from match %s", matchID)
		}()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
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
				client.WriteMessage(websocket.TextMessage, pong)
			}
		}
	}()
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
			if now.Sub(match.UpdatedAt) > MatchExpirationTime {
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
			log.Printf("[Cleanup] Expired match removed: %s", id)
		}
		s.matchesMu.Unlock()

		if len(toDelete) > 0 {
			runtime.GC()
		}

		// Limpar estados MatchZy expirados
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
		totalClients := 0
		for _, match := range s.matches {
			match.ClientsMu.RLock()
			totalClients += len(match.Clients)
			match.ClientsMu.RUnlock()
		}
		s.matchesMu.RUnlock()

		log.Printf("[MEM] Alloc: %dMB | Sys: %dMB | GC: %d | Goroutines: %d | Matches: %d | Clients: %d",
			m.Alloc/1024/1024,
			m.Sys/1024/1024,
			m.NumGC,
			runtime.NumGoroutine(),
			matchCount,
			totalClients,
		)
	}
}

func main() {
	port := 8080
	authToken := "orbital_gotv_secret"

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
