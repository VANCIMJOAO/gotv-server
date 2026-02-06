package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// SupabaseClient cliente para comunicação com Supabase
type SupabaseClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// TeamFromDB representa um time do banco de dados
type TeamFromDB struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Tag       string     `json:"tag"`
	LogoURL   *string    `json:"logo_url"`
	CreatedAt string     `json:"created_at"`
	UpdatedAt *string    `json:"updated_at"`
	Players   []PlayerFromDB `json:"players,omitempty"`
}

// PlayerFromDB representa um jogador do banco de dados
type PlayerFromDB struct {
	ID        string  `json:"id"`
	ProfileID *string `json:"profile_id"`
	Nickname  string  `json:"nickname"`
	Name      *string `json:"name"`
	SteamID   *string `json:"steam_id"`
	TeamID    *string `json:"team_id"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
}

// MatchFromDB representa uma partida do banco de dados com teams
type MatchFromDB struct {
	ID           string      `json:"id"`
	TournamentID string      `json:"tournament_id"`
	Team1ID      string      `json:"team1_id"`
	Team2ID      string      `json:"team2_id"`
	Team1Score   int         `json:"team1_score"`
	Team2Score   int         `json:"team2_score"`
	WinnerID     *string     `json:"winner_id"`
	Status       string      `json:"status"`
	Round        *string     `json:"round"`
	BestOf       int         `json:"best_of"`
	MapName      *string     `json:"map_name"`
	IsLive       *bool       `json:"is_live"`
	MatchPhase   *string     `json:"match_phase"`
	Team1        *TeamFromDB `json:"team1,omitempty"`
	Team2        *TeamFromDB `json:"team2,omitempty"`
}

// NewSupabaseClient cria um novo cliente Supabase
func NewSupabaseClient() *SupabaseClient {
	baseURL := os.Getenv("SUPABASE_URL")
	apiKey := os.Getenv("SUPABASE_ANON_KEY")

	if baseURL == "" || apiKey == "" {
		log.Printf("[Supabase] Warning: SUPABASE_URL or SUPABASE_ANON_KEY not set")
		return nil
	}

	return &SupabaseClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// FetchTeamsWithPlayers busca todos os times com seus jogadores
func (c *SupabaseClient) FetchTeamsWithPlayers() ([]TeamFromDB, error) {
	if c == nil {
		return nil, fmt.Errorf("supabase client not initialized")
	}

	// Buscar times
	teams, err := c.fetchTeams()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch teams: %w", err)
	}

	// Buscar jogadores
	players, err := c.fetchPlayers()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch players: %w", err)
	}

	// Agrupar jogadores por time
	playersByTeam := make(map[string][]PlayerFromDB)
	for _, player := range players {
		if player.TeamID != nil {
			playersByTeam[*player.TeamID] = append(playersByTeam[*player.TeamID], player)
		}
	}

	// Adicionar jogadores aos times
	for i := range teams {
		teams[i].Players = playersByTeam[teams[i].ID]
	}

	return teams, nil
}

// fetchTeams busca todos os times
func (c *SupabaseClient) fetchTeams() ([]TeamFromDB, error) {
	url := fmt.Sprintf("%s/rest/v1/teams?select=*", c.baseURL)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(body))
	}

	var teams []TeamFromDB
	if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
		return nil, err
	}

	return teams, nil
}

// fetchPlayers busca todos os jogadores
func (c *SupabaseClient) fetchPlayers() ([]PlayerFromDB, error) {
	url := fmt.Sprintf("%s/rest/v1/team_players?select=*", c.baseURL)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(body))
	}

	var players []PlayerFromDB
	if err := json.NewDecoder(resp.Body).Decode(&players); err != nil {
		return nil, err
	}

	return players, nil
}

// TeamRegistryCache cache de times para identificação rápida
type TeamRegistryCache struct {
	client      *SupabaseClient
	teams       map[string]*TeamFromDB  // teamId -> team
	playerTeams map[string]string       // steamId -> teamId
	lastFetch   time.Time
	cacheTTL    time.Duration
	mu          sync.RWMutex
}

// NewTeamRegistryCache cria um novo cache de times
func NewTeamRegistryCache(client *SupabaseClient) *TeamRegistryCache {
	return &TeamRegistryCache{
		client:      client,
		teams:       make(map[string]*TeamFromDB),
		playerTeams: make(map[string]string),
		cacheTTL:    5 * time.Minute,
	}
}

// Refresh atualiza o cache buscando do Supabase
func (r *TeamRegistryCache) Refresh() error {
	if r.client == nil {
		return fmt.Errorf("supabase client not available")
	}

	teams, err := r.client.FetchTeamsWithPlayers()
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Limpar e recarregar
	r.teams = make(map[string]*TeamFromDB)
	r.playerTeams = make(map[string]string)

	for i := range teams {
		team := &teams[i]
		r.teams[team.ID] = team

		for _, player := range team.Players {
			if player.SteamID != nil && *player.SteamID != "" {
				r.playerTeams[*player.SteamID] = team.ID
			}
		}
	}

	r.lastFetch = time.Now()
	log.Printf("[TeamRegistry] Refreshed cache: %d teams, %d players mapped", len(r.teams), len(r.playerTeams))

	return nil
}

// RefreshIfNeeded atualiza o cache se expirou
func (r *TeamRegistryCache) RefreshIfNeeded() {
	r.mu.RLock()
	needsRefresh := time.Since(r.lastFetch) > r.cacheTTL
	r.mu.RUnlock()

	if needsRefresh {
		if err := r.Refresh(); err != nil {
			log.Printf("[TeamRegistry] Failed to refresh: %v", err)
		}
	}
}

// GetTeamBySteamID retorna o time de um jogador pelo SteamID
func (r *TeamRegistryCache) GetTeamBySteamID(steamID string) *TeamFromDB {
	r.mu.RLock()
	defer r.mu.RUnlock()

	teamID, exists := r.playerTeams[steamID]
	if !exists {
		return nil
	}

	return r.teams[teamID]
}

// GetTeamByID retorna um time pelo ID
func (r *TeamRegistryCache) GetTeamByID(teamID string) *TeamFromDB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.teams[teamID]
}

// GetAllTeams retorna todos os times
func (r *TeamRegistryCache) GetAllTeams() []*TeamFromDB {
	r.mu.RLock()
	defer r.mu.RUnlock()

	teams := make([]*TeamFromDB, 0, len(r.teams))
	for _, team := range r.teams {
		teams = append(teams, team)
	}
	return teams
}

// GetProfileIDBySteamID retorna o profile_id de um jogador pelo SteamID
func (r *TeamRegistryCache) GetProfileIDBySteamID(steamID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	teamID, exists := r.playerTeams[steamID]
	if !exists {
		return ""
	}

	team := r.teams[teamID]
	if team == nil {
		return ""
	}

	for _, player := range team.Players {
		if player.SteamID != nil && *player.SteamID == steamID && player.ProfileID != nil {
			return *player.ProfileID
		}
	}
	return ""
}

// FetchMatchDetails busca uma partida com seus times do Supabase
func (c *SupabaseClient) FetchMatchDetails(matchID string) (*MatchFromDB, error) {
	if c == nil {
		return nil, fmt.Errorf("supabase client not initialized")
	}

	// Buscar partida com teams via embedding
	url := fmt.Sprintf("%s/rest/v1/matches?id=eq.%s&select=*,team1:teams!matches_team1_id_fkey(id,name,tag,logo_url),team2:teams!matches_team2_id_fkey(id,name,tag,logo_url)", c.baseURL, matchID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch match: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(body))
	}

	var matches []MatchFromDB
	if err := json.NewDecoder(resp.Body).Decode(&matches); err != nil {
		return nil, fmt.Errorf("failed to decode match: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("match not found: %s", matchID)
	}

	return &matches[0], nil
}

// UpdateMatchLive marca uma partida como ao vivo no Supabase
func (c *SupabaseClient) UpdateMatchLive(matchID string) error {
	if c == nil {
		return fmt.Errorf("supabase client not initialized")
	}

	url := fmt.Sprintf("%s/rest/v1/matches?id=eq.%s", c.baseURL, matchID)

	payload := map[string]interface{}{
		"status":      "live",
		"match_phase": "live",
		"is_live":     true,
		"started_at":  time.Now().UTC().Format(time.RFC3339),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PATCH", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=minimal")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to update match live: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[Supabase] Match %s marked as LIVE", matchID)
	return nil
}

// UpdateMatchScores atualiza os scores de uma partida em tempo real
func (c *SupabaseClient) UpdateMatchScores(matchID string, team1Score, team2Score int) error {
	if c == nil {
		return fmt.Errorf("supabase client not initialized")
	}

	url := fmt.Sprintf("%s/rest/v1/matches?id=eq.%s", c.baseURL, matchID)

	payload := map[string]interface{}{
		"team1_score": team1Score,
		"team2_score": team2Score,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PATCH", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=minimal")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to update match scores: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// InsertMatchPlayerStats insere stats de jogadores no banco
func (c *SupabaseClient) InsertMatchPlayerStats(stats []map[string]interface{}) error {
	if c == nil {
		return fmt.Errorf("supabase client not initialized")
	}

	url := fmt.Sprintf("%s/rest/v1/match_player_stats", c.baseURL)

	body, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=minimal")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to insert player stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("supabase returned status %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[Supabase] Inserted %d player stats records", len(stats))
	return nil
}
