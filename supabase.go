package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
	Nickname  string  `json:"nickname"`
	Name      *string `json:"name"`
	SteamID   *string `json:"steam_id"`
	TeamID    *string `json:"team_id"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
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
