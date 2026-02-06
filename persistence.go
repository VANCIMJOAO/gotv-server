package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// StatsPersister persiste dados de partidas no Supabase e avança o bracket
type StatsPersister struct {
	supabase     *SupabaseClient
	teamRegistry *TeamRegistryCache
	finishAPIURL string // URL base do Next.js (ex: https://orbital-store.vercel.app)
}

// NewStatsPersister cria um novo persistidor de stats
func NewStatsPersister(supabase *SupabaseClient, teamRegistry *TeamRegistryCache, finishAPIURL string) *StatsPersister {
	return &StatsPersister{
		supabase:     supabase,
		teamRegistry: teamRegistry,
		finishAPIURL: finishAPIURL,
	}
}

// PersistMatchStats persiste os stats dos jogadores no Supabase
// Usa os dados do parser GOTV (kills, deaths, damage, etc) mapeados para profile_ids
func (sp *StatsPersister) PersistMatchStats(dbMatchID string, match *ActiveMatch, state *MatchZyState) error {
	if sp.supabase == nil {
		return fmt.Errorf("supabase client not initialized")
	}
	if sp.teamRegistry == nil {
		return fmt.Errorf("team registry not initialized")
	}

	// Garantir que o cache está atualizado
	sp.teamRegistry.RefreshIfNeeded()

	// Buscar detalhes da partida do banco para ter team1_id e team2_id
	matchDetails, err := sp.supabase.FetchMatchDetails(dbMatchID)
	if err != nil {
		return fmt.Errorf("failed to fetch match details: %w", err)
	}

	// Pegar estado dos jogadores do parser GOTV
	match.Mu.RLock()
	players := make([]PlayerState, len(match.State.Players))
	copy(players, match.State.Players)
	currentRound := match.State.CurrentRound
	match.Mu.RUnlock()

	if len(players) == 0 {
		log.Printf("[Persistence] No player data available for match %s", dbMatchID)
		return nil
	}

	var stats []map[string]interface{}

	for _, player := range players {
		if player.SteamID == "" {
			continue
		}

		// Mapear SteamID → profile_id
		profileID := sp.teamRegistry.GetProfileIDBySteamID(player.SteamID)
		if profileID == "" {
			log.Printf("[Persistence] Could not find profile_id for SteamID %s (%s)", player.SteamID, player.Name)
			continue
		}

		// Determinar team_id do jogador
		team := sp.teamRegistry.GetTeamBySteamID(player.SteamID)
		if team == nil {
			log.Printf("[Persistence] Could not find team for SteamID %s (%s)", player.SteamID, player.Name)
			continue
		}

		// Calcular ADR
		roundsPlayed := currentRound
		if roundsPlayed == 0 {
			roundsPlayed = 1
		}
		adr := float64(player.Damage) / float64(roundsPlayed)

		stat := map[string]interface{}{
			"match_id":      dbMatchID,
			"profile_id":    profileID,
			"team_id":       team.ID,
			"kills":         player.Kills,
			"deaths":        player.Deaths,
			"assists":       player.Assists,
			"headshots":     player.Headshots,
			"total_damage":  player.Damage,
			"adr":           adr,
			"rounds_played": roundsPlayed,
		}

		stats = append(stats, stat)
	}

	if len(stats) == 0 {
		log.Printf("[Persistence] No valid player stats to persist for match %s", dbMatchID)
		return nil
	}

	// Inserir stats no Supabase
	if err := sp.supabase.InsertMatchPlayerStats(stats); err != nil {
		return fmt.Errorf("failed to insert player stats: %w", err)
	}

	log.Printf("[Persistence] Persisted stats for %d players in match %s", len(stats), dbMatchID)

	// Chamar finish API para avançar bracket
	team1Score := 0
	team2Score := 0
	if state.Team1 != nil {
		team1Score = state.Team1.Score
	}
	if state.Team2 != nil {
		team2Score = state.Team2.Score
	}

	if err := sp.CallFinishAPI(dbMatchID, team1Score, team2Score); err != nil {
		log.Printf("[Persistence] Warning: Failed to call finish API for match %s: %v", dbMatchID, err)
		// Não retornar erro - stats já foram persistidas
	}

	// Atualizar gotv_match_id no banco para referência
	if match.MatchID != "" && matchDetails != nil {
		sp.updateGOTVMatchID(dbMatchID, match.MatchID)
	}

	return nil
}

// CallFinishAPI chama a API de finish do Next.js para avançar o bracket
func (sp *StatsPersister) CallFinishAPI(dbMatchID string, team1Score, team2Score int) error {
	if sp.finishAPIURL == "" {
		return fmt.Errorf("finish API URL not configured")
	}

	url := fmt.Sprintf("%s/api/matches/%s/finish", sp.finishAPIURL, dbMatchID)

	payload := map[string]interface{}{
		"team1_score": team1Score,
		"team2_score": team2Score,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call finish API: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("finish API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[Persistence] Finish API called successfully for match %s (score: %d-%d)", dbMatchID, team1Score, team2Score)
	return nil
}

// updateGOTVMatchID salva o GOTV match ID no banco para referência
func (sp *StatsPersister) updateGOTVMatchID(dbMatchID, gotvMatchID string) {
	if sp.supabase == nil {
		return
	}

	url := fmt.Sprintf("%s/rest/v1/matches?id=eq.%s", sp.supabase.baseURL, dbMatchID)

	payload := map[string]interface{}{
		"gotv_match_id": gotvMatchID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("PATCH", url, bytes.NewReader(body))
	if err != nil {
		return
	}

	req.Header.Set("apikey", sp.supabase.apiKey)
	req.Header.Set("Authorization", "Bearer "+sp.supabase.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=minimal")

	resp, err := sp.supabase.client.Do(req)
	if err != nil {
		log.Printf("[Persistence] Failed to update gotv_match_id: %v", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("[Persistence] Saved GOTV match ID %s for DB match %s", gotvMatchID, dbMatchID)
}
