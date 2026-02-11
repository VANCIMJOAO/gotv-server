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
	finishAPIURL string
}

// NewStatsPersister cria um novo persistidor de stats
func NewStatsPersister(supabase *SupabaseClient, teamRegistry *TeamRegistryCache, finishAPIURL string) *StatsPersister {
	return &StatsPersister{
		supabase:     supabase,
		teamRegistry: teamRegistry,
		finishAPIURL: finishAPIURL,
	}
}

// PersistMatchStatsFromMatchZy persiste os stats dos jogadores usando dados do webhook MatchZy
// O MatchZy envia stats completos de cada jogador no map_result (kills, deaths, damage, etc.)
func (sp *StatsPersister) PersistMatchStatsFromMatchZy(dbMatchID string, state *MatchZyState, mapResult *MatchZyMapResult) error {
	if sp.supabase == nil {
		return fmt.Errorf("supabase client not initialized")
	}
	if sp.teamRegistry == nil {
		return fmt.Errorf("team registry not initialized")
	}

	sp.teamRegistry.RefreshIfNeeded()

	// Buscar detalhes da partida do banco para ter team1_id e team2_id
	matchDetails, err := sp.supabase.FetchMatchDetails(dbMatchID)
	if err != nil {
		return fmt.Errorf("failed to fetch match details: %w", err)
	}

	var stats []map[string]interface{}

	// Processar jogadores do team1
	for _, player := range mapResult.Team1.Players {
		stat := sp.buildPlayerStat(dbMatchID, matchDetails.Team1ID, player)
		if stat != nil {
			stats = append(stats, stat)
		}
	}

	// Processar jogadores do team2
	for _, player := range mapResult.Team2.Players {
		stat := sp.buildPlayerStat(dbMatchID, matchDetails.Team2ID, player)
		if stat != nil {
			stats = append(stats, stat)
		}
	}

	if len(stats) == 0 {
		log.Printf("[Persistence] No valid player stats to persist for match %s", dbMatchID)
		return nil
	}

	if err := sp.supabase.InsertMatchPlayerStats(stats); err != nil {
		return fmt.Errorf("failed to insert player stats: %w", err)
	}

	log.Printf("[Persistence] Persisted stats for %d players in match %s", len(stats), dbMatchID)
	return nil
}

// buildPlayerStat constrói o registro de stats de um jogador
func (sp *StatsPersister) buildPlayerStat(dbMatchID string, teamID string, player MatchZyStatsPlayer) map[string]interface{} {
	if player.SteamID == "" {
		return nil
	}

	// Mapear SteamID → profile_id
	profileID := sp.teamRegistry.GetProfileIDBySteamID(player.SteamID)
	if profileID == "" {
		log.Printf("[Persistence] Could not find profile_id for SteamID %s (%s)", player.SteamID, player.Name)
		return nil
	}

	// Calcular ADR
	roundsPlayed := player.Stats.RoundsPlayed
	if roundsPlayed == 0 {
		roundsPlayed = 1
	}
	adr := float64(player.Stats.Damage) / float64(roundsPlayed)

	return map[string]interface{}{
		"match_id":      dbMatchID,
		"profile_id":    profileID,
		"team_id":       teamID,
		"kills":         player.Stats.Kills,
		"deaths":        player.Stats.Deaths,
		"assists":       player.Stats.Assists,
		"headshots":     player.Stats.HeadshotKills,
		"total_damage":  player.Stats.Damage,
		"adr":           adr,
		"rounds_played": roundsPlayed,
	}
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

// EventPersister persiste eventos e rounds em tempo real no Supabase
type EventPersister struct {
	supabase     *SupabaseClient
	teamRegistry *TeamRegistryCache
}

// NewEventPersister cria um novo persistidor de eventos
func NewEventPersister(supabase *SupabaseClient, teamRegistry *TeamRegistryCache) *EventPersister {
	return &EventPersister{
		supabase:     supabase,
		teamRegistry: teamRegistry,
	}
}

// PersistRound persiste um registro de round no banco
func (ep *EventPersister) PersistRound(dbMatchID string, roundData RoundPersistData) {
	if ep.supabase == nil {
		return
	}

	record := map[string]interface{}{
		"match_id":     dbMatchID,
		"round_number": roundData.RoundNumber,
		"win_reason":   roundData.WinReason,
		"ct_score":     roundData.CTScore,
		"t_score":      roundData.TScore,
	}

	if roundData.WinnerTeamID != "" {
		record["winner_team_id"] = roundData.WinnerTeamID
	}
	if roundData.CTTeamID != "" {
		record["ct_team_id"] = roundData.CTTeamID
	}
	if roundData.TTeamID != "" {
		record["t_team_id"] = roundData.TTeamID
	}
	if roundData.DurationSeconds > 0 {
		record["duration_seconds"] = roundData.DurationSeconds
	}
	if roundData.FirstKillProfileID != "" {
		record["first_kill_profile_id"] = roundData.FirstKillProfileID
	}
	if roundData.FirstDeathProfileID != "" {
		record["first_death_profile_id"] = roundData.FirstDeathProfileID
	}
	if roundData.BombPlantedBy != "" {
		record["bomb_planted_by"] = roundData.BombPlantedBy
	}
	if roundData.BombDefusedBy != "" {
		record["bomb_defused_by"] = roundData.BombDefusedBy
	}
	if roundData.BombPlantSite != "" {
		record["bomb_plant_site"] = roundData.BombPlantSite
	}

	if err := ep.supabase.InsertMatchRound(record); err != nil {
		log.Printf("[EventPersist] Failed to persist round %d: %v", roundData.RoundNumber, err)
	} else {
		log.Printf("[EventPersist] Round %d persisted for match %s", roundData.RoundNumber, dbMatchID)
	}
}

// RoundPersistData dados para persistir um round
type RoundPersistData struct {
	RoundNumber         int
	WinnerTeamID        string
	WinReason           string
	CTTeamID            string
	TTeamID             string
	CTScore             int
	TScore              int
	DurationSeconds     int
	FirstKillProfileID  string
	FirstDeathProfileID string
	BombPlantedBy       string
	BombDefusedBy       string
	BombPlantSite       string
}
