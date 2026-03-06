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

// StatsPersister persiste dados de partidas no Supabase e avanca o bracket
type StatsPersister struct {
	supabase     *SupabaseClient
	teamRegistry *TeamRegistryCache
	finishAPIURL string
	authSecret   string
}

// NewStatsPersister cria um novo persistidor de stats
func NewStatsPersister(supabase *SupabaseClient, teamRegistry *TeamRegistryCache, finishAPIURL, authSecret string) *StatsPersister {
	return &StatsPersister{
		supabase:     supabase,
		teamRegistry: teamRegistry,
		finishAPIURL: finishAPIURL,
		authSecret:   authSecret,
	}
}

// PersistMatchStatsFromMatchZy persiste os stats dos jogadores usando dados do webhook MatchZy
func (sp *StatsPersister) PersistMatchStatsFromMatchZy(dbMatchID string, state *MatchZyState, mapResult *MatchZyMapResult) error {
	if sp.supabase == nil {
		return fmt.Errorf("supabase client not initialized")
	}
	if sp.teamRegistry == nil {
		return fmt.Errorf("team registry not initialized")
	}

	sp.teamRegistry.RefreshIfNeeded()

	matchDetails, err := sp.supabase.FetchMatchDetails(dbMatchID)
	if err != nil {
		return fmt.Errorf("failed to fetch match details: %w", err)
	}

	// Buscar match_map_id
	var matchMapID string
	state.mu.RLock()
	matchMapID = state.CurrentMatchMapID
	mapNumber := state.CurrentMap
	state.mu.RUnlock()

	if matchMapID == "" && sp.supabase != nil {
		if id, err := sp.supabase.GetMatchMapID(dbMatchID, mapNumber); err == nil {
			matchMapID = id
		}
	}

	var stats []map[string]interface{}

	for _, player := range mapResult.Team1.Players {
		stat := sp.buildPlayerStat(dbMatchID, matchMapID, matchDetails.Team1ID, player)
		if stat != nil {
			stats = append(stats, stat)
		}
	}

	for _, player := range mapResult.Team2.Players {
		stat := sp.buildPlayerStat(dbMatchID, matchMapID, matchDetails.Team2ID, player)
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

// buildPlayerStat constroi o registro de stats de um jogador
func (sp *StatsPersister) buildPlayerStat(dbMatchID string, matchMapID string, teamID string, player MatchZyStatsPlayer) map[string]interface{} {
	if player.SteamID == "" {
		return nil
	}

	profileID := sp.teamRegistry.GetProfileIDBySteamID(player.SteamID)
	if profileID == "" {
		log.Printf("[Persistence] Could not find profile_id for SteamID %s (%s)", player.SteamID, player.Name)
		return nil
	}

	roundsPlayed := player.Stats.RoundsPlayed
	if roundsPlayed == 0 {
		roundsPlayed = 1
	}
	adr := float64(player.Stats.Damage) / float64(roundsPlayed)

	firstKills := player.Stats.FirstKillsCT + player.Stats.FirstKillsT
	firstDeaths := player.Stats.FirstDeathsCT + player.Stats.FirstDeathsT
	clutchWins := player.Stats.OneV1 + player.Stats.OneV2 + player.Stats.OneV3 + player.Stats.OneV4 + player.Stats.OneV5

	stat := map[string]interface{}{
		"match_id":        dbMatchID,
		"profile_id":      profileID,
		"team_id":         teamID,
		"kills":           player.Stats.Kills,
		"deaths":          player.Stats.Deaths,
		"assists":         player.Stats.Assists,
		"headshots":       player.Stats.HeadshotKills,
		"total_damage":    player.Stats.Damage,
		"adr":             adr,
		"rounds_played":   roundsPlayed,
		"flash_assists":   player.Stats.FlashAssists,
		"enemies_flashed": player.Stats.EnemiesFlashed,
		"first_kills":     firstKills,
		"first_deaths":    firstDeaths,
		"two_kills":       player.Stats.TwoK,
		"three_kills":     player.Stats.ThreeK,
		"four_kills":      player.Stats.FourK,
		"aces":            player.Stats.FiveK,
		"kast_percentage": float64(player.Stats.Kast),
		"clutch_wins":     clutchWins,
		"ct_kills":        player.Stats.FirstKillsCT,
		"t_kills":         player.Stats.FirstKillsT,
		"ct_deaths":       player.Stats.FirstDeathsCT,
		"t_deaths":        player.Stats.FirstDeathsT,
	}

	if matchMapID != "" {
		stat["match_map_id"] = matchMapID
	}

	return stat
}

// CallFinishAPI chama a API de finish do Next.js para avancar o bracket
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
	if sp.authSecret != "" {
		req.Header.Set("Authorization", "Bearer "+sp.authSecret)
	}

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

// EventPersister persiste rounds em tempo real no Supabase
type EventPersister struct {
	supabase *SupabaseClient
}

// NewEventPersister cria um novo persistidor de eventos
func NewEventPersister(supabase *SupabaseClient) *EventPersister {
	return &EventPersister{
		supabase: supabase,
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

	if roundData.MatchMapID != "" {
		record["match_map_id"] = roundData.MatchMapID
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

	if err := ep.supabase.InsertMatchRound(record); err != nil {
		log.Printf("[EventPersist] Failed to persist round %d: %v", roundData.RoundNumber, err)
	} else {
		log.Printf("[EventPersist] Round %d persisted for match %s", roundData.RoundNumber, dbMatchID)
	}
}

// RoundPersistData dados para persistir um round
type RoundPersistData struct {
	MatchMapID      string
	RoundNumber     int
	WinnerTeamID    string
	WinReason       string
	CTTeamID        string
	TTeamID         string
	CTScore         int
	TScore          int
	DurationSeconds int
}
