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

// PersistKillEvent persiste um evento de kill no banco
func (ep *EventPersister) PersistKillEvent(dbMatchID string, event GameEvent) {
	if ep.supabase == nil {
		return
	}

	data := event.Data
	if data == nil {
		return
	}

	// Extrair SteamIDs do attacker e victim
	var attackerProfileID, victimProfileID *string
	var weapon *string
	isHeadshot := false

	if attacker, ok := data["attacker"].(map[string]interface{}); ok {
		if steamID, ok := attacker["steamId"].(string); ok && steamID != "" && steamID != "0" {
			profileID := ep.teamRegistry.GetProfileIDBySteamID(steamID)
			if profileID != "" {
				attackerProfileID = &profileID
			}
		}
	}

	if victim, ok := data["victim"].(map[string]interface{}); ok {
		if steamID, ok := victim["steamId"].(string); ok && steamID != "" && steamID != "0" {
			profileID := ep.teamRegistry.GetProfileIDBySteamID(steamID)
			if profileID != "" {
				victimProfileID = &profileID
			}
		}
	}

	if w, ok := data["weapon"].(string); ok {
		weapon = &w
	}

	if hs, ok := data["headshot"].(bool); ok {
		isHeadshot = hs
	}

	// Construir event_data com informações extras
	eventData := map[string]interface{}{
		"wallbang":     data["wallbang"],
		"throughSmoke": data["throughSmoke"],
		"noScope":      data["noScope"],
		"flash":        data["flash"],
	}
	// Incluir assister se existir
	if assister, ok := data["assister"].(map[string]interface{}); ok {
		if steamID, ok := assister["steamId"].(string); ok && steamID != "" && steamID != "0" {
			assisterProfileID := ep.teamRegistry.GetProfileIDBySteamID(steamID)
			if assisterProfileID != "" {
				eventData["assister_profile_id"] = assisterProfileID
			}
		}
	}

	eventDataJSON, _ := json.Marshal(eventData)

	record := map[string]interface{}{
		"match_id":     dbMatchID,
		"event_type":   "kill",
		"round_number": event.Round,
		"tick":         event.Tick,
		"event_data":   string(eventDataJSON),
		"is_headshot":  isHeadshot,
	}

	if attackerProfileID != nil {
		record["attacker_profile_id"] = *attackerProfileID
	}
	if victimProfileID != nil {
		record["victim_profile_id"] = *victimProfileID
	}
	if weapon != nil {
		record["weapon"] = *weapon
	}

	if err := ep.supabase.InsertMatchEvent(record); err != nil {
		log.Printf("[EventPersist] Failed to persist kill event: %v", err)
	}
}

// PersistBombEvent persiste um evento de bomba (planted/defused/exploded) no banco
func (ep *EventPersister) PersistBombEvent(dbMatchID string, event GameEvent) {
	if ep.supabase == nil {
		return
	}

	data := event.Data
	if data == nil {
		return
	}

	var attackerProfileID *string

	// Para bomb_planted: o "planter" é o attacker
	if planter, ok := data["planter"].(map[string]interface{}); ok {
		if steamID, ok := planter["steamId"].(string); ok && steamID != "" && steamID != "0" {
			profileID := ep.teamRegistry.GetProfileIDBySteamID(steamID)
			if profileID != "" {
				attackerProfileID = &profileID
			}
		}
	}

	// Para bomb_defused: o "defuser" é o attacker
	if defuser, ok := data["defuser"].(map[string]interface{}); ok {
		if steamID, ok := defuser["steamId"].(string); ok && steamID != "" && steamID != "0" {
			profileID := ep.teamRegistry.GetProfileIDBySteamID(steamID)
			if profileID != "" {
				attackerProfileID = &profileID
			}
		}
	}

	eventData := map[string]interface{}{}
	if site, ok := data["site"].(string); ok {
		eventData["site"] = site
	}

	eventDataJSON, _ := json.Marshal(eventData)

	record := map[string]interface{}{
		"match_id":     dbMatchID,
		"event_type":   event.Type,
		"round_number": event.Round,
		"tick":         event.Tick,
		"event_data":   string(eventDataJSON),
	}

	if attackerProfileID != nil {
		record["attacker_profile_id"] = *attackerProfileID
	}

	if err := ep.supabase.InsertMatchEvent(record); err != nil {
		log.Printf("[EventPersist] Failed to persist %s event: %v", event.Type, err)
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
	RoundNumber       int
	WinnerTeamID      string
	WinReason         string
	CTTeamID          string
	TTeamID           string
	CTScore           int
	TScore            int
	DurationSeconds   int
	FirstKillProfileID string
	FirstDeathProfileID string
	BombPlantedBy     string
	BombDefusedBy     string
	BombPlantSite     string
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
