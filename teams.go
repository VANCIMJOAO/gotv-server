package main

import (
	"log"
)

// IdentifiedTeam representa um time identificado na partida
type IdentifiedTeam struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Tag     string `json:"tag"`
	LogoURL string `json:"logoUrl,omitempty"`
	Side    string `json:"side"` // "CT" ou "T"
}

// TeamIdentifier identifica times em uma partida baseado nos jogadores
type TeamIdentifier struct {
	registry *TeamRegistryCache
}

// NewTeamIdentifier cria um novo identificador de times
func NewTeamIdentifier(registry *TeamRegistryCache) *TeamIdentifier {
	return &TeamIdentifier{
		registry: registry,
	}
}

// IdentifyTeams identifica os times baseado nos jogadores da partida
// Retorna o time CT e o time T (podem ser nil se não identificados)
func (ti *TeamIdentifier) IdentifyTeams(players []PlayerState) (*IdentifiedTeam, *IdentifiedTeam) {
	if ti.registry == nil {
		return nil, nil
	}

	// Atualizar cache se necessário
	ti.registry.RefreshIfNeeded()

	// Contar jogadores de cada time em cada lado
	// teamCounts[teamID] = {ctCount, tCount}
	type teamCount struct {
		ctCount int
		tCount  int
		team    *TeamFromDB
	}
	teamCounts := make(map[string]*teamCount)

	for _, player := range players {
		team := ti.registry.GetTeamBySteamID(player.SteamID)
		if team == nil {
			continue
		}

		if _, exists := teamCounts[team.ID]; !exists {
			teamCounts[team.ID] = &teamCount{team: team}
		}

		if player.Team == "CT" {
			teamCounts[team.ID].ctCount++
		} else if player.Team == "T" {
			teamCounts[team.ID].tCount++
		}
	}

	// Se não temos pelo menos 2 times identificados, retornar nil
	if len(teamCounts) < 2 {
		return nil, nil
	}

	// Encontrar o time com mais jogadores em CT e o time com mais em T
	var teamCT, teamT *IdentifiedTeam
	var maxCT, maxT int
	minThreshold := 3 // Mínimo de jogadores para considerar um time identificado

	for _, tc := range teamCounts {
		// Verificar se este time tem maioria em CT
		if tc.ctCount >= minThreshold && tc.ctCount > tc.tCount && tc.ctCount > maxCT {
			maxCT = tc.ctCount
			logoURL := ""
			if tc.team.LogoURL != nil {
				logoURL = *tc.team.LogoURL
			}
			teamCT = &IdentifiedTeam{
				ID:      tc.team.ID,
				Name:    tc.team.Name,
				Tag:     tc.team.Tag,
				LogoURL: logoURL,
				Side:    "CT",
			}
		}

		// Verificar se este time tem maioria em T
		if tc.tCount >= minThreshold && tc.tCount > tc.ctCount && tc.tCount > maxT {
			maxT = tc.tCount
			logoURL := ""
			if tc.team.LogoURL != nil {
				logoURL = *tc.team.LogoURL
			}
			teamT = &IdentifiedTeam{
				ID:      tc.team.ID,
				Name:    tc.team.Name,
				Tag:     tc.team.Tag,
				LogoURL: logoURL,
				Side:    "T",
			}
		}
	}

	// Log de identificação
	if teamCT != nil || teamT != nil {
		ctName := "Unknown"
		tName := "Unknown"
		if teamCT != nil {
			ctName = teamCT.Name
		}
		if teamT != nil {
			tName = teamT.Name
		}
		log.Printf("[TeamIdentifier] Identified teams - CT: %s, T: %s", ctName, tName)
	}

	return teamCT, teamT
}

// IdentifyTeamsLenient identifica times com threshold menor (2 jogadores)
// Útil para partidas com menos jogadores ou durante warmup
func (ti *TeamIdentifier) IdentifyTeamsLenient(players []PlayerState) (*IdentifiedTeam, *IdentifiedTeam) {
	if ti.registry == nil {
		return nil, nil
	}

	ti.registry.RefreshIfNeeded()

	type teamCount struct {
		ctCount int
		tCount  int
		team    *TeamFromDB
	}
	teamCounts := make(map[string]*teamCount)

	for _, player := range players {
		team := ti.registry.GetTeamBySteamID(player.SteamID)
		if team == nil {
			continue
		}

		if _, exists := teamCounts[team.ID]; !exists {
			teamCounts[team.ID] = &teamCount{team: team}
		}

		if player.Team == "CT" {
			teamCounts[team.ID].ctCount++
		} else if player.Team == "T" {
			teamCounts[team.ID].tCount++
		}
	}

	if len(teamCounts) < 2 {
		return nil, nil
	}

	var teamCT, teamT *IdentifiedTeam
	var maxCT, maxT int
	minThreshold := 2 // Threshold menor

	for _, tc := range teamCounts {
		if tc.ctCount >= minThreshold && tc.ctCount > tc.tCount && tc.ctCount > maxCT {
			maxCT = tc.ctCount
			logoURL := ""
			if tc.team.LogoURL != nil {
				logoURL = *tc.team.LogoURL
			}
			teamCT = &IdentifiedTeam{
				ID:      tc.team.ID,
				Name:    tc.team.Name,
				Tag:     tc.team.Tag,
				LogoURL: logoURL,
				Side:    "CT",
			}
		}

		if tc.tCount >= minThreshold && tc.tCount > tc.ctCount && tc.tCount > maxT {
			maxT = tc.tCount
			logoURL := ""
			if tc.team.LogoURL != nil {
				logoURL = *tc.team.LogoURL
			}
			teamT = &IdentifiedTeam{
				ID:      tc.team.ID,
				Name:    tc.team.Name,
				Tag:     tc.team.Tag,
				LogoURL: logoURL,
				Side:    "T",
			}
		}
	}

	return teamCT, teamT
}
