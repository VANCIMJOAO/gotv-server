package main

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	dem "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/common"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/msg"
)

// GameEvent representa um evento do jogo
type GameEvent struct {
	Type      string                 `json:"type"`
	Tick      int                    `json:"tick"`
	Round     int                    `json:"round"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// BroadcastParser parseia broadcasts CSTV/GOTV+ em tempo real
type BroadcastParser struct {
	matchID     string
	broadcastURL string
	parser      dem.Parser
	state       *MatchState
	events      []GameEvent
	mu          sync.RWMutex
	isRunning   bool
	stopChan    chan struct{}
	onUpdate    func(*MatchState)
	onEvent     func(GameEvent)
}

// NewBroadcastParser cria um novo parser de broadcast
func NewBroadcastParser(matchID string) *BroadcastParser {
	return &BroadcastParser{
		matchID: matchID,
		state: &MatchState{
			MatchID:    matchID,
			Status:     "warmup",
			RoundPhase: "warmup",
			Players:    []PlayerState{},
		},
		events:   []GameEvent{},
		stopChan: make(chan struct{}),
	}
}

// SetCallbacks define callbacks para atualizações
func (p *BroadcastParser) SetCallbacks(onUpdate func(*MatchState), onEvent func(GameEvent)) {
	p.onUpdate = onUpdate
	p.onEvent = onEvent
}

// ParseBroadcast conecta a um broadcast CSTV e parseia em tempo real
func (p *BroadcastParser) ParseBroadcast(broadcastURL string) error {
	p.broadcastURL = broadcastURL
	log.Printf("[Parser] Connecting to broadcast: %s", broadcastURL)

	// Criar parser de broadcast CSTV
	parser, err := dem.NewCSTVBroadcastParser(broadcastURL)
	if err != nil {
		return fmt.Errorf("failed to create broadcast parser: %w", err)
	}
	p.parser = parser

	p.isRunning = true
	p.setupEventHandlers()

	// Parse em background
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Parser] Recovered from panic: %v", r)
			}
			p.isRunning = false
			parser.Close()
		}()

		err := parser.ParseToEnd()
		if err != nil {
			log.Printf("[Parser] Broadcast parse ended: %v", err)
			// Se o erro for de entidade, tentar reconectar
			if strings.Contains(err.Error(), "entity") {
				log.Printf("[Parser] Entity error detected - broadcast may have started mid-game")
				log.Printf("[Parser] Tip: Restart the match with 'mp_restartgame 1' for clean parsing")
			}
		} else {
			log.Printf("[Parser] Broadcast parse completed")
		}
	}()

	return nil
}

// Stop para o parser
func (p *BroadcastParser) Stop() {
	if p.isRunning {
		close(p.stopChan)
		if p.parser != nil {
			p.parser.Close()
		}
	}
}

// setupEventHandlers configura os handlers de eventos
func (p *BroadcastParser) setupEventHandlers() {
	parser := p.parser

	// Header do demo (contém nome do mapa)
	parser.RegisterEventHandler(func(e msg.CDemoFileHeader) {
		mapName := e.GetMapName()
		if mapName != "" {
			p.mu.Lock()
			p.state.MapName = mapName
			p.state.Status = "live"
			p.mu.Unlock()

			log.Printf("[Parser] Map from header: %s", mapName)
			p.emitUpdate()
		}
	})

	// ServerInfo também contém o nome do mapa (fallback para broadcasts)
	// Usar RegisterNetMessageHandler para mensagens de rede
	parser.RegisterNetMessageHandler(func(e *msg.CSVCMsg_ServerInfo) {
		mapName := e.GetMapName()
		log.Printf("[Parser] CSVCMsg_ServerInfo received - MapName: '%s'", mapName)
		if mapName != "" {
			p.mu.Lock()
			// Só atualizar se ainda não temos o nome do mapa
			if p.state.MapName == "" {
				p.state.MapName = mapName
				log.Printf("[Parser] Map from ServerInfo: %s", mapName)
			}
			p.mu.Unlock()
			p.emitUpdate()
		}
	})

	// Tentar obter nome do mapa via ConVars (net message)
	parser.RegisterNetMessageHandler(func(e *msg.CNETMsg_SetConVar) {
		if e.Convars != nil {
			for _, cvar := range e.Convars.Cvars {
				name := cvar.GetName()
				value := cvar.GetValue()
				// Log convars relacionados a mapa para debug
				if strings.Contains(strings.ToLower(name), "map") {
					log.Printf("[Parser] NetMsg ConVar: %s = %s", name, value)
				}
				// Verificar convars conhecidos para nome do mapa
				if name == "host_map" || name == "mp_mapname" || name == "mapname" {
					p.mu.Lock()
					if p.state.MapName == "" && value != "" {
						p.state.MapName = value
						log.Printf("[Parser] Map from NetMsg ConVar %s: %s", name, value)
					}
					p.mu.Unlock()
					p.emitUpdate()
				}
			}
		}
	})

	// ConVarsUpdated event - outra fonte de convars
	parser.RegisterEventHandler(func(e events.ConVarsUpdated) {
		for name, value := range e.UpdatedConVars {
			// Log convars relacionados a mapa para debug
			if strings.Contains(strings.ToLower(name), "map") {
				log.Printf("[Parser] Event ConVar: %s = %s", name, value)
			}
			// Verificar convars conhecidos para nome do mapa
			if name == "host_map" || name == "mp_mapname" || name == "mapname" || name == "mapgroup" {
				p.mu.Lock()
				if p.state.MapName == "" && value != "" {
					p.state.MapName = value
					log.Printf("[Parser] Map from Event ConVar %s: %s", name, value)
				}
				p.mu.Unlock()
				p.emitUpdate()
			}
		}
	})

	// DataTablesParsed - momento ideal para verificar GameState
	parser.RegisterEventHandler(func(e events.DataTablesParsed) {
		log.Printf("[Parser] DataTablesParsed event received")
	})

	// Match começou (sai do warmup)
	parser.RegisterEventHandler(func(e events.MatchStart) {
		p.mu.Lock()
		p.state.Status = "live"
		// Reset round counter quando o match realmente começa (pós-warmup)
		p.state.CurrentRound = 0
		p.state.ScoreCT = 0
		p.state.ScoreT = 0
		p.mu.Unlock()

		p.emitUpdate()
		log.Printf("[Parser] Match started - reset round counter for competitive play")
	})

	// Início do round
	parser.RegisterEventHandler(func(e events.RoundStart) {
		// Usar o round do GameState (mais preciso que incrementar manualmente)
		gs := parser.GameState()
		currentRound := 0
		if gs != nil {
			// TotalRoundsPlayed retorna rounds completos, então +1 para o round atual
			currentRound = gs.TotalRoundsPlayed() + 1
		}

		p.mu.Lock()
		// Só atualizar se o round do GameState for válido
		if currentRound > 0 {
			p.state.CurrentRound = currentRound
		} else {
			p.state.CurrentRound++
		}
		p.state.RoundPhase = "freezetime"

		// Reset round stats
		for i := range p.state.Players {
			p.state.Players[i].RoundKills = 0
			p.state.Players[i].RoundDamage = 0
		}
		roundNum := p.state.CurrentRound
		p.mu.Unlock()

		p.emitEvent(GameEvent{
			Type:      "round_start",
			Tick:      parser.CurrentFrame(),
			Round:     roundNum,
			Timestamp: time.Now(),
			Data:      map[string]interface{}{"round": roundNum},
		})
		p.emitUpdate()
		log.Printf("[Parser] Round %d started (GameState TotalRoundsPlayed: %d)", roundNum, currentRound-1)
	})

	// Fim do freezetime
	parser.RegisterEventHandler(func(e events.RoundFreezetimeEnd) {
		p.mu.Lock()
		p.state.RoundPhase = "live"
		p.mu.Unlock()
		p.emitUpdate()
	})

	// Fim do round
	parser.RegisterEventHandler(func(e events.RoundEnd) {
		winner := "T"
		if e.Winner == common.TeamCounterTerrorists {
			winner = "CT"
		}

		// Usar score do GameState (mais preciso que incrementar manualmente)
		gs := parser.GameState()
		scoreCT := 0
		scoreT := 0
		if gs != nil {
			scoreCT = gs.TeamCounterTerrorists().Score()
			scoreT = gs.TeamTerrorists().Score()
		}

		p.mu.Lock()
		p.state.ScoreCT = scoreCT
		p.state.ScoreT = scoreT
		p.state.RoundPhase = "over"
		p.mu.Unlock()

		p.emitEvent(GameEvent{
			Type:      "round_end",
			Tick:      parser.CurrentFrame(),
			Round:     p.state.CurrentRound,
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"winner":  winner,
				"reason":  fmt.Sprintf("%v", e.Reason),
				"scoreCT": scoreCT,
				"scoreT":  scoreT,
			},
		})
		p.emitUpdate()
		log.Printf("[Parser] Round %d ended - %s wins - Score: CT %d - T %d",
			p.state.CurrentRound, winner, scoreCT, scoreT)
	})

	// Kill
	parser.RegisterEventHandler(func(e events.Kill) {
		attackerInfo := p.getPlayerInfo(e.Killer)
		victimInfo := p.getPlayerInfo(e.Victim)
		assisterInfo := p.getPlayerInfo(e.Assister)

		weaponName := "unknown"
		if e.Weapon != nil {
			weaponName = e.Weapon.String()
		}

		// Atualizar RoundKills e Headshots (stats não rastreados pelo GameState)
		// Para jogadores reais: comparar por SteamID
		// Para bots (SteamID64 == 0): comparar por nome como fallback
		p.mu.Lock()

		// Debug: mostrar info do killer
		if e.Killer != nil {
			log.Printf("[Parser] Kill event - Killer: %s (SteamID64: %d)", e.Killer.Name, e.Killer.SteamID64)
		}

		foundKiller := false
		for i := range p.state.Players {
			// Verificar killer
			if e.Killer != nil {
				killerSteamID := fmt.Sprintf("%d", e.Killer.SteamID64)
				// Jogador real: comparar por SteamID, Bot: comparar por nome
				isKiller := (e.Killer.SteamID64 != 0 && p.state.Players[i].SteamID == killerSteamID) ||
					(e.Killer.SteamID64 == 0 && p.state.Players[i].Name == e.Killer.Name)
				if isKiller {
					foundKiller = true
					p.state.Players[i].RoundKills++
					if e.IsHeadshot {
						p.state.Players[i].Headshots++
					}
					log.Printf("[Parser] Stats updated for %s: RoundKills=%d, Headshots=%d",
						p.state.Players[i].Name, p.state.Players[i].RoundKills, p.state.Players[i].Headshots)
				}
			}
			// Verificar victim
			if e.Victim != nil {
				victimSteamID := fmt.Sprintf("%d", e.Victim.SteamID64)
				// Jogador real: comparar por SteamID, Bot: comparar por nome
				isVictim := (e.Victim.SteamID64 != 0 && p.state.Players[i].SteamID == victimSteamID) ||
					(e.Victim.SteamID64 == 0 && p.state.Players[i].Name == e.Victim.Name)
				if isVictim {
					p.state.Players[i].IsAlive = false
				}
			}
		}

		if e.Killer != nil && !foundKiller {
			log.Printf("[Parser] WARNING: Killer %s (SteamID64: %d) not found in players list!", e.Killer.Name, e.Killer.SteamID64)
			log.Printf("[Parser] Current players:")
			for _, player := range p.state.Players {
				log.Printf("[Parser]   - %s (SteamID: %s)", player.Name, player.SteamID)
			}
		}

		p.mu.Unlock()

		p.emitEvent(GameEvent{
			Type:      "kill",
			Tick:      parser.CurrentFrame(),
			Round:     p.state.CurrentRound,
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"attacker":     attackerInfo,
				"victim":       victimInfo,
				"assister":     assisterInfo,
				"weapon":       weaponName,
				"headshot":     e.IsHeadshot,
				"wallbang":     e.PenetratedObjects > 0,
				"throughSmoke": e.ThroughSmoke,
				"noScope":      e.NoScope,
				"flash":        e.AttackerBlind,
			},
		})

		if attackerInfo != nil && victimInfo != nil {
			hs := ""
			if e.IsHeadshot {
				hs = " (HS)"
			}
			log.Printf("[Parser] KILL: %s killed %s with %s%s",
				attackerInfo["name"], victimInfo["name"], weaponName, hs)
		}

		// Atualizar jogadores imediatamente após kill para sincronizar stats
		p.updatePlayers()
		p.emitUpdate()
	})

	// Dano
	parser.RegisterEventHandler(func(e events.PlayerHurt) {
		if e.Attacker == nil || e.Player == nil {
			return
		}

		attackerSteamID := fmt.Sprintf("%d", e.Attacker.SteamID64)
		foundAttacker := false

		p.mu.Lock()
		for i := range p.state.Players {
			// Jogador real: comparar por SteamID, Bot: comparar por nome
			isAttacker := (e.Attacker.SteamID64 != 0 && p.state.Players[i].SteamID == attackerSteamID) ||
				(e.Attacker.SteamID64 == 0 && p.state.Players[i].Name == e.Attacker.Name)
			if isAttacker {
				foundAttacker = true
				p.state.Players[i].Damage += e.HealthDamage
				p.state.Players[i].RoundDamage += e.HealthDamage
			}
		}
		p.mu.Unlock()

		if !foundAttacker && e.HealthDamage > 0 {
			log.Printf("[Parser] WARNING: Damage attacker %s (SteamID64: %d) not found! Damage: %d",
				e.Attacker.Name, e.Attacker.SteamID64, e.HealthDamage)
		}
	})

	// Bomba plantada
	parser.RegisterEventHandler(func(e events.BombPlanted) {
		site := "A"
		if e.Site == events.BombsiteB {
			site = "B"
		}

		p.mu.Lock()
		p.state.RoundPhase = "bomb_planted"
		p.state.Bomb = &BombState{
			State:         "planted",
			Site:          site,
			TimeRemaining: 40, // C4 timer padrão
		}
		p.mu.Unlock()

		planterInfo := p.getPlayerInfo(e.Player)
		p.emitEvent(GameEvent{
			Type:      "bomb_planted",
			Tick:      parser.CurrentFrame(),
			Round:     p.state.CurrentRound,
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"site":    site,
				"planter": planterInfo,
			},
		})
		p.emitUpdate()
		log.Printf("[Parser] BOMB planted on %s", site)
	})

	// Bomba defusada
	parser.RegisterEventHandler(func(e events.BombDefused) {
		p.mu.Lock()
		if p.state.Bomb != nil {
			p.state.Bomb.State = "defused"
		}
		p.mu.Unlock()

		defuserInfo := p.getPlayerInfo(e.Player)
		p.emitEvent(GameEvent{
			Type:      "bomb_defused",
			Tick:      parser.CurrentFrame(),
			Round:     p.state.CurrentRound,
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"defuser": defuserInfo,
			},
		})
		p.emitUpdate()
		log.Printf("[Parser] BOMB defused!")
	})

	// Bomba explodiu
	parser.RegisterEventHandler(func(e events.BombExplode) {
		p.mu.Lock()
		if p.state.Bomb != nil {
			p.state.Bomb.State = "exploded"
		}
		p.mu.Unlock()

		p.emitEvent(GameEvent{
			Type:      "bomb_exploded",
			Tick:      parser.CurrentFrame(),
			Round:     p.state.CurrentRound,
			Timestamp: time.Now(),
			Data:      map[string]interface{}{},
		})
		p.emitUpdate()
		log.Printf("[Parser] BOMB exploded!")
	})

	// Atualizar jogadores periodicamente durante o parse
	parser.RegisterEventHandler(func(e events.FrameDone) {
		if parser.CurrentFrame()%64 == 0 { // A cada ~0.5 segundo (64 tick)
			p.updatePlayers()
			p.emitUpdate()
		}
	})
}

// getPlayerKey retorna uma chave única para identificar jogadores
// Para jogadores reais: usa SteamID
// Para bots (SteamID == "0"): usa nome
func getPlayerKey(steamID, name string) string {
	if steamID == "0" {
		return "bot:" + name
	}
	return steamID
}

// updatePlayers atualiza a lista de jogadores
func (p *BroadcastParser) updatePlayers() {
	gs := p.parser.GameState()
	if gs == nil {
		return
	}

	// Primeiro, criar um mapa dos stats existentes para preservar
	// Usa chave composta: SteamID para jogadores reais, nome para bots
	p.mu.RLock()
	existingStats := make(map[string]PlayerState)
	for _, existing := range p.state.Players {
		key := getPlayerKey(existing.SteamID, existing.Name)
		existingStats[key] = existing
	}
	p.mu.RUnlock()

	var players []PlayerState
	// Usar TeamMembers para pegar todos os jogadores de cada time (incluindo bots)
	allPlayers := append(gs.TeamCounterTerrorists().Members(), gs.TeamTerrorists().Members()...)

	for _, player := range allPlayers {
		if player == nil {
			continue
		}

		team := "T"
		if player.Team == common.TeamCounterTerrorists {
			team = "CT"
		}

		var weapons []string
		for _, w := range player.Weapons() {
			if w != nil {
				weapons = append(weapons, w.String())
			}
		}

		activeWeapon := ""
		if player.ActiveWeapon() != nil {
			activeWeapon = player.ActiveWeapon().String()
		}

		steamID := fmt.Sprintf("%d", player.SteamID64)
		pos := player.Position()
		playerState := PlayerState{
			SteamID:      steamID,
			Name:         player.Name,
			Team:         team,
			Health:       player.Health(),
			Armor:        player.Armor(),
			HasHelmet:    player.HasHelmet(),
			HasDefuser:   player.HasDefuseKit(),
			Money:        player.Money(),
			IsAlive:      player.IsAlive(),
			Position:     Position{X: pos.X, Y: pos.Y, Z: pos.Z},
			ViewAngle:    player.ViewDirectionY(),
			ActiveWeapon: activeWeapon,
			Weapons:      weapons,
			// Usar stats do GameState (fonte mais precisa)
			Kills:        player.Kills(),
			Deaths:       player.Deaths(),
			Assists:      player.Assists(),
		}

		// Preservar stats que rastreamos manualmente (não disponíveis no GameState)
		// Usa chave composta para funcionar com bots e jogadores reais
		playerKey := getPlayerKey(steamID, player.Name)
		if existing, ok := existingStats[playerKey]; ok {
			playerState.RoundKills = existing.RoundKills
			playerState.RoundDamage = existing.RoundDamage
			playerState.Headshots = existing.Headshots
			playerState.Damage = existing.Damage
		}

		players = append(players, playerState)
	}

	// Ordenar jogadores por SteamID para manter ordem consistente
	sort.Slice(players, func(i, j int) bool {
		return players[i].SteamID < players[j].SteamID
	})

	// Atualizar score do GameState também
	scoreCT := gs.TeamCounterTerrorists().Score()
	scoreT := gs.TeamTerrorists().Score()

	p.mu.Lock()
	p.state.Players = players
	p.state.ScoreCT = scoreCT
	p.state.ScoreT = scoreT
	p.state.LastTick = p.parser.CurrentFrame()
	p.state.UpdatedAt = time.Now()
	p.mu.Unlock()
}

// getTeamString converte Team para string
func getTeamString(team common.Team) string {
	switch team {
	case common.TeamCounterTerrorists:
		return "CT"
	case common.TeamTerrorists:
		return "T"
	case common.TeamSpectators:
		return "SPEC"
	default:
		return "UNASSIGNED"
	}
}

// getPlayerInfo extrai info básica de um jogador
func (p *BroadcastParser) getPlayerInfo(player *common.Player) map[string]interface{} {
	if player == nil {
		return nil
	}
	return map[string]interface{}{
		"steamId": fmt.Sprintf("%d", player.SteamID64),
		"name":    player.Name,
		"team":    getTeamString(player.Team),
	}
}

// GetState retorna o estado atual
func (p *BroadcastParser) GetState() *MatchState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Copiar estado
	stateCopy := *p.state
	stateCopy.Players = make([]PlayerState, len(p.state.Players))
	copy(stateCopy.Players, p.state.Players)

	return &stateCopy
}

// GetEvents retorna todos os eventos
func (p *BroadcastParser) GetEvents() []GameEvent {
	p.mu.RLock()
	defer p.mu.RUnlock()

	eventsCopy := make([]GameEvent, len(p.events))
	copy(eventsCopy, p.events)
	return eventsCopy
}

// emitUpdate notifica sobre atualização de estado
func (p *BroadcastParser) emitUpdate() {
	if p.onUpdate != nil {
		p.onUpdate(p.GetState())
	}
}

// emitEvent notifica sobre novo evento
func (p *BroadcastParser) emitEvent(event GameEvent) {
	p.mu.Lock()
	p.events = append(p.events, event)
	p.mu.Unlock()

	if p.onEvent != nil {
		p.onEvent(event)
	}
}
