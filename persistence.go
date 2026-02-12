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

// StatsPersister chama a finish API para avancar o bracket
// Toda a persistencia de stats/rounds/events agora e feita pelo Next.js webhook
type StatsPersister struct {
	finishAPIURL string
}

// NewStatsPersister cria um novo persistidor
func NewStatsPersister(finishAPIURL string) *StatsPersister {
	return &StatsPersister{
		finishAPIURL: finishAPIURL,
	}
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
