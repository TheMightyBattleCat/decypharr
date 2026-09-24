package server

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// keepUnsentArrWantedSearch gives each posted Arr that has no "wanted_search"
// key the live value for its name. config.MergeJSON replaces arrays whole, so
// without this a Settings page that predates the setting (a minified
// config.js not yet rebuilt) would switch every Arr's wanted search off on
// each save. next is the decoded arrs list, index-aligned with body's.
func keepUnsentArrWantedSearch(body []byte, live, next []config.Arr) {
	var patch struct {
		Arrs []map[string]stdjson.RawMessage `json:"arrs"`
	}
	if err := stdjson.Unmarshal(body, &patch); err != nil || patch.Arrs == nil || len(patch.Arrs) != len(next) {
		return // no arrs posted: the merge kept the live list
	}
	byName := make(map[string]config.ArrWantedSearch, len(live))
	for _, a := range live {
		byName[a.Name] = a.WantedSearch
	}
	for i, raw := range patch.Arrs {
		if _, sent := raw["wanted_search"]; sent {
			continue
		}
		if ws, ok := byName[next[i].Name]; ok {
			next[i].WantedSearch = ws
		}
	}
}

// validateArrWantedSearch trims each Arr's wanted-search schedule and refuses
// one that is on without a clock time or cron expression, so the page shows
// the problem instead of the job silently never being registered.
func validateArrWantedSearch(arrs []config.Arr) error {
	for i := range arrs {
		ws := &arrs[i].WantedSearch
		ws.Schedule = strings.TrimSpace(ws.Schedule)
		if !ws.Enabled {
			continue
		}
		if ws.Schedule == "" {
			return fmt.Errorf("Arr %q: scheduled wanted search is on but has no schedule", arrs[i].Name)
		}
		if _, err := utils.ConvertToClockOrCronJobDef(ws.Schedule); err != nil {
			return fmt.Errorf("Arr %q: wanted search schedule %w", arrs[i].Name, err)
		}
	}
	return nil
}

// handleWantedSearchStatus reports each Arr's wanted-search schedule, next
// firing and last outcome for the Settings page.
func (s *Server) handleWantedSearchStatus(w http.ResponseWriter, r *http.Request) {
	ws := s.manager.WantedSearch()
	if ws == nil {
		http.Error(w, "Wanted search is not available", http.StatusServiceUnavailable)
		return
	}
	utils.JSONResponse(w, ws.Status(), http.StatusOK)
}

// handleWantedSearchRun sends one Arr's wanted search now ({"name": "..."}),
// using its saved settings.
func (s *Server) handleWantedSearchRun(w http.ResponseWriter, r *http.Request) {
	ws := s.manager.WantedSearch()
	if ws == nil {
		http.Error(w, "Wanted search is not available", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := stdjson.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		http.Error(w, "Request body must name an Arr: {\"name\": \"sonarr\"}", http.StatusBadRequest)
		return
	}
	res, err := ws.RunNow(req.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, res, http.StatusOK)
}
