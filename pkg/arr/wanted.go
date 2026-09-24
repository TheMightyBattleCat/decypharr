package arr

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// SystemStatus is the part of GET /api/v3/system/status the wanted search
// reads. AppName says which Arr answered ("Sonarr", "Radarr"), which the
// name/host guess in inferType cannot: an Arr auto-detected from a download
// client is named after its category ("tv") and often has no "sonarr" in its
// host either.
type SystemStatus struct {
	AppName      string `json:"appName"`
	InstanceName string `json:"instanceName"`
	Version      string `json:"version"`
}

// GetSystemStatus asks the Arr which app it is.
func (a *Arr) GetSystemStatus(ctx context.Context) (SystemStatus, error) {
	var st SystemStatus
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/system/status", nil, &st)
	if err != nil {
		return st, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return st, fmt.Errorf("system status: %s", resp.Status)
	}
	return st, nil
}

// TypeFromAppName maps system/status's appName onto a Type; Others when it
// is not an Arr this package knows.
func TypeFromAppName(appName string) Type {
	switch Type(strings.ToLower(strings.TrimSpace(appName))) {
	case Sonarr:
		return Sonarr
	case Radarr:
		return Radarr
	case Lidarr:
		return Lidarr
	case Readarr:
		return Readarr
	default:
		return Others
	}
}

// CommandResult is the part of a POST /api/v3/command response the wanted
// search records. The Arr hands back the command already queued or running
// when an identical one exists, so Status can be "started" for a search it
// did not start again.
type CommandResult struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// SearchWantedMissing starts the Arr's own search for every monitored item
// it is missing - the command behind Wanted -> Missing -> Search All. app is
// the Arr's type as system/status reports it. Payloads per the Arrs' command
// classes: Sonarr's MissingEpisodeSearch takes "monitored" (default true);
// Radarr's MissingMoviesSearch always searches monitored movies and skips
// ones already in its queue.
func (a *Arr) SearchWantedMissing(ctx context.Context, app Type) (CommandResult, error) {
	var payload any
	switch app {
	case Sonarr:
		payload = struct {
			Name      string `json:"name"`
			Monitored bool   `json:"monitored"`
		}{Name: "MissingEpisodeSearch", Monitored: true}
	case Radarr:
		payload = struct {
			Name string `json:"name"`
		}{Name: "MissingMoviesSearch"}
	default:
		return CommandResult{}, fmt.Errorf("wanted search supports Sonarr and Radarr, not %s", app)
	}
	var res CommandResult
	resp, err := a.RequestCtx(ctx, http.MethodPost, "api/v3/command", payload, &res)
	if err != nil {
		return res, fmt.Errorf("wanted search: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return res, fmt.Errorf("wanted search: %s", resp.Status)
	}
	return res, nil
}
