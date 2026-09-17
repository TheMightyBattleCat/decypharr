package server

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
	"github.com/sourcegraph/conc/iter"
	"golang.org/x/crypto/bcrypt"
)

type mountCacheCleaner interface {
	CleanupCache() (map[string]any, error)
}

type mountCachePurger interface {
	PurgeCache() (map[string]any, error)
}

func (s *Server) handleGetArrs(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.Arr().GetAll(), http.StatusOK)
}

func (s *Server) handleAddContent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	arrName := r.FormValue("arr")
	action := r.FormValue("action")
	debridName := r.FormValue("debrid")
	callbackUrl := r.FormValue("callbackUrl")
	downloadFolder := r.FormValue("downloadFolder")
	if downloadFolder == "" {
		downloadFolder = config.Get().DownloadFolder
	}
	skipMultiSeason := r.FormValue("skipMultiSeason") == "true"

	dlUncached := r.FormValue("downloadUncached") == "true"
	var downloadUncached *bool
	if dlUncached {
		downloadUncached = &dlUncached
	}
	rmTrackerUrls := r.FormValue("rmTrackerUrls") == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	cfg := config.Get()
	if cfg.AlwaysRmTrackerUrls {
		rmTrackerUrls = true
	}

	_arr := s.manager.Arr().Get(arrName)
	if _arr == nil {
		// These are not found in the config. They are throwaway arrs.
		_arr = arr.New(arrName, "", "", false, downloadUncached, "", "")
	}

	// Unified task type for all content types
	type addTask struct {
		taskType   string // "torrent", "nzbURL", "nzbFile"
		magnet     *utils.Magnet
		nzbContent []byte
		name       string
		source     string // for error messages
	}

	var tasks []addTask

	// Collect torrent URLs
	if urls := r.FormValue("urls"); urls != "" {
		for u := range strings.SplitSeq(urls, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				magnet, err := utils.GetMagnetFromUrl(trimmed, rmTrackerUrls)
				if err != nil {
					tasks = append(tasks, addTask{
						taskType: "error",
						source:   fmt.Sprintf("Failed to parse URL %s: %v", trimmed, err),
					})
					continue
				}
				tasks = append(tasks, addTask{taskType: "torrent", magnet: magnet, source: fmt.Sprintf("URL %s", trimmed)})
			}
		}
	}

	// Collect torrent files
	if files := r.MultipartForm.File["files"]; len(files) > 0 {
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to open file %s: %v", fileHeader.Filename, err),
				})
				continue
			}

			magnet, err := utils.GetMagnetFromFile(file, fileHeader.Filename, rmTrackerUrls)
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to parse torrent file %s: %v", fileHeader.Filename, err),
				})
				continue
			}
			tasks = append(tasks, addTask{taskType: "torrent", magnet: magnet, source: fmt.Sprintf("File %s", fileHeader.Filename), name: fileHeader.Filename})
		}
	}

	// Collect NZB URLs
	if nzbURLs := r.FormValue("nzbURLs"); nzbURLs != "" {
		for u := range strings.SplitSeq(nzbURLs, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				filename, content, err := utils.DownloadFile(trimmed, utils.WithHeader("User-Agent", s.nzbUserAgent))
				if err != nil {
					tasks = append(tasks, addTask{
						taskType: "error",
						source:   fmt.Sprintf("Failed to fetch NZB from URL %s: %v", trimmed, err),
					})
					continue
				}
				tasks = append(tasks, addTask{taskType: "nzb", nzbContent: content, name: filename, source: fmt.Sprintf("NZB URL %s", trimmed)})
			}
		}
	}

	// Collect NZB files
	if nzbFiles := r.MultipartForm.File["nzbFiles"]; len(nzbFiles) > 0 {
		for _, fileHeader := range nzbFiles {
			content, err := getNZBContentFromFile(fileHeader)
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to read NZB file %s: %v", fileHeader.Filename, err),
				})
				continue
			}
			tasks = append(tasks, addTask{taskType: "nzb", nzbContent: content, source: fmt.Sprintf("NZB File %s", fileHeader.Filename), name: fileHeader.Filename})
		}
	}

	// Parse all tasks in parallel using iter.Map
	mapper := iter.Mapper[addTask, *manager.ImportRequest]{
		MaxGoroutines: min(len(tasks), 10),
	}

	results := mapper.Map(tasks, func(task *addTask) *manager.ImportRequest {
		switch task.taskType {
		case "error":
			// Task already failed during collection phase
			return &manager.ImportRequest{
				Status: "error",
				Error:  fmt.Sprintf("Failed to import torrent %s: %v", task.name, task.magnet),
			}

		case "torrent":
			importReq := manager.NewTorrentRequest(debridName, downloadFolder, task.magnet, _arr, config.DownloadAction(action), downloadUncached, callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			if err := s.manager.AddNewTorrent(ctx, importReq); err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add torrent")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			return importReq

		case "nzb":
			importReq := manager.NewNZBRequest(task.name, downloadFolder, task.nzbContent, _arr, config.DownloadAction(action), callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			nzoID, err := s.manager.AddNewNZB(ctx, importReq)
			if err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add NZB")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			importReq.Id = nzoID
			return importReq

		default:
			return nil
		}
	})

	// Filter out nil results
	filtered := make([]*manager.ImportRequest, 0, len(results))
	for _, r := range results {
		if r != nil {
			filtered = append(filtered, r)
		}
	}

	utils.JSONResponse(w, filtered, http.StatusOK)
}

func getNZBContentFromFile(fileHeader *multipart.FileHeader) ([]byte, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read NZB content
	nzbContent, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return nzbContent, nil
}

func (s *Server) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	v := version.GetInfo()
	utils.JSONResponse(w, v, http.StatusOK)
}

func (s *Server) handleRunMountCacheCleanup(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	cleaner, ok := mountMgr.(mountCacheCleaner)
	if !ok {
		http.Error(w, "Manual cache cleanup is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	cleanupStats, err := cleaner.CleanupCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to run mount cache cleanup")
		http.Error(w, "Failed to run mount cache cleanup", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.Refresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  cleanupStats,
	}, http.StatusOK)
}

func (s *Server) handlePurgeMountCache(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	purger, ok := mountMgr.(mountCachePurger)
	if !ok {
		http.Error(w, "Cache purge is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	purgeStats, err := purger.PurgeCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to purge mount cache")
		http.Error(w, "Failed to purge mount cache", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.Refresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  purgeStats,
	}, http.StatusOK)
}

func (s *Server) handleGetTorrents(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for server-side filtering, sorting, and pagination
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort_by"))
	sortOrder := strings.TrimSpace(r.URL.Query().Get("sort_order"))

	if sortBy == "" {
		sortBy = "added_on"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// GetReader all torrents
	allTorrents := s.manager.Queue().ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	for _, t := range allTorrents {
		t.Sanitize()
	}

	// Apply filters
	filteredTorrents := make([]*storage.Entry, 0)
	for _, t := range allTorrents {
		// Search filter - search in name and hash
		if search != "" {
			searchIn := strings.ToLower(t.Name + " " + t.InfoHash)
			if !strings.Contains(searchIn, search) {
				continue
			}
		}

		// Category filter
		if category != "" && t.Category != category {
			continue
		}

		// State filter
		if state != "" && t.State != storage.TorrentState(state) {
			continue
		}

		filteredTorrents = append(filteredTorrents, t)
	}

	// Apply sorting
	sortQueuedTorrents(filteredTorrents, sortBy, sortOrder)

	// Calculate pagination
	total := len(filteredTorrents)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	// Apply pagination
	var paginatedTorrents []*storage.Entry
	if offset < total {
		end := min(offset+limit, total)
		paginatedTorrents = filteredTorrents[offset:end]
	} else {
		paginatedTorrents = []*storage.Entry{}
	}

	// GetReader unique categories
	categorySet := make(map[string]bool)
	for _, t := range allTorrents {
		if t.Category != "" {
			categorySet[t.Category] = true
		}
	}

	categories := make([]string, 0, len(categorySet))
	for c := range categorySet {
		categories = append(categories, c)
	}

	utils.JSONResponse(w, map[string]any{
		"torrents":    paginatedTorrents,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
		"has_prev":    page > 1,
		"has_next":    page < totalPages,
		"categories":  categories,
	}, http.StatusOK)
}

// sortQueuedTorrents sorts torrents based on the given field and order
func sortQueuedTorrents(torrents []*storage.Entry, sortBy, sortOrder string) {
	if len(torrents) == 0 {
		return
	}

	less := func(i, j int) bool {
		var result bool
		switch sortBy {
		case "name":
			result = strings.ToLower(torrents[i].Name) < strings.ToLower(torrents[j].Name)
		case "size":
			result = torrents[i].Size < torrents[j].Size
		case "added_on":
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		case "progress":
			result = torrents[i].Progress < torrents[j].Progress
		case "category":
			result = strings.ToLower(torrents[i].Category) < strings.ToLower(torrents[j].Category)
		case "state":
			result = torrents[i].State < torrents[j].State
		default:
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		}

		if sortOrder == "desc" {
			return !result
		}
		return result
	}

	sort.Slice(torrents, less)
}

func (s *Server) handleDeleteTorrent(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	var cleanup func(torrent *storage.Entry) error

	if removeFromDebrid {
		cleanup = func(t *storage.Entry) error {
			exists, _ := s.manager.EntryExists(t.InfoHash)
			if exists {
				// Remove the entry from manager fully, which will handle removing from debrid and deleting the entry
				return s.manager.DeleteEntry(t.InfoHash, true)
			}
			go s.manager.RemoveTorrentPlacements(t)
			return nil
		}
	}

	if err := s.manager.Queue().Delete(hash, cleanup); err != nil {
		s.logger.Error().Err(err).Str("hash", hash).Msg("Failed to delete entry from queue")
		http.Error(w, "Failed to delete entry from queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteTorrents(w http.ResponseWriter, r *http.Request) {
	hashesStr := r.URL.Query().Get("hashes")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hashesStr == "" {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	hashes := strings.Split(hashesStr, ",")
	var cleanup func(torrent *storage.Entry) error
	if removeFromDebrid {
		cleanup = func(t *storage.Entry) error {
			exists, _ := s.manager.EntryExists(t.InfoHash)
			if exists {
				// Remove the entry from manager fully, which will handle removing from debrid and deleting the entry
				return s.manager.DeleteEntry(t.InfoHash, true)
			}
			go s.manager.RemoveTorrentPlacements(t)
			return nil
		}
	}
	if err := s.manager.Queue().DeleteWhere("", config.ProtocolAll, "", hashes, cleanup); err != nil {
		s.logger.Error().Err(err).Msg("Failed to delete torrents")
		http.Error(w, "Failed to delete torrents", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	arrStorage := s.manager.Arr()
	cfg := config.Get()
	cfg.Arrs = arrStorage.SyncToConfig()

	// Create response with API token info
	type ConfigResponse struct {
		*config.Config
		APIToken     string `json:"api_token,omitempty"`
		AuthUsername string `json:"auth_username,omitempty"`
	}

	response := &ConfigResponse{Config: cfg}

	// AddOrUpdate API token and auth information
	auth := cfg.GetAuth()
	if auth != nil {
		if auth.APIToken != "" {
			response.APIToken = auth.APIToken
		}
		response.AuthUsername = auth.Username
	}

	utils.JSONResponse(w, response, http.StatusOK)
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	currentConfig := config.Get()
	before, err := json.Marshal(currentConfig)
	if err != nil {
		http.Error(w, "Failed to read current config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxConfigBodyBytes))
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The form sends only the settings it has inputs for. Lay it over the live
	// config so every other setting keeps its value instead of resetting.
	merged, err := config.MergeJSON(before, body)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to decode config update request")
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var newConfig config.Config
	if err := json.Unmarshal(merged, &newConfig); err != nil {
		s.logger.Error().Err(err).Msg("Failed to decode config update request")
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Basic validation
	if newConfig.BindAddress == "" {
		newConfig.BindAddress = "0.0.0.0"
	}
	if newConfig.Port == "" {
		newConfig.Port = "8282"
	}

	// Preserve fields that shouldn't be overwritten by frontend
	newConfig.Auth = currentConfig.GetAuth()
	// The frontend config form doesn't include use_auth or enable_webdav_auth,
	// so they would be zero-valued (false) in the decoded payload. Preserve
	// them from the live config so auth isn't silently disabled on every save.
	newConfig.UseAuth = currentConfig.UseAuth
	newConfig.EnableWebdavAuth = currentConfig.EnableWebdavAuth
	// The frontend settings form doesn't include webhook_token, so it would
	// be zero-valued (empty) in the decoded payload. Preserve it from the
	// live config the same way Auth is preserved above, so saving any other
	// setting doesn't silently disable Arr webhook authentication.
	newConfig.WebhookToken = currentConfig.WebhookToken
	// Saved by its own endpoint (handleUpdateWebhookTeardown), same reason.
	newConfig.ArrWebhookTeardown = currentConfig.ArrWebhookTeardown
	// The general settings form has no fields for these Repair knobs (they're
	// only ever set via the dedicated repair-config / overlay endpoints), so
	// they'd decode to zero-valued/"unset" here and get silently reset to
	// their defaults on every unrelated settings save - the same class of bug
	// that used to silently wipe the Arr webhook token.
	newConfig.Repair.PlaybackPadding = currentConfig.Repair.PlaybackPadding
	newConfig.Repair.Par2Repair = currentConfig.Repair.Par2Repair
	newConfig.Repair.PadMaxRunSegments = currentConfig.Repair.PadMaxRunSegments
	newConfig.Repair.PadMaxTotalSegments = currentConfig.Repair.PadMaxTotalSegments
	newConfig.Repair.PadMaxByteRatio = currentConfig.Repair.PadMaxByteRatio
	newConfig.Repair.Par2RepairMode = currentConfig.Repair.Par2RepairMode
	newConfig.Repair.Par2RepairMinSegments = currentConfig.Repair.Par2RepairMinSegments
	newConfig.Repair.PrecacheReadAhead = currentConfig.Repair.PrecacheReadAhead
	// Precache config (threshold/concurrency/next-episodes/max-bytes) has its
	// own dedicated save path (handleUpdatePrecacheConfig) - the general
	// settings form has no fields for it either, so without this it would
	// silently reset to defaults on every unrelated settings save, same bug
	// class as the Repair fields preserved above.
	newConfig.Precache = currentConfig.Precache
	// Plex has a dedicated endpoint (handleUpdatePlexConfig); preserve it here so
	// an unrelated settings save (e.g. editing an Arr) can't zero the Plex URL and
	// token, which silently opens the precache session gate. Same bug class as the
	// Precache/Repair preserves above.
	newConfig.Plex = currentConfig.Plex

	// Filter out empty or incomplete arrs
	validArrs := make([]config.Arr, 0, len(newConfig.Arrs))
	for _, a := range newConfig.Arrs {
		if a.Name != "" && a.Host != "" && a.Token != "" {
			validArrs = append(validArrs, a)
		}
	}
	newConfig.Arrs = validArrs

	// Sync arr storage with the new configuration
	s.manager.Arr().SyncFromConfig(newConfig.Arrs)

	// Save the updated config. This also applies defaults to newConfig, so the
	// restart comparison below sees a fully-normalized config on both sides.
	if err := newConfig.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Error saving config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.logConfigChanges("settings", before, &newConfig)

	// Only restart when a field that needs it actually changed (HTTP bind,
	// debrid/usenet clients, or the mount). For everything else, apply the new
	// config live so users aren't disrupted by a full restart on every save.
	restarted := config.Get().RequiresRestart(&newConfig)
	if restarted {
		go s.Restart()
	} else {
		config.Get().ApplyRuntime(&newConfig)
		// Reschedule/reapply the repair sweep if its settings changed.
		if svc := s.manager.Repair(); svc != nil {
			if err := svc.ApplyConfig(); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to apply repair config after live update")
			}
		}
	}

	utils.JSONResponse(w, map[string]any{"status": "success", "restarted": restarted}, http.StatusOK)
}

// maxConfigBodyBytes bounds a settings save's request body.
const maxConfigBodyBytes = 4 << 20

// logConfigChanges logs which config keys a save changed - names only, never
// values (the config holds tokens and passwords). On a production install Repair was
// switched off by a save on 2026-09-16 and nothing recorded which save or
// which field.
func (s *Server) logConfigChanges(source string, before []byte, after *config.Config) {
	now, err := json.Marshal(after)
	if err != nil {
		return
	}
	changed, err := config.ChangedJSONKeys(before, now)
	if err != nil || len(changed) == 0 {
		return
	}
	s.logger.Info().Str("source", source).Strs("changed_keys", changed).Msg("Settings saved")
	if slices.Contains(changed, "repair.enabled") && !after.Repair.Enabled {
		s.logger.Warn().Str("source", source).Msg("Settings save turned Repair off: no scheduled sweep until it is enabled again")
	}
}

func (s *Server) handleGetRepairConfig(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, map[string]any{
		"repair":   config.Get().Repair,
		"defaults": config.RepairPolicyDefaults(),
	}, http.StatusOK)
}

func (s *Server) handleUpdateRepairConfig(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	before, err := json.Marshal(cfg)
	if err != nil {
		http.Error(w, "Failed to read current config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	current, err := json.Marshal(cfg.Repair)
	if err != nil {
		http.Error(w, "Failed to read current repair config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxConfigBodyBytes))
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The Repair page's forms each send their own fields; the rest of the
	// repair settings keep their live values (see config.MergeJSON).
	merged, err := config.MergeJSON(current, body)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req config.RepairConfig
	if err := json.Unmarshal(merged, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Enabled {
		if strings.TrimSpace(req.Schedule) == "" {
			http.Error(w, "Schedule is required when repair is enabled", http.StatusBadRequest)
			return
		}
		if _, err := utils.ConvertToJobDef(req.Schedule); err != nil {
			http.Error(w, fmt.Sprintf("Invalid schedule: %v", err), http.StatusBadRequest)
			return
		}
		if req.RecheckInterval != "" {
			if _, err := utils.ParseDuration(req.RecheckInterval); err != nil {
				http.Error(w, fmt.Sprintf("Invalid recheck_interval: %v", err), http.StatusBadRequest)
				return
			}
		}
		if req.Source != "" && req.Source != config.RepairSourceArr && req.Source != config.RepairSourceManaged {
			http.Error(w, "Invalid source (must be 'arr' or 'managed')", http.StatusBadRequest)
			return
		}
	}
	if req.NNTPConnectionPercent < 0 || req.NNTPConnectionPercent > 100 {
		http.Error(w, "Invalid nntp_connection_percent (must be between 0 and 100)", http.StatusBadRequest)
		return
	}
	if req.VerificationConnections < 0 || req.VerificationConnections > 500 {
		http.Error(w, "Invalid verification_connections (must be between 0 and 500)", http.StatusBadRequest)
		return
	}
	for _, f := range []struct{ name, value string }{
		{"decode_detect_bytes", req.DecodeDetectBytes},
		{"decode_head_bytes", req.DecodeHeadBytes},
	} {
		v := strings.TrimSpace(f.value)
		if v == "" {
			continue
		}
		if n, err := config.ParseSize(v); err != nil || n <= 0 {
			http.Error(w, fmt.Sprintf("Invalid %s (expected a size like \"3GB\")", f.name), http.StatusBadRequest)
			return
		}
	}
	switch req.Par2RepairMode {
	case "", config.Par2RepairModeAutoAll, config.Par2RepairModeAutoThreshold, config.Par2RepairModeManual:
	default:
		http.Error(w, "Invalid par2_repair_mode (must be 'auto_all', 'auto_threshold', or 'manual')", http.StatusBadRequest)
		return
	}

	cfg.Repair = req
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save repair config")
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.logConfigChanges("repair", before, cfg)

	if svc := s.manager.Repair(); svc != nil {
		if err := svc.ApplyConfig(); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to apply repair config")
			http.Error(w, "Saved, but failed to apply: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	utils.JSONResponse(w, cfg.Repair, http.StatusOK)
}

func (s *Server) handleGetPrecacheConfig(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, config.Get().Precache, http.StatusOK)
}

// handleUpdatePrecacheConfig saves config.Precache (threshold/concurrency/
// next-episodes/max-bytes) - separate from handleUpdateRepairConfig because
// it's a sibling Config field, not part of RepairConfig (see PrecacheConfig's
// doc comment). Bounds-checking here rejects clearly-invalid input;
// Config.applyPrecacheDefaults (run by cfg.Save()) is what actually clamps
// PrecacheMaxBytes to its [0, 256GiB] range.
func (s *Server) handleUpdatePrecacheConfig(w http.ResponseWriter, r *http.Request) {
	var req config.PrecacheConfig
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.PrecacheThresholdPercent < 0 || req.PrecacheThresholdPercent > 100 {
		http.Error(w, "Invalid precache_threshold_percent (must be between 1 and 100, or 0 for default)", http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	cfg.Precache = req
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save precache config")
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, cfg.Precache, http.StatusOK)
}

// plexTokenPlaceholder stands in for config.PlexConfig.Token in every
// response that leaves the server, so the token itself is never sent back
// to the browser. handleUpdatePlexConfig and handlePlexTestConnection both
// treat it (or a blank field) as "unchanged - use the saved token".
const plexTokenPlaceholder = "********"

// handleGetPlexConfig returns config.Plex (see PlexConfig's doc comment)
// with Token shadowed by plexTokenPlaceholder when set, so the repair page
// can render "token is set" without the token ever reaching the browser.
func (s *Server) handleGetPlexConfig(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get().Plex
	if cfg.Token != "" {
		cfg.Token = plexTokenPlaceholder
	}
	utils.JSONResponse(w, cfg, http.StatusOK)
}

// handleUpdatePlexConfig saves config.Plex - its own dedicated endpoint,
// like repair/precache, rather than a field on the general settings form,
// so an unrelated settings save can't silently reset it (same bug class as
// the Repair/Precache preserve block in handleUpdateConfig). Token is
// write-only: a blank field or the plexTokenPlaceholder echoed back by
// handleGetPlexConfig leaves the saved token untouched, mirroring Auth's
// preserve-on-blank handling above.
func (s *Server) handleUpdatePlexConfig(w http.ResponseWriter, r *http.Request) {
	var req config.PlexConfig
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	if req.Token == "" || req.Token == plexTokenPlaceholder {
		req.Token = cfg.Plex.Token
	}
	switch req.ReapMode {
	case config.PlexReapOff, config.PlexReapDryRun, config.PlexReapOn:
	case "":
		// An older page that doesn't send the field keeps the saved mode.
		req.ReapMode = cfg.Plex.ReapMode
	default:
		http.Error(w, "invalid plex_reap_mode: "+string(req.ReapMode), http.StatusBadRequest)
		return
	}
	cfg.Plex = req
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save plex config")
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := cfg.Plex
	if resp.Token != "" {
		resp.Token = plexTokenPlaceholder
	}
	utils.JSONResponse(w, resp, http.StatusOK)
}

// handlePlexTestConnection probes a Plex server's /status/sessions endpoint
// with the url/token from the request body (not the saved config, mirroring
// handleSpeedTest) so the repair page's "Test connection" button can
// validate settings before they're saved. A plexTokenPlaceholder token
// falls back to the saved one so "Test" works without re-typing it.
// handlePlexReapStatus reports the reaper's mode, pending jobs, recent
// decisions and backlog scan.
func (s *Server) handlePlexReapStatus(w http.ResponseWriter, r *http.Request) {
	reaper := s.manager.PlexReaper()
	if reaper == nil {
		utils.JSONResponse(w, manager.PlexReapStatus{}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, reaper.Status(), http.StatusOK)
}

// handlePlexReapScan starts a backlog scan (read-only; nothing is removed).
func (s *Server) handlePlexReapScan(w http.ResponseWriter, r *http.Request) {
	reaper := s.manager.PlexReaper()
	if reaper == nil || !config.Get().Plex.Enabled() {
		http.Error(w, "Plex is not configured", http.StatusBadRequest)
		return
	}
	if !reaper.StartScan() {
		http.Error(w, "A scan is already running", http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]any{"started": true}, http.StatusAccepted)
}

// handlePlexReapApply removes the stale versions of chosen backlog items
// (rating_keys; empty = every reapable one), re-checking every guard first.
func (s *Server) handlePlexReapApply(w http.ResponseWriter, r *http.Request) {
	reaper := s.manager.PlexReaper()
	if reaper == nil || !config.Get().Plex.Enabled() {
		http.Error(w, "Plex is not configured", http.StatusBadRequest)
		return
	}
	var req struct {
		RatingKeys []string `json:"rating_keys"`
	}
	if r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	if !reaper.StartApply(req.RatingKeys) {
		http.Error(w, "A scan or apply is already running", http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]any{"started": true}, http.StatusAccepted)
}

// handleUpdateWebhookTeardown saves ArrWebhookTeardown on its own, like the
// webhook token, so the general settings form can't reset it.
func (s *Server) handleUpdateWebhookTeardown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	cfg := config.Get()
	cfg.ArrWebhookTeardown = req.Enabled
	if err := cfg.Save(); err != nil {
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]any{"enabled": cfg.ArrWebhookTeardown}, http.StatusOK)
}

func (s *Server) handlePlexTestConnection(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL   string `json:"url"`
		Token string `json:"token"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	if req.Token == "" || req.Token == plexTokenPlaceholder {
		req.Token = config.Get().Plex.Token
	}

	if err := manager.TestPlexConnection(config.PlexConfig{URL: req.URL, Token: req.Token}); err != nil {
		utils.JSONResponse(w, map[string]any{"ok": false, "error": err.Error()}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, map[string]any{"ok": true}, http.StatusOK)
}

func (s *Server) handleRepairStatus(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		utils.JSONResponse(w, manager.RepairStatus{}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, svc.Status(), http.StatusOK)
}

// handlePrecacheStatus reports the read-ahead / next-episode precache
// feature's live config and state (see Manager.PrecacheStatus) for the
// repair/overlay GUI's summary.
func (s *Server) handlePrecacheStatus(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.PrecacheStatus(), http.StatusOK)
}

// PrecachePurgeResult is the response for handlePurgeIncompletePrecache -
// see Precache.PurgeIncomplete.
type PrecachePurgeResult struct {
	Deleted         []string          `json:"deleted"`
	SkippedInflight []string          `json:"skipped_inflight"`
	Failed          []PurgeFailureDTO `json:"failed"`
	FreedBytes      int64             `json:"freed_bytes"`
}

// PurgeFailureDTO mirrors manager.PurgeFailure for the API response.
type PurgeFailureDTO struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// handlePurgeIncompletePrecache clears precache entries whose DFS cache
// coverage never reached 1.0 (abandoned read-aheads, episodes precached but
// never watched, etc). Defaults to a dry run - pass ?execute=true to actually
// delete. Entries with a burst currently writing into them are always
// skipped (see Precache.InflightHas), dry-run or not, so the preview matches
// what an execute=true call would really do.
func (s *Server) handlePurgeIncompletePrecache(w http.ResponseWriter, r *http.Request) {
	execute, _ := strconv.ParseBool(strings.TrimSpace(r.URL.Query().Get("execute")))
	deleted, skippedInflight, failed, freedBytes, err := s.manager.PurgeIncompletePrecache(execute)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	failedDTO := make([]PurgeFailureDTO, 0, len(failed))
	for _, f := range failed {
		failedDTO = append(failedDTO, PurgeFailureDTO{Name: f.Name, Reason: f.Reason})
	}
	utils.JSONResponse(w, PrecachePurgeResult{
		Deleted:         deleted,
		SkippedInflight: skippedInflight,
		Failed:          failedDTO,
		FreedBytes:      freedBytes,
	}, http.StatusOK)
}

// handleRescanPrecache re-runs the on-disk cache scan so entries cached
// after startup - a fresh import, or Plex reading a file during intro
// detection or a library scan - show up in the readiness table without
// waiting for a restart. See Precache.Rescan.
func (s *Server) handleRescanPrecache(w http.ResponseWriter, r *http.Request) {
	s.manager.RescanPrecache()
	utils.JSONResponse(w, map[string]bool{"ok": true}, http.StatusOK)
}

// handlePausePrecache sets or clears the precache feature's global runtime
// pause (see Manager.SetPrecachePaused) - halting new read-ahead/next-episode
// bursts from starting without touching the saved read-ahead setting.
// Anything already downloading finishes; the pause itself is in-memory only
// and clears on restart.
func (s *Server) handlePausePrecache(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Paused bool `json:"paused"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.manager.SetPrecachePaused(req.Paused)
	utils.JSONResponse(w, map[string]bool{"paused": req.Paused}, http.StatusOK)
}

// handlePausePrecacheEntry sets or clears the runtime pause for one
// (info_hash,filename) pair (see Manager.SetPrecacheEntryPaused).
func (s *Server) handlePausePrecacheEntry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		InfoHash string `json:"info_hash"`
		Filename string `json:"filename"`
		Paused   bool   `json:"paused"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.InfoHash = strings.TrimSpace(req.InfoHash)
	req.Filename = strings.TrimSpace(req.Filename)
	if req.InfoHash == "" || req.Filename == "" {
		http.Error(w, "info_hash and filename are required", http.StatusBadRequest)
		return
	}
	s.manager.SetPrecacheEntryPaused(req.InfoHash, req.Filename, req.Paused)
	utils.JSONResponse(w, map[string]bool{"paused": req.Paused}, http.StatusOK)
}

func (s *Server) handleRunRepair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IgnoreLastChecked bool   `json:"ignore_last_checked,omitempty"`
		Force             bool   `json:"force,omitempty"`
		AutoRepair        *bool  `json:"auto_repair,omitempty"`
		UnrestrictLink    bool   `json:"unrestrict_link,omitempty"`
		Protocol          string `json:"protocol,omitempty"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	ignoreLastChecked := req.IgnoreLastChecked || req.Force
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("ignore_last_checked"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("force"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	autoRepair := req.AutoRepair
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("auto_repair"))) {
	case "1", "true", "yes", "on":
		v := true
		autoRepair = &v
	case "0", "false", "no", "off":
		v := false
		autoRepair = &v
	}
	unrestrictLink := req.UnrestrictLink
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("unrestrict_link"))) {
	case "1", "true", "yes", "on":
		unrestrictLink = true
	case "0", "false", "no", "off":
		unrestrictLink = false
	}
	protocolScope := strings.ToLower(strings.TrimSpace(req.Protocol))
	if queryProtocol := strings.TrimSpace(r.URL.Query().Get("protocol")); queryProtocol != "" {
		protocolScope = strings.ToLower(queryProtocol)
	}
	switch protocolScope {
	case "", "all", "both", "torrent", "nzb":
		if protocolScope == "both" {
			protocolScope = "all"
		}
	default:
		http.Error(w, "Invalid protocol; expected all, torrent, or nzb", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	id, err := svc.RunNow(manager.RepairRunOptions{
		IgnoreLastChecked: ignoreLastChecked,
		AutoRepair:        autoRepair,
		UnrestrictLink:    unrestrictLink,
		ProtocolScope:     protocolScope,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]string{"run_id": id}, http.StatusOK)
}

func (s *Server) handleStopRepair(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	if err := svc.StopRun(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListRepairRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.manager.Storage().ListRepairRuns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, runs, http.StatusOK)
}

func (s *Server) handleGetRepairRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No run ID provided", http.StatusBadRequest)
		return
	}
	run, err := s.manager.Storage().GetRepairRun(id)
	if err != nil {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleClearRepairRuns(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Storage().ClearRepairRuns(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListEntryHealth(w http.ResponseWriter, r *http.Request) {
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	out := make([]*storage.EntryHealth, 0)
	_ = s.manager.Storage().ForEachEntryHealth(func(state *storage.EntryHealth) error {
		if statusFilter != "" && string(state.Status) != statusFilter {
			return nil
		}
		// A health record's backing entry can be deleted through a path that
		// leaves the record itself behind (see Manager.EntryNameHasBackingEntry) -
		// never show a record for something that no longer exists.
		if !s.manager.EntryNameHasBackingEntry(state.EntryName) {
			return nil
		}
		out = append(out, state)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		return out[i].EntryName < out[j].EntryName
	})
	utils.JSONResponse(w, out, http.StatusOK)
}

func (s *Server) handleGetEntryHealth(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	state, err := s.manager.Storage().GetEntryHealth(name)
	if err != nil {
		http.Error(w, "Entry health not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

func (s *Server) handleRecheckMedia(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Arr         string `json:"arr"`
		MediaID     string `json:"media_id"`
		Fix         bool   `json:"fix"`
		ForceDecode bool   `json:"force_decode"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.MediaID) == "" {
		http.Error(w, "media_id is required", http.StatusBadRequest)
		return
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.RecheckMedia(s.manager.Context(), strings.TrimSpace(req.Arr), strings.TrimSpace(req.MediaID), req.Fix, req.ForceDecode)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		// Returning the run record (when present) gives the caller the
		// failure detail captured in storage as well as the message.
		if run != nil {
			utils.JSONResponse(w, map[string]any{
				"error": err.Error(),
				"run":   run,
			}, status)
			return
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleRecheckEntry(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	fix := r.URL.Query().Get("fix") == "true"
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	var wait time.Duration
	if raw := r.URL.Query().Get("wait"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			http.Error(w, "Invalid wait (a duration such as 90s)", http.StatusBadRequest)
			return
		}
		wait = min(d, maxRecheckWait)
	}
	state, err := svc.RecheckEntry(s.manager.Context(), name, fix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if wait > 0 {
		// ?wait=<duration>: answer with the finished record, or 202 with the
		// in-progress reply (its active_run_id is the run to wait for) when the
		// recheck is still going.
		if svc.WaitRecheck(r.Context(), state.ActiveRunID, wait) {
			final, err := s.manager.Storage().GetEntryHealth(name)
			if err != nil || final == nil {
				http.Error(w, "Entry health not found: the recheck removed it", http.StatusNotFound)
				return
			}
			utils.JSONResponse(w, final, http.StatusOK)
			return
		}
		utils.JSONResponse(w, state, http.StatusAccepted)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

// maxRecheckWait caps how long a recheck request with ?wait= holds the
// connection.
const maxRecheckWait = 10 * time.Minute

// handleFixBroken kicks off the Arr delete + re-search pass on currently
// broken entries. Body: {"names": ["...", ...]}. Empty/missing names ⇒ fix
// every broken entry in storage.
func (s *Server) handleFixBroken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names,omitempty"`
	}
	// Body is optional; ignore decode errors for empty / missing bodies.
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.FixBroken(s.manager.Context(), req.Names)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

// handleListDebridGone lists torrent entries no configured debrid can serve
// (their debrid is gone from config, or re-insertion gave up) and what a bulk
// fix would do with each: delete the orphans no Arr points at, re-grab the
// rest. Lists every Arr's library first, so it takes about a minute.
// ?limit=N caps both name lists (default 50); totals always cover every entry.
func (s *Server) handleListDebridGone(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	res, err := svc.FindDebridGone(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	res.Entries = res.Entries[:min(limit, len(res.Entries))]
	res.Orphans = res.Orphans[:min(limit, len(res.Orphans))]
	utils.JSONResponse(w, res, http.StatusOK)
}

// handleFixDebridGone runs one bulk batch over unservable torrent entries.
// With ?delete=1 it deletes up to ?delete_limit=N (default 200) orphans no
// Arr points at; it then marks up to ?limit=N (default 50) entries an Arr
// still points at broken and runs Fix broken on them: delete, blocklist and
// re-search through their Arr. 409 while another repair run is active.
func (s *Server) handleFixDebridGone(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	opts := manager.DebridGoneFixOptions{Delete: q.Get("delete") == "1" || q.Get("delete") == "true"}
	opts.Limit, _ = strconv.Atoi(q.Get("limit"))
	opts.DeleteLimit, _ = strconv.Atoi(q.Get("delete_limit"))
	res, err := svc.FixDebridGone(s.manager.Context(), opts)
	if err != nil && res.Deleted > 0 {
		// The deletes happened; report them with why the re-grab did not start.
		res.Error = err.Error()
		err = nil
	}
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, res, http.StatusOK)
}

// handleListUnverified lists healthy entries with a file the last probe could
// not verify, each file with its reason (storage.EntryHealth.UnverifiedFiles).
func (s *Server) handleListUnverified(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	list, err := svc.ListUnverified()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if list == nil {
		list = []*storage.EntryHealth{}
	}
	utils.JSONResponse(w, list, http.StatusOK)
}

// handleReplaceUnverified re-grabs the replaceable files (tail truncated,
// volumes out of order) of unverified
// entries without blocklisting their releases. Body: {"names": [...],
// "limit": N}; no names means every such entry, limit defaults to 25. 409
// while another repair run is active.
func (s *Server) handleReplaceUnverified(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names,omitempty"`
		Limit int      `json:"limit,omitempty"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	res, err := svc.ReplaceUnverified(s.manager.Context(), req.Names, req.Limit)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, res, http.StatusOK)
}

// handleClearBroken clears currently broken files without asking the Arr to
// re-search for replacements. Body: {"names": ["...", ...]}. Empty/missing
// names ⇒ clear every broken entry in storage.
func (s *Server) handleClearBroken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names,omitempty"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.ClearBroken(s.manager.Context(), req.Names)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

// handleClearSuperseded checks every currently-broken entry against the
// configured Arrs and clears whichever ones the library has already
// replaced with a working copy, without touching anything the Arrs still
// reference. Returns a JSON summary for the UI to toast. A failure to build
// the Arr reference set clears nothing and is reported as 502, since a
// partial/failed Arr lookup must never be treated as "nothing is referenced".
func (s *Server) handleClearSuperseded(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	result, err := svc.ClearSuperseded(s.manager.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	utils.JSONResponse(w, result, http.StatusOK)
}

func (s *Server) handleClearRepairState(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Statuses []string `json:"statuses"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	statuses := make([]storage.HealthStatus, 0, len(req.Statuses))
	for _, raw := range req.Statuses {
		status, ok := parseRepairHealthStatus(raw)
		if !ok {
			http.Error(w, "Invalid repair health status: "+raw, http.StatusBadRequest)
			return
		}
		statuses = append(statuses, status)
	}
	if len(statuses) == 0 {
		http.Error(w, "At least one status is required", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	result, err := svc.ClearStates(statuses)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, result, http.StatusOK)
}

// handleClearDecodeVerification zeroes the DecodeVerifiedAt,
// DecodeVerifiedFingerprint and DecodeVerifiedCoverage fields on all
// EntryHealth records, forcing the next sweep to re-run decode verification on
// every entry.
func (s *Server) handleClearDecodeVerification(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	cleared, err := svc.ClearDecodeVerification()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]any{"cleared": cleared}, http.StatusOK)
}

func parseRepairHealthStatus(raw string) (storage.HealthStatus, bool) {
	switch storage.HealthStatus(strings.ToLower(strings.TrimSpace(raw))) {
	case storage.HealthHealthy:
		return storage.HealthHealthy, true
	case storage.HealthBroken:
		return storage.HealthBroken, true
	case storage.HealthRepairing:
		return storage.HealthRepairing, true
	case storage.HealthStale:
		return storage.HealthStale, true
	case storage.HealthUnknown:
		return storage.HealthUnknown, true
	case storage.HealthUnsupported:
		return storage.HealthUnsupported, true
	default:
		return "", false
	}
}

func (s *Server) handleRefreshAPIToken(w http.ResponseWriter, _ *http.Request) {
	token, err := s.refreshAPIToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to refresh API token")
		http.Error(w, "Failed to refresh token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]any{
		"token":   token,
		"message": "API token refreshed successfully",
	}, http.StatusOK)
}

func (s *Server) handleUpdateAuth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	auth := cfg.GetAuth()
	if auth == nil {
		auth = &config.Auth{}
	}

	// Check if trying to disable authentication (both empty)
	if req.Username == "" && req.Password == "" {
		// Disable authentication
		cfg.UseAuth = false
		auth.Username = ""
		auth.Password = ""
		if err := cfg.SaveAuth(auth); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save auth config")
			http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
			return
		}
		if err := cfg.Save(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save config")
			http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
			return
		}

		utils.JSONResponse(w, map[string]string{
			"message": "Authentication disabled successfully",
		}, http.StatusOK)
		return
	}

	// Validate required fields
	if req.Username == "" {
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}
	if req.Password == "" {
		http.Error(w, "Password is required", http.StatusBadRequest)
		return
	}
	if req.Password != req.ConfirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to hash password")
		http.Error(w, "Failed to process password", http.StatusInternalServerError)
		return
	}

	// Update auth settings
	auth.Username = req.Username
	auth.Password = string(hashedPassword)
	cfg.UseAuth = true

	// Save auth config
	if err := cfg.SaveAuth(auth); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save auth config")
		http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
		return
	}

	// Save main config
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]string{
		"message": "Authentication settings updated successfully",
	}, http.StatusOK)
}
