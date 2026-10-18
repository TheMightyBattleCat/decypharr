package server

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) WebRoutes() http.Handler {
	r := chi.NewRouter()

	// Apply setup redirect middleware globally
	r.Use(s.setupRedirectMiddleware)

	// Static assets - always public
	staticFS, _ := fs.Sub(assetsEmbed, "assets/build")
	imagesFS, _ := fs.Sub(imagesEmbed, "assets/images")
	r.Handle("/assets/*", http.StripPrefix(s.urlBase+"assets/", http.FileServer(http.FS(staticFS))))
	r.Handle("/images/*", http.StripPrefix(s.urlBase+"images/", http.FileServer(http.FS(imagesFS))))

	// Public routes - no auth needed
	r.Get("/version", s.handleGetVersion)
	r.Get("/login", s.LoginHandler)
	r.Post("/login", s.LoginHandler)
	r.Get("/register", s.RegisterHandler)
	r.Post("/register", s.RegisterHandler)
	r.Post("/skip-auth", s.skipAuthHandler)

	// Setup wizard - public, no auth required
	r.Get("/setup", s.SetupHandler)
	r.Post("/api/setup/complete", s.setupCompleteHandler)
	r.Post("/api/setup/restore/snapshots", s.setupRestoreListHandler)
	r.Post("/api/setup/restore", s.setupRestoreHandler)
	r.Post("/api/setup/restore/upload", s.setupRestoreUploadHandler)

	// Protected routes - require auth
	r.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		// Web pages
		r.Get("/", s.IndexHandler)
		r.Get("/browse", s.BrowseHandler)
		r.Get("/download", s.DownloadHandler)
		r.Get("/repair", s.RepairHandler)
		r.Get("/backups", s.BackupsHandler)
		r.Get("/links", s.LinksHandler)
		r.Get("/stats", s.StatsHandler)
		r.Get("/settings", s.ConfigHandler)

		// API routes
		r.Route("/api", func(r chi.Router) {
			// Arr management
			r.Get("/arrs", s.handleGetArrs)
			r.Get("/arrs/wanted-search", s.handleWantedSearchStatus)
			r.Post("/arrs/wanted-search/run", s.handleWantedSearchRun)
			r.Post("/add", s.handleAddContent)

			// Repair / health-checker operations
			r.Get("/repair/config", s.handleGetRepairConfig)
			r.Put("/repair/config", s.handleUpdateRepairConfig)
			r.Get("/repair/status", s.handleRepairStatus)
			r.Post("/repair/run", s.handleRunRepair)
			r.Post("/repair/stop", s.handleStopRepair)
			r.Post("/repair/recheck/media", s.handleRecheckMedia)
			r.Post("/repair/fix", s.handleFixBroken)
			r.Post("/repair/clear", s.handleClearBroken)
			r.Get("/repair/debrid-gone", s.handleListDebridGone)
			r.Post("/repair/debrid-gone/fix", s.handleFixDebridGone)
			r.Get("/repair/unverified", s.handleListUnverified)
			r.Post("/repair/unverified/replace", s.handleReplaceUnverified)
			r.Post("/repair/clear-superseded", s.handleClearSuperseded)
			r.Post("/repair/clear-state", s.handleClearRepairState)
			r.Post("/repair/clear-decode-verification", s.handleClearDecodeVerification)
			r.Get("/repair/runs", s.handleListRepairRuns)
			r.Get("/repair/runs/{id}", s.handleGetRepairRun)
			r.Delete("/repair/runs", s.handleClearRepairRuns)
			r.Get("/repair/health", s.handleListEntryHealth)
			r.Get("/repair/health/{name}", s.handleGetEntryHealth)
			r.Post("/repair/health/{name}/check", s.handleRecheckEntry)

			// Overlay management (playback padding / PAR2 patch state)
			r.Get("/overlay/files", s.handleListOverlayFiles)
			r.Get("/overlay/disk-usage", s.handleOverlayDiskUsage)
			r.Get("/overlay/repair-history", s.handleListPar2RepairHistory)
			r.Get("/overlay/repair-history/{id}", s.handleGetPar2RepairHistoryEntry)
			r.Delete("/overlay/repair-history", s.handleClearPar2RepairHistory)
			r.Post("/overlay/repair-now", s.handleOverlayRepairNow)
			r.Get("/overlay/repair-progress", s.handleOverlayRepairProgress)
			r.Post("/overlay/verify", s.handleOverlayVerify)
			r.Post("/overlay/screen", s.handleOverlayScreen)
			r.Post("/overlay/reclaim", s.handleOverlayReclaim)
			r.Post("/overlay/research", s.handleOverlayResearch)
			r.Get("/overlay/gc-orphans/count", s.handleOverlayOrphanCount)
			r.Post("/overlay/gc-orphans", s.handleOverlayGCOrphans)
			r.Post("/overlay/reap", s.handleOverlayReap)

			// Proactive pre-caching / read-ahead repair status
			r.Get("/precache/status", s.handlePrecacheStatus)
			r.Get("/precache/config", s.handleGetPrecacheConfig)
			r.Put("/precache/config", s.handleUpdatePrecacheConfig)
			r.Post("/precache/purge-incomplete", s.handlePurgeIncompletePrecache)
			r.Post("/precache/rescan", s.handleRescanPrecache)
			r.Post("/precache/pause", s.handlePausePrecache)
			r.Post("/precache/entry-pause", s.handlePausePrecacheEntry)

			// Plex server settings - gates Precache's read-ahead behind an
			// active "now playing" session (see PlexConfig's doc comment)
			r.Get("/plex/config", s.handleGetPlexConfig)
			r.Put("/plex/config", s.handleUpdatePlexConfig)
			r.Post("/plex/test", s.handlePlexTestConnection)

			// Plex stale-version reaper (see manager.PlexReaper)
			r.Get("/plex/reap/status", s.handlePlexReapStatus)
			r.Post("/plex/reap/scan", s.handlePlexReapScan)
			r.Post("/plex/reap/apply", s.handlePlexReapApply)

			// Backups
			r.Get("/backups/status", s.handleBackupStatus)
			r.Post("/backups/hold/release", s.handleBackupReleaseHold)
			r.Post("/backups/running/stop", s.handleBackupStopRun)
			r.Post("/backups/check/run", s.handleBackupRunCheck)
			r.Post("/backups/offsite/run", s.handleBackupRunOffsite)
			r.Get("/backups/snapshots", s.handleBackupListSnapshots)
			r.Get("/backups/snapshots/{id}", s.handleBackupGetSnapshot)
			r.Delete("/backups/snapshots/{id}", s.handleBackupDeleteSnapshot)
			r.Post("/backups/jobs/{name}/run", s.handleBackupRunJob)
			r.Post("/backups/snapshots/{id}/verify", s.handleBackupVerify)
			r.Get("/backups/snapshots/{id}/download", s.handleBackupDownload)
			r.Post("/backups/snapshots/{id}/restore/db", s.handleBackupRestoreDB)
			r.Post("/backups/snapshots/{id}/restore/nzb", s.handleBackupRestoreNZB)
			r.Post("/backups/snapshots/{id}/restore/links/plan", s.handleBackupLinksPlan)
			r.Post("/backups/snapshots/{id}/restore/links/apply", s.handleBackupLinksApply)
			r.Get("/backups/config-history", s.handleBackupConfigHistory)
			r.Get("/backups/config-history/{id}/diff", s.handleBackupConfigDiff)
			r.Post("/backups/config-history/{id}/revert", s.handleBackupConfigRevert)
			r.Get("/backups/removed", s.handleBackupListRemoved)
			r.Post("/backups/removed/{key}/restore", s.handleBackupRestoreRemoved)
			r.Post("/backups/retention/preview", s.handleBackupRetentionPreview)
			r.Post("/backups/schedule/check", s.handleBackupScheduleCheck)

			// Library Links
			r.Get("/links/status", s.handleLinksStatus)
			r.Post("/links/check", s.handleLinksCheck)
			r.Get("/links", s.handleLinksList)
			r.Get("/links/missing", s.handleLinksMissing)
			r.Post("/links/cleanup/plan", s.handleLinksCleanupPlan)
			r.Post("/links/cleanup/apply", s.handleLinksCleanupApply)

			// Torrent management
			r.Get("/torrents", s.handleGetTorrents)
			r.Delete("/torrents/{category}/{hash}", s.handleDeleteTorrent)
			r.Delete("/torrents", s.handleDeleteTorrents) // Fixed trailing slash

			// Browse - WebDAV-style hierarchical file browser
			r.Route("/browse", func(r chi.Router) {
				// Hierarchical browse endpoints
				r.Get("/", s.handleBrowseMount)                                    // Mount: groups (__all__, __bad__, etc.)
				r.Get("/{group}", s.handleBrowseGroup)                             // Group: torrents
				r.Get("/{group}/{subgroup}/{torrent}", s.handleBrowseTorrentFiles) // Torrent files (with subgroup)
				r.Get("/{group}/{torrent}", s.handleBrowseTorrentFiles)            // Torrent files (without subgroup) - This route needs to come after the subgroup route

				// Torrent operations
				r.Delete("/torrents/{id}", s.handleDeleteBrowseTorrent)
				r.Delete("/torrents/batch", s.handleBatchDeleteBrowseTorrents)

				// Stale NZB cleanup
				r.Get("/stale-nzbs/preview", s.handleStaleNZBsPreview)
				r.Post("/stale-nzbs/cleanup", s.handleStaleNZBsCleanup)
				r.Get("/stale-nzbs/progress", s.handleStaleNZBsProgress)

				// File download
				r.Get("/download/{torrent}/{file}", s.handleDownloadFile)
			})

			// STRM reconciler
			r.Post("/strm/regenerate", s.handleStrmRegenerate)

			// Config/Auth
			r.Get("/config", s.handleGetConfig)
			r.Post("/config", s.handleUpdateConfig)
			r.Post("/virtual-folders/preview", s.handlePreviewVirtualFolder)
			r.Post("/mount/cache/cleanup", s.handleRunMountCacheCleanup)
			r.Post("/mount/cache/purge", s.handlePurgeMountCache)
			r.Post("/refresh-token", s.handleRefreshAPIToken)
			r.Post("/update-auth", s.handleUpdateAuth)
			r.Post("/webhook-token/refresh", s.handleRefreshWebhookToken)
			r.Post("/webhook-teardown", s.handleUpdateWebhookTeardown)
		})
	})

	return r
}
