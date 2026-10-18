package manager

import (
	"context"
	"path/filepath"
	"runtime/debug"
	"sort"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
)

// initBackup builds the backup service and installs it. It runs during New,
// before the server starts, so handlers never see it half-built. The jobs are
// registered in StartWorker.
func (m *Manager) initBackup() {
	d := baseBackupDeps(m.storage, config.GetMainPath())
	d.Arrs = m.backupArrSources
	d.Rescanner = m.backupRescan
	d.Streaming = func() bool { return m.GetActiveStreamsCount() > 0 }
	d.Notify = func(ev config.NotificationEvent, status, msg string, err error) {
		if m.Notifications != nil {
			m.Notifications.Notify(notifications.Event{Type: ev, Status: status, Message: msg, Error: err})
		}
	}
	d.Scheduler = m.scheduler
	svc := backup.NewService(d)
	m.wireBackupServices(svc)
	m.backupSvc = svc
	m.SetBackup(svc)
	backup.SetPreActionHook(svc.PreAction)
}

// wireBackupServices gives the service its settings history and undelete
// helpers. The history keeps its blobs in a repository of its own (never the
// snapshot repository, whose prune sweeps blobs no snapshot references) and
// hooks every settings save.
func (m *Manager) wireBackupServices(svc *backup.BackupService) {
	dataDir := config.GetMainPath()
	live := backup.LiveRepo{Open: svc.Repo}

	histDir := filepath.Join(dataDir, "config-history")
	if blobs, err := backup.OpenRepo(histDir); err != nil {
		m.logger.Warn().Err(err).Msg("backup: settings history is off")
	} else {
		h := backup.NewHistory(backup.HistoryOptions{
			Dir:   histDir,
			Blobs: blobs,
			// Versions a kept snapshot references are never trimmed.
			Snapshots: live,
			Source:    func() string { return "settings" },
			Keep:      config.Get().Backup.ConfigHistoryMax,
			Applied: func(previous, published *config.Config) {
				if _, err := m.ApplySavedConfig(previous, published); err != nil {
					m.logger.Warn().Err(err).Msg("backup: a settings revert could not be fully applied")
				}
			},
		})
		svc.History = h
		config.OnSaved = h.OnSaved
	}

	svc.Undelete = backup.NewUndelete(backup.UndeleteOptions{
		Repo:           live,
		Store:          m.storage,
		MetaDir:        filepath.Join(dataDir, "usenet", "meta"),
		OverlayDir:     filepath.Join(dataDir, "usenet", "overlay"),
		VolumeOrderDir: filepath.Join(dataDir, "usenet", "volume-order-backup"),
		Lookup:         backupEntryLookup{st: m.storage},
		MountRoot:      config.Get().Mount.MountPath,
		Rescanner:      m.backupRescan,
	})
}

// baseBackupDeps is the part of the service wiring the server and the
// command line share: where the data is, how the database is exported and
// how links are classified.
func baseBackupDeps(st *storage.Storage, dataDir string) backup.Deps {
	return backup.Deps{
		DataDir:   dataDir,
		Config:    func() config.Backup { return config.Get().Backup },
		Build:     backupBuildInfo(),
		DBExport:  backup.NewDBExporter(st),
		DBWrites:  st.WriteCount,
		Lookup:    backupEntryLookup{st: st},
		MountRoot: func() string { return config.Get().Mount.MountPath },
	}
}

// NewOfflineBackupService builds the backup service for the command line:
// the same wiring as the server, over the database in dataDir, with no Arr
// clients, notifications or schedule. The repository is repoDir whatever the
// settings say. The returned func closes the database.
func NewOfflineBackupService(dataDir, repoDir string) (*backup.BackupService, func(), error) {
	config.SetConfigPath(dataDir)
	st, err := storage.NewStorage(filepath.Join(dataDir, "db"))
	if err != nil {
		return nil, nil, err
	}
	d := baseBackupDeps(st, dataDir)
	base := d.Config
	d.Config = func() config.Backup {
		c := base()
		c.Dir = repoDir
		return c
	}
	return backup.NewService(d), func() { _ = st.Close() }, nil
}

func backupBuildInfo() backup.BuildInfo {
	bi := backup.BuildInfo{Version: version.GetInfo().String(), DBFormat: storage.DBFormat}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				bi.Commit = s.Value
			}
		}
	}
	return bi
}

// backupArrSources lists the Sonarr and Radarr instances with the path maps
// from the backup settings.
func (m *Manager) backupArrSources() []backup.ArrSource {
	if m.arr == nil {
		return nil
	}
	maps := config.Get().Backup.ArrPathMaps
	var out []backup.ArrSource
	for _, a := range m.arr.GetAll() {
		client := backup.NewArrClient(a)
		if client == nil {
			continue
		}
		src := backup.ArrSource{Name: a.Name, Client: client}
		var prefixes []string
		for arrPath := range maps[a.Name] {
			prefixes = append(prefixes, arrPath)
		}
		sort.Strings(prefixes)
		for _, p := range prefixes {
			src.PathMap = append(src.PathMap, backup.PathMapping{Arr: p, Local: maps[a.Name][p]})
		}
		out = append(out, src)
	}
	return out
}

func (m *Manager) backupRescan(ctx context.Context, arrName string, itemID int) error {
	if m.arr == nil {
		return nil
	}
	a := m.arr.Get(arrName)
	if a == nil {
		return nil
	}
	if c := backup.NewArrClient(a); c != nil {
		return c.Rescan(ctx, itemID)
	}
	return nil
}

// backupEntryLookup answers the link classifier from the database alone.
type backupEntryLookup struct{ st *storage.Storage }

func (l backupEntryLookup) EntryExists(entry string) bool {
	_, err := l.st.GetEntryItem(entry)
	return err == nil
}

func (l backupEntryLookup) FileInEntry(entry, file string) bool {
	item, err := l.st.GetEntryItem(entry)
	if err != nil {
		return false
	}
	_, err = item.GetFile(file)
	return err == nil
}

// EntryID is the info hash of the entry's first file by name.
func (l backupEntryLookup) EntryID(entry string) string {
	item, err := l.st.GetEntryItem(entry)
	if err != nil {
		return ""
	}
	names := make([]string, 0, len(item.Files))
	for n, f := range item.Files {
		if !f.Deleted && f.InfoHash != "" {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return item.Files[names[0]].InfoHash
}

func (l backupEntryLookup) EntryByID(id string) (string, bool) {
	var found string
	_ = l.st.ForEachEntryItem(func(it *storage.EntryItem) error {
		if found != "" {
			return nil
		}
		for _, f := range it.Files {
			if !f.Deleted && f.InfoHash == id {
				found = it.Name
				break
			}
		}
		return nil
	})
	return found, found != ""
}
