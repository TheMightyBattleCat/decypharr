//go:build windows

package backup

// diskInfo is not measured on Windows.
func diskInfo(dir, dataDir string) (free int64, same bool) { return 0, false }
