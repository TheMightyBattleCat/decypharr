package arr

import "os"

type Movie struct {
	Title         string `json:"title"`
	OriginalTitle string `json:"originalTitle"`
	Path          string `json:"path"`
	Runtime       int    `json:"runtime"` // minutes
	MovieFile     struct {
		MovieId      int    `json:"movieId"`
		RelativePath string `json:"relativePath"`
		Path         string `json:"path"`
		Id           int    `json:"id"`
		Size         int64  `json:"size"`
	} `json:"movieFile"`
	Id int `json:"id"`
}

type ContentFile struct {
	Name         string `json:"name"`
	Path         string `json:"path"`
	Id           int    `json:"id"`
	EpisodeId    int    `json:"showId"`
	FileId       int    `json:"fileId"`
	TargetPath   string `json:"targetPath"`
	EntryName    string `json:"entryName,omitempty"`
	IsSymlink    bool   `json:"isSymlink"`
	IsBroken     bool   `json:"isBroken"`
	SeasonNumber int    `json:"seasonNumber"`
	// EpisodeNumber is the Sonarr episode number within SeasonNumber (0 for
	// movies, or when Sonarr has no episode mapped to this file at all - see
	// EpisodeCountConfirmed). For a multi-episode file this is the FIRST
	// episode's number.
	EpisodeNumber int   `json:"episodeNumber,omitempty"`
	Processed     bool  `json:"processed"`
	Size          int64 `json:"size"`

	// RuntimeSec is this file's expected playback duration in seconds, sourced
	// from the Arr (movie runtime, or the summed/derived runtime of the
	// episode(s) the file contains). 0 means unknown - callers should fall
	// back to a ceiling-only sanity check rather than a ratio comparison.
	RuntimeSec int `json:"runtimeSec,omitempty"`
	// EpisodeCount is the number of episodes this file is believed to contain
	// (1 for movies and single episodes, >1 for multi-episode files).
	EpisodeCount int `json:"episodeCount,omitempty"`
	// EpisodeCountConfirmed is true when EpisodeCount came from an actual
	// Arr episode mapping rather than a filename guess (e.g. an "E01-E02"
	// span parse). Callers should require a wider safety margin when false.
	EpisodeCountConfirmed bool `json:"episodeCountConfirmed,omitempty"`
}

func (file *ContentFile) Delete() {
	// This is useful for when sonarr bulk delete fails(this usually happens)
	// and we need to delete the file manually
	_ = os.Remove(file.Path) //nolint:nolintlint
}

type Content struct {
	Title string        `json:"title"`
	Id    int           `json:"id"`
	Files []ContentFile `json:"files"`
}

// NextEpisodeInfo describes the episode immediately after a given
// (season, episode) in a series - see Arr.NextEpisode.
type NextEpisodeInfo struct {
	EpisodeId     int // Sonarr's episode row id, for Arr.SearchEpisode
	SeasonNumber  int
	EpisodeNumber int
	HasFile       bool
	FileId        int
	Path          string
	Size          int64
}

type seriesFile struct {
	SeriesId     int    `json:"seriesId"`
	SeasonNumber int    `json:"seasonNumber"`
	Path         string `json:"path"`
	Id           int    `json:"id"`
	Size         int64  `json:"size"`
}
