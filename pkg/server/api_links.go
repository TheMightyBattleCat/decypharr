package server

import (
	"net/http"
	"strconv"

	"github.com/sirrobot01/decypharr/pkg/backup"
)

func (s *Server) handleLinksStatus(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().LinksStatus(r.Context())
	s.backupReply(w, res, err)
}

func (s *Server) handleLinksCheck(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().LinksCheck(r.Context())
	s.backupReply(w, res, err)
}

func (s *Server) handleLinksList(w http.ResponseWriter, r *http.Request) {
	q := backup.LinksQuery{
		State: backup.LinkState(r.URL.Query().Get("state")),
		Root:  r.URL.Query().Get("root"),
		Q:     r.URL.Query().Get("q"),
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v > 0 {
		q.Page = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("page_size")); err == nil && v > 0 {
		q.PageSize = v
	}
	res, err := s.backupService().ListLinks(r.Context(), q)
	s.backupReply(w, res, err)
}

func (s *Server) handleLinksMissing(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().LinksMissing(r.Context())
	if err == nil && res == nil {
		res = []backup.MissingLink{}
	}
	s.backupReply(w, res, err)
}

func (s *Server) handleLinksCleanupPlan(w http.ResponseWriter, r *http.Request) {
	var req backup.LinksCleanupPlanRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	res, err := s.backupService().PlanLinksCleanup(r.Context(), req)
	s.backupReply(w, res, err)
}

func (s *Server) handleLinksCleanupApply(w http.ResponseWriter, r *http.Request) {
	var req backup.LinksCleanupApplyRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	// Like the other risky actions, a cleanup is preceded by a snapshot.
	if s.preActionRefused(w, r, backup.ActionLinksCleanup) {
		return
	}
	res, err := s.backupService().ApplyLinksCleanup(r.Context(), req)
	s.backupReply(w, res, err)
}
