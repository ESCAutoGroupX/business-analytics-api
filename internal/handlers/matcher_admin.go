package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/matcher"
)

// MatcherAdminHandler owns POST /admin/matcher/preview and
// GET /admin/matcher/preview/status. Preview mode only — never writes
// into wf_transaction_matches or wf_matcher_runs. All proposed matches
// land in /tmp/matcher-preview-<run_id>.log for human review.
type MatcherAdminHandler struct {
	GormDB *gorm.DB

	// In-memory run registry. Preview state is intentionally not
	// persisted — this whole flow is a review-the-log workflow.
	mu   sync.Mutex
	runs map[string]*previewRun
}

type previewRun struct {
	RunID       string                   `json:"run_id"`
	Status      string                   `json:"status"` // 'running' | 'success' | 'failed'
	LogPath     string                   `json:"log_path"`
	Stats       *matcher.RunStats        `json:"stats"`
	Sample      []matcher.MatchProposal  `json:"sample_proposals,omitempty"`
	StartedAt   time.Time                `json:"started_at"`
	FinishedAt  *time.Time               `json:"finished_at,omitempty"`
	Error       string                   `json:"error,omitempty"`
}

func NewMatcherAdminHandler(db *gorm.DB) *MatcherAdminHandler {
	return &MatcherAdminHandler{
		GormDB: db,
		runs:   map[string]*previewRun{},
	}
}

type previewBody struct {
	Limit         int   `json:"limit"`
	OnlyUnmatched *bool `json:"only_unmatched"` // pointer so "omitted" ≠ "explicitly false"
}

// POST /admin/matcher/preview
func (h *MatcherAdminHandler) StartPreview(c *gin.Context) {
	var body previewBody
	if err := c.ShouldBindJSON(&body); err != nil {
		body = previewBody{}
	}
	// Default OnlyUnmatched=true unless the client explicitly sent false.
	onlyUnmatched := true
	if body.OnlyUnmatched != nil {
		onlyUnmatched = *body.OnlyUnmatched
	}

	runID := time.Now().UTC().Format("20060102T150405Z")
	logPath := fmt.Sprintf("/tmp/matcher-preview-%s.log", runID)

	h.mu.Lock()
	h.runs[runID] = &previewRun{
		RunID:     runID,
		Status:    "running",
		LogPath:   logPath,
		StartedAt: time.Now().UTC(),
	}
	h.mu.Unlock()

	go h.runPreview(runID, matcher.PreviewOpts{
		Limit:         body.Limit,
		OnlyUnmatched: onlyUnmatched,
		LogPath:       logPath,
	})

	c.JSON(http.StatusAccepted, gin.H{
		"run_id":   runID,
		"log_path": logPath,
		"started":  true,
	})
}

func (h *MatcherAdminHandler) runPreview(runID string, opts matcher.PreviewOpts) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("matcher preview %s: panic: %v", runID, r)
			h.mu.Lock()
			if state, ok := h.runs[runID]; ok {
				state.Status = "failed"
				state.Error = fmt.Sprintf("panic: %v", r)
				now := time.Now().UTC()
				state.FinishedAt = &now
			}
			h.mu.Unlock()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	stats, proposals, err := matcher.RunPreview(ctx, h.GormDB, opts)
	now := time.Now().UTC()

	h.mu.Lock()
	state, ok := h.runs[runID]
	if !ok {
		h.mu.Unlock()
		return
	}
	state.Stats = stats
	state.FinishedAt = &now
	if err != nil {
		state.Status = "failed"
		state.Error = err.Error()
	} else {
		state.Status = "success"
	}
	// Surface the first 5 proposals in the status response for quick eyeballing.
	n := len(proposals)
	if n > 5 {
		n = 5
	}
	state.Sample = make([]matcher.MatchProposal, n)
	copy(state.Sample, proposals[:n])
	h.mu.Unlock()

	log.Printf("matcher preview %s: done matched=%d suspect=%d ambiguous=%d unmatched=%d elapsed_ms=%d",
		runID, stats.MatchedCount, stats.SuspectCount, stats.AmbiguousCount, stats.UnmatchedCount, stats.ElapsedMs)
}

// GET /admin/matcher/preview/status?run_id=...
func (h *MatcherAdminHandler) PreviewStatus(c *gin.Context) {
	runID := c.Query("run_id")
	if runID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "run_id required"})
		return
	}
	h.mu.Lock()
	state, ok := h.runs[runID]
	h.mu.Unlock()
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown run_id"})
		return
	}
	c.JSON(http.StatusOK, state)
}
