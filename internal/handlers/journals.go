package handlers

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type JournalHandler struct {
	GormDB *gorm.DB
}

type journalLineRequest struct {
	AccountCode string  `json:"AccountCode" binding:"required"`
	Description string  `json:"Description"`
	LineAmount  float64 `json:"LineAmount"`
}

type journalRequest struct {
	Reference    string               `json:"reference"`
	Date         string               `json:"date" binding:"required"`
	JournalLines []journalLineRequest `json:"journal_lines" binding:"required"`
}

func validateJournalLines(lines []journalLineRequest) error {
	if len(lines) < 2 {
		return fmt.Errorf("at least 2 line items required")
	}
	sum := 0.0
	for i, l := range lines {
		if l.AccountCode == "" {
			return fmt.Errorf("line %d: AccountCode is required", i+1)
		}
		sum += l.LineAmount
	}
	if math.Abs(sum) > 0.01 {
		return fmt.Errorf("journal does not balance: sum is %.2f (must be 0)", sum)
	}
	return nil
}

// POST /xero/journals
func (h *JournalHandler) CreateJournal(c *gin.Context) {
	var req journalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	if err := validateJournalLines(req.JournalLines); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	journalDate, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid date format, use YYYY-MM-DD"})
		return
	}

	linesJSON, _ := json.Marshal(req.JournalLines)
	sourceType := "MANJOURNAL"
	ref := req.Reference

	journal := models.XeroJournal{
		XeroID:      "manual-" + uuid.New().String(),
		TenantID:    "",
		JournalDate: &journalDate,
		SourceType:  &sourceType,
		Reference:   &ref,
		JournalLines: models.JSONB(linesJSON),
		CreatedDateUTC: func() *time.Time { t := time.Now(); return &t }(),
	}

	if err := h.GormDB.Create(&journal).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create journal: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, journal)
}

// PATCH /xero/journals/:id
func (h *JournalHandler) UpdateJournal(c *gin.Context) {
	id := c.Param("id")

	var existing models.XeroJournal
	if err := h.GormDB.First(&existing, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "journal not found"})
		return
	}

	if existing.SourceType == nil || *existing.SourceType != "MANJOURNAL" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "only manual journal entries can be edited"})
		return
	}

	var req journalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	if err := validateJournalLines(req.JournalLines); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	journalDate, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid date format, use YYYY-MM-DD"})
		return
	}

	linesJSON, _ := json.Marshal(req.JournalLines)

	updates := map[string]interface{}{
		"journal_date":  &journalDate,
		"reference":     &req.Reference,
		"journal_lines": models.JSONB(linesJSON),
	}

	if err := h.GormDB.Model(&existing).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update journal"})
		return
	}

	h.GormDB.First(&existing, id)
	c.JSON(http.StatusOK, existing)
}

// DELETE /xero/journals/:id
func (h *JournalHandler) DeleteJournal(c *gin.Context) {
	id := c.Param("id")

	var existing models.XeroJournal
	if err := h.GormDB.First(&existing, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "journal not found"})
		return
	}

	if existing.SourceType == nil || *existing.SourceType != "MANJOURNAL" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "only manual journal entries can be deleted"})
		return
	}

	if err := h.GormDB.Delete(&existing).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete journal"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true})
}
