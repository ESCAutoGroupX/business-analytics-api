package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type CardAssignmentHandler struct {
	GormDB *gorm.DB
}

type cardAssignmentRequest struct {
	CardLast4      string `json:"card_last4" binding:"required"`
	CardholderName string `json:"cardholder_name"`
	LocationName   string `json:"location_name" binding:"required"`
	PlaidAccountID string `json:"plaid_account_id"`
}

// GET /card-assignments
func (h *CardAssignmentHandler) ListCardAssignments(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var assignments []models.CardLocationAssignment
	if err := h.GormDB.Where("user_id = ?", uid).Order("created_at DESC").Find(&assignments).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query card assignments"})
		return
	}

	c.JSON(http.StatusOK, assignments)
}

// POST /card-assignments
func (h *CardAssignmentHandler) CreateCardAssignment(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var req cardAssignmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	assignment := models.CardLocationAssignment{
		UserID:         uid,
		CardLast4:      req.CardLast4,
		CardholderName: req.CardholderName,
		LocationName:   req.LocationName,
		PlaidAccountID: req.PlaidAccountID,
	}

	if err := h.GormDB.Create(&assignment).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"detail": "card assignment already exists or failed to create", "error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, assignment)
}

// PUT /card-assignments/:id
func (h *CardAssignmentHandler) UpdateCardAssignment(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid id"})
		return
	}

	var assignment models.CardLocationAssignment
	if err := h.GormDB.Where("id = ? AND user_id = ?", id, uid).First(&assignment).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "card assignment not found"})
		return
	}

	var req cardAssignmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	assignment.CardLast4 = req.CardLast4
	assignment.CardholderName = req.CardholderName
	assignment.LocationName = req.LocationName
	assignment.PlaidAccountID = req.PlaidAccountID

	if err := h.GormDB.Save(&assignment).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update card assignment", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, assignment)
}

// DELETE /card-assignments/:id
func (h *CardAssignmentHandler) DeleteCardAssignment(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid id"})
		return
	}

	var assignment models.CardLocationAssignment
	if err := h.GormDB.Where("id = ? AND user_id = ?", id, uid).First(&assignment).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "card assignment not found"})
		return
	}

	if err := h.GormDB.Delete(&assignment).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete card assignment"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Card assignment deleted"})
}
