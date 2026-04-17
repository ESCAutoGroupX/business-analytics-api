package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type DashboardLayoutHandler struct {
	GormDB *gorm.DB
}

type dashboardLayoutPutRequest struct {
	Layout   json.RawMessage `json:"layout" binding:"required"`
	IsLocked bool            `json:"is_locked"`
}

// GET /dashboard-layouts/:page
func (h *DashboardLayoutHandler) Get(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	if uid == "" || uid == "<nil>" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing user"})
		return
	}
	page := c.Param("page")
	if page == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "page required"})
		return
	}

	var row models.UserDashboardLayout
	if err := h.GormDB.Where("user_id = ? AND page = ?", uid, page).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusOK, nil)
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, row)
}

// PUT /dashboard-layouts/:page
func (h *DashboardLayoutHandler) Put(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	if uid == "" || uid == "<nil>" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing user"})
		return
	}
	page := c.Param("page")
	if page == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "page required"})
		return
	}

	var req dashboardLayoutPutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}
	if len(req.Layout) == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "layout required"})
		return
	}

	var row models.UserDashboardLayout
	err := h.GormDB.Where("user_id = ? AND page = ?", uid, page).First(&row).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	row.UserID = uid
	row.Page = page
	row.Layout = models.JSONB(req.Layout)
	row.IsLocked = req.IsLocked

	if errors.Is(err, gorm.ErrRecordNotFound) {
		if err := h.GormDB.Create(&row).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	} else {
		if err := h.GormDB.Save(&row).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, row)
}

// DELETE /dashboard-layouts/:page
func (h *DashboardLayoutHandler) Delete(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	if uid == "" || uid == "<nil>" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing user"})
		return
	}
	page := c.Param("page")
	if page == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "page required"})
		return
	}

	if err := h.GormDB.Where("user_id = ? AND page = ?", uid, page).Delete(&models.UserDashboardLayout{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true})
}
