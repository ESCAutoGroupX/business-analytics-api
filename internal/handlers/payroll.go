package handlers

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type PayrollHandler struct {
	GormDB *gorm.DB
}

type payrollCreateRequest struct {
	Date         string  `json:"date" binding:"required"`
	EmployeeName string  `json:"employee_name" binding:"required"`
	GLCode       string  `json:"gl_code" binding:"required"`
	Description  *string `json:"description"`
	GrossPay     float64 `json:"gross_pay" binding:"required"`
	Taxes        float64 `json:"taxes" binding:"required"`
	NetPay       float64 `json:"net_pay" binding:"required"`
	LocationID   int     `json:"location_id" binding:"required"`
}

type payrollUpdateRequest struct {
	Date         *string  `json:"date"`
	EmployeeName *string  `json:"employee_name"`
	GLCode       *string  `json:"gl_code"`
	Description  *string  `json:"description"`
	GrossPay     *float64 `json:"gross_pay"`
	Taxes        *float64 `json:"taxes"`
	NetPay       *float64 `json:"net_pay"`
	LocationID   *int     `json:"location_id"`
}

type payrollResponse struct {
	ID           int        `json:"id"`
	Date         string     `json:"date"`
	EmployeeName string     `json:"employee_name"`
	GLCode       string     `json:"gl_code"`
	Description  *string    `json:"description"`
	GrossPay     float64    `json:"gross_pay"`
	Taxes        float64    `json:"taxes"`
	NetPay       float64    `json:"net_pay"`
	LocationID   int        `json:"location_id"`
	LocationName *string    `json:"location_name"`
	CreatedAt    *time.Time `json:"created_at"`
	UpdatedAt    *time.Time `json:"updated_at"`
}

type adjustmentCreateRequest struct {
	Date         string   `json:"date" binding:"required"`
	GLCode       string   `json:"gl_code" binding:"required"`
	Description  *string  `json:"description"`
	Debit        *float64 `json:"debit"`
	Credit       *float64 `json:"credit"`
	LocationID   int      `json:"location_id" binding:"required"`
	Notes        *string  `json:"notes"`
	EmployeeName *string  `json:"employee_name"`
}

type adjustmentUpdateRequest struct {
	Date         *string  `json:"date"`
	GLCode       *string  `json:"gl_code"`
	Description  *string  `json:"description"`
	Debit        *float64 `json:"debit"`
	Credit       *float64 `json:"credit"`
	Notes        *string  `json:"notes"`
	EmployeeName *string  `json:"employee_name"`
	LocationID   *int     `json:"location_id"`
}

type adjustmentResponse struct {
	ID           int        `json:"id"`
	Date         string     `json:"date"`
	GLCode       string     `json:"gl_code"`
	Description  *string    `json:"description"`
	Debit        *float64   `json:"debit"`
	Credit       *float64   `json:"credit"`
	LocationID   int        `json:"location_id"`
	Notes        *string    `json:"notes"`
	EmployeeName *string    `json:"employee_name"`
	LocationName *string    `json:"location_name"`
	CreatedAt    *time.Time `json:"created_at"`
	UpdatedAt    *time.Time `json:"updated_at"`
}

func (h *PayrollHandler) getLocationName(locationID int) *string {
	var loc models.Location
	if err := h.GormDB.Select("location_name").First(&loc, "id = ?", locationID).Error; err != nil {
		return nil
	}
	return &loc.LocationName
}

func payrollToResponse(p *models.PayrollEntry, locName *string) payrollResponse {
	return payrollResponse{
		ID:           p.ID,
		Date:         p.Date,
		EmployeeName: p.EmployeeName,
		GLCode:       p.GLCode,
		Description:  p.Description,
		GrossPay:     p.GrossPay,
		Taxes:        p.Taxes,
		NetPay:       p.NetPay,
		LocationID:   p.LocationID,
		LocationName: locName,
		CreatedAt:    &p.CreatedAt,
		UpdatedAt:    &p.UpdatedAt,
	}
}

func adjToResponse(a *models.PayrollAdjustment, locName *string) adjustmentResponse {
	locID := 0
	if a.LocationID != nil {
		locID = *a.LocationID
	}
	return adjustmentResponse{
		ID:           a.ID,
		Date:         a.Date,
		GLCode:       a.GLCode,
		Description:  a.Description,
		Debit:        a.Debit,
		Credit:       a.Credit,
		LocationID:   locID,
		Notes:        a.Notes,
		EmployeeName: a.EmployeeName,
		LocationName: locName,
		CreatedAt:    &a.CreatedAt,
		UpdatedAt:    &a.UpdatedAt,
	}
}

// POST /payroll/
func (h *PayrollHandler) CreatePayroll(c *gin.Context) {
	var req payrollCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate location
	var count int64
	h.GormDB.Model(&models.Location{}).Where("id = ?", req.LocationID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	entry := models.PayrollEntry{
		Date:         req.Date,
		EmployeeName: req.EmployeeName,
		GLCode:       req.GLCode,
		Description:  req.Description,
		GrossPay:     req.GrossPay,
		Taxes:        req.Taxes,
		NetPay:       req.NetPay,
		LocationID:   req.LocationID,
		UserID:       &uid,
	}

	if err := h.GormDB.Create(&entry).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create payroll", "error": err.Error()})
		return
	}

	h.getPayrollByID(c, entry.ID)
}

// GET /payroll/
func (h *PayrollHandler) GetAllPayrolls(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	query := h.GormDB.Model(&models.PayrollEntry{})
	if !strings.EqualFold(roleStr, "admin") {
		query = query.Where("user_id = ?", uid)
	}

	var entries []models.PayrollEntry
	if err := query.Offset(skip).Limit(limit).Find(&entries).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payrolls", "error": err.Error()})
		return
	}

	result := make([]payrollResponse, len(entries))
	for i := range entries {
		result[i] = payrollToResponse(&entries[i], h.getLocationName(entries[i].LocationID))
	}

	c.JSON(http.StatusOK, result)
}

// GET /payroll/:payroll_id
func (h *PayrollHandler) GetPayroll(c *gin.Context) {
	payrollID, err := strconv.Atoi(c.Param("payroll_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid payroll_id"})
		return
	}
	h.getPayrollByID(c, payrollID)
}

// PATCH /payroll/:payroll_id
func (h *PayrollHandler) UpdatePayroll(c *gin.Context) {
	payrollID, err := strconv.Atoi(c.Param("payroll_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid payroll_id"})
		return
	}

	var entry models.PayrollEntry
	if err := h.GormDB.First(&entry, "id = ?", payrollID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}

	var req payrollUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	if req.Date == nil && req.EmployeeName == nil && req.GLCode == nil && req.Description == nil &&
		req.GrossPay == nil && req.Taxes == nil && req.NetPay == nil && req.LocationID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "At least one field must be provided for update"})
		return
	}

	if req.LocationID != nil {
		var count int64
		h.GormDB.Model(&models.Location{}).Where("id = ?", *req.LocationID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
			return
		}
	}

	updates := map[string]interface{}{}
	if req.Date != nil {
		updates["date"] = *req.Date
	}
	if req.EmployeeName != nil {
		updates["employee_name"] = *req.EmployeeName
	}
	if req.GLCode != nil {
		updates["gl_code"] = *req.GLCode
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.GrossPay != nil {
		updates["gross_pay"] = *req.GrossPay
	}
	if req.Taxes != nil {
		updates["taxes"] = *req.Taxes
	}
	if req.NetPay != nil {
		updates["net_pay"] = *req.NetPay
	}
	if req.LocationID != nil {
		updates["location_id"] = *req.LocationID
	}

	updates["updated_at"] = time.Now().UTC()
	if err := h.GormDB.Model(&entry).Updates(updates).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update payroll", "error": err.Error()})
		return
	}

	h.getPayrollByID(c, payrollID)
}

// DELETE /payroll/:payroll_id
func (h *PayrollHandler) DeletePayroll(c *gin.Context) {
	payrollID, err := strconv.Atoi(c.Param("payroll_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid payroll_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var entry models.PayrollEntry
	if err := h.GormDB.First(&entry, "id = ?", payrollID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}

	if !strings.EqualFold(roleStr, "admin") && (entry.UserID == nil || *entry.UserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to delete this payroll entry"})
		return
	}

	h.GormDB.Delete(&entry)
	c.JSON(http.StatusOK, gin.H{"message": "Payroll deleted successfully!"})
}

// POST /payroll/adjustments
func (h *PayrollHandler) CreateAdjustment(c *gin.Context) {
	var req adjustmentCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var count int64
	h.GormDB.Model(&models.Location{}).Where("id = ?", req.LocationID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	adj := models.PayrollAdjustment{
		Date:         req.Date,
		GLCode:       req.GLCode,
		Description:  req.Description,
		Debit:        req.Debit,
		Credit:       req.Credit,
		LocationID:   &req.LocationID,
		UserID:       &uid,
		Notes:        req.Notes,
		EmployeeName: req.EmployeeName,
	}

	if err := h.GormDB.Create(&adj).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create adjustment", "error": err.Error()})
		return
	}

	h.getAdjustmentByID(c, adj.ID)
}

// GET /payroll/adjustments
func (h *PayrollHandler) ListAdjustments(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	query := h.GormDB.Model(&models.PayrollAdjustment{})
	if !strings.EqualFold(roleStr, "admin") {
		query = query.Where("user_id = ?", uid)
	}

	var adjustments []models.PayrollAdjustment
	if err := query.Offset(skip).Limit(limit).Find(&adjustments).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query adjustments", "error": err.Error()})
		return
	}

	result := make([]adjustmentResponse, len(adjustments))
	for i := range adjustments {
		locID := 0
		if adjustments[i].LocationID != nil {
			locID = *adjustments[i].LocationID
		}
		result[i] = adjToResponse(&adjustments[i], h.getLocationName(locID))
	}

	c.JSON(http.StatusOK, result)
}

// GET /payroll/adjustments/:adjustment_id
func (h *PayrollHandler) GetAdjustment(c *gin.Context) {
	adjustmentID, err := strconv.Atoi(c.Param("adjustment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid adjustment_id"})
		return
	}
	h.getAdjustmentByID(c, adjustmentID)
}

// PATCH /payroll/adjustments/:adjustment_id
func (h *PayrollHandler) UpdateAdjustment(c *gin.Context) {
	adjustmentID, err := strconv.Atoi(c.Param("adjustment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid adjustment_id"})
		return
	}

	var adj models.PayrollAdjustment
	if err := h.GormDB.First(&adj, "id = ?", adjustmentID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayrollAccountAdjustment not found"})
		return
	}

	var req adjustmentUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	if req.Date == nil && req.GLCode == nil && req.Description == nil && req.Debit == nil &&
		req.Credit == nil && req.Notes == nil && req.EmployeeName == nil && req.LocationID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "At least one field must be provided for update"})
		return
	}

	if req.LocationID != nil {
		var count int64
		h.GormDB.Model(&models.Location{}).Where("id = ?", *req.LocationID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
			return
		}
	}

	updates := map[string]interface{}{}
	if req.Date != nil {
		updates["date"] = *req.Date
	}
	if req.GLCode != nil {
		updates["gl_code"] = *req.GLCode
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.Debit != nil {
		updates["debit"] = *req.Debit
	}
	if req.Credit != nil {
		updates["credit"] = *req.Credit
	}
	if req.Notes != nil {
		updates["notes"] = *req.Notes
	}
	if req.EmployeeName != nil {
		updates["employee_name"] = *req.EmployeeName
	}
	if req.LocationID != nil {
		updates["location_id"] = *req.LocationID
	}

	updates["updated_at"] = time.Now().UTC()
	if err := h.GormDB.Model(&adj).Updates(updates).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update adjustment", "error": err.Error()})
		return
	}

	h.getAdjustmentByID(c, adjustmentID)
}

// DELETE /payroll/adjustments/:adjustment_id
func (h *PayrollHandler) DeleteAdjustment(c *gin.Context) {
	adjustmentID, err := strconv.Atoi(c.Param("adjustment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid adjustment_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var adj models.PayrollAdjustment
	if err := h.GormDB.First(&adj, "id = ?", adjustmentID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayrollAccountAdjustment not found"})
		return
	}

	if !strings.EqualFold(roleStr, "admin") && (adj.UserID == nil || *adj.UserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to delete this payroll adjustment"})
		return
	}

	h.GormDB.Delete(&adj)
	c.JSON(http.StatusOK, gin.H{"message": "Payroll adjustment deleted successfully!"})
}

func (h *PayrollHandler) getPayrollByID(c *gin.Context, id int) {
	var entry models.PayrollEntry
	if err := h.GormDB.First(&entry, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}

	c.JSON(http.StatusOK, payrollToResponse(&entry, h.getLocationName(entry.LocationID)))
}

func (h *PayrollHandler) getAdjustmentByID(c *gin.Context, id int) {
	var adj models.PayrollAdjustment
	if err := h.GormDB.First(&adj, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayrollAccountAdjustment not found"})
		return
	}

	locID := 0
	if adj.LocationID != nil {
		locID = *adj.LocationID
	}
	c.JSON(http.StatusOK, adjToResponse(&adj, h.getLocationName(locID)))
}
