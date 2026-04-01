package handlers

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type AccountingHandler struct {
	GormDB *gorm.DB
}

type accountCreateRequest struct {
	Code              string  `json:"code" binding:"required"`
	Description       string  `json:"description" binding:"required"`
	ParentID          *string `json:"parent_id"`
	Name              *string `json:"name"`
	AccountType       *string `json:"account_type"`
	IsActive          *bool   `json:"is_active"`
	LocationID        *int    `json:"location_id"`
	IsPartsVendor     *string `json:"is_parts_vendor"`
	IsCogsVendor      *bool   `json:"is_cogs_vendor"`
	IsStatementVendor *bool   `json:"is_statement_vendor"`
}

type accountUpdateRequest struct {
	Code              *string `json:"code"`
	Description       *string `json:"description"`
	ParentID          *string `json:"parent_id"`
	Name              *string `json:"name"`
	AccountType       *string `json:"account_type"`
	IsActive          *bool   `json:"is_active"`
	LocationID        *int    `json:"location_id"`
	IsPartsVendor     *string `json:"is_parts_vendor"`
	IsCogsVendor      *bool   `json:"is_cogs_vendor"`
	IsStatementVendor *bool   `json:"is_statement_vendor"`
}

type accountResponse struct {
	ID                string      `json:"id"`
	Code              string      `json:"code"`
	Description       string      `json:"description"`
	ParentID          *string     `json:"parent_id"`
	Name              *string     `json:"name"`
	AccountType       *string     `json:"account_type"`
	IsActive          *bool       `json:"is_active"`
	CreatedAt         time.Time   `json:"created_at"`
	UpdatedAt         time.Time   `json:"updated_at"`
	Location          interface{} `json:"location"`
	IsPartsVendor     *string     `json:"is_parts_vendor"`
	IsCogsVendor      *bool       `json:"is_cogs_vendor"`
	IsStatementVendor *bool       `json:"is_statement_vendor"`
}

var validAccountTypes = map[string]bool{
	"asset": true, "liability": true, "equity": true, "revenue": true,
	"expense": true, "bank": true, "accounts_receivable": true,
	"current_asset": true, "non_current_asset": true, "fixed_asset": true,
	"accounts_payable": true, "current_liability": true,
	"non_current_liability": true, "retained_earnings": true,
	"direct_costs": true, "overhead": true, "other_income": true,
	"sales_tax": true, "unpaid_expense_claims": true, "tracking": true,
	"historical_adjustment": true, "rounding": true,
}

func validAccountTypesList() []string {
	keys := make([]string, 0, len(validAccountTypes))
	for k := range validAccountTypes {
		keys = append(keys, k)
	}
	return keys
}

func acctToResponse(a *models.ChartOfAccount) accountResponse {
	resp := accountResponse{
		ID:                a.ID,
		Code:              a.Code,
		ParentID:          a.ParentID,
		Name:              a.Name,
		AccountType:       a.AccountType,
		IsActive:          a.IsActive,
		IsPartsVendor:     a.IsPartsVendor,
		IsCogsVendor:      a.IsCogsVendor,
		IsStatementVendor: a.IsStatementVendor,
		CreatedAt:         a.CreatedAt,
		UpdatedAt:         a.UpdatedAt,
	}
	if a.Description != nil {
		resp.Description = *a.Description
	}
	if a.Location != nil {
		resp.Location = gin.H{
			"id": a.Location.ID, "location_name": a.Location.LocationName,
			"address_line_1": a.Location.AddressLine1, "address_line_2": a.Location.AddressLine2,
			"city": a.Location.City, "state_province": a.Location.StateProvince,
			"postal_code": a.Location.PostalCode, "country": a.Location.Country,
			"shop_id": a.Location.ShopID,
			"created_at": a.Location.CreatedAt, "updated_at": a.Location.UpdatedAt,
		}
	}
	return resp
}

func (h *AccountingHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if !strings.EqualFold(fmt.Sprintf("%v", role), "admin") {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Admin access required"})
		return false
	}
	return true
}

// POST /accounting/
func (h *AccountingHandler) CreateAccount(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	var req accountCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check duplicate code
	var count int64
	h.GormDB.Model(&models.ChartOfAccount{}).Where("code = ?", req.Code).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Account with code '%s' already exists.", req.Code)})
		return
	}

	// Validate parent_id
	if req.ParentID != nil && *req.ParentID != "" {
		h.GormDB.Model(&models.ChartOfAccount{}).Where("id = ?", *req.ParentID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Parent account %s does not exist.", *req.ParentID)})
			return
		}
	}

	// Validate account_type
	accountType := "revenue"
	if req.AccountType != nil && *req.AccountType != "" {
		at := strings.ToLower(*req.AccountType)
		if !validAccountTypes[at] {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid account type: %s. Valid types are: %v", *req.AccountType, validAccountTypesList())})
			return
		}
		accountType = at
	}

	// Validate location_id
	if req.LocationID != nil {
		h.GormDB.Model(&models.Location{}).Where("id = ?", *req.LocationID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Location with id %d does not exist.", *req.LocationID)})
			return
		}
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}
	isPartsVendor := "NEVER"
	if req.IsPartsVendor != nil {
		isPartsVendor = *req.IsPartsVendor
	}
	isCogsVendor := false
	if req.IsCogsVendor != nil {
		isCogsVendor = *req.IsCogsVendor
	}
	isStatementVendor := false
	if req.IsStatementVendor != nil {
		isStatementVendor = *req.IsStatementVendor
	}

	desc := req.Description

	acct := models.ChartOfAccount{
		ID:                uuid.New().String(),
		Code:              req.Code,
		Description:       &desc,
		ParentID:          req.ParentID,
		Name:              req.Name,
		AccountType:       &accountType,
		IsActive:          &isActive,
		LocationID:        req.LocationID,
		IsPartsVendor:     &isPartsVendor,
		IsCogsVendor:      &isCogsVendor,
		IsStatementVendor: &isStatementVendor,
	}

	if err := h.GormDB.Create(&acct).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create account", "error": err.Error()})
		return
	}

	h.getAccountByID(c, acct.ID)
}

// GET /accounting/
func (h *AccountingHandler) ListAccounts(c *gin.Context) {
	accountType := c.Query("account_type")
	isActiveStr := c.DefaultQuery("is_active", "true")

	query := h.GormDB.Preload("Location")

	if accountType != "" {
		at := strings.ToLower(accountType)
		if !validAccountTypes[at] {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid account type: %s. Valid types are: %v", accountType, validAccountTypesList())})
			return
		}
		query = query.Where("account_type = ?", at)
	}

	if isActiveStr == "true" {
		query = query.Where("is_active = ?", true)
	} else if isActiveStr == "false" {
		query = query.Where("is_active = ?", false)
	}

	var accounts []models.ChartOfAccount
	if err := query.Find(&accounts).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query accounts", "error": err.Error()})
		return
	}

	result := make([]accountResponse, len(accounts))
	for i := range accounts {
		result[i] = acctToResponse(&accounts[i])
	}

	c.JSON(http.StatusOK, result)
}

// GET /accounting/:account_id
func (h *AccountingHandler) GetAccount(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}
	accountID := c.Param("account_id")
	h.getAccountByID(c, accountID)
}

// PATCH /accounting/:account_id
func (h *AccountingHandler) UpdateAccount(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	accountID := c.Param("account_id")

	var acct models.ChartOfAccount
	if err := h.GormDB.First(&acct, "id = ?", accountID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Account not found"})
		return
	}

	var req accountUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate code uniqueness
	if req.Code != nil {
		var count int64
		h.GormDB.Model(&models.ChartOfAccount{}).Where("code = ? AND id != ?", *req.Code, accountID).Count(&count)
		if count > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Account with code '%s' already exists.", *req.Code)})
			return
		}
	}

	// Validate parent_id
	if req.ParentID != nil && *req.ParentID != "" {
		var count int64
		h.GormDB.Model(&models.ChartOfAccount{}).Where("id = ?", *req.ParentID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Parent account %s does not exist.", *req.ParentID)})
			return
		}
	}

	// Validate account_type
	if req.AccountType != nil && *req.AccountType != "" {
		at := strings.ToLower(*req.AccountType)
		if !validAccountTypes[at] {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid account type: %s. Valid types are: %v", *req.AccountType, validAccountTypesList())})
			return
		}
		req.AccountType = &at
	}

	// Validate location_id
	if req.LocationID != nil {
		var count int64
		h.GormDB.Model(&models.Location{}).Where("id = ?", *req.LocationID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Location with id %d does not exist.", *req.LocationID)})
			return
		}
	}

	updates := map[string]interface{}{}
	if req.Code != nil {
		updates["code"] = *req.Code
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.ParentID != nil {
		updates["parent_id"] = *req.ParentID
	}
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.AccountType != nil {
		updates["account_type"] = *req.AccountType
	}
	if req.IsActive != nil {
		updates["is_active"] = *req.IsActive
	}
	if req.LocationID != nil {
		updates["location_id"] = *req.LocationID
	}
	if req.IsPartsVendor != nil {
		updates["is_parts_vendor"] = *req.IsPartsVendor
	}
	if req.IsCogsVendor != nil {
		updates["is_cogs_vendor"] = *req.IsCogsVendor
	}
	if req.IsStatementVendor != nil {
		updates["is_statement_vendor"] = *req.IsStatementVendor
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&acct).Updates(updates).Error; err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update account", "error": err.Error()})
			return
		}
	}

	h.getAccountByID(c, accountID)
}

// DELETE /accounting/:account_id
func (h *AccountingHandler) DeleteAccount(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	accountID := c.Param("account_id")

	result := h.GormDB.Delete(&models.ChartOfAccount{}, "id = ?", accountID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete account", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Account not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "GL code deleted successfully!"})
}

// POST /accounting/import-accounts/
func (h *AccountingHandler) ImportAccounts(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}

	if !strings.HasSuffix(strings.ToLower(file.Filename), ".csv") {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Only CSV files are allowed."})
		return
	}

	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to open file"})
		return
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to read file"})
		return
	}

	reader := csv.NewReader(bytes.NewReader(content))
	records, err := reader.ReadAll()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to parse CSV"})
		return
	}

	if len(records) < 2 {
		c.JSON(http.StatusOK, gin.H{"status": "success", "inserted": 0, "errors": []interface{}{}})
		return
	}

	// Find column indices from header
	header := records[0]
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	inserted := 0
	importErrors := []map[string]interface{}{}

	for _, row := range records[1:] {
		code := ""
		name := ""
		typeRaw := ""
		description := ""

		if idx, ok := colIdx["*Code"]; ok && idx < len(row) {
			code = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["*Name"]; ok && idx < len(row) {
			name = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["*Type"]; ok && idx < len(row) {
			typeRaw = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["Description"]; ok && idx < len(row) {
			description = strings.TrimSpace(row[idx])
		}

		if code == "" || name == "" || typeRaw == "" {
			importErrors = append(importErrors, map[string]interface{}{"row": row, "error": "Missing required fields."})
			continue
		}

		typeStr := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(typeRaw), " ", "_"), "-", "_")
		if !validAccountTypes[typeStr] {
			importErrors = append(importErrors, map[string]interface{}{"row": row, "error": fmt.Sprintf("Invalid account type: '%s' (normalized: '%s')", typeRaw, typeStr)})
			continue
		}

		// Check if already exists
		var count int64
		h.GormDB.Model(&models.ChartOfAccount{}).Where("code = ?", code).Count(&count)
		if count > 0 {
			continue
		}

		if description == "" {
			description = name
		}

		isActive := true
		acct := models.ChartOfAccount{
			ID:          uuid.New().String(),
			Code:        code,
			Name:        &name,
			AccountType: &typeStr,
			Description: &description,
			IsActive:    &isActive,
		}

		if err := h.GormDB.Create(&acct).Error; err != nil {
			importErrors = append(importErrors, map[string]interface{}{"row": row, "error": err.Error()})
			continue
		}
		inserted++
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "inserted": inserted, "errors": importErrors})
}

func (h *AccountingHandler) getAccountByID(c *gin.Context, id string) {
	var acct models.ChartOfAccount
	if err := h.GormDB.Preload("Location").First(&acct, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Account with ID %s not found", id)})
		return
	}

	c.JSON(http.StatusOK, acctToResponse(&acct))
}
