package handlers

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AccountingHandler struct {
	DB *pgxpool.Pool
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

func (h *AccountingHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if fmt.Sprintf("%v", role) != "Admin" {
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
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE code = $1)", req.Code).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Account with code '%s' already exists.", req.Code)})
		return
	}

	// Validate parent_id
	if req.ParentID != nil && *req.ParentID != "" {
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE id = $1)", *req.ParentID).Scan(&exists)
		if !exists {
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
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", *req.LocationID).Scan(&exists)
		if !exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Location with id %d does not exist.", *req.LocationID)})
			return
		}
	}

	id := uuid.New().String()
	now := time.Now().UTC()

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

	_, err := h.DB.Exec(context.Background(),
		`INSERT INTO chart_of_accounts (id, code, description, parent_id, name, account_type, is_active, location_id, is_parts_vendor, is_cogs_vendor, is_statement_vendor, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		id, req.Code, req.Description, req.ParentID, req.Name, accountType, isActive, req.LocationID,
		isPartsVendor, isCogsVendor, isStatementVendor, now, now,
	)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create account", "error": err.Error()})
		return
	}

	h.getAccountByID(c, id)
}

// GET /accounting/
func (h *AccountingHandler) ListAccounts(c *gin.Context) {
	accountType := c.Query("account_type")
	isActiveStr := c.DefaultQuery("is_active", "true")

	query := `SELECT id, code, description, parent_id, name, account_type, is_active, location_id, is_parts_vendor, is_cogs_vendor, is_statement_vendor, created_at, updated_at
	           FROM chart_of_accounts WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if accountType != "" {
		at := strings.ToLower(accountType)
		if !validAccountTypes[at] {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid account type: %s. Valid types are: %v", accountType, validAccountTypesList())})
			return
		}
		query += fmt.Sprintf(" AND account_type = $%d", argIdx)
		args = append(args, at)
		argIdx++
	}

	if isActiveStr == "true" {
		query += fmt.Sprintf(" AND is_active = $%d", argIdx)
		args = append(args, true)
		argIdx++
	} else if isActiveStr == "false" {
		query += fmt.Sprintf(" AND is_active = $%d", argIdx)
		args = append(args, false)
		argIdx++
	}

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query accounts", "error": err.Error()})
		return
	}
	defer rows.Close()

	accounts := []accountResponse{}
	for rows.Next() {
		a, err := h.scanAccount(rows)
		if err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan account", "error": err.Error()})
			return
		}
		accounts = append(accounts, a)
	}

	c.JSON(http.StatusOK, accounts)
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

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE id = $1)", accountID).Scan(&exists)
	if !exists {
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
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE code = $1 AND id != $2)", *req.Code, accountID).Scan(&exists)
		if exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Account with code '%s' already exists.", *req.Code)})
			return
		}
	}

	// Validate parent_id
	if req.ParentID != nil && *req.ParentID != "" {
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE id = $1)", *req.ParentID).Scan(&exists)
		if !exists {
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
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", *req.LocationID).Scan(&exists)
		if !exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Location with id %d does not exist.", *req.LocationID)})
			return
		}
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.Code != nil {
		addClause("code", *req.Code)
	}
	if req.Description != nil {
		addClause("description", *req.Description)
	}
	if req.ParentID != nil {
		addClause("parent_id", *req.ParentID)
	}
	if req.Name != nil {
		addClause("name", *req.Name)
	}
	if req.AccountType != nil {
		addClause("account_type", *req.AccountType)
	}
	if req.IsActive != nil {
		addClause("is_active", *req.IsActive)
	}
	if req.LocationID != nil {
		addClause("location_id", *req.LocationID)
	}
	if req.IsPartsVendor != nil {
		addClause("is_parts_vendor", *req.IsPartsVendor)
	}
	if req.IsCogsVendor != nil {
		addClause("is_cogs_vendor", *req.IsCogsVendor)
	}
	if req.IsStatementVendor != nil {
		addClause("is_statement_vendor", *req.IsStatementVendor)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, accountID)
		query := fmt.Sprintf("UPDATE chart_of_accounts SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err := h.DB.Exec(context.Background(), query, args...)
		if err != nil {
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

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM chart_of_accounts WHERE id = $1", accountID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete account", "error": err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
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
	errors := []map[string]interface{}{}

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
			errors = append(errors, map[string]interface{}{"row": row, "error": "Missing required fields."})
			continue
		}

		typeStr := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(typeRaw), " ", "_"), "-", "_")
		if !validAccountTypes[typeStr] {
			errors = append(errors, map[string]interface{}{"row": row, "error": fmt.Sprintf("Invalid account type: '%s' (normalized: '%s')", typeRaw, typeStr)})
			continue
		}

		// Check if already exists
		var exists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM chart_of_accounts WHERE code = $1)", code).Scan(&exists)
		if exists {
			continue
		}

		if description == "" {
			description = name
		}

		id := uuid.New().String()
		now := time.Now().UTC()

		_, err := h.DB.Exec(context.Background(),
			`INSERT INTO chart_of_accounts (id, code, name, account_type, description, is_active, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, true, $6, $7)`,
			id, code, name, typeStr, description, now, now,
		)
		if err != nil {
			errors = append(errors, map[string]interface{}{"row": row, "error": err.Error()})
			continue
		}
		inserted++
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "inserted": inserted, "errors": errors})
}

type accountScannable interface {
	Scan(dest ...interface{}) error
}

func (h *AccountingHandler) scanAccount(row accountScannable) (accountResponse, error) {
	var a accountResponse
	var locationID *int

	err := row.Scan(&a.ID, &a.Code, &a.Description, &a.ParentID, &a.Name, &a.AccountType,
		&a.IsActive, &locationID, &a.IsPartsVendor, &a.IsCogsVendor, &a.IsStatementVendor,
		&a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return a, err
	}

	// Load location if present
	if locationID != nil {
		var locID int
		var locName, addr1, city, state, postal, country string
		var addr2 *string
		var shopID *int
		var locCreatedAt, locUpdatedAt *time.Time
		lerr := h.DB.QueryRow(context.Background(),
			`SELECT id, location_name, address_line_1, address_line_2, city, state_province, postal_code, country, shop_id, created_at, updated_at
			 FROM locations WHERE id = $1`, *locationID,
		).Scan(&locID, &locName, &addr1, &addr2, &city, &state, &postal, &country, &shopID, &locCreatedAt, &locUpdatedAt)
		if lerr == nil {
			a.Location = gin.H{
				"id": locID, "location_name": locName, "address_line_1": addr1,
				"address_line_2": addr2, "city": city, "state_province": state,
				"postal_code": postal, "country": country, "shop_id": shopID,
				"created_at": locCreatedAt, "updated_at": locUpdatedAt,
			}
		}
	}

	return a, nil
}

func (h *AccountingHandler) getAccountByID(c *gin.Context, id string) {
	row := h.DB.QueryRow(context.Background(),
		`SELECT id, code, description, parent_id, name, account_type, is_active, location_id, is_parts_vendor, is_cogs_vendor, is_statement_vendor, created_at, updated_at
		 FROM chart_of_accounts WHERE id = $1`, id)

	a, err := h.scanAccount(row)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Account with ID %s not found", id)})
		return
	}

	c.JSON(http.StatusOK, a)
}
