package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PayrollHandler struct {
	DB *pgxpool.Pool
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
	var name string
	err := h.DB.QueryRow(context.Background(),
		"SELECT location_name FROM locations WHERE id = $1", locationID).Scan(&name)
	if err != nil {
		return nil
	}
	return &name
}

// POST /payroll/
func (h *PayrollHandler) CreatePayroll(c *gin.Context) {
	var req payrollCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate location
	var locExists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", req.LocationID).Scan(&locExists)
	if !locExists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO payroll (date, employee_name, gl_code, description, gross_pay, taxes, net_pay, location_id, user_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
		req.Date, req.EmployeeName, req.GLCode, req.Description, req.GrossPay, req.Taxes, req.NetPay,
		req.LocationID, uid, time.Now().UTC(), time.Now().UTC(),
	).Scan(&id)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create payroll", "error": err.Error()})
		return
	}

	h.getPayrollByID(c, id)
}

// GET /payroll/
func (h *PayrollHandler) GetAllPayrolls(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	query := `SELECT id, date, employee_name, gl_code, description, gross_pay, taxes, net_pay, location_id, created_at, updated_at
	           FROM payroll`
	args := []interface{}{}
	argIdx := 1

	if roleStr != "Admin" {
		query += fmt.Sprintf(" WHERE user_id = $%d", argIdx)
		args = append(args, uid)
		argIdx++
	}

	query += fmt.Sprintf(" OFFSET $%d LIMIT $%d", argIdx, argIdx+1)
	args = append(args, skip, limit)

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payrolls", "error": err.Error()})
		return
	}
	defer rows.Close()

	payrolls := []payrollResponse{}
	for rows.Next() {
		var p payrollResponse
		var dateVal time.Time
		if err := rows.Scan(&p.ID, &dateVal, &p.EmployeeName, &p.GLCode, &p.Description,
			&p.GrossPay, &p.Taxes, &p.NetPay, &p.LocationID, &p.CreatedAt, &p.UpdatedAt); err != nil {
			continue
		}
		p.Date = dateVal.Format("2006-01-02")
		p.LocationName = h.getLocationName(p.LocationID)
		payrolls = append(payrolls, p)
	}

	c.JSON(http.StatusOK, payrolls)
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

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM payroll WHERE id = $1)", payrollID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}

	var req payrollUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check at least one field
	if req.Date == nil && req.EmployeeName == nil && req.GLCode == nil && req.Description == nil &&
		req.GrossPay == nil && req.Taxes == nil && req.NetPay == nil && req.LocationID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "At least one field must be provided for update"})
		return
	}

	if req.LocationID != nil {
		var locExists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", *req.LocationID).Scan(&locExists)
		if !locExists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
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

	if req.Date != nil {
		addClause("date", *req.Date)
	}
	if req.EmployeeName != nil {
		addClause("employee_name", *req.EmployeeName)
	}
	if req.GLCode != nil {
		addClause("gl_code", *req.GLCode)
	}
	if req.Description != nil {
		addClause("description", *req.Description)
	}
	if req.GrossPay != nil {
		addClause("gross_pay", *req.GrossPay)
	}
	if req.Taxes != nil {
		addClause("taxes", *req.Taxes)
	}
	if req.NetPay != nil {
		addClause("net_pay", *req.NetPay)
	}
	if req.LocationID != nil {
		addClause("location_id", *req.LocationID)
	}

	addClause("updated_at", time.Now().UTC())
	args = append(args, payrollID)
	query := fmt.Sprintf("UPDATE payroll SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
	_, err = h.DB.Exec(context.Background(), query, args...)
	if err != nil {
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

	// Check existence and ownership
	var payrollUserID *string
	err = h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM payroll WHERE id = $1", payrollID).Scan(&payrollUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}

	if roleStr != "Admin" && (payrollUserID == nil || *payrollUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to delete this payroll entry"})
		return
	}

	h.DB.Exec(context.Background(), "DELETE FROM payroll WHERE id = $1", payrollID)
	c.JSON(http.StatusOK, gin.H{"message": "Payroll deleted successfully!"})
}

// POST /payroll/adjustments
func (h *PayrollHandler) CreateAdjustment(c *gin.Context) {
	var req adjustmentCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var locExists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", req.LocationID).Scan(&locExists)
	if !locExists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO payroll_account_adjustments (date, gl_code, description, debit, credit, location_id, user_id, notes, employee_name, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
		req.Date, req.GLCode, req.Description, req.Debit, req.Credit, req.LocationID,
		uid, req.Notes, req.EmployeeName, time.Now().UTC(), time.Now().UTC(),
	).Scan(&id)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create adjustment", "error": err.Error()})
		return
	}

	h.getAdjustmentByID(c, id)
}

// GET /payroll/adjustments
func (h *PayrollHandler) ListAdjustments(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	query := `SELECT id, date, gl_code, description, debit, credit, location_id, notes, employee_name, created_at, updated_at
	           FROM payroll_account_adjustments`
	args := []interface{}{}
	argIdx := 1

	if roleStr != "Admin" {
		query += fmt.Sprintf(" WHERE user_id = $%d", argIdx)
		args = append(args, uid)
		argIdx++
	}

	query += fmt.Sprintf(" OFFSET $%d LIMIT $%d", argIdx, argIdx+1)
	args = append(args, skip, limit)

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query adjustments", "error": err.Error()})
		return
	}
	defer rows.Close()

	adjustments := []adjustmentResponse{}
	for rows.Next() {
		var a adjustmentResponse
		var dateVal time.Time
		if err := rows.Scan(&a.ID, &dateVal, &a.GLCode, &a.Description, &a.Debit, &a.Credit,
			&a.LocationID, &a.Notes, &a.EmployeeName, &a.CreatedAt, &a.UpdatedAt); err != nil {
			continue
		}
		a.Date = dateVal.Format("2006-01-02")
		a.LocationName = h.getLocationName(a.LocationID)
		adjustments = append(adjustments, a)
	}

	c.JSON(http.StatusOK, adjustments)
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

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM payroll_account_adjustments WHERE id = $1)", adjustmentID).Scan(&exists)
	if !exists {
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
		var locExists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", *req.LocationID).Scan(&locExists)
		if !locExists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid location ID"})
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

	if req.Date != nil {
		addClause("date", *req.Date)
	}
	if req.GLCode != nil {
		addClause("gl_code", *req.GLCode)
	}
	if req.Description != nil {
		addClause("description", *req.Description)
	}
	if req.Debit != nil {
		addClause("debit", *req.Debit)
	}
	if req.Credit != nil {
		addClause("credit", *req.Credit)
	}
	if req.Notes != nil {
		addClause("notes", *req.Notes)
	}
	if req.EmployeeName != nil {
		addClause("employee_name", *req.EmployeeName)
	}
	if req.LocationID != nil {
		addClause("location_id", *req.LocationID)
	}

	addClause("updated_at", time.Now().UTC())
	args = append(args, adjustmentID)
	query := fmt.Sprintf("UPDATE payroll_account_adjustments SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
	_, err = h.DB.Exec(context.Background(), query, args...)
	if err != nil {
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

	var adjUserID *string
	err = h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM payroll_account_adjustments WHERE id = $1", adjustmentID).Scan(&adjUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayrollAccountAdjustment not found"})
		return
	}

	if roleStr != "Admin" && (adjUserID == nil || *adjUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to delete this payroll adjustment"})
		return
	}

	h.DB.Exec(context.Background(), "DELETE FROM payroll_account_adjustments WHERE id = $1", adjustmentID)
	c.JSON(http.StatusOK, gin.H{"message": "Payroll adjustment deleted successfully!"})
}

func (h *PayrollHandler) getPayrollByID(c *gin.Context, id int) {
	var p payrollResponse
	var dateVal time.Time
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, date, employee_name, gl_code, description, gross_pay, taxes, net_pay, location_id, created_at, updated_at
		 FROM payroll WHERE id = $1`, id,
	).Scan(&p.ID, &dateVal, &p.EmployeeName, &p.GLCode, &p.Description,
		&p.GrossPay, &p.Taxes, &p.NetPay, &p.LocationID, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payroll not found"})
		return
	}
	p.Date = dateVal.Format("2006-01-02")
	p.LocationName = h.getLocationName(p.LocationID)

	c.JSON(http.StatusOK, p)
}

func (h *PayrollHandler) getAdjustmentByID(c *gin.Context, id int) {
	var a adjustmentResponse
	var dateVal time.Time
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, date, gl_code, description, debit, credit, location_id, notes, employee_name, created_at, updated_at
		 FROM payroll_account_adjustments WHERE id = $1`, id,
	).Scan(&a.ID, &dateVal, &a.GLCode, &a.Description, &a.Debit, &a.Credit,
		&a.LocationID, &a.Notes, &a.EmployeeName, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayrollAccountAdjustment not found"})
		return
	}
	a.Date = dateVal.Format("2006-01-02")
	a.LocationName = h.getLocationName(a.LocationID)

	c.JSON(http.StatusOK, a)
}
