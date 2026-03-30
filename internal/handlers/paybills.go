package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PayBillHandler struct {
	DB *pgxpool.Pool
}

// --- PayBill types ---

type lineItemCreate struct {
	Item        *string  `json:"item"`
	Description *string  `json:"description"`
	Qty         *float64 `json:"qty"`
	UnitPrice   *float64 `json:"unit_price"`
	TotalAmount *float64 `json:"total_amount"`
	GLCodeID    *string  `json:"gl_code_id"`
	LocationID  *int     `json:"location_id"`
}

type paybillCreateRequest struct {
	VendorID            string           `json:"vendor_id" binding:"required"`
	Amount              float64          `json:"amount" binding:"required"`
	Date                string           `json:"date" binding:"required"`
	Category            *string          `json:"category"`
	InvoiceURL          *string          `json:"invoice_url"`
	PaymentMethodID     *string          `json:"payment_method_id"`
	ChequeNumber        *string          `json:"cheque_number"`
	ChequeClearingDate  *string          `json:"cheque_clearing_date"`
	SpentAs             *string          `json:"spent_as"`
	Reference           *string          `json:"reference"`
	PaidByCheck         *bool            `json:"paid_by_check"`
	LineItems           []lineItemCreate `json:"line_items"`
}

type paybillUpdateRequest struct {
	VendorID            *string          `json:"vendor_id"`
	Amount              *float64         `json:"amount"`
	Date                *string          `json:"date"`
	Category            *string          `json:"category"`
	InvoiceURL          *string          `json:"invoice_url"`
	PaymentMethodID     *string          `json:"payment_method_id"`
	ChequeNumber        *string          `json:"cheque_number"`
	ChequeClearingDate  *string          `json:"cheque_clearing_date"`
	SpentAs             *string          `json:"spent_as"`
	Reference           *string          `json:"reference"`
	PaidByCheck         *bool            `json:"paid_by_check"`
	LineItems           *[]lineItemCreate `json:"line_items"`
}

type paybillResponse struct {
	ID                  int               `json:"id"`
	VendorID            string            `json:"vendor_id"`
	Amount              float64           `json:"amount"`
	Date                *string           `json:"date"`
	Category            *string           `json:"category"`
	InvoiceURL          *string           `json:"invoice_url"`
	PaymentMethodID     *string           `json:"payment_method_id"`
	ChequeNumber        *string           `json:"cheque_number"`
	ChequeClearingDate  *string           `json:"cheque_clearing_date"`
	SpentAs             *string           `json:"spent_as"`
	Reference           *string           `json:"reference"`
	PaidByCheck         *bool             `json:"paid_by_check"`
	UserID              *string           `json:"user_id"`
	CreatedAt           *time.Time        `json:"created_at"`
	UpdatedAt           *time.Time        `json:"updated_at"`
	LineItems           []lineItemResponse `json:"line_items"`
	SchedulePayments    []schedPayResponse `json:"schedule_payments"`
}

type lineItemResponse struct {
	ID          int      `json:"id"`
	PaybillID   int      `json:"paybill_id"`
	Item        *string  `json:"item"`
	Description *string  `json:"description"`
	Qty         *float64 `json:"qty"`
	UnitPrice   *float64 `json:"unit_price"`
	TotalAmount *float64 `json:"total_amount"`
	GLCodeID    *string  `json:"gl_code_id"`
	LocationID  *int     `json:"location_id"`
}

// --- SchedulePayment types ---

type schedPayCreateRequest struct {
	PaybillID   int     `json:"paybill_id" binding:"required"`
	RepeatEvery *int    `json:"repeat_every"`
	Frequency   *string `json:"frequency"`
	StartDate   *string `json:"start_date"`
	EndDate     *string `json:"end_date"`
	Enabled     *bool   `json:"enabled"`
}

type schedPayUpdateRequest struct {
	RepeatEvery *int    `json:"repeat_every"`
	Frequency   *string `json:"frequency"`
	StartDate   *string `json:"start_date"`
	EndDate     *string `json:"end_date"`
	Enabled     *bool   `json:"enabled"`
}

type schedPayResponse struct {
	ID          int        `json:"id"`
	PaybillID   int        `json:"paybill_id"`
	RepeatEvery *int       `json:"repeat_every"`
	Frequency   *string    `json:"frequency"`
	StartDate   *string    `json:"start_date"`
	EndDate     *string    `json:"end_date"`
	Enabled     *bool      `json:"enabled"`
	CreatedAt   *time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
}

// --- Reminder types ---

type reminderResponse struct {
	ID                 int        `json:"id"`
	ScheduledPaymentID *int       `json:"scheduled_payment_id"`
	ReminderType       *string    `json:"reminder_type"`
	Message            *string    `json:"message"`
	ReminderDate       *string    `json:"reminder_date"`
	Acknowledged       bool       `json:"acknowledged"`
	CreatedAt          *time.Time `json:"created_at"`
	UpdatedAt          *time.Time `json:"updated_at"`
}

// --- Manual Bill types ---

type manualBillCreateRequest struct {
	VendorID           string   `json:"vendor_id" binding:"required"`
	Amount             float64  `json:"amount" binding:"required"`
	DueDate            string   `json:"due_date" binding:"required"`
	PayableType        *string  `json:"payable_type"`
	GLCodeID           *string  `json:"gl_code_id"`
	StatementPeriod    *string  `json:"statement_period"`
	StatementStartDate *string  `json:"statement_start_date"`
	StatementEndDate   *string  `json:"statement_end_date"`
	Notes              *string  `json:"notes"`
}

type manualBillUpdateRequest struct {
	VendorID           *string  `json:"vendor_id"`
	Amount             *float64 `json:"amount"`
	DueDate            *string  `json:"due_date"`
	PayableType        *string  `json:"payable_type"`
	GLCodeID           *string  `json:"gl_code_id"`
	StatementPeriod    *string  `json:"statement_period"`
	StatementStartDate *string  `json:"statement_start_date"`
	StatementEndDate   *string  `json:"statement_end_date"`
	Notes              *string  `json:"notes"`
	Status             *string  `json:"status"`
}

type manualBillResponse struct {
	ID                 int        `json:"id"`
	VendorID           string     `json:"vendor_id"`
	Amount             float64    `json:"amount"`
	DueDate            *string    `json:"due_date"`
	PayableType        *string    `json:"payable_type"`
	GLCodeID           *string    `json:"gl_code_id"`
	StatementPeriod    *string    `json:"statement_period"`
	StatementStartDate *string    `json:"statement_start_date"`
	StatementEndDate   *string    `json:"statement_end_date"`
	Notes              *string    `json:"notes"`
	Status             *string    `json:"status"`
	UserID             *string    `json:"user_id"`
	CreatedAt          *time.Time `json:"created_at"`
	UpdatedAt          *time.Time `json:"updated_at"`
}

// =============================
// PayBill CRUD
// =============================

// POST /paybills/
func (h *PayBillHandler) CreatePayBill(c *gin.Context) {
	var req paybillCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	now := time.Now().UTC()

	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO pay_bills (vendor_id, amount, date, category, invoice_url, payment_method_id,
		 cheque_number, cheque_clearing_date, spent_as, reference, paid_by_check, user_id, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id`,
		req.VendorID, req.Amount, req.Date, req.Category, req.InvoiceURL, req.PaymentMethodID,
		req.ChequeNumber, req.ChequeClearingDate, req.SpentAs, req.Reference, req.PaidByCheck,
		uid, now, now,
	).Scan(&id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create paybill"})
		return
	}

	// Insert line items
	for _, item := range req.LineItems {
		h.DB.Exec(context.Background(),
			`INSERT INTO pay_bill_line_items (paybill_id, item, description, qty, unit_price, total_amount, gl_code_id, location_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			id, item.Item, item.Description, item.Qty, item.UnitPrice, item.TotalAmount, item.GLCodeID, item.LocationID)
	}

	h.getPayBillByID(c, id)
}

// GET /paybills/
func (h *PayBillHandler) ListPayBills(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	scheduled := c.Query("scheduled")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	query := "SELECT id FROM pay_bills WHERE user_id = $1"
	args := []interface{}{uid}
	argIdx := 2

	if scheduled == "true" {
		query += " AND EXISTS (SELECT 1 FROM schedule_payments WHERE schedule_payments.paybill_id = pay_bills.id)"
	} else if scheduled == "false" {
		query += " AND NOT EXISTS (SELECT 1 FROM schedule_payments WHERE schedule_payments.paybill_id = pay_bills.id)"
	}

	query += fmt.Sprintf(" ORDER BY updated_at DESC OFFSET $%d LIMIT $%d", argIdx, argIdx+1)
	args = append(args, skip, limit)

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query paybills"})
		return
	}
	defer rows.Close()

	paybills := []paybillResponse{}
	for rows.Next() {
		var pbID int
		rows.Scan(&pbID)
		pb := h.loadPayBill(pbID)
		if pb != nil {
			paybills = append(paybills, *pb)
		}
	}

	if len(paybills) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No data found"})
		return
	}

	c.JSON(http.StatusOK, paybills)
}

// GET /paybills/:paybill_id
func (h *PayBillHandler) GetPayBill(c *gin.Context) {
	paybillID, err := strconv.Atoi(c.Param("paybill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid paybill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pay_bills WHERE id = $1 AND user_id = $2)", paybillID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayBill not found"})
		return
	}

	h.getPayBillByID(c, paybillID)
}

// PATCH /paybills/:paybill_id
func (h *PayBillHandler) UpdatePayBill(c *gin.Context) {
	paybillID, err := strconv.Atoi(c.Param("paybill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid paybill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pay_bills WHERE id = $1 AND user_id = $2)", paybillID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayBill not found"})
		return
	}

	var req paybillUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.VendorID != nil {
		addClause("vendor_id", *req.VendorID)
	}
	if req.Amount != nil {
		addClause("amount", *req.Amount)
	}
	if req.Date != nil {
		addClause("date", *req.Date)
	}
	if req.Category != nil {
		addClause("category", *req.Category)
	}
	if req.InvoiceURL != nil {
		addClause("invoice_url", *req.InvoiceURL)
	}
	if req.PaymentMethodID != nil {
		addClause("payment_method_id", *req.PaymentMethodID)
	}
	if req.ChequeNumber != nil {
		addClause("cheque_number", *req.ChequeNumber)
	}
	if req.ChequeClearingDate != nil {
		addClause("cheque_clearing_date", *req.ChequeClearingDate)
	}
	if req.SpentAs != nil {
		addClause("spent_as", *req.SpentAs)
	}
	if req.Reference != nil {
		addClause("reference", *req.Reference)
	}
	if req.PaidByCheck != nil {
		addClause("paid_by_check", *req.PaidByCheck)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, paybillID)
		query := fmt.Sprintf("UPDATE pay_bills SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		h.DB.Exec(context.Background(), query, args...)
	}

	// Replace line items if provided
	if req.LineItems != nil {
		h.DB.Exec(context.Background(), "DELETE FROM pay_bill_line_items WHERE paybill_id = $1", paybillID)
		for _, item := range *req.LineItems {
			h.DB.Exec(context.Background(),
				`INSERT INTO pay_bill_line_items (paybill_id, item, description, qty, unit_price, total_amount, gl_code_id, location_id)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				paybillID, item.Item, item.Description, item.Qty, item.UnitPrice, item.TotalAmount, item.GLCodeID, item.LocationID)
		}
	}

	h.getPayBillByID(c, paybillID)
}

// DELETE /paybills/:paybill_id
func (h *PayBillHandler) DeletePayBill(c *gin.Context) {
	paybillID, err := strconv.Atoi(c.Param("paybill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid paybill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	tag, err := h.DB.Exec(context.Background(),
		"DELETE FROM pay_bills WHERE id = $1 AND user_id = $2", paybillID, uid)
	if err != nil || tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayBill not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "PayBill deleted successfully!"})
}

// =============================
// Schedule Payments
// =============================

// POST /paybills/schedule-payment/
func (h *PayBillHandler) CreateSchedulePayment(c *gin.Context) {
	var req schedPayCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pay_bills WHERE id = $1 AND user_id = $2)", req.PaybillID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayBill not found"})
		return
	}

	now := time.Now().UTC()
	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO schedule_payments (paybill_id, repeat_every, frequency, start_date, end_date, enabled, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
		req.PaybillID, req.RepeatEvery, req.Frequency, req.StartDate, req.EndDate, req.Enabled, now, now,
	).Scan(&id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create schedule payment"})
		return
	}

	h.getSchedulePaymentByIDInternal(c, id)
}

// GET /paybills/schedule-payment/
func (h *PayBillHandler) ListSchedulePayments(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	rows, err := h.DB.Query(context.Background(),
		`SELECT sp.id, sp.paybill_id, sp.repeat_every, sp.frequency, sp.start_date, sp.end_date, sp.enabled, sp.created_at, sp.updated_at
		 FROM schedule_payments sp
		 JOIN pay_bills pb ON sp.paybill_id = pb.id
		 WHERE pb.user_id = $1
		 OFFSET $2 LIMIT $3`, uid, skip, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query schedule payments"})
		return
	}
	defer rows.Close()

	payments := []schedPayResponse{}
	for rows.Next() {
		var sp schedPayResponse
		var startDate, endDate *time.Time
		if err := rows.Scan(&sp.ID, &sp.PaybillID, &sp.RepeatEvery, &sp.Frequency, &startDate, &endDate, &sp.Enabled, &sp.CreatedAt, &sp.UpdatedAt); err != nil {
			continue
		}
		if startDate != nil {
			s := startDate.Format("2006-01-02")
			sp.StartDate = &s
		}
		if endDate != nil {
			s := endDate.Format("2006-01-02")
			sp.EndDate = &s
		}
		payments = append(payments, sp)
	}

	c.JSON(http.StatusOK, payments)
}

// GET /paybills/schedule-payment/:schedule_payment_id
func (h *PayBillHandler) GetSchedulePayment(c *gin.Context) {
	spID, err := strconv.Atoi(c.Param("schedule_payment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid schedule_payment_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM schedule_payments sp JOIN pay_bills pb ON sp.paybill_id = pb.id WHERE sp.id = $1 AND pb.user_id = $2)`,
		spID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Scheduled payment not found"})
		return
	}

	h.getSchedulePaymentByIDInternal(c, spID)
}

// PATCH /paybills/schedule-payment/:schedule_payment_id
func (h *PayBillHandler) UpdateSchedulePayment(c *gin.Context) {
	spID, err := strconv.Atoi(c.Param("schedule_payment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid schedule_payment_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM schedule_payments sp JOIN pay_bills pb ON sp.paybill_id = pb.id WHERE sp.id = $1 AND pb.user_id = $2)`,
		spID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Scheduled payment not found"})
		return
	}

	var req schedPayUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.RepeatEvery != nil {
		addClause("repeat_every", *req.RepeatEvery)
	}
	if req.Frequency != nil {
		addClause("frequency", *req.Frequency)
	}
	if req.StartDate != nil {
		addClause("start_date", *req.StartDate)
	}
	if req.EndDate != nil {
		addClause("end_date", *req.EndDate)
	}
	if req.Enabled != nil {
		addClause("enabled", *req.Enabled)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, spID)
		query := fmt.Sprintf("UPDATE schedule_payments SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		h.DB.Exec(context.Background(), query, args...)
	}

	h.getSchedulePaymentByIDInternal(c, spID)
}

// DELETE /paybills/schedule-payment/:schedule_payment_id
func (h *PayBillHandler) DeleteSchedulePayment(c *gin.Context) {
	spID, err := strconv.Atoi(c.Param("schedule_payment_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid schedule_payment_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM schedule_payments sp JOIN pay_bills pb ON sp.paybill_id = pb.id WHERE sp.id = $1 AND pb.user_id = $2)`,
		spID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Scheduled payment not found"})
		return
	}

	h.DB.Exec(context.Background(), "DELETE FROM schedule_payments WHERE id = $1", spID)
	c.JSON(http.StatusOK, gin.H{"message": "Scheduled payment deleted successfully!"})
}

// =============================
// Reminders
// =============================

// GET /paybills/reminders/
func (h *PayBillHandler) ListReminders(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		`SELECT id, scheduled_payment_id, reminder_type, message, reminder_date, acknowledged, created_at, updated_at
		 FROM reminders WHERE acknowledged = false ORDER BY reminder_date`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query reminders"})
		return
	}
	defer rows.Close()

	reminders := []reminderResponse{}
	for rows.Next() {
		var r reminderResponse
		var reminderDate *time.Time
		if err := rows.Scan(&r.ID, &r.ScheduledPaymentID, &r.ReminderType, &r.Message, &reminderDate, &r.Acknowledged, &r.CreatedAt, &r.UpdatedAt); err != nil {
			continue
		}
		if reminderDate != nil {
			s := reminderDate.Format("2006-01-02")
			r.ReminderDate = &s
		}
		reminders = append(reminders, r)
	}

	c.JSON(http.StatusOK, reminders)
}

// PATCH /paybills/reminders/:reminder_id/acknowledge
func (h *PayBillHandler) AcknowledgeReminder(c *gin.Context) {
	reminderID, err := strconv.Atoi(c.Param("reminder_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid reminder_id"})
		return
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM reminders WHERE id = $1)", reminderID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Reminder not found"})
		return
	}

	h.DB.Exec(context.Background(),
		"UPDATE reminders SET acknowledged = true WHERE id = $1", reminderID)

	var r reminderResponse
	var reminderDate *time.Time
	h.DB.QueryRow(context.Background(),
		`SELECT id, scheduled_payment_id, reminder_type, message, reminder_date, acknowledged, created_at, updated_at
		 FROM reminders WHERE id = $1`, reminderID,
	).Scan(&r.ID, &r.ScheduledPaymentID, &r.ReminderType, &r.Message, &reminderDate, &r.Acknowledged, &r.CreatedAt, &r.UpdatedAt)
	if reminderDate != nil {
		s := reminderDate.Format("2006-01-02")
		r.ReminderDate = &s
	}

	c.JSON(http.StatusOK, r)
}

// =============================
// Manual Bills
// =============================

// POST /paybills/manual-bills/
func (h *PayBillHandler) CreateManualBill(c *gin.Context) {
	var req manualBillCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	now := time.Now().UTC()

	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO manual_bill_entries (vendor_id, amount, due_date, payable_type, gl_code_id,
		 statement_period, statement_start_date, statement_end_date, notes, user_id, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
		req.VendorID, req.Amount, req.DueDate, req.PayableType, req.GLCodeID,
		req.StatementPeriod, req.StatementStartDate, req.StatementEndDate, req.Notes,
		uid, now, now,
	).Scan(&id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid data or constraint violation while creating manual bill."})
		return
	}

	h.getManualBillByIDInternal(c, id, http.StatusCreated)
}

// GET /paybills/manual-bills/
func (h *PayBillHandler) ListManualBills(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	rows, err := h.DB.Query(context.Background(),
		`SELECT id, vendor_id, amount, due_date, payable_type, gl_code_id,
		 statement_period, statement_start_date, statement_end_date, notes, status, user_id, created_at, updated_at
		 FROM manual_bill_entries WHERE user_id = $1 ORDER BY created_at DESC OFFSET $2 LIMIT $3`,
		uid, skip, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Database error: %v", err)})
		return
	}
	defer rows.Close()

	bills := []manualBillResponse{}
	for rows.Next() {
		b := h.scanManualBill(rows)
		if b != nil {
			bills = append(bills, *b)
		}
	}

	// Calculate stats from all bills (unpaginated)
	allRows, err := h.DB.Query(context.Background(),
		`SELECT amount, due_date, status, updated_at FROM manual_bill_entries WHERE user_id = $1`, uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Database error: %v", err)})
		return
	}
	defer allRows.Close()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	oneWeek := today.AddDate(0, 0, 7)
	thirtyDaysAgo := time.Now().UTC().AddDate(0, 0, -30)

	totalCurrent := 0.0
	withinOneWeek := 0.0
	overdue := 0.0
	paidLast30 := 0.0

	for allRows.Next() {
		var amt float64
		var dueDate *time.Time
		var status *string
		var updatedAt *time.Time
		allRows.Scan(&amt, &dueDate, &status, &updatedAt)

		totalCurrent += amt

		statusLower := ""
		if status != nil {
			statusLower = strings.ToLower(*status)
		}

		if dueDate != nil {
			dd := dueDate.Truncate(24 * time.Hour)
			if statusLower == "unpaid" && !dd.Before(today) && !dd.After(oneWeek) {
				withinOneWeek += amt
			}
			if statusLower == "unpaid" && dd.Before(today) {
				overdue += amt
			}
		}
		if statusLower == "paid" && updatedAt != nil && updatedAt.After(thirtyDaysAgo) {
			paidLast30 += amt
		}
	}

	var totalCount int
	h.DB.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM manual_bill_entries WHERE user_id = $1", uid).Scan(&totalCount)

	c.JSON(http.StatusOK, gin.H{
		"bills": bills,
		"stats": gin.H{
			"total_current_payables":     math.Round(totalCurrent*100) / 100,
			"payables_within_one_week":   math.Round(withinOneWeek*100) / 100,
			"overdue_payable":            math.Round(overdue*100) / 100,
			"paid_payables_last_30_days": math.Round(paidLast30*100) / 100,
		},
		"total_count": totalCount,
	})
}

// GET /paybills/manual-bills/:bill_id
func (h *PayBillHandler) GetManualBill(c *gin.Context) {
	billID, err := strconv.Atoi(c.Param("bill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid bill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM manual_bill_entries WHERE id = $1 AND user_id = $2)", billID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Manual bill not found"})
		return
	}

	h.getManualBillByIDInternal(c, billID, http.StatusOK)
}

// PATCH /paybills/manual-bills/:bill_id
func (h *PayBillHandler) UpdateManualBill(c *gin.Context) {
	billID, err := strconv.Atoi(c.Param("bill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid bill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM manual_bill_entries WHERE id = $1 AND user_id = $2)", billID, uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Manual bill not found"})
		return
	}

	var req manualBillUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.VendorID != nil {
		addClause("vendor_id", *req.VendorID)
	}
	if req.Amount != nil {
		addClause("amount", *req.Amount)
	}
	if req.DueDate != nil {
		addClause("due_date", *req.DueDate)
	}
	if req.PayableType != nil {
		addClause("payable_type", *req.PayableType)
	}
	if req.GLCodeID != nil {
		addClause("gl_code_id", *req.GLCodeID)
	}
	if req.StatementPeriod != nil {
		addClause("statement_period", *req.StatementPeriod)
	}
	if req.StatementStartDate != nil {
		addClause("statement_start_date", *req.StatementStartDate)
	}
	if req.StatementEndDate != nil {
		addClause("statement_end_date", *req.StatementEndDate)
	}
	if req.Notes != nil {
		addClause("notes", *req.Notes)
	}
	if req.Status != nil {
		addClause("status", *req.Status)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, billID)
		query := fmt.Sprintf("UPDATE manual_bill_entries SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid data or constraint violation while updating manual bill."})
			return
		}
	}

	h.getManualBillByIDInternal(c, billID, http.StatusOK)
}

// DELETE /paybills/manual-bills/:bill_id
func (h *PayBillHandler) DeleteManualBill(c *gin.Context) {
	billID, err := strconv.Atoi(c.Param("bill_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid bill_id"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	tag, err := h.DB.Exec(context.Background(),
		"DELETE FROM manual_bill_entries WHERE id = $1 AND user_id = $2", billID, uid)
	if err != nil || tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Manual bill not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Manual bill deleted successfully"})
}

// =============================
// Helpers
// =============================

func (h *PayBillHandler) getPayBillByID(c *gin.Context, id int) {
	pb := h.loadPayBill(id)
	if pb == nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "PayBill not found"})
		return
	}
	c.JSON(http.StatusOK, pb)
}

func (h *PayBillHandler) loadPayBill(id int) *paybillResponse {
	var pb paybillResponse
	var dateVal *time.Time
	var clearingDate *time.Time

	err := h.DB.QueryRow(context.Background(),
		`SELECT id, vendor_id, amount, date, category, invoice_url, payment_method_id,
		 cheque_number, cheque_clearing_date, spent_as, reference, paid_by_check, user_id, created_at, updated_at
		 FROM pay_bills WHERE id = $1`, id,
	).Scan(&pb.ID, &pb.VendorID, &pb.Amount, &dateVal, &pb.Category, &pb.InvoiceURL, &pb.PaymentMethodID,
		&pb.ChequeNumber, &clearingDate, &pb.SpentAs, &pb.Reference, &pb.PaidByCheck, &pb.UserID, &pb.CreatedAt, &pb.UpdatedAt)
	if err != nil {
		return nil
	}

	if dateVal != nil {
		s := dateVal.Format("2006-01-02")
		pb.Date = &s
	}
	if clearingDate != nil {
		s := clearingDate.Format("2006-01-02")
		pb.ChequeClearingDate = &s
	}

	// Load line items
	pb.LineItems = []lineItemResponse{}
	liRows, err := h.DB.Query(context.Background(),
		`SELECT id, paybill_id, item, description, qty, unit_price, total_amount, gl_code_id, location_id
		 FROM pay_bill_line_items WHERE paybill_id = $1`, id)
	if err == nil {
		defer liRows.Close()
		for liRows.Next() {
			var li lineItemResponse
			liRows.Scan(&li.ID, &li.PaybillID, &li.Item, &li.Description, &li.Qty, &li.UnitPrice, &li.TotalAmount, &li.GLCodeID, &li.LocationID)
			pb.LineItems = append(pb.LineItems, li)
		}
	}

	// Load schedule payments
	pb.SchedulePayments = []schedPayResponse{}
	spRows, err := h.DB.Query(context.Background(),
		`SELECT id, paybill_id, repeat_every, frequency, start_date, end_date, enabled, created_at, updated_at
		 FROM schedule_payments WHERE paybill_id = $1`, id)
	if err == nil {
		defer spRows.Close()
		for spRows.Next() {
			var sp schedPayResponse
			var startDate, endDate *time.Time
			spRows.Scan(&sp.ID, &sp.PaybillID, &sp.RepeatEvery, &sp.Frequency, &startDate, &endDate, &sp.Enabled, &sp.CreatedAt, &sp.UpdatedAt)
			if startDate != nil {
				s := startDate.Format("2006-01-02")
				sp.StartDate = &s
			}
			if endDate != nil {
				s := endDate.Format("2006-01-02")
				sp.EndDate = &s
			}
			pb.SchedulePayments = append(pb.SchedulePayments, sp)
		}
	}

	return &pb
}

func (h *PayBillHandler) getSchedulePaymentByIDInternal(c *gin.Context, id int) {
	var sp schedPayResponse
	var startDate, endDate *time.Time
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, paybill_id, repeat_every, frequency, start_date, end_date, enabled, created_at, updated_at
		 FROM schedule_payments WHERE id = $1`, id,
	).Scan(&sp.ID, &sp.PaybillID, &sp.RepeatEvery, &sp.Frequency, &startDate, &endDate, &sp.Enabled, &sp.CreatedAt, &sp.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Scheduled payment not found"})
		return
	}
	if startDate != nil {
		s := startDate.Format("2006-01-02")
		sp.StartDate = &s
	}
	if endDate != nil {
		s := endDate.Format("2006-01-02")
		sp.EndDate = &s
	}
	c.JSON(http.StatusOK, sp)
}

type manualBillScannable interface {
	Scan(dest ...interface{}) error
}

func (h *PayBillHandler) scanManualBill(row manualBillScannable) *manualBillResponse {
	var b manualBillResponse
	var dueDate, stmtStartDate, stmtEndDate *time.Time

	err := row.Scan(&b.ID, &b.VendorID, &b.Amount, &dueDate, &b.PayableType, &b.GLCodeID,
		&b.StatementPeriod, &stmtStartDate, &stmtEndDate, &b.Notes, &b.Status, &b.UserID, &b.CreatedAt, &b.UpdatedAt)
	if err != nil {
		return nil
	}

	if dueDate != nil {
		s := dueDate.Format("2006-01-02")
		b.DueDate = &s
	}
	if stmtStartDate != nil {
		s := stmtStartDate.Format("2006-01-02")
		b.StatementStartDate = &s
	}
	if stmtEndDate != nil {
		s := stmtEndDate.Format("2006-01-02")
		b.StatementEndDate = &s
	}

	return &b
}

func (h *PayBillHandler) getManualBillByIDInternal(c *gin.Context, id int, statusCode int) {
	row := h.DB.QueryRow(context.Background(),
		`SELECT id, vendor_id, amount, due_date, payable_type, gl_code_id,
		 statement_period, statement_start_date, statement_end_date, notes, status, user_id, created_at, updated_at
		 FROM manual_bill_entries WHERE id = $1`, id)

	b := h.scanManualBill(row)
	if b == nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Manual bill not found"})
		return
	}

	c.JSON(statusCode, b)
}
