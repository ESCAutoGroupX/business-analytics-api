package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PaymentMethodHandler struct {
	DB *pgxpool.Pool
}

type paymentMethodCreateRequest struct {
	PlaidAccountID       *string  `json:"plaid_account_id"`
	Title                *string  `json:"title"`
	Description          *string  `json:"description"`
	MethodType           string   `json:"method_type" binding:"required"`
	ChequeSeriesStart    *string  `json:"cheque_series_start"`
	ChequeSeriesEnd      *string  `json:"cheque_series_end"`
	BankName             *string  `json:"bank_name"`
	BankAccountNumber    *string  `json:"bank_account_number"`
	BankRoutingNumber    *string  `json:"bank_routing_number"`
	CardLast4Digits      *string  `json:"card_last_4_digits"`
	CardName             *string  `json:"card_name"`
	Subtype              *string  `json:"subtype"`
	HolderCategory       *string  `json:"holder_category"`
	BalanceAvailable     *float64 `json:"balance_available"`
	BalanceCurrent       *float64 `json:"balance_current"`
	CreditLimit          *float64 `json:"credit_limit"`
	IsoCurrencyCode      *string  `json:"iso_currency_code"`
	UnofficialCurrencyCode *string `json:"unofficial_currency_code"`
	MinimumBalance       *float64 `json:"minimum_balance"`
	StartingCreditCardBal *float64 `json:"starting_credit_card_bal"`
	SortingOrder         *int     `json:"sorting_order"`
	LocationID           *string  `json:"location_id"`
}

type paymentMethodUpdateRequest struct {
	Title                *string  `json:"title"`
	Description          *string  `json:"description"`
	MethodType           *string  `json:"method_type"`
	ChequeSeriesStart    *string  `json:"cheque_series_start"`
	ChequeSeriesEnd      *string  `json:"cheque_series_end"`
	BankName             *string  `json:"bank_name"`
	BankAccountNumber    *string  `json:"bank_account_number"`
	BankRoutingNumber    *string  `json:"bank_routing_number"`
	CardLast4Digits      *string  `json:"card_last_4_digits"`
	CardName             *string  `json:"card_name"`
	Subtype              *string  `json:"subtype"`
	HolderCategory       *string  `json:"holder_category"`
	BalanceAvailable     *float64 `json:"balance_available"`
	BalanceCurrent       *float64 `json:"balance_current"`
	CreditLimit          *float64 `json:"credit_limit"`
	IsoCurrencyCode      *string  `json:"iso_currency_code"`
	UnofficialCurrencyCode *string `json:"unofficial_currency_code"`
	MinimumBalance       *float64 `json:"minimum_balance"`
	StartingCreditCardBal *float64 `json:"starting_credit_card_bal"`
	SortingOrder         *int     `json:"sorting_order"`
	LocationID           *string  `json:"location_id"`
}

type paymentMethodResponse struct {
	ID                    string     `json:"id"`
	PlaidAccountID        *string    `json:"plaid_account_id"`
	Title                 *string    `json:"title"`
	Description           *string    `json:"description"`
	MethodType            *string    `json:"method_type"`
	ChequeSeriesStart     *string    `json:"cheque_series_start"`
	ChequeSeriesEnd       *string    `json:"cheque_series_end"`
	BankName              *string    `json:"bank_name"`
	BankAccountNumber     *string    `json:"bank_account_number"`
	BankRoutingNumber     *string    `json:"bank_routing_number"`
	CardLast4Digits       *string    `json:"card_last_4_digits"`
	CardName              *string    `json:"card_name"`
	Subtype               *string    `json:"subtype"`
	HolderCategory        *string    `json:"holder_category"`
	BalanceAvailable      *float64   `json:"balance_available"`
	BalanceCurrent        *float64   `json:"balance_current"`
	CreditLimit           *float64   `json:"credit_limit"`
	IsoCurrencyCode       *string    `json:"iso_currency_code"`
	UnofficialCurrencyCode *string   `json:"unofficial_currency_code"`
	TotalAmount           *float64   `json:"total_amount"`
	MinimumBalance        *float64   `json:"minimum_balance"`
	StartingCreditCardBal *float64   `json:"starting_credit_card_bal"`
	SortingOrder          *int       `json:"sorting_order"`
	LocationID            *string    `json:"location_id"`
	CreatedAt             *time.Time `json:"created_at"`
	UpdatedAt             *time.Time `json:"updated_at"`
}

var validPaymentMethodTypes = []string{"CASH", "CREDIT_CARD", "CHECK", "DEPOSITORY", "LOAN", "BROKERAGE", "INVESTMENT"}

func isValidPaymentMethodType(t string) bool {
	for _, v := range validPaymentMethodTypes {
		if strings.EqualFold(v, t) {
			return true
		}
	}
	return false
}

func (h *PaymentMethodHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if fmt.Sprintf("%v", role) != "Admin" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Only admin can manage payment methods."})
		return false
	}
	return true
}

// POST /payment-methods/
func (h *PaymentMethodHandler) CreatePaymentMethod(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	var req paymentMethodCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	if !isValidPaymentMethodType(req.MethodType) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid payment method type. Valid values are: %v", validPaymentMethodTypes)})
		return
	}

	methodType := strings.ToUpper(req.MethodType)

	// Validate fields based on type
	if methodType == "CHECK" {
		if req.ChequeSeriesStart == nil || req.ChequeSeriesEnd == nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Cheque series start and end are required for cheque payment method."})
			return
		}
	} else if methodType == "CREDIT_CARD" {
		if req.CardLast4Digits == nil || req.CardName == nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Card last 4 digits and card name are required for credit card payment method."})
			return
		}
	} else if methodType == "CASH" {
		req.ChequeSeriesStart = nil
		req.ChequeSeriesEnd = nil
		req.CardLast4Digits = nil
		req.CardName = nil
	}

	id := uuid.New().String()
	now := time.Now().UTC()

	_, err := h.DB.Exec(context.Background(),
		`INSERT INTO payment_methods (id, plaid_account_id, title, description, method_type,
		 cheque_series_start, cheque_series_end, bank_name, bank_account_number, bank_routing_number,
		 card_last_4_digits, card_name, subtype, holder_category, balance_available, balance_current,
		 credit_limit, iso_currency_code, unofficial_currency_code, minimum_balance,
		 starting_credit_card_bal, sorting_order, location_id, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
		id, req.PlaidAccountID, req.Title, req.Description, methodType,
		req.ChequeSeriesStart, req.ChequeSeriesEnd, req.BankName, req.BankAccountNumber, req.BankRoutingNumber,
		req.CardLast4Digits, req.CardName, req.Subtype, req.HolderCategory, req.BalanceAvailable, req.BalanceCurrent,
		req.CreditLimit, req.IsoCurrencyCode, req.UnofficialCurrencyCode, req.MinimumBalance,
		req.StartingCreditCardBal, req.SortingOrder, req.LocationID, now, now,
	)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create payment method", "error": err.Error()})
		return
	}

	h.getPaymentMethodByID(c, id)
}

// GET /payment-methods/
func (h *PaymentMethodHandler) ListPaymentMethods(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		`SELECT id, plaid_account_id, title, description, method_type,
		 cheque_series_start, cheque_series_end, bank_name, bank_account_number, bank_routing_number,
		 card_last_4_digits, card_name, subtype, holder_category, balance_available, balance_current,
		 credit_limit, iso_currency_code, unofficial_currency_code, total_amount, minimum_balance,
		 starting_credit_card_bal, sorting_order, location_id, created_at, updated_at
		 FROM payment_methods ORDER BY created_at DESC`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	methods := []paymentMethodResponse{}
	for rows.Next() {
		var pm paymentMethodResponse
		if err := rows.Scan(&pm.ID, &pm.PlaidAccountID, &pm.Title, &pm.Description, &pm.MethodType,
			&pm.ChequeSeriesStart, &pm.ChequeSeriesEnd, &pm.BankName, &pm.BankAccountNumber, &pm.BankRoutingNumber,
			&pm.CardLast4Digits, &pm.CardName, &pm.Subtype, &pm.HolderCategory, &pm.BalanceAvailable, &pm.BalanceCurrent,
			&pm.CreditLimit, &pm.IsoCurrencyCode, &pm.UnofficialCurrencyCode, &pm.TotalAmount, &pm.MinimumBalance,
			&pm.StartingCreditCardBal, &pm.SortingOrder, &pm.LocationID, &pm.CreatedAt, &pm.UpdatedAt); err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan payment method", "error": err.Error()})
			return
		}
		methods = append(methods, pm)
	}

	c.JSON(http.StatusOK, methods)
}

// GET /payment-methods/:payment_method_id
func (h *PaymentMethodHandler) GetPaymentMethod(c *gin.Context) {
	pmID := c.Param("payment_method_id")
	h.getPaymentMethodByID(c, pmID)
}

// PATCH /payment-methods/:payment_method_id
func (h *PaymentMethodHandler) UpdatePaymentMethod(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	pmID := c.Param("payment_method_id")

	// Check exists and get current starting_credit_card_bal
	var currentStartingBal *float64
	err := h.DB.QueryRow(context.Background(),
		"SELECT starting_credit_card_bal FROM payment_methods WHERE id = $1", pmID).Scan(&currentStartingBal)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment method not found"})
		return
	}

	var req paymentMethodUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate method_type if provided
	if req.MethodType != nil {
		if !isValidPaymentMethodType(*req.MethodType) {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid payment method type. Valid values are: %v", validPaymentMethodTypes)})
			return
		}
		mt := strings.ToUpper(*req.MethodType)
		req.MethodType = &mt

		if mt == "CHECK" {
			if req.ChequeSeriesStart == nil || req.ChequeSeriesEnd == nil {
				c.JSON(http.StatusBadRequest, gin.H{"detail": "Cheque series start and end are required for cheque payment method."})
				return
			}
		} else if mt == "CREDIT_CARD" {
			if req.CardLast4Digits == nil || req.CardName == nil {
				c.JSON(http.StatusBadRequest, gin.H{"detail": "Card last 4 digits and card name are required for credit card payment method."})
				return
			}
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

	if req.Title != nil {
		addClause("title", *req.Title)
	}
	if req.Description != nil {
		addClause("description", *req.Description)
	}
	if req.MethodType != nil {
		addClause("method_type", *req.MethodType)
	}
	if req.ChequeSeriesStart != nil {
		addClause("cheque_series_start", *req.ChequeSeriesStart)
	}
	if req.ChequeSeriesEnd != nil {
		addClause("cheque_series_end", *req.ChequeSeriesEnd)
	}
	if req.BankName != nil {
		addClause("bank_name", *req.BankName)
	}
	if req.BankAccountNumber != nil {
		addClause("bank_account_number", *req.BankAccountNumber)
	}
	if req.BankRoutingNumber != nil {
		addClause("bank_routing_number", *req.BankRoutingNumber)
	}
	if req.CardLast4Digits != nil {
		addClause("card_last_4_digits", *req.CardLast4Digits)
	}
	if req.CardName != nil {
		addClause("card_name", *req.CardName)
	}
	if req.Subtype != nil {
		addClause("subtype", *req.Subtype)
	}
	if req.HolderCategory != nil {
		addClause("holder_category", *req.HolderCategory)
	}
	if req.BalanceAvailable != nil {
		addClause("balance_available", *req.BalanceAvailable)
	}
	if req.BalanceCurrent != nil {
		addClause("balance_current", *req.BalanceCurrent)
	}
	if req.CreditLimit != nil {
		addClause("credit_limit", *req.CreditLimit)
	}
	if req.IsoCurrencyCode != nil {
		addClause("iso_currency_code", *req.IsoCurrencyCode)
	}
	if req.UnofficialCurrencyCode != nil {
		addClause("unofficial_currency_code", *req.UnofficialCurrencyCode)
	}
	if req.MinimumBalance != nil {
		addClause("minimum_balance", *req.MinimumBalance)
	}
	if req.SortingOrder != nil {
		addClause("sorting_order", *req.SortingOrder)
	}
	if req.LocationID != nil {
		addClause("location_id", *req.LocationID)
	}

	// Check if starting_credit_card_bal is being updated
	if req.StartingCreditCardBal != nil {
		addClause("starting_credit_card_bal", *req.StartingCreditCardBal)
		// If the value changed, set is_balance_updated
		if currentStartingBal == nil || *req.StartingCreditCardBal != *currentStartingBal {
			addClause("is_balance_updated", true)
		}
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, pmID)
		query := fmt.Sprintf("UPDATE payment_methods SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update payment method", "error": err.Error()})
			return
		}
	}

	h.getPaymentMethodByID(c, pmID)
}

// DELETE /payment-methods/:payment_method_id
func (h *PaymentMethodHandler) DeletePaymentMethod(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	pmID := c.Param("payment_method_id")

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM payment_methods WHERE id = $1", pmID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete payment method", "error": err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment method not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Payment method deleted successfully"})
}

func (h *PaymentMethodHandler) getPaymentMethodByID(c *gin.Context, id string) {
	var pm paymentMethodResponse
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, plaid_account_id, title, description, method_type,
		 cheque_series_start, cheque_series_end, bank_name, bank_account_number, bank_routing_number,
		 card_last_4_digits, card_name, subtype, holder_category, balance_available, balance_current,
		 credit_limit, iso_currency_code, unofficial_currency_code, total_amount, minimum_balance,
		 starting_credit_card_bal, sorting_order, location_id, created_at, updated_at
		 FROM payment_methods WHERE id = $1`, id,
	).Scan(&pm.ID, &pm.PlaidAccountID, &pm.Title, &pm.Description, &pm.MethodType,
		&pm.ChequeSeriesStart, &pm.ChequeSeriesEnd, &pm.BankName, &pm.BankAccountNumber, &pm.BankRoutingNumber,
		&pm.CardLast4Digits, &pm.CardName, &pm.Subtype, &pm.HolderCategory, &pm.BalanceAvailable, &pm.BalanceCurrent,
		&pm.CreditLimit, &pm.IsoCurrencyCode, &pm.UnofficialCurrencyCode, &pm.TotalAmount, &pm.MinimumBalance,
		&pm.StartingCreditCardBal, &pm.SortingOrder, &pm.LocationID, &pm.CreatedAt, &pm.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment method not found"})
		return
	}

	c.JSON(http.StatusOK, pm)
}
