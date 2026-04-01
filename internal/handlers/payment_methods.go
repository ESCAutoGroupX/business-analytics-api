package handlers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type PaymentMethodHandler struct {
	GormDB *gorm.DB
}

type paymentMethodCreateRequest struct {
	PlaidAccountID         *string  `json:"plaid_account_id"`
	Title                  *string  `json:"title"`
	Description            *string  `json:"description"`
	MethodType             string   `json:"method_type" binding:"required"`
	ChequeSeriesStart      *string  `json:"cheque_series_start"`
	ChequeSeriesEnd        *string  `json:"cheque_series_end"`
	BankName               *string  `json:"bank_name"`
	BankAccountNumber      *string  `json:"bank_account_number"`
	BankRoutingNumber      *string  `json:"bank_routing_number"`
	CardLast4Digits        *string  `json:"card_last_4_digits"`
	CardName               *string  `json:"card_name"`
	Subtype                *string  `json:"subtype"`
	HolderCategory         *string  `json:"holder_category"`
	BalanceAvailable       *float64 `json:"balance_available"`
	BalanceCurrent         *float64 `json:"balance_current"`
	CreditLimit            *float64 `json:"credit_limit"`
	IsoCurrencyCode        *string  `json:"iso_currency_code"`
	UnofficialCurrencyCode *string  `json:"unofficial_currency_code"`
	MinimumBalance         *float64 `json:"minimum_balance"`
	StartingCreditCardBal  *float64 `json:"starting_credit_card_bal"`
	SortingOrder           *int     `json:"sorting_order"`
	LocationID             *string  `json:"location_id"`
}

type paymentMethodUpdateRequest struct {
	Title                  *string  `json:"title"`
	Description            *string  `json:"description"`
	MethodType             *string  `json:"method_type"`
	ChequeSeriesStart      *string  `json:"cheque_series_start"`
	ChequeSeriesEnd        *string  `json:"cheque_series_end"`
	BankName               *string  `json:"bank_name"`
	BankAccountNumber      *string  `json:"bank_account_number"`
	BankRoutingNumber      *string  `json:"bank_routing_number"`
	CardLast4Digits        *string  `json:"card_last_4_digits"`
	CardName               *string  `json:"card_name"`
	Subtype                *string  `json:"subtype"`
	HolderCategory         *string  `json:"holder_category"`
	BalanceAvailable       *float64 `json:"balance_available"`
	BalanceCurrent         *float64 `json:"balance_current"`
	CreditLimit            *float64 `json:"credit_limit"`
	IsoCurrencyCode        *string  `json:"iso_currency_code"`
	UnofficialCurrencyCode *string  `json:"unofficial_currency_code"`
	MinimumBalance         *float64 `json:"minimum_balance"`
	StartingCreditCardBal  *float64 `json:"starting_credit_card_bal"`
	SortingOrder           *int     `json:"sorting_order"`
	LocationID             *string  `json:"location_id"`
}

type paymentMethodResponse struct {
	ID                     string     `json:"id"`
	PlaidAccountID         *string    `json:"plaid_account_id"`
	Title                  *string    `json:"title"`
	Description            *string    `json:"description"`
	MethodType             *string    `json:"method_type"`
	ChequeSeriesStart      *string    `json:"cheque_series_start"`
	ChequeSeriesEnd        *string    `json:"cheque_series_end"`
	BankName               *string    `json:"bank_name"`
	BankAccountNumber      *string    `json:"bank_account_number"`
	BankRoutingNumber      *string    `json:"bank_routing_number"`
	CardLast4Digits        *string    `json:"card_last_4_digits"`
	CardName               *string    `json:"card_name"`
	Subtype                *string    `json:"subtype"`
	HolderCategory         *string    `json:"holder_category"`
	BalanceAvailable       *float64   `json:"balance_available"`
	BalanceCurrent         *float64   `json:"balance_current"`
	CreditLimit            *float64   `json:"credit_limit"`
	IsoCurrencyCode        *string    `json:"iso_currency_code"`
	UnofficialCurrencyCode *string    `json:"unofficial_currency_code"`
	TotalAmount            *float64   `json:"total_amount"`
	MinimumBalance         *float64   `json:"minimum_balance"`
	StartingCreditCardBal  *float64   `json:"starting_credit_card_bal"`
	SortingOrder           *int       `json:"sorting_order"`
	LocationID             *string    `json:"location_id"`
	CreatedAt              *time.Time `json:"created_at"`
	UpdatedAt              *time.Time `json:"updated_at"`
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

func pmToResponse(pm *models.PaymentMethod) paymentMethodResponse {
	return paymentMethodResponse{
		ID:                     pm.ID,
		PlaidAccountID:         pm.PlaidAccountID,
		Title:                  pm.Title,
		Description:            pm.Description,
		MethodType:             &pm.MethodType,
		ChequeSeriesStart:      pm.ChequeSeriesStart,
		ChequeSeriesEnd:        pm.ChequeSeriesEnd,
		BankName:               pm.BankName,
		BankAccountNumber:      pm.BankAccountNumber,
		BankRoutingNumber:      pm.BankRoutingNumber,
		CardLast4Digits:        pm.CardLast4Digits,
		CardName:               pm.CardName,
		Subtype:                pm.Subtype,
		HolderCategory:         pm.HolderCategory,
		BalanceAvailable:       pm.BalanceAvailable,
		BalanceCurrent:         pm.BalanceCurrent,
		CreditLimit:            pm.CreditLimit,
		IsoCurrencyCode:        pm.IsoCurrencyCode,
		UnofficialCurrencyCode: pm.UnofficialCurrencyCode,
		TotalAmount:            pm.TotalAmount,
		MinimumBalance:         pm.MinimumBalance,
		StartingCreditCardBal:  pm.StartingCreditCardBal,
		SortingOrder:           pm.SortingOrder,
		LocationID:             pm.LocationID,
		CreatedAt:              &pm.CreatedAt,
		UpdatedAt:              &pm.UpdatedAt,
	}
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

	pm := models.PaymentMethod{
		ID:                     uuid.New().String(),
		PlaidAccountID:         req.PlaidAccountID,
		Title:                  req.Title,
		Description:            req.Description,
		MethodType:             methodType,
		ChequeSeriesStart:      req.ChequeSeriesStart,
		ChequeSeriesEnd:        req.ChequeSeriesEnd,
		BankName:               req.BankName,
		BankAccountNumber:      req.BankAccountNumber,
		BankRoutingNumber:      req.BankRoutingNumber,
		CardLast4Digits:        req.CardLast4Digits,
		CardName:               req.CardName,
		Subtype:                req.Subtype,
		HolderCategory:         req.HolderCategory,
		BalanceAvailable:       req.BalanceAvailable,
		BalanceCurrent:         req.BalanceCurrent,
		CreditLimit:            req.CreditLimit,
		IsoCurrencyCode:        req.IsoCurrencyCode,
		UnofficialCurrencyCode: req.UnofficialCurrencyCode,
		MinimumBalance:         req.MinimumBalance,
		StartingCreditCardBal:  req.StartingCreditCardBal,
		SortingOrder:           req.SortingOrder,
		LocationID:             req.LocationID,
	}

	if err := h.GormDB.Create(&pm).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create payment method", "error": err.Error()})
		return
	}

	h.getPaymentMethodByID(c, pm.ID)
}

// GET /payment-methods/
func (h *PaymentMethodHandler) ListPaymentMethods(c *gin.Context) {
	var methods []models.PaymentMethod
	if err := h.GormDB.Order("created_at DESC").Find(&methods).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}

	result := make([]paymentMethodResponse, len(methods))
	for i := range methods {
		result[i] = pmToResponse(&methods[i])
	}

	c.JSON(http.StatusOK, result)
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
	var existing models.PaymentMethod
	if err := h.GormDB.First(&existing, "id = ?", pmID).Error; err != nil {
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

	updates := map[string]interface{}{}

	if req.Title != nil {
		updates["title"] = *req.Title
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.MethodType != nil {
		updates["method_type"] = *req.MethodType
	}
	if req.ChequeSeriesStart != nil {
		updates["cheque_series_start"] = *req.ChequeSeriesStart
	}
	if req.ChequeSeriesEnd != nil {
		updates["cheque_series_end"] = *req.ChequeSeriesEnd
	}
	if req.BankName != nil {
		updates["bank_name"] = *req.BankName
	}
	if req.BankAccountNumber != nil {
		updates["bank_account_number"] = *req.BankAccountNumber
	}
	if req.BankRoutingNumber != nil {
		updates["bank_routing_number"] = *req.BankRoutingNumber
	}
	if req.CardLast4Digits != nil {
		updates["card_last_4_digits"] = *req.CardLast4Digits
	}
	if req.CardName != nil {
		updates["card_name"] = *req.CardName
	}
	if req.Subtype != nil {
		updates["subtype"] = *req.Subtype
	}
	if req.HolderCategory != nil {
		updates["holder_category"] = *req.HolderCategory
	}
	if req.BalanceAvailable != nil {
		updates["balance_available"] = *req.BalanceAvailable
	}
	if req.BalanceCurrent != nil {
		updates["balance_current"] = *req.BalanceCurrent
	}
	if req.CreditLimit != nil {
		updates["credit_limit"] = *req.CreditLimit
	}
	if req.IsoCurrencyCode != nil {
		updates["iso_currency_code"] = *req.IsoCurrencyCode
	}
	if req.UnofficialCurrencyCode != nil {
		updates["unofficial_currency_code"] = *req.UnofficialCurrencyCode
	}
	if req.MinimumBalance != nil {
		updates["minimum_balance"] = *req.MinimumBalance
	}
	if req.SortingOrder != nil {
		updates["sorting_order"] = *req.SortingOrder
	}
	if req.LocationID != nil {
		updates["location_id"] = *req.LocationID
	}

	// Check if starting_credit_card_bal is being updated
	if req.StartingCreditCardBal != nil {
		updates["starting_credit_card_bal"] = *req.StartingCreditCardBal
		// If the value changed, set is_balance_updated
		if existing.StartingCreditCardBal == nil || *req.StartingCreditCardBal != *existing.StartingCreditCardBal {
			updates["is_balance_updated"] = true
		}
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&existing).Updates(updates).Error; err != nil {
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

	result := h.GormDB.Delete(&models.PaymentMethod{}, "id = ?", pmID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete payment method", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Payment method not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Payment method deleted successfully"})
}

func (h *PaymentMethodHandler) getPaymentMethodByID(c *gin.Context, id string) {
	var pm models.PaymentMethod
	if err := h.GormDB.First(&pm, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Payment method not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment method", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, pmToResponse(&pm))
}
