package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

type PlaidHandler struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
}

type publicTokenRequest struct {
	PublicToken string `json:"public_token" binding:"required"`
	UserID      string `json:"user_id" binding:"required"`
}

type transactionsRequest struct {
	StartDate string `json:"start_date" binding:"required"`
	EndDate   string `json:"end_date" binding:"required"`
}

type linkTokenRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

func (h *PlaidHandler) plaidBaseURL() string {
	env := h.Cfg.PlaidEnv
	switch env {
	case "production":
		return "https://production.plaid.com"
	case "development":
		return "https://development.plaid.com"
	default:
		return "https://sandbox.plaid.com"
	}
}

func (h *PlaidHandler) plaidRequest(endpoint string, body interface{}) (map[string]interface{}, error) {
	payload := map[string]interface{}{
		"client_id": h.Cfg.PlaidClientID,
		"secret":    h.Cfg.PlaidSecret,
	}
	if bodyMap, ok := body.(map[string]interface{}); ok {
		for k, v := range bodyMap {
			payload[k] = v
		}
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(h.plaidBaseURL()+endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		detail := "Plaid API error"
		if msg, ok := result["error_message"].(string); ok {
			detail = msg
		}
		return nil, fmt.Errorf("%s", detail)
	}

	return result, nil
}

// POST /plaid/exchange_public_token
func (h *PlaidHandler) ExchangePublicToken(c *gin.Context) {
	var req publicTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	result, err := h.plaidRequest("/item/public_token/exchange", map[string]interface{}{
		"public_token": req.PublicToken,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	accessToken, _ := result["access_token"].(string)

	// Verify user exists
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", req.UserID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	_, err = h.DB.Exec(context.Background(),
		"UPDATE users SET plaid_access_token = $1 WHERE id = $2", accessToken, req.UserID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to store access token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Successfully linked bank account"})
}

// POST /plaid/fetch_transactions
func (h *PlaidHandler) FetchTransactions(c *gin.Context) {
	var req transactionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var accessToken *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT plaid_access_token FROM users WHERE id = $1", uid).Scan(&accessToken)
	if err != nil || accessToken == nil || *accessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Plaid access token not found for the user"})
		return
	}

	result, err := h.plaidRequest("/transactions/get", map[string]interface{}{
		"access_token": *accessToken,
		"start_date":   req.StartDate,
		"end_date":     req.EndDate,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GET /plaid/sync_transactions
func (h *PlaidHandler) SyncTransactions(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var accessToken *string
	var cursor *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT plaid_access_token, plaid_cursor FROM users WHERE id = $1", uid).Scan(&accessToken, &cursor)
	if err != nil || accessToken == nil || *accessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Plaid access token not found for the user"})
		return
	}

	reqBody := map[string]interface{}{
		"access_token": *accessToken,
	}
	if cursor != nil && *cursor != "" {
		reqBody["cursor"] = *cursor
	}

	result, err := h.plaidRequest("/transactions/sync", reqBody)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	// Update cursor
	if newCursor, ok := result["next_cursor"].(string); ok && newCursor != "" {
		h.DB.Exec(context.Background(),
			"UPDATE users SET plaid_cursor = $1 WHERE id = $2", newCursor, uid)
	}

	c.JSON(http.StatusOK, result)
}

// POST /plaid/link-token
func (h *PlaidHandler) CreateLinkToken(c *gin.Context) {
	var req linkTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Verify user exists
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", req.UserID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	result, err := h.plaidRequest("/link/token/create", map[string]interface{}{
		"user":             map[string]string{"client_user_id": req.UserID},
		"products":         []string{"transactions"},
		"country_codes":    []string{"US"},
		"language":         "en",
		"client_name":      "Business Analytics",
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	linkToken, _ := result["link_token"].(string)
	c.JSON(http.StatusOK, gin.H{"link_token": linkToken})
}

// POST /plaid/sandbox/connect-bank
func (h *PlaidHandler) SandboxConnectBank(c *gin.Context) {
	uid := c.Query("user_id")
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id query parameter is required"})
		return
	}

	// Create sandbox public token
	sandboxResult, err := h.plaidRequest("/sandbox/public_token/create", map[string]interface{}{
		"institution_id":   "ins_109508",
		"initial_products": []string{"transactions", "auth"},
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	publicToken, _ := sandboxResult["public_token"].(string)

	// Exchange for access token
	exchangeResult, err := h.plaidRequest("/item/public_token/exchange", map[string]interface{}{
		"public_token": publicToken,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	accessToken, _ := exchangeResult["access_token"].(string)

	// Store in DB
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	h.DB.Exec(context.Background(),
		"UPDATE users SET plaid_access_token = $1 WHERE id = $2", accessToken, uid)

	c.JSON(http.StatusOK, gin.H{
		"message":      "Sandbox bank account linked",
		"access_token": accessToken,
	})
}
