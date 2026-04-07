package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type PlaidHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
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
	itemID, _ := result["item_id"].(string)

	// Verify user exists
	var user models.User
	if err := h.GormDB.First(&user, "id = ?", req.UserID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Look up institution name via /item/get
	institutionName := "Unknown"
	institutionID := ""
	itemResult, itemErr := h.plaidRequest("/item/get", map[string]interface{}{
		"access_token": accessToken,
	})
	if itemErr == nil {
		if item, ok := itemResult["item"].(map[string]interface{}); ok {
			if instID, ok := item["institution_id"].(string); ok {
				institutionID = instID
				// Try to get institution name
				instResult, instErr := h.plaidRequest("/institutions/get_by_id", map[string]interface{}{
					"institution_id": instID,
					"country_codes":  []string{"US"},
				})
				if instErr == nil {
					if inst, ok := instResult["institution"].(map[string]interface{}); ok {
						if name, ok := inst["name"].(string); ok {
							institutionName = name
						}
					}
				}
			}
		}
	}

	// Save to plaid_items table
	plaidItem := models.PlaidItem{
		UserID:          req.UserID,
		ItemID:          itemID,
		AccessToken:     accessToken,
		InstitutionID:   institutionID,
		InstitutionName: institutionName,
	}
	if err := h.GormDB.Create(&plaidItem).Error; err != nil {
		log.Printf("ERROR saving plaid_item: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to store plaid item", "error": err.Error()})
		return
	}

	// Also update users.plaid_access_token for backwards compatibility
	if err := h.GormDB.Model(&user).Update("plaid_access_token", accessToken).Error; err != nil {
		log.Printf("ERROR updating user plaid_access_token: %v", err)
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

	// Query all plaid_items (company-wide — all users see the same transactions)
	var items []models.PlaidItem
	if err := h.GormDB.Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	// Fallback: if no plaid_items, try legacy tokens from users table
	if len(items) == 0 {
		var users []models.User
		if err := h.GormDB.Select("plaid_access_token").Where("plaid_access_token IS NOT NULL AND plaid_access_token != ''").Find(&users).Error; err != nil || len(users) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "No Plaid access tokens found"})
			return
		}
		for _, u := range users {
			items = append(items, models.PlaidItem{AccessToken: *u.PlaidAccessToken})
		}
	}

	var allAccounts []interface{}
	var allTransactions []interface{}

	for _, item := range items {
		result, err := h.plaidRequest("/transactions/get", map[string]interface{}{
			"access_token": item.AccessToken,
			"start_date":   req.StartDate,
			"end_date":     req.EndDate,
		})
		if err != nil {
			log.Printf("WARN: failed to fetch transactions for item %s: %v", item.ItemID, err)
			continue
		}

		if accounts, ok := result["accounts"].([]interface{}); ok {
			allAccounts = append(allAccounts, accounts...)
		}
		if transactions, ok := result["transactions"].([]interface{}); ok {
			allTransactions = append(allTransactions, transactions...)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"accounts":     allAccounts,
		"transactions": allTransactions,
	})
}

// GET /plaid/sync_transactions
func (h *PlaidHandler) SyncTransactions(c *gin.Context) {
	// Query all plaid_items (company-wide — all users see the same transactions)
	var items []models.PlaidItem
	if err := h.GormDB.Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	// Fallback: if no plaid_items, try legacy tokens from users table
	if len(items) == 0 {
		var users []models.User
		if err := h.GormDB.Select("id, plaid_access_token, plaid_cursor").Where("plaid_access_token IS NOT NULL AND plaid_access_token != ''").Find(&users).Error; err != nil || len(users) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "No Plaid access tokens found"})
			return
		}
		for _, u := range users {
			cursor := ""
			if u.PlaidCursor != nil {
				cursor = *u.PlaidCursor
			}
			items = append(items, models.PlaidItem{UserID: fmt.Sprintf("%v", u.ID), AccessToken: *u.PlaidAccessToken, Cursor: cursor})
		}
	}

	var allAdded []interface{}
	var allModified []interface{}
	var allRemoved []interface{}

	for i, item := range items {
		reqBody := map[string]interface{}{
			"access_token": item.AccessToken,
		}
		if item.Cursor != "" {
			reqBody["cursor"] = item.Cursor
		}

		result, err := h.plaidRequest("/transactions/sync", reqBody)
		if err != nil {
			log.Printf("WARN: failed to sync transactions for item %s: %v", item.ItemID, err)
			continue
		}

		if added, ok := result["added"].([]interface{}); ok {
			allAdded = append(allAdded, added...)
		}
		if modified, ok := result["modified"].([]interface{}); ok {
			allModified = append(allModified, modified...)
		}
		if removed, ok := result["removed"].([]interface{}); ok {
			allRemoved = append(allRemoved, removed...)
		}

		// Update cursor for this item
		if newCursor, ok := result["next_cursor"].(string); ok && newCursor != "" {
			if item.ID > 0 {
				h.GormDB.Model(&items[i]).Update("cursor", newCursor)
			} else {
				// Legacy fallback — update users table
				h.GormDB.Model(&models.User{}).Where("id = ?", item.UserID).Update("plaid_cursor", newCursor)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"added":    allAdded,
		"modified": allModified,
		"removed":  allRemoved,
	})
}

// POST /plaid/link-token
func (h *PlaidHandler) CreateLinkToken(c *gin.Context) {
	var req linkTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Verify user exists
	var count int64
	h.GormDB.Model(&models.User{}).Where("id = ?", req.UserID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	result, err := h.plaidRequest("/link/token/create", map[string]interface{}{
		"user":          map[string]string{"client_user_id": req.UserID},
		"products":      []string{"transactions"},
		"country_codes": []string{"US"},
		"language":      "en",
		"client_name":   "Business Analytics",
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	linkToken, _ := result["link_token"].(string)
	c.JSON(http.StatusOK, gin.H{"link_token": linkToken})
}

// GET /plaid/items — list all connected Plaid items for the current user
func (h *PlaidHandler) ListPlaidItems(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var items []models.PlaidItem
	if err := h.GormDB.Where("user_id = ?", uid).Order("created_at DESC").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	c.JSON(http.StatusOK, items)
}

// DELETE /plaid/items/:id — disconnect a Plaid item
func (h *PlaidHandler) DeletePlaidItem(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	itemID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid item id"})
		return
	}

	var item models.PlaidItem
	if err := h.GormDB.Where("id = ? AND user_id = ?", itemID, uid).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "plaid item not found"})
		return
	}

	if err := h.GormDB.Delete(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete plaid item"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Plaid item disconnected"})
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
	itemID, _ := exchangeResult["item_id"].(string)

	// Store in DB
	var user models.User
	if err := h.GormDB.First(&user, "id = ?", uid).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Save to plaid_items table
	plaidItem := models.PlaidItem{
		UserID:          uid,
		ItemID:          itemID,
		AccessToken:     accessToken,
		InstitutionName: "First Platypus Bank (Sandbox)",
	}
	if err := h.GormDB.Create(&plaidItem).Error; err != nil {
		log.Printf("ERROR saving sandbox plaid_item: %v", err)
	}

	// Also update users table for backwards compatibility
	h.GormDB.Model(&user).Update("plaid_access_token", accessToken)

	c.JSON(http.StatusOK, gin.H{
		"message":      "Sandbox bank account linked",
		"access_token": accessToken,
	})
}

