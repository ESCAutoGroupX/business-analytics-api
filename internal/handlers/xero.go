package handlers

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type XeroHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

const xeroScopes = "openid profile email offline_access accounting.invoices accounting.payments accounting.banktransactions accounting.contacts accounting.settings accounting.reports.read accounting.manualjournals accounting.attachments accounting.budgets assets"

// GET /xero/authorize?user_id=XXX — redirects to Xero OAuth consent screen
func (h *XeroHandler) Authorize(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id query parameter is required"})
		return
	}

	params := url.Values{
		"response_type": {"code"},
		"client_id":     {h.Cfg.XeroClientID},
		"redirect_uri":  {h.Cfg.XeroRedirectURI},
		"scope":         {xeroScopes},
		"state":         {userID},
	}

	authURL := "https://login.xero.com/identity/connect/authorize?" + params.Encode()
	c.Redirect(http.StatusFound, authURL)
}

// GET /xero/callback?code=XXX&state=XXX — handles OAuth callback from Xero
func (h *XeroHandler) Callback(c *gin.Context) {
	code := c.Query("code")
	userID := c.Query("state")

	if code == "" || userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "missing code or state"})
		return
	}

	// Exchange code for tokens
	tokenData, err := h.exchangeCode(code)
	if err != nil {
		log.Printf("Xero token exchange error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to exchange code", "error": err.Error()})
		return
	}

	accessToken, _ := tokenData["access_token"].(string)
	refreshToken, _ := tokenData["refresh_token"].(string)
	expiresIn, _ := tokenData["expires_in"].(float64)

	if accessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "no access_token in response"})
		return
	}

	expiresAt := time.Now().Add(time.Duration(expiresIn) * time.Second)

	// Get tenant list from Xero connections API
	tenants, err := h.fetchTenants(accessToken)
	if err != nil {
		log.Printf("Xero fetch tenants error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to fetch tenants", "error": err.Error()})
		return
	}

	// Save each tenant as a connection
	for _, tenant := range tenants {
		tenantID, _ := tenant["tenantId"].(string)
		tenantName, _ := tenant["tenantName"].(string)
		if tenantID == "" {
			continue
		}

		conn := models.XeroConnection{
			UserID:       userID,
			TenantID:     tenantID,
			TenantName:   tenantName,
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
			ExpiresAt:    expiresAt,
			Scopes:       xeroScopes,
		}

		// Upsert: update tokens if tenant already exists for this user
		var existing models.XeroConnection
		if err := h.GormDB.Where("tenant_id = ?", tenantID).First(&existing).Error; err == nil {
			existing.AccessToken = accessToken
			existing.RefreshToken = refreshToken
			existing.ExpiresAt = expiresAt
			existing.UserID = userID
			existing.TenantName = tenantName
			h.GormDB.Save(&existing)
		} else {
			if err := h.GormDB.Create(&conn).Error; err != nil {
				log.Printf("Xero save connection error: %v", err)
			}
		}
	}

	// Redirect user back to frontend settings
	frontendURL := h.Cfg.FrontendURL
	if frontendURL == "" {
		frontendURL = "https://businessanalyticsinc.com"
	}
	c.Redirect(http.StatusFound, frontendURL+"/settings?xero=connected")
}

// GET /xero/connections — list all connections for the current user
func (h *XeroHandler) ListConnections(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var connections []models.XeroConnection
	if err := h.GormDB.Where("user_id = ?", uid).Order("created_at DESC").Find(&connections).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query xero connections"})
		return
	}

	c.JSON(http.StatusOK, connections)
}

// DELETE /xero/connections/:id — remove a connection
func (h *XeroHandler) DeleteConnection(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid id"})
		return
	}

	var conn models.XeroConnection
	if err := h.GormDB.Where("id = ? AND user_id = ?", id, uid).First(&conn).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "xero connection not found"})
		return
	}

	if err := h.GormDB.Delete(&conn).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete xero connection"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Xero connection removed"})
}

// POST /xero/refresh — refresh token for a connection
func (h *XeroHandler) RefreshToken(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var req struct {
		ConnectionID int `json:"connection_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var conn models.XeroConnection
	if err := h.GormDB.Where("id = ? AND user_id = ?", req.ConnectionID, uid).First(&conn).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "xero connection not found"})
		return
	}

	tokenData, err := h.refreshToken(conn.RefreshToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to refresh token", "error": err.Error()})
		return
	}

	accessToken, _ := tokenData["access_token"].(string)
	refreshToken, _ := tokenData["refresh_token"].(string)
	expiresIn, _ := tokenData["expires_in"].(float64)

	if accessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "no access_token in refresh response"})
		return
	}

	conn.AccessToken = accessToken
	conn.RefreshToken = refreshToken
	conn.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)

	if err := h.GormDB.Save(&conn).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update tokens"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Token refreshed", "expires_at": conn.ExpiresAt})
}

// ── Helpers ────────────────────────────────────────────

func (h *XeroHandler) basicAuthHeader() string {
	creds := h.Cfg.XeroClientID + ":" + h.Cfg.XeroClientSecret
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
}

func (h *XeroHandler) exchangeCode(code string) (map[string]interface{}, error) {
	data := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {h.Cfg.XeroRedirectURI},
	}

	req, err := http.NewRequest("POST", "https://identity.xero.com/connect/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", h.basicAuthHeader())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero token exchange failed (%d): %s", resp.StatusCode, string(body))
	}

	return result, nil
}

func (h *XeroHandler) refreshToken(refreshToken string) (map[string]interface{}, error) {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	}

	req, err := http.NewRequest("POST", "https://identity.xero.com/connect/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", h.basicAuthHeader())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero token refresh failed (%d): %s", resp.StatusCode, string(body))
	}

	return result, nil
}

func (h *XeroHandler) fetchTenants(accessToken string) ([]map[string]interface{}, error) {
	req, err := http.NewRequest("GET", "https://api.xero.com/connections", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero connections request failed (%d): %s", resp.StatusCode, string(body))
	}

	// Xero returns either an array of tenants or a single object
	var tenants []map[string]interface{}
	if err := json.Unmarshal(body, &tenants); err != nil {
		// Try single object
		var single map[string]interface{}
		if err2 := json.Unmarshal(body, &single); err2 != nil {
			return nil, fmt.Errorf("failed to parse tenants response: %v", err)
		}
		tenants = []map[string]interface{}{single}
	}

	return tenants, nil
}

// xeroAPIRequest makes an authenticated request to the Xero API with tenant header.
// Exported for use by other handlers that need to call Xero endpoints.
func (h *XeroHandler) XeroAPIRequest(method, endpoint string, tenantID, accessToken string, reqBody interface{}) ([]byte, error) {
	var bodyReader io.Reader
	if reqBody != nil {
		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, "https://api.xero.com/api.xro/2.0"+endpoint, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Xero-Tenant-Id", tenantID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero API error (%d): %s", resp.StatusCode, string(body))
	}

	return body, nil
}
