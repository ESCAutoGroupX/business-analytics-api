package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

const defaultSMSAPIBaseURL = "https://sms-api.carshopanalytics.com"

// ShopIDs maps shop display names to their SMS platform identifiers.
var ShopIDs = map[string]int{
	"Alpharetta":    2708,
	"Piedmont":      3202,
	"Tracy":         3236,
	"Duluth":        3244,
	"Houston":       5513,
	"Roswell":       5528,
	"Cedar Springs": 5802,
	"Preston":       2536,
	"Highlands":     10247,
	"Sandy Springs": 16255,
}

// SMSClient provides authenticated access to the SMS API.
type SMSClient struct {
	BaseURL  string
	Token    string
	TokenExp time.Time
	Cfg      *config.Config
	mu       sync.Mutex
	client   *http.Client
}

// RepairOrder represents a repair order returned by the SMS API.
type RepairOrder struct {
	ID           json.Number  `json:"id"`
	RONumber     string       `json:"ro_number"`
	ShopID       int          `json:"shop_id"`
	Status       string       `json:"status"`
	PostedDate   string       `json:"posted_date"`
	CustomerName string       `json:"customer_name"`
	VehicleInfo  string       `json:"vehicle_info"`
	TotalAmount  float64      `json:"total_amount"`
	LineItems    []ROLineItem `json:"line_items"`
}

// ROLineItem represents a single line item within a repair order.
type ROLineItem struct {
	ID          string  `json:"id"`
	PartNumber  string  `json:"part_number"`
	Description string  `json:"description"`
	Quantity    int     `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	Total       float64 `json:"total"`
	Category    string  `json:"category"`
}

// DailySalesReport represents aggregated daily sales data for a shop.
type DailySalesReport struct {
	Date       string  `json:"date"`
	ShopID     int     `json:"shop_id"`
	TotalSales float64 `json:"total_sales"`
	TotalParts float64 `json:"total_parts"`
	TotalLabor float64 `json:"total_labor"`
	ROCount    int     `json:"ro_count"`
}

// NewSMSClient creates a new SMSClient using the provided configuration.
// If cfg.SMSAPIBaseURL is empty, it defaults to https://sms-api.carshopanalytics.com.
func NewSMSClient(cfg *config.Config) *SMSClient {
	baseURL := cfg.SMSAPIBaseURL
	if baseURL == "" {
		baseURL = defaultSMSAPIBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	return &SMSClient{
		BaseURL: baseURL,
		Cfg:     cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetToken returns a valid bearer token, refreshing it if expired or absent.
// It is safe for concurrent use.
func (c *SMSClient) GetToken() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Token != "" && c.TokenExp.After(time.Now()) {
		return c.Token, nil
	}

	loginURL := fmt.Sprintf("%s/auth/login", c.BaseURL)

	payload := map[string]string{
		"email":    c.Cfg.SMSAPIEmail,
		"password": c.Cfg.SMSAPIPassword,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("sms_api: failed to marshal login payload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, loginURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("sms_api: failed to create login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("sms_api: login request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("sms_api: failed to read login response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("sms_api: login returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("sms_api: failed to parse login response: %w", err)
	}

	if result.Token == "" {
		return "", fmt.Errorf("sms_api: login response contained no token")
	}

	c.Token = result.Token
	c.TokenExp = time.Now().Add(55 * time.Minute)

	log.Printf("sms_api: obtained new auth token, expires at %s", c.TokenExp.Format(time.RFC3339))

	return c.Token, nil
}

// GetPostedROs fetches repair orders with status "posted" for a given shop and date range.
func (c *SMSClient) GetPostedROs(shopID int, from, to time.Time) ([]RepairOrder, error) {
	token, err := c.GetToken()
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/repair-orders?shop_id=%d&status=posted&posted_date_start=%s&posted_date_end=%s",
		c.BaseURL, shopID, from.Format("2006-01-02"), to.Format("2006-01-02"))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to create GetPostedROs request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sms_api: GetPostedROs request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to read GetPostedROs response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sms_api: GetPostedROs returned status %d: %s", resp.StatusCode, string(respBody))
	}

	// The API may return {"data": [...]} or a bare array [...].
	var ros []RepairOrder

	var wrapped struct {
		Data []RepairOrder `json:"data"`
	}
	if err := json.Unmarshal(respBody, &wrapped); err == nil && wrapped.Data != nil {
		ros = wrapped.Data
	} else if err := json.Unmarshal(respBody, &ros); err != nil {
		return nil, fmt.Errorf("sms_api: failed to parse GetPostedROs response: %w", err)
	}

	return ros, nil
}

// GetRODetail fetches the full detail for a single repair order by ID.
func (c *SMSClient) GetRODetail(roID string) (*RepairOrder, error) {
	token, err := c.GetToken()
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/repair-orders/%s", c.BaseURL, roID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to create GetRODetail request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sms_api: GetRODetail request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to read GetRODetail response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sms_api: GetRODetail returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var ro RepairOrder
	if err := json.Unmarshal(respBody, &ro); err != nil {
		return nil, fmt.Errorf("sms_api: failed to parse GetRODetail response: %w", err)
	}

	return &ro, nil
}

// GetDailySales fetches the daily sales report for a given shop and date.
func (c *SMSClient) GetDailySales(shopID int, date time.Time) (*DailySalesReport, error) {
	token, err := c.GetToken()
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/reports/daily-sales?shop_id=%d&date=%s",
		c.BaseURL, shopID, date.Format("2006-01-02"))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to create GetDailySales request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sms_api: GetDailySales request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sms_api: failed to read GetDailySales response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sms_api: GetDailySales returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var report DailySalesReport
	if err := json.Unmarshal(respBody, &report); err != nil {
		return nil, fmt.Errorf("sms_api: failed to parse GetDailySales response: %w", err)
	}

	return &report, nil
}
