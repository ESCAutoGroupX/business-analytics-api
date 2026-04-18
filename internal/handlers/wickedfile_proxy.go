package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// WickedFileProxyHandler fronts alpha.wickedfile.com by replaying the
// operator's browser session cookies stored in integration_settings.
type WickedFileProxyHandler struct {
	GormDB *gorm.DB

	client   *http.Client
	mu       sync.Mutex
	lastCall time.Time
}

func NewWickedFileProxyHandler(db *gorm.DB) *WickedFileProxyHandler {
	return &WickedFileProxyHandler{
		GormDB: db,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

const (
	wfBaseURL      = "https://alpha.wickedfile.com"
	wfExpiredMsg   = "WickedFile session expired - please update cookies in Settings"
	wfKeyAWSALB    = "wickedfile_awsalb"
	wfKeyAWSALBCOR = "wickedfile_awsalbcors"
	wfKeyToken     = "wickedfile_token"
	wfKeyExpires   = "wickedfile_cookie_expires"
	wfMinGap       = 500 * time.Millisecond
)

type wfCookies struct {
	AWSALB     string
	AWSALBCORS string
	Token      string
	Expires    time.Time
	HasExpires bool
}

func (h *WickedFileProxyHandler) readSetting(key string) string {
	var val string
	h.GormDB.Raw("SELECT value FROM integration_settings WHERE key = ?", key).Scan(&val)
	return val
}

func (h *WickedFileProxyHandler) loadCookies() (wfCookies, bool) {
	c := wfCookies{
		AWSALB:     h.readSetting(wfKeyAWSALB),
		AWSALBCORS: h.readSetting(wfKeyAWSALBCOR),
		Token:      h.readSetting(wfKeyToken),
	}
	if exp := h.readSetting(wfKeyExpires); exp != "" {
		if t, err := time.Parse(time.RFC3339Nano, exp); err == nil {
			c.Expires = t
			c.HasExpires = true
		} else if t, err := time.Parse(time.RFC3339, exp); err == nil {
			c.Expires = t
			c.HasExpires = true
		}
	}
	valid := c.AWSALB != "" && c.AWSALBCORS != "" && c.Token != "" &&
		(!c.HasExpires || c.Expires.After(time.Now()))
	return c, valid
}

// throttle enforces wfMinGap between outbound WickedFile calls.
func (h *WickedFileProxyHandler) throttle() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.lastCall.IsZero() {
		if d := time.Since(h.lastCall); d < wfMinGap {
			time.Sleep(wfMinGap - d)
		}
	}
	h.lastCall = time.Now()
}

func (h *WickedFileProxyHandler) newRequest(method, url string, body io.Reader, c wfCookies) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", fmt.Sprintf("AWSALB=%s; AWSALBCORS=%s; token=%s", c.AWSALB, c.AWSALBCORS, c.Token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; business-analytics-api)")
	req.Header.Set("Accept", "*/*")
	return req, nil
}

// StreamPDF fetches a WickedFile scan-page PDF and streams the response body
// into the given gin.Context. Returns nil on success, or an error if the
// session is expired / upstream failed. Callers that have already written
// a response on error should check the returned value rather than double-write.
func (h *WickedFileProxyHandler) StreamPDF(c *gin.Context, scanPageID string) error {
	cookies, valid := h.loadCookies()
	if !valid {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": wfExpiredMsg})
		return fmt.Errorf("wf session invalid")
	}
	h.throttle()

	url := fmt.Sprintf("%s/store/scan/image/%s", wfBaseURL, scanPageID)
	req, err := h.newRequest(http.MethodGet, url, nil, cookies)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": wfExpiredMsg})
		return fmt.Errorf("wf upstream %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("WickedFile returned %d", resp.StatusCode)})
		return fmt.Errorf("wf upstream %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/pdf"
	}
	c.Header("Content-Type", ct)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		c.Header("Content-Length", cl)
	}
	c.Status(http.StatusOK)
	if _, err := io.Copy(c.Writer, resp.Body); err != nil {
		log.Printf("WF proxy: copy error for scan %s: %v", scanPageID, err)
		return err
	}
	return nil
}

// GET /wf/document/:scanPageId/pdf
func (h *WickedFileProxyHandler) ProxyPDF(c *gin.Context) {
	scanPageID := c.Param("scanPageId")
	if scanPageID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "scanPageId required"})
		return
	}
	_ = h.StreamPDF(c, scanPageID)
}

// GET /wf/document/:scanPageId/metadata
func (h *WickedFileProxyHandler) ProxyMetadata(c *gin.Context) {
	scanPageID := c.Param("scanPageId")
	if scanPageID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "scanPageId required"})
		return
	}
	cookies, valid := h.loadCookies()
	if !valid {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": wfExpiredMsg})
		return
	}
	h.throttle()

	bodyJSON, _ := json.Marshal(map[string]string{"scanPageId": scanPageID})
	url := wfBaseURL + "/store/scan_wrap/get_scan_page"
	req, err := h.newRequest(http.MethodPost, url, bytes.NewReader(bodyJSON), cookies)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": wfExpiredMsg})
		return
	}
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return
	}
	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("WickedFile returned %d: %s", resp.StatusCode, string(respBytes))})
		return
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": "invalid WickedFile response: " + err.Error()})
		return
	}
	h.cacheMetadata(scanPageID, parsed)
	c.JSON(http.StatusOK, parsed)
}

// cacheMetadata writes the interesting fields from a WickedFile get_scan_page
// response back onto the matching documents row keyed by wf_scan_id.
func (h *WickedFileProxyHandler) cacheMetadata(scanPageID string, parsed map[string]interface{}) {
	scanPage, _ := parsed["scanPage"].(map[string]interface{})
	if scanPage == nil {
		scanPage = parsed
	}
	updates := map[string]interface{}{}

	pickString := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := scanPage[k].(string); ok && v != "" {
				return v
			}
		}
		return ""
	}
	pickFloat := func(keys ...string) (float64, bool) {
		for _, k := range keys {
			switch v := scanPage[k].(type) {
			case float64:
				return v, true
			case string:
				if v == "" {
					continue
				}
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					return f, true
				}
			}
		}
		return 0, false
	}

	if s := pickString("s3Key", "s3_key", "s3key"); s != "" {
		updates["wf_s3_key"] = s
	}
	if v := pickString("vendorName", "vendor_name", "vendor"); v != "" {
		updates["vendor_name"] = v
	}
	if amt, ok := pickFloat("totalAmount", "total_amount", "total"); ok {
		updates["total_amount"] = amt
	}

	if len(updates) == 0 {
		return
	}
	if err := h.GormDB.Model(&models.Document{}).
		Where("wf_scan_id = ?", scanPageID).
		Updates(updates).Error; err != nil {
		log.Printf("WF metadata cache: %v", err)
	}
}

// PUT /settings/wickedfile-cookies
type wfCookiesBody struct {
	AWSALB     string `json:"awsalb"`
	AWSALBCORS string `json:"awsalbcors"`
	Token      string `json:"token"`
	Expires    string `json:"expires"`
}

func (h *WickedFileProxyHandler) SaveCookies(c *gin.Context) {
	var body wfCookiesBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	write := func(key, val string) {
		if val == "" {
			return
		}
		h.GormDB.Exec(
			`INSERT INTO integration_settings (key, value, updated_at)
			 VALUES (?, ?, NOW())
			 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
			key, val,
		)
	}
	write(wfKeyAWSALB, body.AWSALB)
	write(wfKeyAWSALBCOR, body.AWSALBCORS)
	write(wfKeyToken, body.Token)
	write(wfKeyExpires, body.Expires)

	c.JSON(http.StatusOK, gin.H{"message": "cookies saved"})
}

// GET /settings/wickedfile-cookies/status
func (h *WickedFileProxyHandler) CookieStatus(c *gin.Context) {
	cookies, valid := h.loadCookies()
	resp := gin.H{"valid": valid}
	if cookies.HasExpires {
		resp["expires"] = cookies.Expires.Format(time.RFC3339)
		days := int(time.Until(cookies.Expires).Hours() / 24)
		if days < 0 {
			days = 0
		}
		resp["days_remaining"] = days
	} else {
		resp["expires"] = nil
		resp["days_remaining"] = 0
	}
	c.JSON(http.StatusOK, resp)
}
