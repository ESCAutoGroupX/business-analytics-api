package handlers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// WickedFileProxyHandler fronts alpha.wickedfile.com by replaying the
// operator's browser session cookies stored in integration_settings. It
// can also re-authenticate by logging in with stored credentials and a
// user-supplied Authy 2FA code.
type WickedFileProxyHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config

	client *http.Client

	mu       sync.Mutex
	lastCall time.Time

	pendingMu      sync.Mutex
	pendingCookies []*http.Cookie
	pendingAt      time.Time

	syncMu    sync.Mutex
	syncState wfSyncState
}

type wfSyncState struct {
	Running   bool
	Processed int
	Total     int
	StartedAt time.Time
}

func NewWickedFileProxyHandler(db *gorm.DB, cfg *config.Config) *WickedFileProxyHandler {
	return &WickedFileProxyHandler{
		GormDB: db,
		Cfg:    cfg,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

const (
	wfBaseURL      = "https://alpha.wickedfile.com"
	wfExpiredMsg   = "WickedFile session expired - please reconnect in Settings"
	wfKeyUsername  = "WICKEDFILE_USERNAME"
	wfKeyPassword  = "WICKEDFILE_PASSWORD"
	wfKeyAWSALB    = "WICKEDFILE_AWSALB"
	wfKeyAWSALBCOR = "WICKEDFILE_AWSALBCORS"
	wfKeyToken     = "WICKEDFILE_TOKEN"
	wfKeyExpires   = "WICKEDFILE_COOKIE_EXPIRES"
	wfMinGap       = 500 * time.Millisecond
	wfPendingTTL   = 10 * time.Minute
)

type wfCookies struct {
	AWSALB     string
	AWSALBCORS string
	Token      string
	Expires    time.Time
	HasExpires bool
}

// ── integration_settings helpers ─────────────────────────────

func (h *WickedFileProxyHandler) readSetting(key string) string {
	var val string
	h.GormDB.Raw("SELECT value FROM integration_settings WHERE key = ?", key).Scan(&val)
	return val
}

func (h *WickedFileProxyHandler) writeSetting(key, val string) {
	h.GormDB.Exec(
		`INSERT INTO integration_settings (key, value, updated_at)
		 VALUES (?, ?, NOW())
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
		key, val,
	)
}

// ── AES-GCM helpers for password storage ─────────────────────

func (h *WickedFileProxyHandler) aesKey() []byte {
	secret := ""
	if h.Cfg != nil {
		secret = h.Cfg.SecretKey
	}
	if secret == "" {
		secret = "wickedfile-local-default-key"
	}
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

func (h *WickedFileProxyHandler) encrypt(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	block, err := aes.NewCipher(h.aesKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := gcm.Seal(nonce, nonce, []byte(plain), nil)
	return "enc:v1:" + base64.StdEncoding.EncodeToString(ct), nil
}

func (h *WickedFileProxyHandler) decrypt(stored string) (string, error) {
	if stored == "" {
		return "", nil
	}
	if !strings.HasPrefix(stored, "enc:v1:") {
		return stored, nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, "enc:v1:"))
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(h.aesKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(raw) < ns {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ct := raw[:ns], raw[ns:]
	plain, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// ── session cookie load ──────────────────────────────────────

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

func (h *WickedFileProxyHandler) newRequest(method, u string, body io.Reader, c wfCookies) (*http.Request, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", fmt.Sprintf("AWSALB=%s; AWSALBCORS=%s; token=%s", c.AWSALB, c.AWSALBCORS, c.Token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; business-analytics-api)")
	req.Header.Set("Accept", "*/*")
	return req, nil
}

// ── PDF / metadata proxy ─────────────────────────────────────

// StreamPDF fetches a WickedFile scan-page PDF and streams the response body
// into the given gin.Context.
func (h *WickedFileProxyHandler) StreamPDF(c *gin.Context, scanPageID string) error {
	cookies, valid := h.loadCookies()
	if !valid {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": wfExpiredMsg})
		return fmt.Errorf("wf session invalid")
	}
	h.throttle()

	u := fmt.Sprintf("%s/store/scan/image/%s", wfBaseURL, scanPageID)
	req, err := h.newRequest(http.MethodGet, u, nil, cookies)
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

// fetchMetadataRaw calls /store/scan_wrap/get_scan_page and returns parsed JSON.
func (h *WickedFileProxyHandler) fetchMetadataRaw(scanPageID string) (map[string]interface{}, int, error) {
	cookies, valid := h.loadCookies()
	if !valid {
		return nil, http.StatusUnauthorized, fmt.Errorf("%s", wfExpiredMsg)
	}
	h.throttle()

	form := url.Values{}
	form.Set("scanPageId", scanPageID)
	u := wfBaseURL + "/store/scan_wrap/get_scan_page"
	req, err := h.newRequest(http.MethodPost, u, strings.NewReader(form.Encode()), cookies)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, http.StatusUnauthorized, fmt.Errorf("%s", wfExpiredMsg)
	}
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	if resp.StatusCode >= 400 {
		return nil, resp.StatusCode, fmt.Errorf("WickedFile returned %d: %s", resp.StatusCode, string(respBytes))
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("invalid WickedFile response: %v", err)
	}
	return parsed, http.StatusOK, nil
}

// GET /wf/document/:scanPageId/metadata
func (h *WickedFileProxyHandler) ProxyMetadata(c *gin.Context) {
	scanPageID := c.Param("scanPageId")
	if scanPageID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "scanPageId required"})
		return
	}
	parsed, code, err := h.fetchMetadataRaw(scanPageID)
	if err != nil {
		c.JSON(code, gin.H{"detail": err.Error()})
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

// ── legacy cookie save ───────────────────────────────────────

type wfCookiesBody struct {
	AWSALB     string `json:"awsalb"`
	AWSALBCORS string `json:"awsalbcors"`
	Token      string `json:"token"`
	Expires    string `json:"expires"`
}

// PUT /settings/wickedfile-cookies  (manual paste — kept for break-glass)
func (h *WickedFileProxyHandler) SaveCookies(c *gin.Context) {
	var body wfCookiesBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if body.AWSALB != "" {
		h.writeSetting(wfKeyAWSALB, body.AWSALB)
	}
	if body.AWSALBCORS != "" {
		h.writeSetting(wfKeyAWSALBCOR, body.AWSALBCORS)
	}
	if body.Token != "" {
		h.writeSetting(wfKeyToken, body.Token)
	}
	if body.Expires != "" {
		h.writeSetting(wfKeyExpires, body.Expires)
	}
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

// ── auth: credentials / login / 2FA / status ─────────────────

type wfCredsBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// PUT /wf/auth/credentials
func (h *WickedFileProxyHandler) SaveCredentials(c *gin.Context) {
	var body wfCredsBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	if body.Username == "" || body.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "username and password required"})
		return
	}
	h.writeSetting(wfKeyUsername, body.Username)
	enc, err := h.encrypt(body.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "encrypt failed: " + err.Error()})
		return
	}
	h.writeSetting(wfKeyPassword, enc)
	c.JSON(http.StatusOK, gin.H{"saved": true})
}

// POST /wf/auth/login
func (h *WickedFileProxyHandler) Login(c *gin.Context) {
	username := h.readSetting(wfKeyUsername)
	stored := h.readSetting(wfKeyPassword)
	password, err := h.decrypt(stored)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "decrypt failed: " + err.Error()})
		return
	}
	if username == "" || password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "WickedFile credentials not set — save them first"})
		return
	}

	form := url.Values{}
	form.Set("username", username)
	form.Set("password", password)
	form.Set("rememberDate", strconv.FormatInt(time.Now().UnixMilli(), 10))

	req, err := http.NewRequest(http.MethodPost, wfBaseURL+"/auth/user/login", strings.NewReader(form.Encode()))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; business-analytics-api)")
	req.Header.Set("Accept", "*/*")

	resp, err := h.client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("WickedFile login returned %d: %s", resp.StatusCode, string(respBody))})
		return
	}

	cookies := resp.Cookies()
	h.pendingMu.Lock()
	h.pendingCookies = cookies
	h.pendingAt = time.Now()
	h.pendingMu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"requires_2fa": true,
		"message":      "Enter Authy code",
	})
}

type wfVerifyBody struct {
	Code string `json:"code"`
}

// POST /wf/auth/verify-2fa
func (h *WickedFileProxyHandler) Verify2FA(c *gin.Context) {
	var body wfVerifyBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	code := strings.TrimSpace(body.Code)
	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "code required"})
		return
	}

	h.pendingMu.Lock()
	pending := h.pendingCookies
	pendingAt := h.pendingAt
	h.pendingMu.Unlock()

	if len(pending) == 0 || time.Since(pendingAt) > wfPendingTTL {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "no pending login — start /wf/auth/login first"})
		return
	}

	form := url.Values{}
	form.Set("code", code)

	req, err := http.NewRequest(http.MethodPost, wfBaseURL+"/auth/user/twoFA", strings.NewReader(form.Encode()))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; business-analytics-api)")
	req.Header.Set("Accept", "*/*")
	for _, ck := range pending {
		req.AddCookie(&http.Cookie{Name: ck.Name, Value: ck.Value})
	}

	resp, err := h.client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": err.Error()})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("2FA verify returned %d: %s", resp.StatusCode, string(respBody))})
		return
	}

	// merge cookies from pending + verify response
	all := map[string]string{}
	for _, ck := range pending {
		all[ck.Name] = ck.Value
	}
	for _, ck := range resp.Cookies() {
		all[ck.Name] = ck.Value
	}

	awsalb, awsalbcors, token := all["AWSALB"], all["AWSALBCORS"], all["token"]
	if awsalb == "" || awsalbcors == "" || token == "" {
		c.JSON(http.StatusBadGateway, gin.H{
			"detail":        "2FA succeeded but expected cookies missing",
			"cookies_found": mapKeys(all),
		})
		return
	}

	expires := time.Now().Add(7 * 24 * time.Hour).UTC()
	h.writeSetting(wfKeyAWSALB, awsalb)
	h.writeSetting(wfKeyAWSALBCOR, awsalbcors)
	h.writeSetting(wfKeyToken, token)
	h.writeSetting(wfKeyExpires, expires.Format(time.RFC3339Nano))

	h.pendingMu.Lock()
	h.pendingCookies = nil
	h.pendingMu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"expires": expires.Format(time.RFC3339),
	})
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// GET /wf/auth/status
func (h *WickedFileProxyHandler) AuthStatus(c *gin.Context) {
	cookies, valid := h.loadCookies()
	username := h.readSetting(wfKeyUsername)
	masked := ""
	if username != "" {
		if at := strings.IndexByte(username, '@'); at > 0 {
			prefix := username[:at]
			if len(prefix) <= 2 {
				masked = prefix[:1] + "***" + username[at:]
			} else {
				masked = prefix[:2] + strings.Repeat("*", len(prefix)-2) + username[at:]
			}
		} else {
			masked = "***"
		}
	}

	resp := gin.H{
		"connected": valid,
		"username":  masked,
	}
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

// ── metadata sync job ────────────────────────────────────────

// POST /wf/documents/sync-metadata
func (h *WickedFileProxyHandler) StartMetadataSync(c *gin.Context) {
	h.syncMu.Lock()
	if h.syncState.Running {
		pending := h.syncState.Total - h.syncState.Processed
		h.syncMu.Unlock()
		c.JSON(http.StatusOK, gin.H{
			"started": false,
			"pending": pending,
			"message": "sync already running",
		})
		return
	}

	var total int64
	h.GormDB.Raw(`
		SELECT COUNT(*) FROM documents
		WHERE ocr_agent_version = 'wf-import-v1'
		  AND wf_scan_id IS NOT NULL AND wf_scan_id <> ''
		  AND (vendor_name IS NULL OR vendor_name = '' OR total_amount IS NULL)
	`).Scan(&total)

	h.syncState = wfSyncState{
		Running:   true,
		Processed: 0,
		Total:     int(total),
		StartedAt: time.Now(),
	}
	h.syncMu.Unlock()

	go h.runMetadataSync()

	c.JSON(http.StatusOK, gin.H{
		"started": true,
		"pending": total,
	})
}

// GET /wf/documents/sync-status
func (h *WickedFileProxyHandler) MetadataSyncStatus(c *gin.Context) {
	h.syncMu.Lock()
	defer h.syncMu.Unlock()
	c.JSON(http.StatusOK, gin.H{
		"processed": h.syncState.Processed,
		"total":     h.syncState.Total,
		"running":   h.syncState.Running,
	})
}

func (h *WickedFileProxyHandler) runMetadataSync() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("wf sync-metadata: panic: %v", r)
		}
		h.syncMu.Lock()
		h.syncState.Running = false
		h.syncMu.Unlock()
	}()

	log.Printf("wf sync-metadata: starting")
	const batchSize = 10
	for {
		var ids []string
		err := h.GormDB.Raw(`
			SELECT wf_scan_id FROM documents
			WHERE ocr_agent_version = 'wf-import-v1'
			  AND wf_scan_id IS NOT NULL AND wf_scan_id <> ''
			  AND (vendor_name IS NULL OR vendor_name = '' OR total_amount IS NULL)
			ORDER BY id
			LIMIT ?
		`, batchSize).Scan(&ids).Error
		if err != nil {
			log.Printf("wf sync-metadata: query error: %v", err)
			return
		}
		if len(ids) == 0 {
			log.Printf("wf sync-metadata: complete")
			return
		}

		processedAny := false
		for _, scanID := range ids {
			parsed, code, err := h.fetchMetadataRaw(scanID)
			if err != nil {
				log.Printf("wf sync-metadata: scan %s: %v (%d)", scanID, err, code)
				if code == http.StatusUnauthorized {
					return
				}
				// mark processed to avoid loop: best effort — skip by bumping counter
				h.syncMu.Lock()
				h.syncState.Processed++
				h.syncMu.Unlock()
				continue
			}
			h.cacheMetadata(scanID, parsed)
			processedAny = true
			h.syncMu.Lock()
			h.syncState.Processed++
			h.syncMu.Unlock()
		}

		// throttle already sleeps 500ms between fetches, but sleep a small
		// gap between batches too to be gentle on WickedFile.
		if !processedAny {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

