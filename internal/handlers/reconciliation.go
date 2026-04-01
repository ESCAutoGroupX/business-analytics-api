package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type ReconciliationHandler struct {
	GormDB *gorm.DB
}

var worldpayTerminalMap = map[string]int{
	"6450": 3202, "6469": 3244, "6487": 5528, "6423": 5513,
	"6496": 3236, "6389": 5802, "6799": 2708, "6502": 2536,
	"0139": 16255, "4112": 10247,
}

var amexTerminalMap = map[string]int{
	"0557": 3244, "1778": 5528, "0064": 16255, "3273": 2536,
	"3299": 3236, "3653": 5802, "3182": 5513, "3847": 10247,
}

var worldpayRE = regexp.MustCompile(`xxxxxxxx(\d{4})\s*$`)
var amexRE = regexp.MustCompile(`xxxxxx(\d{4})\s*$`)

var genericNames = []string{
	"Worldpay COMB. DEP.",
	"Worldpay NET SETLMT",
	"AMERICAN EXPRESS SETTLEMENT",
	"Worldpay CREDIT DEP",
	"DEPOSIT",
	"CASH DEPOSIT - THANK YOU",
}

func extractShopFromName(name string) *int {
	if m := worldpayRE.FindStringSubmatch(name); len(m) == 2 {
		if shopID, ok := worldpayTerminalMap[m[1]]; ok {
			return &shopID
		}
	}
	if m := amexRE.FindStringSubmatch(name); len(m) == 2 {
		if shopID, ok := amexTerminalMap[m[1]]; ok {
			return &shopID
		}
	}
	return nil
}

func nextBusinessDay(d time.Time) time.Time {
	nxt := d.AddDate(0, 0, 1)
	if nxt.Weekday() == time.Saturday {
		nxt = nxt.AddDate(0, 0, 2)
	} else if nxt.Weekday() == time.Sunday {
		nxt = nxt.AddDate(0, 0, 1)
	}
	return nxt
}

func reconciliationStatus(variance, smsVal, bankVal float64) string {
	if smsVal > 0 && bankVal == 0 {
		return "RED"
	}
	if math.Abs(variance) >= 500 {
		return "RED"
	}
	if math.Abs(variance) >= 50 {
		return "YELLOW"
	}
	return "GREEN"
}

var statusRank = map[string]int{"GREEN": 0, "YELLOW": 1, "RED": 2}

// GET /api/reconciliation/daily-match
func (h *ReconciliationHandler) DailyMatch(c *gin.Context) {
	dateStr := c.Query("date")
	smsToken := c.Query("sms_token")

	if dateStr == "" || smsToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "date and sms_token are required"})
		return
	}

	// STEP 1 — Get SMS reported collections
	smsURL := fmt.Sprintf("https://sms-api.carshopanalytics.com/api/v1/reconciliation/daily-summary?date=%s", dateStr)

	client := &http.Client{Timeout: 30 * time.Second}
	req, _ := http.NewRequest("GET", smsURL, nil)
	req.Header.Set("Authorization", "Bearer "+smsToken)

	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": fmt.Sprintf("Failed to reach SMS API: %s", err.Error())})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("SMS API error: %s", string(body))})
		return
	}

	var smsData map[string]interface{}
	json.Unmarshal(body, &smsData)

	type smsTotals struct {
		Card  float64
		Amex  float64
		Cash  float64
		Check float64
		Total float64
	}

	smsMap := map[int]smsTotals{}
	if shops, ok := smsData["shops"].([]interface{}); ok {
		for _, s := range shops {
			shop, ok := s.(map[string]interface{})
			if !ok {
				continue
			}
			shopIDFloat, ok := shop["tekmetric_shop_id"].(float64)
			if !ok {
				continue
			}
			shopID := int(shopIDFloat)
			pt, _ := shop["payment_totals"].(map[string]interface{})
			visa := toFloat(pt["Visa"])
			mastercard := toFloat(pt["Mastercard"])
			discover := toFloat(pt["Discover"])
			other := toFloat(pt["Other"])
			amex := toFloat(pt["Amex"])
			cash := toFloat(pt["Cash"])
			check := toFloat(pt["Check"])
			totalCollected := toFloat(shop["total_collected"])

			smsMap[shopID] = smsTotals{
				Card:  visa + mastercard + discover + other,
				Amex:  amex,
				Cash:  cash,
				Check: check,
				Total: totalCollected,
			}
		}
	}

	// STEP 2 — Calculate settlement dates
	targetDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid date format"})
		return
	}

	worldpayDate := nextBusinessDay(targetDate)

	amexSearchDates := []time.Time{}
	d := targetDate
	for len(amexSearchDates) < 5 {
		d = nextBusinessDay(d)
		amexSearchDates = append(amexSearchDates, d)
	}

	// STEP 3 — Pull Plaid deposits from local DB using GORM
	// Worldpay query
	wpQuery := h.GormDB.Model(&models.Transaction{}).
		Select("name, ABS(amount) AS amount").
		Where("date = ? AND amount < 0 AND name ILIKE ?", worldpayDate.Format("2006-01-02"), "%worldpay%")

	for _, gn := range genericNames {
		wpQuery = wpQuery.Where("name != ?", gn)
	}

	type txNameAmount struct {
		Name   string
		Amount float64
	}

	var wpResults []txNameAmount
	wpQuery.Find(&wpResults)

	type bankEntry struct {
		Worldpay float64
		Amex     float64
	}

	bankData := map[int]*bankEntry{}
	type unmatchedDeposit struct {
		Name   string  `json:"name"`
		Amount float64 `json:"amount"`
	}
	unmatched := []unmatchedDeposit{}

	for _, wp := range wpResults {
		shopID := extractShopFromName(wp.Name)
		if shopID != nil {
			if bankData[*shopID] == nil {
				bankData[*shopID] = &bankEntry{}
			}
			bankData[*shopID].Worldpay += wp.Amount
		} else {
			unmatched = append(unmatched, unmatchedDeposit{Name: wp.Name, Amount: wp.Amount})
		}
	}

	// Amex matching — reverse lookup
	amexShopToTerminal := map[int]string{}
	for terminal, shopID := range amexTerminalMap {
		amexShopToTerminal[shopID] = terminal
	}

	amexDateStrs := make([]string, len(amexSearchDates))
	for i, ad := range amexSearchDates {
		amexDateStrs[i] = ad.Format("2006-01-02")
	}

	for shopID, sms := range smsMap {
		if sms.Amex <= 0 {
			continue
		}
		terminal, ok := amexShopToTerminal[shopID]
		if !ok {
			continue
		}

		terminalPattern := fmt.Sprintf("%%xxxxxx%s%%", terminal)

		var amexResults []txNameAmount
		h.GormDB.Model(&models.Transaction{}).
			Select("name, ABS(amount) AS amount").
			Where("amount < 0 AND name ILIKE ? AND date IN ?", terminalPattern, amexDateStrs).
			Find(&amexResults)

		for _, ar := range amexResults {
			if math.Abs(ar.Amount-sms.Amex) <= 1.0 {
				if bankData[shopID] == nil {
					bankData[shopID] = &bankEntry{}
				}
				bankData[shopID].Amex = ar.Amount
				break
			}
		}
	}

	// STEP 4 — Compare per shop
	type cardDetail struct {
		SMSTotal    float64 `json:"sms_total"`
		BankDeposit float64 `json:"bank_deposit"`
		Variance    float64 `json:"variance"`
		Status      string  `json:"status"`
	}

	type shopResult struct {
		TekmetricShopID int        `json:"tekmetric_shop_id"`
		Card            cardDetail `json:"card"`
		Amex            cardDetail `json:"amex"`
		CashPending     float64    `json:"cash_pending"`
		CheckPending    float64    `json:"check_pending"`
		OverallStatus   string     `json:"overall_status"`
	}

	shopIDs := []int{}
	for id := range smsMap {
		shopIDs = append(shopIDs, id)
	}
	sort.Ints(shopIDs)

	shops := []shopResult{}
	green, yellow, red := 0, 0, 0

	for _, shopID := range shopIDs {
		sms := smsMap[shopID]
		bank := bankData[shopID]
		if bank == nil {
			bank = &bankEntry{}
		}

		cardSMS := math.Round(sms.Card*100) / 100
		cardBank := math.Round(bank.Worldpay*100) / 100
		cardVariance := math.Round((cardBank-cardSMS)*100) / 100
		cardStatus := reconciliationStatus(cardVariance, cardSMS, cardBank)

		amexSMS := math.Round(sms.Amex*100) / 100
		amexBank := math.Round(bank.Amex*100) / 100
		amexVariance := math.Round((amexBank-amexSMS)*100) / 100
		amexStatus := reconciliationStatus(amexVariance, amexSMS, amexBank)

		overall := cardStatus
		if statusRank[amexStatus] > statusRank[overall] {
			overall = amexStatus
		}

		switch overall {
		case "GREEN":
			green++
		case "YELLOW":
			yellow++
		default:
			red++
		}

		shops = append(shops, shopResult{
			TekmetricShopID: shopID,
			Card:            cardDetail{SMSTotal: cardSMS, BankDeposit: cardBank, Variance: cardVariance, Status: cardStatus},
			Amex:            cardDetail{SMSTotal: amexSMS, BankDeposit: amexBank, Variance: amexVariance, Status: amexStatus},
			CashPending:     math.Round(sms.Cash*100) / 100,
			CheckPending:    math.Round(sms.Check*100) / 100,
			OverallStatus:   overall,
		})
	}

	// Round unmatched amounts
	for i := range unmatched {
		unmatched[i].Amount = math.Round(unmatched[i].Amount*100) / 100
	}

	c.JSON(http.StatusOK, gin.H{
		"date": dateStr,
		"settlement_dates": gin.H{
			"worldpay": worldpayDate.Format("2006-01-02"),
			"amex":     "exact_amount_match_5day_window",
		},
		"summary": gin.H{
			"total_shops":        len(shops),
			"green":              green,
			"yellow":             yellow,
			"red":                red,
			"unmatched_deposits": unmatched,
		},
		"shops": shops,
	})
}

// GET /api/reconciliation/deposit-detail
func (h *ReconciliationHandler) DepositDetail(c *gin.Context) {
	amountStr := c.Query("amount")
	smsToken := c.Query("sms_token")

	if amountStr == "" || smsToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "amount and sms_token are required"})
		return
	}

	params := fmt.Sprintf("amount=%s&tolerance=1.00", amountStr)
	if dateStr := c.Query("date"); dateStr != "" {
		params += "&date=" + dateStr
	}
	if shopID := c.Query("shop_id"); shopID != "" {
		params += "&shop_id=" + shopID
	}

	url := "https://sms-api.carshopanalytics.com/api/v1/reconciliation/deposits/match?" + params

	client := &http.Client{Timeout: 30 * time.Second}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+smsToken)

	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": fmt.Sprintf("Failed to reach SMS API: %s", err.Error())})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"detail": fmt.Sprintf("SMS API error: %s", string(respBody))})
		return
	}

	var result interface{}
	json.Unmarshal(respBody, &result)
	c.JSON(http.StatusOK, result)
}

func toFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	}
	return 0
}

// Ensure strings import is used
var _ = strings.TrimSpace
