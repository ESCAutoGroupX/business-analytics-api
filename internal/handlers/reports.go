package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type ReportHandler struct {
	GormDB *gorm.DB
}

// GET /reports/profit-loss
func (h *ReportHandler) ProfitLossReport(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	location := c.Query("location")

	if startDate != "" {
		if !isValidDate(startDate) {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid start_date format. Use YYYY-MM-DD"})
			return
		}
	}
	if endDate != "" {
		if !isValidDate(endDate) {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid end_date format. Use YYYY-MM-DD"})
			return
		}
	}

	query := h.GormDB.Model(&models.Transaction{}).Select("date, amount, category, location")

	if startDate != "" {
		query = query.Where("date >= ?", startDate)
	}
	if endDate != "" {
		query = query.Where("date <= ?", endDate)
	}
	if location != "" {
		query = query.Where("location LIKE ?", "%"+location+"%")
	}

	type txRow struct {
		Date     *string
		Amount   *float64
		Category *string
		Location *string
	}

	var rows []txRow
	if err := query.Find(&rows).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Internal server error: %s", err.Error()), "error": err.Error()})
		return
	}

	type reportEntry struct {
		GrossSales          float64            `json:"gross_sales"`
		CreditCardTotal     float64            `json:"credit_card_total"`
		CashCheckTotal      float64            `json:"cash_check_total"`
		CreditCardBreakdown map[string]float64 `json:"credit_card_breakdown"`
	}

	grossPerDay := map[string]float64{}

	for _, row := range rows {
		if row.Date == nil {
			continue
		}
		day := *row.Date
		amt := 0.0
		if row.Amount != nil {
			amt = *row.Amount
		}
		grossPerDay[day] += amt
	}

	// Build daily report
	allDays := []string{}
	for day := range grossPerDay {
		allDays = append(allDays, day)
	}
	sort.Strings(allDays)

	report := map[string]reportEntry{}
	for _, day := range allDays {
		gross := grossPerDay[day]
		report[day] = reportEntry{
			GrossSales:          roundTo2(gross),
			CreditCardTotal:     0,
			CashCheckTotal:      roundTo2(gross),
			CreditCardBreakdown: map[string]float64{},
		}
	}

	c.JSON(http.StatusOK, report)
}

// GET /reports/credit-card-summary
func (h *ReportHandler) CreditCardSummary(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	location := c.Query("location")
	cardName := c.Query("card_name")

	if startDate != "" && !isValidDate(startDate) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid start_date format. Use YYYY-MM-DD"})
		return
	}
	if endDate != "" && !isValidDate(endDate) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid end_date format. Use YYYY-MM-DD"})
		return
	}

	// This query joins transactions with payment_methods and locations.
	// Keep as raw SQL via GORM since it involves multi-table joins with specific column aliases.
	query := h.GormDB.Table("transactions t").
		Select(`t.id, t.date, t.vendor, t.amount, t.source, t.location,
		        pm.title AS pm_title,
		        l.location_name AS loc_name`).
		Joins("LEFT JOIN payment_methods pm ON t.payment_method_id = pm.id").
		Joins("LEFT JOIN locations l ON pm.location_id::int = l.id")

	if startDate != "" {
		query = query.Where("t.date >= ?", startDate)
	}
	if endDate != "" {
		query = query.Where("t.date <= ?", endDate)
	}
	if location != "" {
		query = query.Where("t.location LIKE ?", "%"+location+"%")
	}
	if cardName != "" {
		query = query.Where("t.source LIKE ?", "%"+cardName+"%")
	}

	type ccRow struct {
		ID       *int
		Date     *string
		Vendor   *string
		Amount   *float64
		Source   *string
		Location *string
		PmTitle  *string
		LocName  *string
	}

	var rows []ccRow
	if err := query.Find(&rows).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Internal server error: %s", err.Error()), "error": err.Error()})
		return
	}

	results := []map[string]string{}

	for _, row := range rows {
		dateStr := "Unknown"
		if row.Date != nil {
			dateStr = *row.Date
		}
		vendorStr := "Unknown"
		if row.Vendor != nil {
			vendorStr = *row.Vendor
		}
		amountStr := "$0.00"
		if row.Amount != nil {
			amountStr = fmt.Sprintf("$%.2f", *row.Amount)
		}

		card := ""
		if row.PmTitle != nil {
			card = *row.PmTitle
		}

		locationName := ""
		if row.LocName != nil {
			locationName = *row.LocName
		} else if row.Location != nil && *row.Location != "" {
			var locMap map[string]interface{}
			if err := json.Unmarshal([]byte(*row.Location), &locMap); err == nil {
				if ln, ok := locMap["location_name"].(string); ok {
					locationName = ln
				}
			}
		}

		flag := "Duplicate"
		if row.Amount != nil {
			if *row.Amount < 500 {
				flag = "Verified"
			} else if *row.Amount > 1000 {
				flag = "Suspicious"
			}
		}

		action := "Unknown"
		if row.ID != nil {
			action = fmt.Sprintf("/transactions/%d/details", *row.ID)
		}

		results = append(results, map[string]string{
			"date":     dateStr,
			"vendor":   vendorStr,
			"amount":   amountStr,
			"gl_code":  "",
			"card":     card,
			"location": locationName,
			"flag":     flag,
			"action":   action,
		})
	}

	c.JSON(http.StatusOK, results)
}

func isValidDate(s string) bool {
	if len(s) != 10 {
		return false
	}
	for i, c := range s {
		if i == 4 || i == 7 {
			if c != '-' {
				return false
			}
		} else if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func roundTo2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
