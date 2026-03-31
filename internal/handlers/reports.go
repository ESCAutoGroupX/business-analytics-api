package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReportHandler struct {
	DB  *pgxpool.Pool
	Cfg interface{} // for future Tekmetric config
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

	// Build query dynamically
	query := "SELECT date, amount, category, location FROM transactions WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if startDate != "" {
		query += fmt.Sprintf(" AND date >= $%d", argIdx)
		args = append(args, startDate)
		argIdx++
	}
	if endDate != "" {
		query += fmt.Sprintf(" AND date <= $%d", argIdx)
		args = append(args, endDate)
		argIdx++
	}
	if location != "" {
		query += fmt.Sprintf(" AND location LIKE $%d", argIdx)
		args = append(args, "%"+location+"%")
		argIdx++
	}

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Internal server error: %s", err.Error()), "error": err.Error()})
		return
	}
	defer rows.Close()

	type reportEntry struct {
		GrossSales          float64            `json:"gross_sales"`
		CreditCardTotal     float64            `json:"credit_card_total"`
		CashCheckTotal      float64            `json:"cash_check_total"`
		CreditCardBreakdown map[string]float64 `json:"credit_card_breakdown"`
	}

	grossPerDay := map[string]float64{}
	categoryReport := map[string]map[string]float64{}

	for rows.Next() {
		var date *string
		var amount *float64
		var categoryJSON, locationJSON *string

		if err := rows.Scan(&date, &amount, &categoryJSON, &locationJSON); err != nil {
			continue
		}

		if date == nil {
			continue
		}
		day := *date
		amt := 0.0
		if amount != nil {
			amt = *amount
		}

		grossPerDay[day] += amt

		// Category-based aggregation
		categoryName := "Uncategorized"
		if categoryJSON != nil && *categoryJSON != "" {
			var catMap map[string]interface{}
			if err := json.Unmarshal([]byte(*categoryJSON), &catMap); err == nil {
				if primary, ok := catMap["primary"].(string); ok {
					categoryName = primary
				}
			}
		}

		loc := "Unknown"
		if locationJSON != nil && *locationJSON != "" {
			var locMap map[string]interface{}
			if err := json.Unmarshal([]byte(*locationJSON), &locMap); err == nil {
				if city, ok := locMap["city"].(string); ok {
					loc = city
				}
			}
		}

		if categoryReport[categoryName] == nil {
			categoryReport[categoryName] = map[string]float64{}
		}
		categoryReport[categoryName][loc] += amt
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

	query := `SELECT t.id, t.date, t.vendor, t.amount, t.source, t.location,
	          pm.title AS pm_title,
	          l.location_name AS loc_name
	          FROM transactions t
	          LEFT JOIN payment_methods pm ON t.payment_method_id = pm.id
	          LEFT JOIN locations l ON pm.location_id = l.id
	          WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if startDate != "" {
		query += fmt.Sprintf(" AND t.date >= $%d", argIdx)
		args = append(args, startDate)
		argIdx++
	}
	if endDate != "" {
		query += fmt.Sprintf(" AND t.date <= $%d", argIdx)
		args = append(args, endDate)
		argIdx++
	}
	if location != "" {
		query += fmt.Sprintf(" AND t.location LIKE $%d", argIdx)
		args = append(args, "%"+location+"%")
		argIdx++
	}
	if cardName != "" {
		query += fmt.Sprintf(" AND t.source LIKE $%d", argIdx)
		args = append(args, "%"+cardName+"%")
		argIdx++
	}

	rows, err := h.DB.Query(context.Background(), query, args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("Internal server error: %s", err.Error()), "error": err.Error()})
		return
	}
	defer rows.Close()

	results := []map[string]string{}

	for rows.Next() {
		var txID *int
		var date, vendor, source, locationData, pmTitle, locName *string
		var amount *float64

		if err := rows.Scan(&txID, &date, &vendor, &amount, &source, &locationData, &pmTitle, &locName); err != nil {
			continue
		}

		dateStr := "Unknown"
		if date != nil {
			dateStr = *date
		}
		vendorStr := "Unknown"
		if vendor != nil {
			vendorStr = *vendor
		}
		amountStr := "$0.00"
		if amount != nil {
			amountStr = fmt.Sprintf("$%.2f", *amount)
		}

		card := ""
		if pmTitle != nil {
			card = *pmTitle
		}

		locationName := ""
		if locName != nil {
			locationName = *locName
		} else if locationData != nil && *locationData != "" {
			var locMap map[string]interface{}
			if err := json.Unmarshal([]byte(*locationData), &locMap); err == nil {
				if ln, ok := locMap["location_name"].(string); ok {
					locationName = ln
				}
			}
		}

		flag := "Duplicate"
		if amount != nil {
			if *amount < 500 {
				flag = "Verified"
			} else if *amount > 1000 {
				flag = "Suspicious"
			}
		}

		action := "Unknown"
		if txID != nil {
			action = fmt.Sprintf("/transactions/%d/details", *txID)
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
