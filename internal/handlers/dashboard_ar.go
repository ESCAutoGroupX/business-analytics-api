package handlers

import (
	"context"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GET /dashboard/account-receivable/dashboard
func (h *DashboardHandler) GetAccountReceivableDashboard(c *gin.Context) {
	ctx := context.Background()
	now := time.Now()
	currentMonth := now.Format("2006-01")
	fifteenDaysAgo := now.AddDate(0, 0, -15)

	// Get all shops with WORKINPROGRESS repair orders in current month
	shopRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT DISTINCT ro.shop_id
		 FROM tekmetric_repair_orders ro
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $1
		 ORDER BY ro.shop_id`, currentMonth)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query repair orders", "error": err.Error()})
		return
	}
	defer shopRows.Close()

	var shopIDs []int
	for shopRows.Next() {
		var shopID int
		shopRows.Scan(&shopID)
		shopIDs = append(shopIDs, shopID)
	}

	totalReceivable015 := 0.0
	totalReceivableOver15 := 0.0
	totalReceivable := 0.0
	totalROCount := 0
	shops := []gin.H{}

	for _, shopID := range shopIDs {
		// Get receivables for 0-15 days (created_date >= 15 days ago)
		var recv015 float64
		var count015 int
		h.sqlDB().QueryRowContext(ctx,
			`SELECT COALESCE(SUM(j.subtotal), 0), COUNT(DISTINCT ro.id)
			 FROM tekmetric_repair_orders ro
			 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
			 WHERE ro.shop_id = $1
			   AND ro.repair_order_status_code = 'WORKINPROGRESS'
			   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $2
			   AND ro.created_date >= $3`, shopID, currentMonth, fifteenDaysAgo).Scan(&recv015, &count015)

		// Get receivables for over 15 days (created_date < 15 days ago)
		var recvOver15 float64
		var countOver15 int
		h.sqlDB().QueryRowContext(ctx,
			`SELECT COALESCE(SUM(j.subtotal), 0), COUNT(DISTINCT ro.id)
			 FROM tekmetric_repair_orders ro
			 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
			 WHERE ro.shop_id = $1
			   AND ro.repair_order_status_code = 'WORKINPROGRESS'
			   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $2
			   AND ro.created_date < $3`, shopID, currentMonth, fifteenDaysAgo).Scan(&recvOver15, &countOver15)

		shopTotal := round2(recv015 + recvOver15)
		roCount := count015 + countOver15

		totalReceivable015 += recv015
		totalReceivableOver15 += recvOver15
		totalReceivable += shopTotal
		totalROCount += roCount

		// Get location name for shop
		var locationName string
		h.sqlDB().QueryRowContext(ctx,
			`SELECT COALESCE(name, '') FROM locations WHERE tekmetric_shop_id = $1`, shopID).Scan(&locationName)

		shops = append(shops, gin.H{
			"shop_id":                 shopID,
			"location_name":           locationName,
			"repair_order_count":      roCount,
			"total_receivable_0_15":   round2(recv015),
			"total_receivable_over_15": round2(recvOver15),
			"total_receivable":        shopTotal,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total": gin.H{
			"total_receivable":          round2(totalReceivable),
			"total_receivable_0_15":     round2(totalReceivable015),
			"total_receivable_over_15":  round2(totalReceivableOver15),
			"total_repair_orders":       totalROCount,
		},
		"shops": shops,
	})
}

// GET /dashboard/account-receivable/summary
func (h *DashboardHandler) GetAccountReceivableSummary(c *gin.Context) {
	ctx := context.Background()
	now := time.Now()
	currentMonth := now.Format("2006-01")
	fifteenDaysAgo := now.AddDate(0, 0, -15)

	var totalReceivable float64
	var totalROs int
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(j.subtotal), 0), COUNT(DISTINCT ro.id)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $1`, currentMonth).Scan(&totalReceivable, &totalROs)

	var recv015 float64
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(j.subtotal), 0)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $1
		   AND ro.created_date >= $2`, currentMonth, fifteenDaysAgo).Scan(&recv015)

	var recvOver15 float64
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(j.subtotal), 0)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $1
		   AND ro.created_date < $2`, currentMonth, fifteenDaysAgo).Scan(&recvOver15)

	c.JSON(http.StatusOK, gin.H{
		"total_receivable":          round2(totalReceivable),
		"total_receivable_0_15":     round2(recv015),
		"total_receivable_over_15":  round2(recvOver15),
		"total_repair_orders":       totalROs,
	})
}

// GET /dashboard/account-receivable/shop/:shop_id
func (h *DashboardHandler) GetAccountReceivableByShop(c *gin.Context) {
	ctx := context.Background()
	shopID := c.Param("shop_id")
	now := time.Now()
	currentMonth := now.Format("2006-01")
	fifteenDaysAgo := now.AddDate(0, 0, -15)

	// Get jobs with repair order, customer, vehicle info
	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT ro.id, ro.tekmetric_ro_id, ro.ro_number, ro.created_date,
		        j.name, j.subtotal, j.parts_total, j.labor_total, j.fee_total, j.discount_total,
		        j.authorized,
		        COALESCE(ro.customer_id, 0), COALESCE(ro.vehicle_id, 0)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.shop_id = $1
		   AND ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $2
		 ORDER BY ro.created_date DESC`, shopID, currentMonth)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query repair orders", "error": err.Error()})
		return
	}
	defer rows.Close()

	recv015 := 0.0
	recvOver15 := 0.0
	totalReceivable := 0.0
	roIDs := map[int]bool{}
	jobs := []gin.H{}

	for rows.Next() {
		var roID, tekmetricROID int
		var roNumber *string
		var createdDate *time.Time
		var jobName *string
		var subtotal, partsTotal, laborTotal, feeTotal, discountTotal float64
		var authorized bool
		var customerID, vehicleID int

		rows.Scan(&roID, &tekmetricROID, &roNumber, &createdDate,
			&jobName, &subtotal, &partsTotal, &laborTotal, &feeTotal, &discountTotal,
			&authorized, &customerID, &vehicleID)

		roIDs[roID] = true
		totalReceivable += subtotal

		isOver15 := false
		if createdDate != nil && createdDate.Before(fifteenDaysAgo) {
			recvOver15 += subtotal
			isOver15 = true
		} else {
			recv015 += subtotal
		}

		entry := gin.H{
			"repair_order_id":  roID,
			"tekmetric_ro_id":  tekmetricROID,
			"ro_number":        roNumber,
			"job_name":         jobName,
			"subtotal":         round2(subtotal),
			"parts_total":      round2(partsTotal),
			"labor_total":      round2(laborTotal),
			"fee_total":        round2(feeTotal),
			"discount_total":   round2(discountTotal),
			"authorized":       authorized,
			"customer_id":      customerID,
			"vehicle_id":       vehicleID,
			"is_over_15_days":  isOver15,
		}
		if createdDate != nil {
			entry["created_date"] = createdDate.Format("2006-01-02")
		}
		jobs = append(jobs, entry)
	}

	c.JSON(http.StatusOK, gin.H{
		"shop_id":                  shopID,
		"repair_order_count":       len(roIDs),
		"total_receivable_0_15":    round2(recv015),
		"total_receivable_over_15": round2(recvOver15),
		"total_receivable":         round2(totalReceivable),
		"jobs":                     jobs,
	})
}

// GET /dashboard/account-receivable/aging_receivables
func (h *DashboardHandler) GetAgingReceivables(c *gin.Context) {
	ctx := context.Background()
	page := parseIntDefault(c.DefaultQuery("page", "1"), 1)
	size := parseIntDefault(c.DefaultQuery("size", "50"), 50)
	now := time.Now()
	fortyFiveDaysAgo := now.AddDate(0, 0, -45)

	// Count total
	var totalRecords int
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT ro.id)
		 FROM tekmetric_repair_orders ro
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND ro.created_date <= $1`, fortyFiveDaysAgo).Scan(&totalRecords)

	totalPages := int(math.Ceil(float64(totalRecords) / float64(size)))

	// Total receivable for aging
	var totalReceivable float64
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(j.subtotal), 0)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND ro.created_date <= $1`, fortyFiveDaysAgo).Scan(&totalReceivable)

	offset := (page - 1) * size
	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT ro.id, ro.tekmetric_ro_id, ro.shop_id, ro.ro_number, ro.created_date,
		        ro.customer_id, ro.vehicle_id, COALESCE(ro.total_sales, 0),
		        COALESCE(ro.amount_paid, 0), COALESCE(ro.taxes, 0)
		 FROM tekmetric_repair_orders ro
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND ro.created_date <= $1
		 ORDER BY ro.created_date ASC
		 OFFSET $2 LIMIT $3`, fortyFiveDaysAgo, offset, size)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query aging receivables", "error": err.Error()})
		return
	}
	defer rows.Close()

	repairOrders := []gin.H{}
	for rows.Next() {
		var roID, tekmetricROID, shopID int
		var roNumber *string
		var createdDate *time.Time
		var customerID, vehicleID *int
		var totalSales, amountPaid, taxes float64

		rows.Scan(&roID, &tekmetricROID, &shopID, &roNumber, &createdDate,
			&customerID, &vehicleID, &totalSales, &amountPaid, &taxes)

		daysAging := 0
		if createdDate != nil {
			daysAging = int(now.Sub(*createdDate).Hours() / 24)
		}

		// Get jobs subtotal for this RO
		var roSubtotal float64
		h.sqlDB().QueryRowContext(ctx,
			`SELECT COALESCE(SUM(subtotal), 0) FROM tekmetric_jobs WHERE repair_order_id = $1`, roID).Scan(&roSubtotal)

		entry := gin.H{
			"id":              roID,
			"tekmetric_ro_id": tekmetricROID,
			"shop_id":         shopID,
			"ro_number":       roNumber,
			"customer_id":     customerID,
			"vehicle_id":      vehicleID,
			"total_sales":     round2(totalSales),
			"amount_paid":     round2(amountPaid),
			"taxes":           round2(taxes),
			"subtotal":        round2(roSubtotal),
			"days_aging":      daysAging,
		}
		if createdDate != nil {
			entry["created_date"] = createdDate.Format("2006-01-02")
		}
		repairOrders = append(repairOrders, entry)
	}

	c.JSON(http.StatusOK, gin.H{
		"total_receivable": round2(totalReceivable),
		"total_records":    totalRecords,
		"total_pages":      totalPages,
		"current_page":     page,
		"page_size":        size,
		"repair_orders":    repairOrders,
	})
}

// GET /dashboard/account-receivable/graph
func (h *DashboardHandler) GetAccountReceivableGraph(c *gin.Context) {
	ctx := context.Background()
	weeks := parseIntDefault(c.DefaultQuery("weeks", "1"), 1)

	now := time.Now()
	startDate := now.AddDate(0, 0, -7*weeks)

	// Get all shops that have WORKINPROGRESS ROs
	shopRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT DISTINCT ro.shop_id
		 FROM tekmetric_repair_orders ro
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		 ORDER BY ro.shop_id`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query shops", "error": err.Error()})
		return
	}
	defer shopRows.Close()

	var shopIDs []int
	for shopRows.Next() {
		var shopID int
		shopRows.Scan(&shopID)
		shopIDs = append(shopIDs, shopID)
	}

	overallTotal := 0.0
	shops := []gin.H{}

	for _, shopID := range shopIDs {
		// Get location name
		var locationName string
		h.sqlDB().QueryRowContext(ctx,
			`SELECT COALESCE(name, '') FROM locations WHERE tekmetric_shop_id = $1`, shopID).Scan(&locationName)

		// For each day in range, compute total receivable for this shop
		daily := []gin.H{}
		current := startDate
		for !current.After(now) {
			dateStr := current.Format("2006-01-02")
			var dayTotal float64
			h.sqlDB().QueryRowContext(ctx,
				`SELECT COALESCE(SUM(j.subtotal), 0)
				 FROM tekmetric_repair_orders ro
				 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
				 WHERE ro.shop_id = $1
				   AND ro.repair_order_status_code = 'WORKINPROGRESS'
				   AND ro.created_date::date <= $2`, shopID, dateStr).Scan(&dayTotal)

			daily = append(daily, gin.H{
				"date":             dateStr,
				"total_receivable": round2(dayTotal),
			})
			current = current.AddDate(0, 0, 1)
		}

		// Latest day total contributes to overall
		if len(daily) > 0 {
			lastDay := daily[len(daily)-1]
			overallTotal += lastDay["total_receivable"].(float64)
		}

		shops = append(shops, gin.H{
			"shop_id":       shopID,
			"location_name": locationName,
			"daily":         daily,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total": gin.H{
			"total_receivable": round2(overallTotal),
		},
		"shops": shops,
	})
}
