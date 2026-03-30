package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

type TekmetricHandler struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
}

// ---------------------------------------------------------------
// Tekmetric API helpers
// ---------------------------------------------------------------

func (h *TekmetricHandler) tekmetricAccessToken() (string, error) {
	baseURL := h.Cfg.TekmetricBaseURL
	if baseURL == "" {
		baseURL = "https://sandbox.tekmetric.com"
	}

	payload := "grant_type=client_credentials"
	req, _ := http.NewRequest("POST", baseURL+"/api/v1/oauth/token", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Basic "+h.Cfg.TekmetricBase64AuthKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if token, ok := result["access_token"].(string); ok {
		return token, nil
	}
	return "", fmt.Errorf("failed to get tekmetric access token")
}

func (h *TekmetricHandler) tekmetricGet(endpoint string, params map[string]string) (map[string]interface{}, error) {
	baseURL := h.Cfg.TekmetricBaseURL
	if baseURL == "" {
		baseURL = "https://sandbox.tekmetric.com"
	}

	token, err := h.tekmetricAccessToken()
	if err != nil {
		return nil, err
	}

	url := baseURL + endpoint
	if len(params) > 0 {
		parts := []string{}
		for k, v := range params {
			parts = append(parts, k+"="+v)
		}
		url += "?" + strings.Join(parts, "&")
	}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, nil
}

// ---------------------------------------------------------------
// GET /tekmetric/shops
// ---------------------------------------------------------------

func (h *TekmetricHandler) GetShops(c *gin.Context) {
	result, err := h.tekmetricGet("/api/v1/shops", nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	shops := []interface{}{}
	if content, ok := result["content"].([]interface{}); ok {
		shops = content
	} else if _, ok := result["shops"]; ok {
		shops, _ = result["shops"].([]interface{})
	}

	c.JSON(http.StatusOK, gin.H{"shops": shops})
}

// ---------------------------------------------------------------
// GET /tekmetric/repair-orders
// ---------------------------------------------------------------

var roSortColumnMap = map[string]string{
	"repairOrderNumber": "ro_number",
	"createdDate":       "created_date",
	"updatedDate":       "updated_date",
	"postedDate":        "posted_date",
	"completedDate":     "completed_date",
	"customerId":        "customer_id",
	"vehicleId":         "vehicle_id",
	"amountPaid":        "amount_paid",
	"totalSales":        "total_sales",
	"laborSales":        "labor_sales",
	"partsSales":        "parts_sales",
	"subletSales":       "sublet_sales",
	"discountTotal":     "discount_total",
	"feeTotal":          "fee_total",
}

func (h *TekmetricHandler) GetRepairOrders(c *gin.Context) {
	shopIDsParam := c.QueryArray("shop_ids")
	if len(shopIDsParam) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "shop_ids is required"})
		return
	}

	shopIDs := []int{}
	for _, s := range shopIDsParam {
		id, err := strconv.Atoi(s)
		if err != nil {
			continue
		}
		shopIDs = append(shopIDs, id)
	}
	if len(shopIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "valid shop_ids required"})
		return
	}

	start := c.Query("start")
	end := c.Query("end")
	postedDateStart := c.Query("posted_date_start")
	postedDateEnd := c.Query("posted_date_end")
	updatedDateStart := c.Query("updated_date_start")
	updatedDateEnd := c.Query("updated_date_end")
	roNumberStr := c.Query("repair_order_number")
	roStatusIDs := c.QueryArray("repair_order_status_id")
	customerIDStr := c.Query("customer_id")
	vehicleIDStr := c.Query("vehicle_id")
	search := c.Query("search")
	sortBy := c.DefaultQuery("sort", "createdDate")
	sortDir := c.DefaultQuery("sort_direction", "DESC")
	sizeStr := c.DefaultQuery("size", "100")
	pageStr := c.DefaultQuery("page", "0")

	size, _ := strconv.Atoi(sizeStr)
	if size > 100 || size < 1 {
		size = 100
	}
	page, _ := strconv.Atoi(pageStr)

	sortCol, ok := roSortColumnMap[sortBy]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid sort field: %s", sortBy)})
		return
	}

	order := "DESC"
	if strings.ToUpper(sortDir) == "ASC" {
		order = "ASC"
	}

	// Build query
	where := "WHERE ro.shop_id = ANY($1)"
	args := []interface{}{shopIDs}
	argIdx := 2

	if len(roStatusIDs) > 0 {
		ids := []int{}
		for _, s := range roStatusIDs {
			if id, err := strconv.Atoi(s); err == nil {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			where += fmt.Sprintf(" AND ro.repair_order_status_id = ANY($%d)", argIdx)
			args = append(args, ids)
			argIdx++
		}
	}

	if roNumberStr != "" {
		if roNum, err := strconv.Atoi(roNumberStr); err == nil {
			where += fmt.Sprintf(" AND ro.ro_number = $%d", argIdx)
			args = append(args, roNum)
			argIdx++
		}
	}

	if customerIDStr != "" {
		if cid, err := strconv.Atoi(customerIDStr); err == nil {
			where += fmt.Sprintf(" AND ro.customer_id = $%d", argIdx)
			args = append(args, cid)
			argIdx++
		}
	}

	if vehicleIDStr != "" {
		if vid, err := strconv.Atoi(vehicleIDStr); err == nil {
			where += fmt.Sprintf(" AND ro.vehicle_id = $%d", argIdx)
			args = append(args, vid)
			argIdx++
		}
	}

	if postedDateStart != "" {
		where += fmt.Sprintf(" AND ro.posted_date >= $%d", argIdx)
		args = append(args, postedDateStart)
		argIdx++
	}
	if postedDateEnd != "" {
		where += fmt.Sprintf(" AND ro.posted_date <= $%d", argIdx)
		args = append(args, postedDateEnd)
		argIdx++
	}

	if updatedDateStart != "" {
		where += fmt.Sprintf(" AND ro.updated_date >= $%d", argIdx)
		args = append(args, updatedDateStart)
		argIdx++
	}
	if updatedDateEnd != "" {
		where += fmt.Sprintf(" AND ro.updated_date <= $%d", argIdx)
		args = append(args, updatedDateEnd)
		argIdx++
	}

	if start != "" {
		where += fmt.Sprintf(" AND ro.created_date >= $%d", argIdx)
		args = append(args, start)
		argIdx++
	}
	if end != "" {
		where += fmt.Sprintf(" AND ro.created_date <= $%d", argIdx)
		args = append(args, end)
		argIdx++
	}

	if search != "" {
		searchTerm := "%" + strings.ToLower(search) + "%"
		where += fmt.Sprintf(" AND (LOWER(ro.repair_order_status_name) LIKE $%d OR CAST(ro.ro_number AS TEXT) LIKE $%d)", argIdx, argIdx)
		args = append(args, searchTerm)
		argIdx++
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM tekmetric_repair_orders ro %s", where)
	var totalCount int
	h.DB.QueryRow(context.Background(), countQuery, args...).Scan(&totalCount)

	totalPages := 0
	if size > 0 {
		totalPages = int(math.Ceil(float64(totalCount) / float64(size)))
	}

	// Fetch data
	offset := page * size
	dataQuery := fmt.Sprintf(
		`SELECT ro.id, ro.ro_number, ro.shop_id, ro.customer_id, ro.vehicle_id,
		 ro.labor_sales, ro.parts_sales, ro.sublet_sales, ro.discount_total, ro.fee_total,
		 ro.taxes, ro.amount_paid, ro.total_sales,
		 ro.posted_date, ro.completed_date, ro.created_date, ro.updated_date,
		 ro.repair_order_status_id, ro.repair_order_status_code, ro.repair_order_status_name,
		 ro.payment_method_id, ro.created_at, ro.updated_at
		 FROM tekmetric_repair_orders ro
		 %s ORDER BY ro.%s %s OFFSET $%d LIMIT $%d`,
		where, sortCol, order, argIdx, argIdx+1)
	args = append(args, offset, size)

	rows, err := h.DB.Query(context.Background(), dataQuery, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("query error: %v", err)})
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		ro := h.scanRepairOrder(rows)
		if ro != nil {
			data = append(data, ro)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"data":           data,
		"total":          totalCount,
		"page":           page,
		"size":           size,
		"total_pages":    totalPages,
		"sort_by":        sortBy,
		"sort_direction": sortDir,
	})
}

// ---------------------------------------------------------------
// GET /tekmetric/repair-orders/:repair_order_id
// ---------------------------------------------------------------

func (h *TekmetricHandler) GetRepairOrderByID(c *gin.Context) {
	roID := c.Param("repair_order_id")

	row := h.DB.QueryRow(context.Background(),
		`SELECT id, ro_number, shop_id, customer_id, vehicle_id,
		 labor_sales, parts_sales, sublet_sales, discount_total, fee_total,
		 taxes, amount_paid, total_sales,
		 posted_date, completed_date, created_date, updated_date,
		 repair_order_status_id, repair_order_status_code, repair_order_status_name,
		 payment_method_id, created_at, updated_at
		 FROM tekmetric_repair_orders WHERE id = $1`, roID)

	ro := h.scanRepairOrderRow(row)
	if ro == nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Repair order not found"})
		return
	}

	c.JSON(http.StatusOK, ro)
}

// ---------------------------------------------------------------
// PATCH /tekmetric/repair-orders/:repair_order_id
// ---------------------------------------------------------------

func (h *TekmetricHandler) PatchRepairOrder(c *gin.Context) {
	roID := c.Param("repair_order_id")

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM tekmetric_repair_orders WHERE id = $1)", roID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Repair order not found"})
		return
	}

	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	validFields := map[string]bool{
		"ro_number": true, "shop_id": true, "customer_id": true, "vehicle_id": true,
		"labor_sales": true, "parts_sales": true, "sublet_sales": true,
		"discount_total": true, "fee_total": true, "taxes": true,
		"amount_paid": true, "total_sales": true,
		"posted_date": true, "completed_date": true, "created_date": true, "updated_date": true,
		"repair_order_status_id": true, "repair_order_status_code": true,
		"repair_order_status_name": true, "payment_method_id": true,
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	for field, value := range body {
		if !validFields[field] {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, argIdx))
		args = append(args, value)
		argIdx++
	}

	if len(setClauses) > 0 {
		setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
		args = append(args, time.Now().UTC())
		argIdx++

		args = append(args, roID)
		query := fmt.Sprintf("UPDATE tekmetric_repair_orders SET %s WHERE id = $%d",
			strings.Join(setClauses, ", "), argIdx)
		h.DB.Exec(context.Background(), query, args...)
	}

	// Return updated
	row := h.DB.QueryRow(context.Background(),
		`SELECT id, ro_number, shop_id, customer_id, vehicle_id,
		 labor_sales, parts_sales, sublet_sales, discount_total, fee_total,
		 taxes, amount_paid, total_sales,
		 posted_date, completed_date, created_date, updated_date,
		 repair_order_status_id, repair_order_status_code, repair_order_status_name,
		 payment_method_id, created_at, updated_at
		 FROM tekmetric_repair_orders WHERE id = $1`, roID)

	ro := h.scanRepairOrderRow(row)
	c.JSON(http.StatusOK, ro)
}

// ---------------------------------------------------------------
// PATCH /tekmetric/repair-orders/bulk/
// ---------------------------------------------------------------

func (h *TekmetricHandler) BulkPatchRepairOrders(c *gin.Context) {
	var req struct {
		Updates []struct {
			RepairOrderID string                 `json:"repair_order_id"`
			UpdateData    map[string]interface{} `json:"update_data"`
		} `json:"updates"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	validFields := map[string]bool{
		"ro_number": true, "shop_id": true, "customer_id": true, "vehicle_id": true,
		"labor_sales": true, "parts_sales": true, "sublet_sales": true,
		"discount_total": true, "fee_total": true, "taxes": true,
		"amount_paid": true, "total_sales": true,
		"posted_date": true, "completed_date": true, "created_date": true, "updated_date": true,
		"repair_order_status_id": true, "repair_order_status_code": true,
		"repair_order_status_name": true, "payment_method_id": true,
	}

	updatedIDs := []string{}

	for _, item := range req.Updates {
		var exists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM tekmetric_repair_orders WHERE id = $1)", item.RepairOrderID).Scan(&exists)
		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Repair order %s not found", item.RepairOrderID)})
			return
		}

		setClauses := []string{}
		args := []interface{}{}
		argIdx := 1

		for field, value := range item.UpdateData {
			if !validFields[field] {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, argIdx))
			args = append(args, value)
			argIdx++
		}

		if len(setClauses) > 0 {
			setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
			args = append(args, time.Now().UTC())
			argIdx++

			args = append(args, item.RepairOrderID)
			query := fmt.Sprintf("UPDATE tekmetric_repair_orders SET %s WHERE id = $%d",
				strings.Join(setClauses, ", "), argIdx)
			h.DB.Exec(context.Background(), query, args...)
		}

		updatedIDs = append(updatedIDs, item.RepairOrderID)
	}

	// Return updated records
	results := []map[string]interface{}{}
	for _, id := range updatedIDs {
		row := h.DB.QueryRow(context.Background(),
			`SELECT id, ro_number, shop_id, customer_id, vehicle_id,
			 labor_sales, parts_sales, sublet_sales, discount_total, fee_total,
			 taxes, amount_paid, total_sales,
			 posted_date, completed_date, created_date, updated_date,
			 repair_order_status_id, repair_order_status_code, repair_order_status_name,
			 payment_method_id, created_at, updated_at
			 FROM tekmetric_repair_orders WHERE id = $1`, id)
		ro := h.scanRepairOrderRow(row)
		if ro != nil {
			results = append(results, ro)
		}
	}

	c.JSON(http.StatusOK, results)
}

// ---------------------------------------------------------------
// GET /tekmetric/custo
// ---------------------------------------------------------------

func (h *TekmetricHandler) GetAllCustomersParallel(c *gin.Context) {
	// Get distinct customer_ids and vehicle_ids from repair orders
	custRows, err := h.DB.Query(context.Background(),
		"SELECT DISTINCT customer_id FROM tekmetric_repair_orders WHERE customer_id IS NOT NULL")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	customerIDs := []int{}
	for custRows.Next() {
		var id int
		custRows.Scan(&id)
		customerIDs = append(customerIDs, id)
	}
	custRows.Close()

	vehRows, err := h.DB.Query(context.Background(),
		"SELECT DISTINCT vehicle_id FROM tekmetric_repair_orders WHERE vehicle_id IS NOT NULL")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	vehicleIDs := []int{}
	for vehRows.Next() {
		var id int
		vehRows.Scan(&id)
		vehicleIDs = append(vehicleIDs, id)
	}
	vehRows.Close()

	totalCustomers := 0
	totalVehicles := 0

	// Fetch and save customers
	for _, cid := range customerIDs {
		result, err := h.tekmetricGet(fmt.Sprintf("/api/v1/customers/%d", cid), nil)
		if err != nil || result == nil {
			continue
		}

		h.saveCustomerToDB(result)
		totalCustomers++
	}

	// Fetch and save vehicles
	for _, vid := range vehicleIDs {
		result, err := h.tekmetricGet(fmt.Sprintf("/api/v1/vehicles/%d", vid), nil)
		if err != nil || result == nil {
			continue
		}

		h.saveVehicleToDB(result)
		totalVehicles++
	}

	c.JSON(http.StatusOK, gin.H{
		"total_customers": totalCustomers,
		"total_vehicles":  totalVehicles,
	})
}

func (h *TekmetricHandler) saveCustomerToDB(data map[string]interface{}) {
	id := data["id"]
	firstName, _ := data["firstName"].(string)
	lastName, _ := data["lastName"].(string)
	email, _ := data["email"].(string)
	phone := fmt.Sprintf("%v", data["phone"])
	address := fmt.Sprintf("%v", data["address"])
	notes, _ := data["notes"].(string)
	customerType := ""
	if ct, ok := data["customerType"].(map[string]interface{}); ok {
		customerType, _ = ct["name"].(string)
	}
	createdDate, _ := data["createdDate"].(string)
	updatedDate, _ := data["updatedDate"].(string)

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM tekmetric_customers WHERE id = $1)", id).Scan(&exists)

	if exists {
		h.DB.Exec(context.Background(),
			`UPDATE tekmetric_customers SET first_name=$1, last_name=$2, email=$3, phone=$4, address=$5,
			 notes=$6, customer_type=$7, created_date=$8, updated_date=$9 WHERE id=$10`,
			firstName, lastName, email, phone, address, notes, customerType, createdDate, updatedDate, id)
	} else {
		h.DB.Exec(context.Background(),
			`INSERT INTO tekmetric_customers (id, first_name, last_name, email, phone, address, notes, customer_type, created_date, updated_date)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			id, firstName, lastName, email, phone, address, notes, customerType, createdDate, updatedDate)
	}
}

func (h *TekmetricHandler) saveVehicleToDB(data map[string]interface{}) {
	id := data["id"]
	customerID := data["customerId"]

	// Check customer exists
	if customerID != nil {
		var custExists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM tekmetric_customers WHERE id = $1)", customerID).Scan(&custExists)
		if !custExists {
			return
		}
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM tekmetric_vehicles WHERE id = $1)", id).Scan(&exists)

	if exists {
		h.DB.Exec(context.Background(),
			`UPDATE tekmetric_vehicles SET customer_id=$1, year=$2, make=$3, model=$4, sub_model=$5,
			 engine=$6, color=$7, license_plate=$8, state=$9, vin=$10, drive_type=$11,
			 transmission=$12, body_type=$13, notes=$14, unit_number=$15, created_date=$16,
			 updated_date=$17, production_date=$18, deleted_date=$19 WHERE id=$20`,
			customerID, data["year"], data["make"], data["model"], data["subModel"],
			data["engine"], data["color"], data["licensePlate"], data["state"], data["vin"],
			data["driveType"], data["transmission"], data["bodyType"], data["notes"],
			data["unitNumber"], data["createdDate"], data["updatedDate"],
			data["productionDate"], data["deletedDate"], id)
	} else {
		h.DB.Exec(context.Background(),
			`INSERT INTO tekmetric_vehicles (id, customer_id, year, make, model, sub_model,
			 engine, color, license_plate, state, vin, drive_type, transmission, body_type,
			 notes, unit_number, created_date, updated_date, production_date, deleted_date)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
			id, customerID, data["year"], data["make"], data["model"], data["subModel"],
			data["engine"], data["color"], data["licensePlate"], data["state"], data["vin"],
			data["driveType"], data["transmission"], data["bodyType"], data["notes"],
			data["unitNumber"], data["createdDate"], data["updatedDate"],
			data["productionDate"], data["deletedDate"])
	}
}

// ---------------------------------------------------------------
// GET /tekmetric/jobs
// ---------------------------------------------------------------

func (h *TekmetricHandler) GetJobs(c *gin.Context) {
	shopIDs := c.QueryArray("shop")
	if len(shopIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "shop is required"})
		return
	}

	params := map[string]string{
		"size": c.DefaultQuery("size", "100"),
		"page": c.DefaultQuery("page", "0"),
	}
	if v := c.Query("vehicleId"); v != "" {
		params["vehicleId"] = v
	}
	if v := c.Query("repairOrderId"); v != "" {
		params["repairOrderId"] = v
	}
	if v := c.Query("customerId"); v != "" {
		params["customerId"] = v
	}
	if v := c.Query("authorized"); v != "" {
		params["authorized"] = v
	}
	if v := c.Query("authorizedDateStart"); v != "" {
		params["authorizedDateStart"] = v
	}
	if v := c.Query("authorizedDateEnd"); v != "" {
		params["authorizedDateEnd"] = v
	}
	if v := c.Query("updatedDateStart"); v != "" {
		params["updatedDateStart"] = v
	}
	if v := c.Query("updatedDateEnd"); v != "" {
		params["updatedDateEnd"] = v
	}
	if v := c.Query("sort"); v != "" {
		params["sort"] = v
	}
	if v := c.Query("sortDirection"); v != "" {
		params["sortDirection"] = v
	}

	allJobs := map[string]interface{}{}

	for _, shopID := range shopIDs {
		params["shop"] = shopID
		result, err := h.tekmetricGet("/api/v1/jobs", params)
		if err != nil {
			continue
		}

		if content, ok := result["content"].([]interface{}); ok {
			allJobs[shopID] = content
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Jobs fetched successfully", "jobs": allJobs})
}

// ---------------------------------------------------------------
// GET /tekmetric/jobs/:job_id
// ---------------------------------------------------------------

func (h *TekmetricHandler) GetJobByID(c *gin.Context) {
	jobID := c.Param("job_id")

	result, err := h.tekmetricGet(fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"job": result})
}

// ---------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------

type roScannable interface {
	Scan(dest ...interface{}) error
}

func (h *TekmetricHandler) scanRepairOrder(row roScannable) map[string]interface{} {
	return h.scanRepairOrderRow(row)
}

func (h *TekmetricHandler) scanRepairOrderRow(row roScannable) map[string]interface{} {
	var id string
	var roNumber, shopID, customerID, vehicleID *int
	var laborSales, partsSales, subletSales, discountTotal, feeTotal *int
	var taxes, amountPaid, totalSales *int
	var postedDate, completedDate, createdDate, updatedDate *time.Time
	var roStatusID *int
	var roStatusCode, roStatusName *string
	var paymentMethodID *string
	var createdAt, updatedAt *time.Time

	err := row.Scan(&id, &roNumber, &shopID, &customerID, &vehicleID,
		&laborSales, &partsSales, &subletSales, &discountTotal, &feeTotal,
		&taxes, &amountPaid, &totalSales,
		&postedDate, &completedDate, &createdDate, &updatedDate,
		&roStatusID, &roStatusCode, &roStatusName,
		&paymentMethodID, &createdAt, &updatedAt)
	if err != nil {
		return nil
	}

	return map[string]interface{}{
		"id":                          id,
		"ro_number":                   roNumber,
		"shop_id":                     shopID,
		"customer_id":                 customerID,
		"vehicle_id":                  vehicleID,
		"labor_sales":                 laborSales,
		"parts_sales":                 partsSales,
		"sublet_sales":                subletSales,
		"discount_total":              discountTotal,
		"fee_total":                   feeTotal,
		"taxes":                       taxes,
		"amount_paid":                 amountPaid,
		"total_sales":                 totalSales,
		"posted_date":                 postedDate,
		"completed_date":              completedDate,
		"created_date":                createdDate,
		"updated_date":                updatedDate,
		"repair_order_status_id":      roStatusID,
		"repair_order_status_code":    roStatusCode,
		"repair_order_status_name":    roStatusName,
		"payment_method_id":           paymentMethodID,
		"created_at":                  createdAt,
		"updated_at":                  updatedAt,
	}
}
