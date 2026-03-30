package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionHandler struct {
	DB *pgxpool.Pool
}

// POST /transactions/
func (h *TransactionHandler) CreateTransaction(c *gin.Context) {
	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	id := uuid.New().String()
	now := time.Now().UTC()

	// Extract known fields
	cols := []string{"id", "user_id", "created_at", "updated_at"}
	placeholders := []string{"$1", "$2", "$3", "$4"}
	args := []interface{}{id, uid, now, now}
	argIdx := 5

	txFields := []string{
		"plaid_id", "account_id", "date", "authorized_date", "transaction_datetime",
		"authorized_datetime", "amount", "currency_iso", "currency_unofficial",
		"available_balance", "current_balance", "balance_limit", "name", "merchant_name",
		"merchant_entity_id", "website", "logo_url", "category", "category_id",
		"personal_finance_category", "pfc_icon_url", "payment_channel", "transaction_type",
		"transaction_code", "running_balance", "pending", "pending_id", "location",
		"counterparties", "payment_meta", "account_owner", "check_number", "vendor",
		"source", "linked_document", "created_by", "documents", "is_duplicated",
		"is_acknowledged", "account_type", "account_subtype", "account_name",
		"ro_number", "ro_id", "payment_method_id", "gl_code_id",
	}

	jsonFields := map[string]bool{
		"category": true, "personal_finance_category": true, "location": true,
		"counterparties": true, "payment_meta": true, "documents": true,
	}

	for _, field := range txFields {
		val, exists := body[field]
		if !exists || val == nil {
			continue
		}
		cols = append(cols, field)
		placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
		if jsonFields[field] {
			jsonBytes, _ := json.Marshal(val)
			args = append(args, string(jsonBytes))
		} else {
			args = append(args, val)
		}
		argIdx++
	}

	query := fmt.Sprintf("INSERT INTO transactions (%s) VALUES (%s)",
		strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	_, err := h.DB.Exec(context.Background(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("failed to create transaction: %v", err)})
		return
	}

	h.getTransactionByIDInternal(c, id)
}

// GET /transactions/
func (h *TransactionHandler) ListTransactions(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	if page < 1 {
		page = 1
	}
	pageSize := 10

	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	transactionType := c.Query("transaction_type")
	vendor := c.Query("vendor")
	category := c.Query("category")
	accountType := c.Query("account_type")
	onlyDuplicates := c.DefaultQuery("only_duplicates", "false") == "true"
	isPending := c.DefaultQuery("is_pending", "false") == "true"
	sortBy := c.DefaultQuery("sort_by", "date")
	sortOrder := c.DefaultQuery("sort_order", "desc")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	where := "WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if roleStr != "Admin" {
		where += fmt.Sprintf(" AND user_id = $%d", argIdx)
		args = append(args, uid)
		argIdx++
	}

	if startDate != "" && endDate != "" {
		where += fmt.Sprintf(" AND date BETWEEN $%d AND $%d", argIdx, argIdx+1)
		args = append(args, startDate, endDate)
		argIdx += 2
	} else if startDate != "" {
		where += fmt.Sprintf(" AND date >= $%d", argIdx)
		args = append(args, startDate)
		argIdx++
	} else if endDate != "" {
		where += fmt.Sprintf(" AND date <= $%d", argIdx)
		args = append(args, endDate)
		argIdx++
	}

	if transactionType != "" {
		where += fmt.Sprintf(" AND transaction_type = $%d", argIdx)
		args = append(args, transactionType)
		argIdx++
	}
	if vendor != "" {
		where += fmt.Sprintf(" AND vendor = $%d", argIdx)
		args = append(args, vendor)
		argIdx++
	}
	if category != "" {
		where += fmt.Sprintf(" AND category_id = $%d", argIdx)
		args = append(args, category)
		argIdx++
	}
	if accountType != "" {
		where += fmt.Sprintf(" AND account_type = $%d", argIdx)
		args = append(args, accountType)
		argIdx++
	}
	if onlyDuplicates {
		where += " AND is_duplicated = true"
	}
	if !isPending {
		where += " AND (pending = false OR pending IS NULL)"
	}

	// Validate sort field
	validSortFields := map[string]string{
		"date": "date", "authorized_date": "authorized_date",
		"transaction_datetime": "transaction_datetime", "authorized_datetime": "authorized_datetime",
		"created_at": "created_at", "updated_at": "updated_at",
		"amount": "amount", "available_balance": "available_balance",
		"current_balance": "current_balance", "balance_limit": "balance_limit",
		"name": "name", "merchant_name": "merchant_name", "vendor": "vendor",
		"transaction_type": "transaction_type", "transaction_code": "transaction_code",
		"payment_channel": "payment_channel", "account_id": "account_id",
		"account_type": "account_type", "account_subtype": "account_subtype",
		"account_name": "account_name", "account_owner": "account_owner",
		"category_id": "category_id", "currency_iso": "currency_iso",
		"currency_unofficial": "currency_unofficial", "source": "source",
		"check_number": "check_number", "plaid_id": "plaid_id",
		"merchant_entity_id": "merchant_entity_id", "website": "website",
		"pending_id": "pending_id", "created_by": "created_by",
		"ro_number": "ro_number", "ro_id": "ro_id",
		"gl_code": "gl_code_id", "payment_method": "payment_method_id",
		"is_pending": "pending",
	}

	sortCol, ok := validSortFields[sortBy]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid sort field: %s", sortBy)})
		return
	}

	order := "DESC"
	if strings.ToLower(sortOrder) == "asc" {
		order = "ASC"
	}

	// Count total
	var totalCount int
	countQuery := "SELECT COUNT(*) FROM transactions " + where
	h.DB.QueryRow(context.Background(), countQuery, args...).Scan(&totalCount)

	totalPages := int(math.Ceil(float64(totalCount) / float64(pageSize)))
	if page > totalPages && totalPages > 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Page not found"})
		return
	}

	offset := (page - 1) * pageSize
	dataQuery := fmt.Sprintf("SELECT id FROM transactions %s ORDER BY %s %s OFFSET $%d LIMIT $%d",
		where, sortCol, order, argIdx, argIdx+1)
	args = append(args, offset, pageSize)

	rows, err := h.DB.Query(context.Background(), dataQuery, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query transactions"})
		return
	}
	defer rows.Close()

	transactions := []map[string]interface{}{}
	for rows.Next() {
		var txID string
		rows.Scan(&txID)
		tx := h.loadTransaction(txID)
		if tx != nil {
			transactions = append(transactions, tx)
		}
	}

	var nextPage interface{} = nil
	if page < totalPages {
		nextPage = page + 1
	}
	pending := totalCount - page*pageSize
	if pending < 0 {
		pending = 0
	}

	c.JSON(http.StatusOK, gin.H{
		"transactions":  transactions,
		"pending_count": pending,
		"next_page":     nextPage,
		"total_pages":   totalPages,
		"current_page":  page,
		"sort_by":       sortBy,
		"sort_order":    sortOrder,
	})
}

// GET /transactions/:transaction_id
func (h *TransactionHandler) GetTransaction(c *gin.Context) {
	txID := c.Param("transaction_id")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var txUserID *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM transactions WHERE id = $1", txID).Scan(&txUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	if roleStr != "Admin" && (txUserID == nil || *txUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to view this transaction"})
		return
	}

	h.getTransactionByIDInternal(c, txID)
}

// PATCH /transactions/:transaction_id
func (h *TransactionHandler) UpdateTransaction(c *gin.Context) {
	txID := c.Param("transaction_id")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var txUserID *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM transactions WHERE id = $1", txID).Scan(&txUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	if roleStr != "Admin" && (txUserID == nil || *txUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to update this transaction"})
		return
	}

	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	if len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "No fields to update"})
		return
	}

	// Capture original data for change log
	originalData := h.loadTransaction(txID)

	jsonFields := map[string]bool{
		"category": true, "personal_finance_category": true, "location": true,
		"counterparties": true, "payment_meta": true, "documents": true,
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	originalLog := map[string]interface{}{}
	editedLog := map[string]interface{}{}

	for field, value := range body {
		if originalData != nil {
			originalLog[field] = originalData[field]
		}
		editedLog[field] = value

		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, argIdx))
		if jsonFields[field] {
			jsonBytes, _ := json.Marshal(value)
			args = append(args, string(jsonBytes))
		} else {
			args = append(args, value)
		}
		argIdx++
	}

	setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
	args = append(args, time.Now().UTC())
	argIdx++

	args = append(args, txID)
	query := fmt.Sprintf("UPDATE transactions SET %s WHERE id = $%d",
		strings.Join(setClauses, ", "), argIdx)

	_, err = h.DB.Exec(context.Background(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update transaction"})
		return
	}

	// Write change log
	origJSON, _ := json.Marshal(originalLog)
	editJSON, _ := json.Marshal(editedLog)
	logID := uuid.New().String()

	h.DB.Exec(context.Background(),
		`INSERT INTO transaction_change_logs (id, transaction_id, original_data, edited_data, changed_by, changed_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		logID, txID, string(origJSON), string(editJSON), uid, time.Now().UTC())

	h.getTransactionByIDInternal(c, txID)
}

// DELETE /transactions/:transaction_id
func (h *TransactionHandler) DeleteTransaction(c *gin.Context) {
	txID := c.Param("transaction_id")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var txUserID *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM transactions WHERE id = $1", txID).Scan(&txUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	if roleStr != "Admin" && (txUserID == nil || *txUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to delete this transaction"})
		return
	}

	h.DB.Exec(context.Background(), "DELETE FROM transactions WHERE id = $1", txID)
	c.JSON(http.StatusOK, gin.H{"message": "Transaction deleted successfully!"})
}

// POST /transactions/import-data
func (h *TransactionHandler) ImportData(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to open file"})
		return
	}
	defer f.Close()

	content, _ := io.ReadAll(f)
	reader := csv.NewReader(strings.NewReader(string(content)))
	records, err := reader.ReadAll()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Error importing data: %v", err)})
		return
	}

	if len(records) < 2 {
		c.JSON(http.StatusOK, gin.H{"message": "Data imported successfully.", "records": []interface{}{}})
		return
	}

	header := records[0]
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	imported := []map[string]interface{}{}
	now := time.Now().UTC()

	for _, row := range records[1:] {
		id := uuid.New().String()
		dateStr := ""
		vendorStr := ""
		amountStr := ""
		categoryStr := ""
		locationStr := ""

		if idx, ok := colIdx["date"]; ok && idx < len(row) {
			dateStr = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["vendor"]; ok && idx < len(row) {
			vendorStr = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["amount"]; ok && idx < len(row) {
			amountStr = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["category"]; ok && idx < len(row) {
			categoryStr = strings.TrimSpace(row[idx])
		}
		if idx, ok := colIdx["location"]; ok && idx < len(row) {
			locationStr = strings.TrimSpace(row[idx])
		}

		amount, _ := strconv.ParseFloat(amountStr, 64)

		_, err := h.DB.Exec(context.Background(),
			`INSERT INTO transactions (id, date, vendor, amount, category, location, created_by, user_id, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, 'system', $7, $8, $9)`,
			id, dateStr, vendorStr, amount, categoryStr, locationStr, uid, now, now)
		if err != nil {
			continue
		}

		imported = append(imported, map[string]interface{}{
			"id": id, "date": dateStr, "vendor": vendorStr, "amount": amount,
		})
	}

	c.JSON(http.StatusOK, gin.H{"message": "Data imported successfully.", "records": imported})
}

// POST /transactions/:transaction_id/upload-document
func (h *TransactionHandler) UploadDocument(c *gin.Context) {
	txID := c.Param("transaction_id")

	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)
	role, _ := c.Get("role")
	roleStr := fmt.Sprintf("%v", role)

	var txUserID *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT user_id FROM transactions WHERE id = $1", txID).Scan(&txUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	if roleStr != "Admin" && (txUserID == nil || *txUserID != uid) {
		c.JSON(http.StatusForbidden, gin.H{"detail": "You do not have permission to upload documents for this transaction"})
		return
	}

	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}

	fileDir := fmt.Sprintf("static/documents/%s", txID)
	os.MkdirAll(fileDir, 0755)
	filePath := filepath.Join(fileDir, file.Filename)

	if err := c.SaveUploadedFile(file, filePath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to save file"})
		return
	}

	// Append to documents JSON array
	var docsJSON *string
	h.DB.QueryRow(context.Background(),
		"SELECT documents FROM transactions WHERE id = $1", txID).Scan(&docsJSON)

	var docs []string
	if docsJSON != nil && *docsJSON != "" {
		json.Unmarshal([]byte(*docsJSON), &docs)
	}
	docs = append(docs, filePath)
	newDocsJSON, _ := json.Marshal(docs)

	h.DB.Exec(context.Background(),
		"UPDATE transactions SET documents = $1 WHERE id = $2", string(newDocsJSON), txID)

	c.JSON(http.StatusOK, gin.H{"message": "Document uploaded successfully."})
}

// GET /transactions/changes/:transaction_id
func (h *TransactionHandler) ListTransactionChanges(c *gin.Context) {
	txID := c.Param("transaction_id")

	rows, err := h.DB.Query(context.Background(),
		`SELECT cl.id, cl.transaction_id, cl.original_data, cl.edited_data,
		        CONCAT(u.first_name, ' ', u.last_name) AS changed_by_name, cl.changed_at
		 FROM transaction_change_logs cl
		 JOIN users u ON cl.changed_by = u.id
		 WHERE cl.transaction_id = $1`, txID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query change logs"})
		return
	}
	defer rows.Close()

	changes := []map[string]interface{}{}
	for rows.Next() {
		var id, transactionID, changedByName string
		var originalDataStr, editedDataStr string
		var changedAt time.Time

		if err := rows.Scan(&id, &transactionID, &originalDataStr, &editedDataStr, &changedByName, &changedAt); err != nil {
			continue
		}

		var origData, editData interface{}
		json.Unmarshal([]byte(originalDataStr), &origData)
		json.Unmarshal([]byte(editedDataStr), &editData)

		changes = append(changes, map[string]interface{}{
			"id":              id,
			"transaction_id":  transactionID,
			"original_data":   origData,
			"edited_data":     editData,
			"changed_by":      changedByName,
			"changed_at":      changedAt,
		})
	}

	c.JSON(http.StatusOK, changes)
}

// POST /transactions/reverse-change
func (h *TransactionHandler) ReverseChange(c *gin.Context) {
	var req struct {
		ChangeLogID string `json:"change_log_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var transactionID, originalDataStr string
	err := h.DB.QueryRow(context.Background(),
		"SELECT transaction_id, original_data FROM transaction_change_logs WHERE id = $1", req.ChangeLogID,
	).Scan(&transactionID, &originalDataStr)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Change log not found"})
		return
	}

	// Check transaction exists
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM transactions WHERE id = $1)", transactionID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	var originalData map[string]interface{}
	json.Unmarshal([]byte(originalDataStr), &originalData)

	jsonFields := map[string]bool{
		"category": true, "personal_finance_category": true, "location": true,
		"counterparties": true, "payment_meta": true, "documents": true,
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	for field, value := range originalData {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, argIdx))
		if jsonFields[field] {
			jsonBytes, _ := json.Marshal(value)
			args = append(args, string(jsonBytes))
		} else {
			args = append(args, value)
		}
		argIdx++
	}

	if len(setClauses) > 0 {
		args = append(args, transactionID)
		query := fmt.Sprintf("UPDATE transactions SET %s WHERE id = $%d",
			strings.Join(setClauses, ", "), argIdx)
		h.DB.Exec(context.Background(), query, args...)
	}

	h.getTransactionByIDInternal(c, transactionID)
}

// PUT /transactions/liability-minimum-balance/:liability_id
func (h *TransactionHandler) UpdateLiabilityMinimumBalance(c *gin.Context) {
	liabilityID := c.Param("liability_id")

	var req struct {
		MinimumBalance       interface{} `json:"minimum_balance" binding:"required"`
		StartingCreditCardBal interface{} `json:"starting_credit_card_bal" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM liabilities WHERE id = $1)", liabilityID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Liability not found"})
		return
	}

	_, err := h.DB.Exec(context.Background(),
		"UPDATE liabilities SET minimum_balance = $1, starting_credit_card_bal = $2 WHERE id = $3",
		req.MinimumBalance, req.StartingCreditCardBal, liabilityID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update liability"})
		return
	}

	var minBal, startBal string
	h.DB.QueryRow(context.Background(),
		"SELECT COALESCE(minimum_balance::text, '0'), COALESCE(starting_credit_card_bal::text, '0') FROM liabilities WHERE id = $1",
		liabilityID).Scan(&minBal, &startBal)

	c.JSON(http.StatusOK, gin.H{
		"message":                  "Minimum balance updated successfully",
		"liability_id":             liabilityID,
		"minimum_balance":          minBal,
		"starting_credit_card_bal": startBal,
	})
}

func (h *TransactionHandler) getTransactionByIDInternal(c *gin.Context, id string) {
	tx := h.loadTransaction(id)
	if tx == nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}
	c.JSON(http.StatusOK, tx)
}

func (h *TransactionHandler) loadTransaction(id string) map[string]interface{} {
	row := h.DB.QueryRow(context.Background(),
		`SELECT id, plaid_id, account_id, date, authorized_date, transaction_datetime, authorized_datetime,
		 amount, currency_iso, currency_unofficial, available_balance, current_balance, balance_limit,
		 name, merchant_name, merchant_entity_id, website, logo_url, category, category_id,
		 personal_finance_category, pfc_icon_url, payment_channel, transaction_type, transaction_code,
		 running_balance, pending, pending_id, location, counterparties, payment_meta,
		 account_owner, check_number, vendor, source, linked_document, created_by, documents,
		 created_at, updated_at, is_duplicated, is_acknowledged, user_id, account_type, account_subtype,
		 account_name, ro_number, ro_id, payment_method_id, gl_code_id
		 FROM transactions WHERE id = $1`, id)

	var txID string
	var plaidID, accountID, currencyISO, currencyUnofficial *string
	var txDate, authorizedDate *time.Time
	var txDatetime, authDatetime *time.Time
	var amount, availBalance, curBalance, balLimit, runningBalance *float64
	var name, merchantName, merchantEntityID, website, logoURL *string
	var categoryJSON, categoryID *string
	var pfcJSON, pfcIconURL, paymentChannel, txType, txCode *string
	var pending *bool
	var pendingID *string
	var locationJSON, counterpartiesJSON, paymentMetaJSON *string
	var accountOwner, checkNumber, vendorStr, source, linkedDoc, createdBy *string
	var documentsJSON *string
	var createdAt, updatedAt *time.Time
	var isDuplicated, isAcknowledged *bool
	var userIDStr *string
	var accountTypeStr, accountSubtype, accountName *string
	var roNumber *int
	var roID, paymentMethodID, glCodeID *string

	err := row.Scan(&txID, &plaidID, &accountID, &txDate, &authorizedDate, &txDatetime, &authDatetime,
		&amount, &currencyISO, &currencyUnofficial, &availBalance, &curBalance, &balLimit,
		&name, &merchantName, &merchantEntityID, &website, &logoURL, &categoryJSON, &categoryID,
		&pfcJSON, &pfcIconURL, &paymentChannel, &txType, &txCode,
		&runningBalance, &pending, &pendingID, &locationJSON, &counterpartiesJSON, &paymentMetaJSON,
		&accountOwner, &checkNumber, &vendorStr, &source, &linkedDoc, &createdBy, &documentsJSON,
		&createdAt, &updatedAt, &isDuplicated, &isAcknowledged, &userIDStr, &accountTypeStr, &accountSubtype,
		&accountName, &roNumber, &roID, &paymentMethodID, &glCodeID)
	if err != nil {
		return nil
	}

	result := map[string]interface{}{
		"id": txID, "plaid_id": plaidID, "account_id": accountID,
		"amount": amount, "currency_iso": currencyISO, "currency_unofficial": currencyUnofficial,
		"available_balance": availBalance, "current_balance": curBalance, "balance_limit": balLimit,
		"name": name, "merchant_name": merchantName, "merchant_entity_id": merchantEntityID,
		"website": website, "logo_url": logoURL, "category_id": categoryID,
		"pfc_icon_url": pfcIconURL, "payment_channel": paymentChannel,
		"transaction_type": txType, "transaction_code": txCode,
		"running_balance": runningBalance, "pending": pending, "pending_id": pendingID,
		"account_owner": accountOwner, "check_number": checkNumber,
		"vendor": vendorStr, "source": source, "linked_document": linkedDoc,
		"created_by": createdBy, "created_at": createdAt, "updated_at": updatedAt,
		"is_duplicated": isDuplicated, "is_acknowledged": isAcknowledged,
		"user_id": userIDStr, "account_type": accountTypeStr, "account_subtype": accountSubtype,
		"account_name": accountName, "ro_number": roNumber, "ro_id": roID,
		"payment_method_id": paymentMethodID, "gl_code_id": glCodeID,
	}

	// Format dates
	if txDate != nil {
		result["date"] = txDate.Format("2006-01-02")
	} else {
		result["date"] = nil
	}
	if authorizedDate != nil {
		result["authorized_date"] = authorizedDate.Format("2006-01-02")
	} else {
		result["authorized_date"] = nil
	}
	result["transaction_datetime"] = txDatetime
	result["authorized_datetime"] = authDatetime

	// Parse JSON fields
	parseJSON := func(s *string) interface{} {
		if s == nil || *s == "" {
			return nil
		}
		var v interface{}
		if json.Unmarshal([]byte(*s), &v) == nil {
			return v
		}
		return *s
	}

	result["category"] = parseJSON(categoryJSON)
	result["personal_finance_category"] = parseJSON(pfcJSON)
	result["location"] = parseJSON(locationJSON)
	result["counterparties"] = parseJSON(counterpartiesJSON)
	result["payment_meta"] = parseJSON(paymentMetaJSON)
	result["documents"] = parseJSON(documentsJSON)

	return result
}
