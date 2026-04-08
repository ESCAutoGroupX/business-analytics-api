package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type AssetImportHandler struct {
	GormDB *gorm.DB
}

// POST /xero/assets/import-csv
func (h *AssetImportHandler) ImportCSV(c *gin.Context) {
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to read CSV headers"})
		return
	}

	colIdx := map[string]int{}
	for i, h := range headers {
		h = strings.TrimSpace(h)
		h = strings.Trim(h, "\"")
		h = strings.TrimPrefix(h, "*")
		h = strings.TrimSpace(h)
		colIdx[h] = i
	}

	if _, ok := colIdx["AssetNumber"]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "CSV must contain AssetNumber column"})
		return
	}

	imported := 0
	var importErrors []string
	rowNum := 1

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			importErrors = append(importErrors, fmt.Sprintf("row %d: %v", rowNum, err))
			continue
		}

		assetNumber := csvGet(row, colIdx, "AssetNumber")
		if assetNumber == "" {
			importErrors = append(importErrors, fmt.Sprintf("row %d: empty AssetNumber", rowNum))
			continue
		}

		status := csvGet(row, colIdx, "AssetStatus")
		depMethod := csvGet(row, colIdx, "Book_DepreciationMethod")
		description := csvGet(row, colIdx, "Description")
		location := csvGet(row, colIdx, "TrackingOption1")
		assetTypeName := csvGet(row, colIdx, "AssetType")

		fields := map[string]interface{}{
			"xero_id":                 fmt.Sprintf("csv-import-%s", assetNumber),
			"tenant_id":               "csv-import",
			"asset_name":              csvGet(row, colIdx, "AssetName"),
			"status":                  csvNilIfEmpty(status),
			"purchase_date":           csvParseDate(csvGet(row, colIdx, "PurchaseDate")),
			"purchase_price":          csvParseFloat(csvGet(row, colIdx, "PurchasePrice")),
			"asset_type":              csvGet(row, colIdx, "AssetType"),
			"asset_type_name":         csvNilIfEmpty(assetTypeName),
			"description":             csvNilIfEmpty(description),
			"location":                csvNilIfEmpty(location),
			"depreciation_method":     csvNilIfEmpty(depMethod),
			"depreciation_rate":       csvParseFloat(csvGet(row, colIdx, "Book_Rate")),
			"effective_life":          csvParseFloat(csvGet(row, colIdx, "Book_EffectiveLife")),
			"book_value":              csvParseFloat(csvGet(row, colIdx, "Book_BookValue")),
			"residual_value":          csvParseFloat(csvGet(row, colIdx, "Book_ResidualValue")),
			"accumulated_depreciation": csvParseFloat(csvGet(row, colIdx, "AccumulatedDepreciation")),
			"depreciation_start_date": csvParseDate(csvGet(row, colIdx, "Book_DepreciationStartDate")),
			"disposal_date":           csvParseDate(csvGet(row, colIdx, "DisposalDate")),
			"synced_at":               time.Now(),
		}

		// Upsert: find by asset_number, update or create
		var existing models.XeroAsset
		result := h.GormDB.Where("asset_number = ?", assetNumber).First(&existing)
		if result.Error == nil {
			if err := h.GormDB.Model(&existing).Updates(fields).Error; err != nil {
				importErrors = append(importErrors, fmt.Sprintf("row %d (%s): %v", rowNum, assetNumber, err))
				continue
			}
		} else {
			fields["asset_number"] = assetNumber
			if err := h.GormDB.Model(&models.XeroAsset{}).Create(fields).Error; err != nil {
				importErrors = append(importErrors, fmt.Sprintf("row %d (%s): %v", rowNum, assetNumber, err))
				continue
			}
		}
		imported++
	}

	c.JSON(http.StatusOK, gin.H{
		"imported": imported,
		"errors":   importErrors,
	})
}

func csvGet(row []string, idx map[string]int, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func csvParseDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	for _, layout := range []string{"1/2/2006", "01/02/2006", "2006-01-02", "1/2/06"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

func csvParseFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "$", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

func csvNilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
