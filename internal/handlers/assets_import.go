package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type createAssetRequest struct {
	AssetName          string   `json:"asset_name" binding:"required"`
	AssetNumber        string   `json:"asset_number"`
	PurchaseDate       string   `json:"purchase_date" binding:"required"`
	PurchasePrice      float64  `json:"purchase_price" binding:"required"`
	Location           string   `json:"location"`
	AssetType          string   `json:"asset_type"`
	Description        string   `json:"description"`
	Status             string   `json:"status"`
	DepreciationMethod string   `json:"depreciation_method"`
	UsefulLife         *int     `json:"useful_life"`
	ResidualValue      *float64 `json:"residual_value"`
}

// POST /xero/assets
func (h *AssetImportHandler) CreateAsset(c *gin.Context) {
	var req createAssetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	// Generate asset number if empty
	if req.AssetNumber == "" {
		var maxNum int
		h.GormDB.Model(&models.XeroAsset{}).Select("COALESCE(MAX(CAST(SUBSTRING(asset_number FROM '[0-9]+$') AS INTEGER)), 0)").Where("asset_number LIKE 'FA-%'").Scan(&maxNum)
		req.AssetNumber = fmt.Sprintf("FA-%04d", maxNum+1)
	}

	asset := models.XeroAsset{
		XeroID:    fmt.Sprintf("manual-%s", req.AssetNumber),
		TenantID:  "",
		AssetName: req.AssetName,
		SyncedAt:  time.Now(),
	}

	an := req.AssetNumber
	asset.AssetNumber = &an

	if req.Status != "" {
		asset.Status = &req.Status
	}
	if req.AssetType != "" {
		asset.AssetTypeName = &req.AssetType
	}
	if req.Description != "" {
		asset.Description = &req.Description
	}
	if req.Location != "" {
		asset.Location = &req.Location
	}
	if req.DepreciationMethod != "" {
		asset.DepreciationMethodOverride = &req.DepreciationMethod
	}

	price := req.PurchasePrice
	asset.PurchasePrice = &price

	if req.ResidualValue != nil {
		asset.ResidualValue = req.ResidualValue
	}
	if req.UsefulLife != nil {
		asset.UsefulLifeYearsOverride = req.UsefulLife
	}

	// Parse purchase date
	if t, err := time.Parse("2006-01-02", req.PurchaseDate); err == nil {
		asset.PurchaseDate = &t
	}

	// Book value = purchase price initially
	bv := req.PurchasePrice
	asset.BookValue = &bv

	if err := h.GormDB.Create(&asset).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, asset)
}

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

	log.Printf("Asset CSV import: parsed headers: %v", colIdx)

	if _, ok := colIdx["AssetNumber"]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "CSV must contain AssetNumber column"})
		return
	}

	// Count rows first for logging
	var rows [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}
	log.Printf("Asset CSV import: processing %d rows", len(rows))

	imported := 0
	var importErrors []string

	for i, row := range rows {
		rowNum := i + 2 // 1-indexed, skip header

		assetNumber := csvGet(row, colIdx, "AssetNumber")
		if assetNumber == "" {
			importErrors = append(importErrors, fmt.Sprintf("row %d: empty AssetNumber", rowNum))
			continue
		}

		// Build the asset struct with all fields populated
		an := assetNumber
		var asset models.XeroAsset

		// Look up existing record
		findErr := h.GormDB.Where("asset_number = ?", assetNumber).First(&asset).Error
		isNew := findErr != nil

		// Always set all fields — use actual DB column struct fields
		asset.XeroID = fmt.Sprintf("csv-%s", assetNumber)
		asset.TenantID = ""
		asset.AssetNumber = &an
		asset.AssetName = csvGet(row, colIdx, "AssetName")
		asset.SyncedAt = time.Now()

		if v := csvGet(row, colIdx, "AssetStatus"); v != "" {
			asset.Status = &v
		}
		if v := csvGet(row, colIdx, "AssetType"); v != "" {
			asset.AssetTypeName = &v
		}
		if v := csvGet(row, colIdx, "Description"); v != "" {
			asset.Description = &v
		}
		if v := csvGet(row, colIdx, "TrackingOption1"); v != "" {
			asset.Location = &v
		}
		if v := csvGet(row, colIdx, "Book_DepreciationMethod"); v != "" {
			asset.DepreciationMethod = &v
		}

		asset.PurchasePrice = csvParseFloat(csvGet(row, colIdx, "PurchasePrice"))
		asset.DepreciationRate = csvParseFloat(csvGet(row, colIdx, "Book_Rate"))
		asset.BookValue = csvParseFloat(csvGet(row, colIdx, "Book_BookValue"))
		asset.ResidualValue = csvParseFloat(csvGet(row, colIdx, "Book_ResidualValue"))
		asset.CurrentAccumDepreciation = csvParseFloat(csvGet(row, colIdx, "AccumulatedDepreciation"))
		asset.PurchaseDate = csvParseDate(csvGet(row, colIdx, "PurchaseDate"))
		asset.DepreciationStartDate = csvParseDate(csvGet(row, colIdx, "Book_DepreciationStartDate"))
		asset.DisposalDate = csvParseDate(csvGet(row, colIdx, "DisposalDate"))

		// EffectiveLife: CSV float → model *int
		if v := csvParseFloat(csvGet(row, colIdx, "Book_EffectiveLife")); v != nil {
			years := int(math.Round(*v))
			asset.EffectiveLifeYears = &years
		}

		if isNew {
			if err := h.GormDB.Create(&asset).Error; err != nil {
				log.Printf("Asset import error for %s (Create): %v", assetNumber, err)
				importErrors = append(importErrors, fmt.Sprintf("row %d (%s): %v", rowNum, assetNumber, err))
				continue
			}
		} else {
			if err := h.GormDB.Save(&asset).Error; err != nil {
				log.Printf("Asset import error for %s (Save): %v", assetNumber, err)
				importErrors = append(importErrors, fmt.Sprintf("row %d (%s): %v", rowNum, assetNumber, err))
				continue
			}
		}
		imported++
	}

	log.Printf("Asset CSV import: imported %d assets, %d errors", imported, len(importErrors))

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
