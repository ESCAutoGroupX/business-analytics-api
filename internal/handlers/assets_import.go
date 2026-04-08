package handlers

import (
	"encoding/csv"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AssetImportHandler struct {
	GormDB *gorm.DB
}

func (h *AssetImportHandler) ImportCSV(c *gin.Context) {
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no file uploaded"})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid CSV: " + err.Error()})
		return
	}

	if len(records) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "CSV has no data rows"})
		return
	}

	// Build header index map
	header := records[0]
	colIdx := make(map[string]int)
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	// Validate required column
	if _, ok := colIdx["AssetNumber"]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "CSV missing required column: AssetNumber"})
		return
	}

	var imported int
	var errors []string

	for i, row := range records[1:] {
		rowNum := i + 2 // 1-indexed, skip header

		assetNumber := getCol(row, colIdx, "AssetNumber")
		if assetNumber == "" {
			errors = append(errors, fmt.Sprintf("row %d: empty AssetNumber, skipping", rowNum))
			continue
		}

		asset := models.XeroAsset{
			AssetName: getCol(row, colIdx, "AssetName"),
		}

		// String pointer fields
		an := assetNumber
		asset.AssetNumber = &an

		if v := getCol(row, colIdx, "AssetStatus"); v != "" {
			asset.Status = &v
		}
		if v := getCol(row, colIdx, "AssetType"); v != "" {
			asset.AssetTypeName = &v
		}
		if v := getCol(row, colIdx, "Description"); v != "" {
			asset.Description = &v
		}
		if v := getCol(row, colIdx, "TrackingOption1"); v != "" {
			asset.Location = &v
		}
		if v := getCol(row, colIdx, "Book_DepreciationMethod"); v != "" {
			asset.DepreciationMethod = &v
		}

		// Float pointer fields
		if v := parseFloat(getCol(row, colIdx, "PurchasePrice")); v != nil {
			asset.PurchasePrice = v
		}
		if v := parseFloat(getCol(row, colIdx, "Book_Rate")); v != nil {
			asset.DepreciationRate = v
		}
		if v := parseFloat(getCol(row, colIdx, "Book_BookValue")); v != nil {
			asset.BookValue = v
		}
		if v := parseFloat(getCol(row, colIdx, "Book_ResidualValue")); v != nil {
			asset.ResidualValue = v
		}
		if v := parseFloat(getCol(row, colIdx, "AccumulatedDepreciation")); v != nil {
			asset.CurrentAccumDepreciation = v
		}

		// EffectiveLife: float in CSV -> int in model
		if v := parseFloat(getCol(row, colIdx, "Book_EffectiveLife")); v != nil {
			years := int(math.Round(*v))
			asset.EffectiveLifeYears = &years
		}

		// Date fields (M/D/YYYY format)
		if v := parseDate(getCol(row, colIdx, "PurchaseDate")); v != nil {
			asset.PurchaseDate = v
		}
		if v := parseDate(getCol(row, colIdx, "DisposalDate")); v != nil {
			asset.DisposalDate = v
		}
		if v := parseDate(getCol(row, colIdx, "Book_DepreciationStartDate")); v != nil {
			asset.DepreciationStartDate = v
		}

		asset.SyncedAt = time.Now()

		// Upsert: ON CONFLICT (asset_number) DO UPDATE
		result := h.GormDB.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "asset_number"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"asset_name", "status", "asset_type_name", "description", "location",
				"purchase_date", "purchase_price", "depreciation_method",
				"depreciation_rate", "effective_life_years", "book_value",
				"residual_value", "current_accum_depreciation",
				"depreciation_start_date", "disposal_date", "synced_at",
			}),
		}).Create(&asset)

		if result.Error != nil {
			errors = append(errors, fmt.Sprintf("row %d (%s): %s", rowNum, assetNumber, result.Error.Error()))
			continue
		}
		imported++
	}

	c.JSON(http.StatusOK, gin.H{
		"imported": imported,
		"errors":   errors,
	})
}

func getCol(row []string, idx map[string]int, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func parseFloat(s string) *float64 {
	if s == "" {
		return nil
	}
	// Remove commas from numbers like "1,234.56"
	s = strings.ReplaceAll(s, ",", "")
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseDate(s string) *time.Time {
	if s == "" {
		return nil
	}
	// Try M/D/YYYY format
	t, err := time.Parse("1/2/2006", s)
	if err != nil {
		// Try YYYY-MM-DD as fallback
		t, err = time.Parse("2006-01-02", s)
		if err != nil {
			return nil
		}
	}
	return &t
}
