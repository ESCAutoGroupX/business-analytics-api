package handlers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type AssetHandler struct {
	GormDB *gorm.DB
}

type assetCreateRequest struct {
	AssetName                      *string  `json:"asset_name"`
	Description                    *string  `json:"description"`
	OriginalCost                   *float64 `json:"original_cost"`
	SalvageValue                   *float64 `json:"salvage_value"`
	UsefulLife                     *int     `json:"useful_life"`
	InitialAccumulatedDepreciation *float64 `json:"initial_accumulated_depreciation"`
	AssetCategory                  *string  `json:"asset_category"`
	FixedAssetAccount              *string  `json:"fixed_asset_account"`
	AccumulatedDepreciationAccount *string  `json:"accumulated_depreciation_account"`
	DepreciationExpenseAccount     *string  `json:"depreciation_expense_account"`
	Method                         *string  `json:"method"`
	DepreciationFrequency          *string  `json:"depreciation_frequency"`
	DepreciationConvention         *string  `json:"depreciation_convention"`
	AcquisitionDate                *string  `json:"acquisition_date"`
	StartDate                      *string  `json:"start_date"`
	EndDate                        *string  `json:"end_date"`
	Location                       *string  `json:"location"`
	VendorID                       *string  `json:"vendor_id"`
}

type assetResponse struct {
	ID                             int        `json:"id"`
	AssetName                      *string    `json:"asset_name"`
	Description                    *string    `json:"description"`
	OriginalCost                   *float64   `json:"original_cost"`
	SalvageValue                   *float64   `json:"salvage_value"`
	UsefulLife                     *int       `json:"useful_life"`
	InitialAccumulatedDepreciation *float64   `json:"initial_accumulated_depreciation"`
	AssetCategory                  *string    `json:"asset_category"`
	FixedAssetAccount              *string    `json:"fixed_asset_account"`
	AccumulatedDepreciationAccount *string    `json:"accumulated_depreciation_account"`
	DepreciationExpenseAccount     *string    `json:"depreciation_expense_account"`
	Method                         *string    `json:"method"`
	DepreciationFrequency          *string    `json:"depreciation_frequency"`
	DepreciationConvention         *string    `json:"depreciation_convention"`
	AcquisitionDate                *string    `json:"acquisition_date"`
	StartDate                      *string    `json:"start_date"`
	EndDate                        *string    `json:"end_date"`
	Location                       *string    `json:"location"`
	VendorID                       *string    `json:"vendor_id"`
	CreatedAt                      *time.Time `json:"created_at"`
	UpdatedAt                      *time.Time `json:"updated_at"`
}

func assetToResponse(a *models.Asset) assetResponse {
	return assetResponse{
		ID:                             a.ID,
		AssetName:                      &a.AssetName,
		Description:                    a.Description,
		OriginalCost:                   &a.OriginalCost,
		SalvageValue:                   a.SalvageValue,
		UsefulLife:                     a.UsefulLife,
		InitialAccumulatedDepreciation: a.InitialAccumulatedDepreciation,
		AssetCategory:                  a.AssetCategory,
		FixedAssetAccount:              a.FixedAssetAccount,
		AccumulatedDepreciationAccount: a.AccumulatedDepreciationAccount,
		DepreciationExpenseAccount:     a.DepreciationExpenseAccount,
		Method:                         a.Method,
		DepreciationFrequency:          a.DepreciationFrequency,
		DepreciationConvention:         a.DepreciationConvention,
		AcquisitionDate:                a.AcquisitionDate,
		StartDate:                      a.StartDate,
		EndDate:                        a.EndDate,
		Location:                       a.LocationField,
		VendorID:                       a.VendorID,
		CreatedAt:                      &a.CreatedAt,
		UpdatedAt:                      &a.UpdatedAt,
	}
}

func (h *AssetHandler) CreateAsset(c *gin.Context) {
	var req assetCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate vendor_id if provided
	if req.VendorID != nil && *req.VendorID != "" {
		var count int64
		h.GormDB.Model(&models.Vendor{}).Where("id = ?", *req.VendorID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid vendor_id: %s. Vendor does not exist.", *req.VendorID)})
			return
		}
	}

	assetID := fmt.Sprintf("AST-%s", strings.ToUpper(uuid.New().String()[:8]))

	// Defaults
	salvageValue := 0.0
	if req.SalvageValue != nil {
		salvageValue = *req.SalvageValue
	}
	initialAccumDepr := 0.0
	if req.InitialAccumulatedDepreciation != nil {
		initialAccumDepr = *req.InitialAccumulatedDepreciation
	}
	assetCategory := "Other"
	if req.AssetCategory != nil {
		assetCategory = *req.AssetCategory
	}
	method := "Straight-Line"
	if req.Method != nil {
		method = *req.Method
	}
	deprFrequency := "Monthly"
	if req.DepreciationFrequency != nil {
		deprFrequency = *req.DepreciationFrequency
	}
	deprConvention := "Full-Month"
	if req.DepreciationConvention != nil {
		deprConvention = *req.DepreciationConvention
	}

	var originalCost float64
	if req.OriginalCost != nil {
		originalCost = *req.OriginalCost
	}

	var assetName string
	if req.AssetName != nil {
		assetName = *req.AssetName
	}

	asset := models.Asset{
		AssetID:                        assetID,
		AssetName:                      assetName,
		Description:                    req.Description,
		OriginalCost:                   originalCost,
		SalvageValue:                   &salvageValue,
		UsefulLife:                     req.UsefulLife,
		InitialAccumulatedDepreciation: &initialAccumDepr,
		AssetCategory:                  &assetCategory,
		FixedAssetAccount:              req.FixedAssetAccount,
		AccumulatedDepreciationAccount: req.AccumulatedDepreciationAccount,
		DepreciationExpenseAccount:     req.DepreciationExpenseAccount,
		Method:                         &method,
		DepreciationFrequency:          &deprFrequency,
		DepreciationConvention:         &deprConvention,
		AcquisitionDate:                req.AcquisitionDate,
		StartDate:                      req.StartDate,
		EndDate:                        req.EndDate,
		LocationField:                  req.Location,
		VendorID:                       req.VendorID,
	}

	if err := h.GormDB.Create(&asset).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create asset", "error": err.Error()})
		return
	}

	h.getAssetByID(c, asset.ID)
}

func (h *AssetHandler) GetAsset(c *gin.Context) {
	assetID, err := strconv.Atoi(c.Param("asset_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid asset_id"})
		return
	}

	h.getAssetByID(c, assetID)
}

func (h *AssetHandler) GetAllAssets(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	var assets []models.Asset
	if err := h.GormDB.Offset(skip).Limit(limit).Find(&assets).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query assets", "error": err.Error()})
		return
	}

	result := make([]assetResponse, len(assets))
	for i := range assets {
		result[i] = assetToResponse(&assets[i])
	}

	c.JSON(http.StatusOK, result)
}

func (h *AssetHandler) UpdateAsset(c *gin.Context) {
	assetID, err := strconv.Atoi(c.Param("asset_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid asset_id"})
		return
	}

	var req assetCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate vendor_id if being updated
	if req.VendorID != nil {
		var count int64
		h.GormDB.Model(&models.Vendor{}).Where("id = ?", *req.VendorID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid vendor_id: %s. Vendor does not exist.", *req.VendorID)})
			return
		}
	}

	var asset models.Asset
	if err := h.GormDB.First(&asset, "id = ?", assetID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
		return
	}

	updates := map[string]interface{}{}
	if req.AssetName != nil {
		updates["asset_name"] = *req.AssetName
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.OriginalCost != nil {
		updates["original_cost"] = *req.OriginalCost
	}
	if req.SalvageValue != nil {
		updates["salvage_value"] = *req.SalvageValue
	}
	if req.UsefulLife != nil {
		updates["useful_life"] = *req.UsefulLife
	}
	if req.InitialAccumulatedDepreciation != nil {
		updates["initial_accumulated_depreciation"] = *req.InitialAccumulatedDepreciation
	}
	if req.AssetCategory != nil {
		updates["asset_category"] = *req.AssetCategory
	}
	if req.FixedAssetAccount != nil {
		updates["fixed_asset_account"] = *req.FixedAssetAccount
	}
	if req.AccumulatedDepreciationAccount != nil {
		updates["accumulated_depreciation_account"] = *req.AccumulatedDepreciationAccount
	}
	if req.DepreciationExpenseAccount != nil {
		updates["depreciation_expense_account"] = *req.DepreciationExpenseAccount
	}
	if req.Method != nil {
		updates["method"] = *req.Method
	}
	if req.DepreciationFrequency != nil {
		updates["depreciation_frequency"] = *req.DepreciationFrequency
	}
	if req.DepreciationConvention != nil {
		updates["depreciation_convention"] = *req.DepreciationConvention
	}
	if req.AcquisitionDate != nil {
		updates["acquisition_date"] = *req.AcquisitionDate
	}
	if req.StartDate != nil {
		updates["start_date"] = *req.StartDate
	}
	if req.EndDate != nil {
		updates["end_date"] = *req.EndDate
	}
	if req.Location != nil {
		updates["location"] = *req.Location
	}
	if req.VendorID != nil {
		updates["vendor_id"] = *req.VendorID
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&asset).Updates(updates).Error; err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update asset", "error": err.Error()})
			return
		}
	}

	h.getAssetByID(c, assetID)
}

func (h *AssetHandler) DeleteAsset(c *gin.Context) {
	assetID, err := strconv.Atoi(c.Param("asset_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid asset_id"})
		return
	}

	result := h.GormDB.Delete(&models.Asset{}, "id = ?", assetID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete asset", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Asset deleted successfully!"})
}

func (h *AssetHandler) getAssetByID(c *gin.Context, id int) {
	var asset models.Asset
	if err := h.GormDB.First(&asset, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query asset", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, assetToResponse(&asset))
}
