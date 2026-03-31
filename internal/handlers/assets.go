package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AssetHandler struct {
	DB *pgxpool.Pool
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

func (h *AssetHandler) CreateAsset(c *gin.Context) {
	var req assetCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate vendor_id if provided
	if req.VendorID != nil && *req.VendorID != "" {
		var exists bool
		err := h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM vendors WHERE id = $1)", *req.VendorID).Scan(&exists)
		if err != nil || !exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid vendor_id: %s. Vendor does not exist.", *req.VendorID)})
			return
		}
	}

	assetID := fmt.Sprintf("AST-%s", strings.ToUpper(uuid.New().String()[:8]))
	now := time.Now().UTC()

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

	var acquisitionDate, startDate, endDate *time.Time
	if req.AcquisitionDate != nil {
		if t, err := time.Parse("2006-01-02", *req.AcquisitionDate); err == nil {
			acquisitionDate = &t
		}
	}
	if req.StartDate != nil {
		if t, err := time.Parse("2006-01-02", *req.StartDate); err == nil {
			startDate = &t
		}
	}
	if req.EndDate != nil {
		if t, err := time.Parse("2006-01-02", *req.EndDate); err == nil {
			endDate = &t
		}
	}

	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO assets (asset_id, asset_name, description, original_cost, salvage_value, useful_life,
		 initial_accumulated_depreciation, asset_category, fixed_asset_account, accumulated_depreciation_account,
		 depreciation_expense_account, method, depreciation_frequency, depreciation_convention,
		 acquisition_date, start_date, end_date, location, vendor_id, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		 RETURNING id`,
		assetID, req.AssetName, req.Description, req.OriginalCost, salvageValue, req.UsefulLife,
		initialAccumDepr, assetCategory, req.FixedAssetAccount, req.AccumulatedDepreciationAccount,
		req.DepreciationExpenseAccount, method, deprFrequency, deprConvention,
		acquisitionDate, startDate, endDate, req.Location, req.VendorID, now, now,
	).Scan(&id)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create asset", "error": err.Error()})
		return
	}

	h.getAssetByID(c, id)
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

	rows, err := h.DB.Query(context.Background(),
		`SELECT id, asset_name, description, original_cost, salvage_value, useful_life,
		 initial_accumulated_depreciation, asset_category, fixed_asset_account, accumulated_depreciation_account,
		 depreciation_expense_account, method, depreciation_frequency, depreciation_convention,
		 acquisition_date, start_date, end_date, location, vendor_id, created_at, updated_at
		 FROM assets OFFSET $1 LIMIT $2`, skip, limit)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query assets", "error": err.Error()})
		return
	}
	defer rows.Close()

	assets := []assetResponse{}
	for rows.Next() {
		a, err := scanAsset(rows)
		if err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan asset", "error": err.Error()})
			return
		}
		assets = append(assets, a)
	}

	c.JSON(http.StatusOK, assets)
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
		var exists bool
		err := h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM vendors WHERE id = $1)", *req.VendorID).Scan(&exists)
		if err != nil || !exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid vendor_id: %s. Vendor does not exist.", *req.VendorID)})
			return
		}
	}

	// Check asset exists
	var exists bool
	err = h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1)", assetID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.AssetName != nil {
		addClause("asset_name", *req.AssetName)
	}
	if req.Description != nil {
		addClause("description", *req.Description)
	}
	if req.OriginalCost != nil {
		addClause("original_cost", *req.OriginalCost)
	}
	if req.SalvageValue != nil {
		addClause("salvage_value", *req.SalvageValue)
	}
	if req.UsefulLife != nil {
		addClause("useful_life", *req.UsefulLife)
	}
	if req.InitialAccumulatedDepreciation != nil {
		addClause("initial_accumulated_depreciation", *req.InitialAccumulatedDepreciation)
	}
	if req.AssetCategory != nil {
		addClause("asset_category", *req.AssetCategory)
	}
	if req.FixedAssetAccount != nil {
		addClause("fixed_asset_account", *req.FixedAssetAccount)
	}
	if req.AccumulatedDepreciationAccount != nil {
		addClause("accumulated_depreciation_account", *req.AccumulatedDepreciationAccount)
	}
	if req.DepreciationExpenseAccount != nil {
		addClause("depreciation_expense_account", *req.DepreciationExpenseAccount)
	}
	if req.Method != nil {
		addClause("method", *req.Method)
	}
	if req.DepreciationFrequency != nil {
		addClause("depreciation_frequency", *req.DepreciationFrequency)
	}
	if req.DepreciationConvention != nil {
		addClause("depreciation_convention", *req.DepreciationConvention)
	}
	if req.AcquisitionDate != nil {
		if t, err := time.Parse("2006-01-02", *req.AcquisitionDate); err == nil {
			addClause("acquisition_date", t)
		}
	}
	if req.StartDate != nil {
		if t, err := time.Parse("2006-01-02", *req.StartDate); err == nil {
			addClause("start_date", t)
		}
	}
	if req.EndDate != nil {
		if t, err := time.Parse("2006-01-02", *req.EndDate); err == nil {
			addClause("end_date", t)
		}
	}
	if req.Location != nil {
		addClause("location", *req.Location)
	}
	if req.VendorID != nil {
		addClause("vendor_id", *req.VendorID)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, assetID)
		query := fmt.Sprintf("UPDATE assets SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
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

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM assets WHERE id = $1", assetID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete asset", "error": err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Asset deleted successfully!"})
}

func (h *AssetHandler) getAssetByID(c *gin.Context, id int) {
	row := h.DB.QueryRow(context.Background(),
		`SELECT id, asset_name, description, original_cost, salvage_value, useful_life,
		 initial_accumulated_depreciation, asset_category, fixed_asset_account, accumulated_depreciation_account,
		 depreciation_expense_account, method, depreciation_frequency, depreciation_convention,
		 acquisition_date, start_date, end_date, location, vendor_id, created_at, updated_at
		 FROM assets WHERE id = $1`, id)

	a, err := scanAssetRow(row)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Asset not found"})
		return
	}

	c.JSON(http.StatusOK, a)
}

type scannable interface {
	Scan(dest ...interface{}) error
}

func scanAsset(rows scannable) (assetResponse, error) {
	return scanAssetRow(rows)
}

func scanAssetRow(row scannable) (assetResponse, error) {
	var a assetResponse
	var acqDate, startDate, endDate *time.Time

	err := row.Scan(&a.ID, &a.AssetName, &a.Description, &a.OriginalCost, &a.SalvageValue, &a.UsefulLife,
		&a.InitialAccumulatedDepreciation, &a.AssetCategory, &a.FixedAssetAccount, &a.AccumulatedDepreciationAccount,
		&a.DepreciationExpenseAccount, &a.Method, &a.DepreciationFrequency, &a.DepreciationConvention,
		&acqDate, &startDate, &endDate, &a.Location, &a.VendorID, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return a, err
	}

	if acqDate != nil {
		s := acqDate.Format("2006-01-02")
		a.AcquisitionDate = &s
	}
	if startDate != nil {
		s := startDate.Format("2006-01-02")
		a.StartDate = &s
	}
	if endDate != nil {
		s := endDate.Format("2006-01-02")
		a.EndDate = &s
	}

	return a, nil
}
