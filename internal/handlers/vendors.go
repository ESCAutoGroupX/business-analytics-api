package handlers

import (
"encoding/json"
"fmt"
"io"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type VendorHandler struct {
	GormDB *gorm.DB
}

type vendorCreateRequest struct {
	Name              string  `json:"name" binding:"required"`
	Category          *string `json:"category"`
	VendorType        *string `json:"vendor_type"`
	ShopName          *string `json:"shop_name"`
	IsPartsVendor     string  `json:"is_parts_vendor"`
	IsCogsVendor      bool    `json:"is_cogs_vendor"`
	IsStatementVendor bool    `json:"is_statement_vendor"`
	GLCodeID          *string `json:"gl_code_id"`
	BillingFrequency  *string `json:"billing_frequency"`
	PaymentTerms      *string `json:"payment_terms"`
	TypicalPOPrefix   *string `json:"typical_po_prefix"`
	ParentBrand       *string `json:"parent_brand"`
	FranchiseNetwork  *string `json:"franchise_network"`
	PaymentBehavior   *string `json:"payment_behavior"`
	PaymentCycleDays  *int    `json:"payment_cycle_days"`
}

type vendorUpdateRequest struct {
	Name              *string `json:"name"`
	Category          *string `json:"category"`
	VendorType        *string `json:"vendor_type"`
	ShopName          *string `json:"shop_name"`
	IsPartsVendor     *string `json:"is_parts_vendor"`
	IsCogsVendor      *bool   `json:"is_cogs_vendor"`
	IsStatementVendor *bool   `json:"is_statement_vendor"`
	GLCodeID          *string `json:"gl_code_id"`
	BillingFrequency  *string `json:"billing_frequency"`
	PaymentTerms      *string `json:"payment_terms"`
	TypicalPOPrefix   *string `json:"typical_po_prefix"`
	StatementDueDay   *int    `json:"statement_due_day"`
	AlertDaysBefore   *int    `json:"alert_days_before"`
	Notes             *string `json:"notes"`
	ParentBrand       *string `json:"parent_brand"`
	FranchiseNetwork  *string `json:"franchise_network"`
	PaymentBehavior   *string `json:"payment_behavior"`
	PaymentCycleDays  *int    `json:"payment_cycle_days"`
}

type vendorResponse struct {
	ID                string      `json:"id"`
	Name              string      `json:"name"`
	Category          *string     `json:"category"`
	VendorType        *string     `json:"vendor_type"`
	ShopName          *string     `json:"shop_name"`
	IsPartsVendor     string      `json:"is_parts_vendor"`
	IsCogsVendor      bool        `json:"is_cogs_vendor"`
	IsStatementVendor bool        `json:"is_statement_vendor"`
	NormalizedName    *string     `json:"normalized_name"`
	BillingFrequency  *string     `json:"billing_frequency"`
	PaymentTerms      *string     `json:"payment_terms"`
	TypicalPOPrefix   *string     `json:"typical_po_prefix"`
	StatementDueDay   *int        `json:"statement_due_day"`
	AlertDaysBefore   *int        `json:"alert_days_before"`
	Notes             *string     `json:"notes"`
	ParentBrand       *string     `json:"parent_brand"`
	FranchiseNetwork  *string     `json:"franchise_network"`
	PaymentBehavior   *string     `json:"payment_behavior"`
	PaymentCycleDays  *int        `json:"payment_cycle_days"`
	GLCodeID          *string     `json:"gl_code_id"`
	CreatedAt         *time.Time  `json:"created_at"`
	UpdatedAt         *time.Time  `json:"updated_at"`
	GLCode            interface{} `json:"gl_code"`
}

func vendorToResponse(v *models.Vendor) vendorResponse {
	resp := vendorResponse{
		ID:                v.ID,
		Name:              v.Name,
		Category:          v.Category,
		VendorType:        v.VendorType,
		ShopName:          v.ShopName,
		IsCogsVendor:      v.IsCogsVendor != nil && *v.IsCogsVendor,
		IsStatementVendor: v.IsStatementVendor != nil && *v.IsStatementVendor,
		NormalizedName:    v.NormalizedName,
		BillingFrequency:  v.BillingFrequency,
		PaymentTerms:      v.PaymentTerms,
		TypicalPOPrefix:   v.TypicalPOPrefix,
		StatementDueDay:   v.StatementDueDay,
		AlertDaysBefore:   v.AlertDaysBefore,
		Notes:             v.Notes,
		ParentBrand:       v.ParentBrand,
		FranchiseNetwork:  v.FranchiseNetwork,
		PaymentBehavior:   v.PaymentBehavior,
		PaymentCycleDays:  v.PaymentCycleDays,
		GLCodeID:          v.GLCodeID,
		CreatedAt:         &v.CreatedAt,
		UpdatedAt:         &v.UpdatedAt,
	}
	if v.IsPartsVendor != nil {
		resp.IsPartsVendor = *v.IsPartsVendor
	}
	if v.GLCode != nil {
		resp.GLCode = gin.H{
			"id": v.GLCode.ID, "name": v.GLCode.Name,
			"account_type": v.GLCode.AccountType, "description": v.GLCode.Description,
		}
	}
	return resp
}

func (h *VendorHandler) CreateVendor(c *gin.Context) {
	var req vendorCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	if req.IsPartsVendor == "" {
		req.IsPartsVendor = "NEVER"
	}

	isCogsVendor := req.IsCogsVendor
	isStatementVendor := req.IsStatementVendor

	normalizedName := strings.ToLower(strings.TrimSpace(req.Name))

	vendor := models.Vendor{
		ID:                uuid.New().String(),
		Name:              req.Name,
		Category:          req.Category,
		VendorType:        req.VendorType,
		ShopName:          req.ShopName,
		IsPartsVendor:     &req.IsPartsVendor,
		IsCogsVendor:      &isCogsVendor,
		IsStatementVendor: &isStatementVendor,
		GLCodeID:          req.GLCodeID,
		NormalizedName:    &normalizedName,
		BillingFrequency:  req.BillingFrequency,
		PaymentTerms:      req.PaymentTerms,
		TypicalPOPrefix:   req.TypicalPOPrefix,
		ParentBrand:       req.ParentBrand,
		FranchiseNetwork:  req.FranchiseNetwork,
		PaymentBehavior:   req.PaymentBehavior,
		PaymentCycleDays:  req.PaymentCycleDays,
	}

	if err := h.GormDB.Create(&vendor).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create vendor", "error": err.Error()})
		return
	}

	h.getVendorByID(c, vendor.ID)
}

func (h *VendorHandler) ListVendors(c *gin.Context) {
	var vendors []models.Vendor
	if err := h.GormDB.Preload("GLCode").Find(&vendors).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query vendors", "error": err.Error()})
		return
	}

	result := make([]vendorResponse, len(vendors))
	for i := range vendors {
		result[i] = vendorToResponse(&vendors[i])
	}

	c.JSON(http.StatusOK, result)
}

func (h *VendorHandler) GetVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")
	h.getVendorByID(c, vendorID)
}

func (h *VendorHandler) PatchVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")

	var vendor models.Vendor
	if err := h.GormDB.First(&vendor, "id = ?", vendorID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
		return
	}

	var req vendorUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	updates := map[string]interface{}{}
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.Category != nil {
		updates["category"] = *req.Category
	}
	if req.VendorType != nil {
		updates["vendor_type"] = *req.VendorType
	}
	if req.ShopName != nil {
		updates["shop_name"] = *req.ShopName
	}
	if req.IsPartsVendor != nil {
		updates["is_parts_vendor"] = *req.IsPartsVendor
	}
	if req.IsCogsVendor != nil {
		updates["is_cogs_vendor"] = *req.IsCogsVendor
	}
	if req.IsStatementVendor != nil {
		updates["is_statement_vendor"] = *req.IsStatementVendor
	}
	if req.GLCodeID != nil {
		updates["gl_code_id"] = *req.GLCodeID
	}
	if req.BillingFrequency != nil {
		updates["billing_frequency"] = *req.BillingFrequency
	}
	if req.PaymentTerms != nil {
		updates["payment_terms"] = *req.PaymentTerms
	}
	if req.TypicalPOPrefix != nil {
		updates["typical_po_prefix"] = *req.TypicalPOPrefix
	}
	if req.StatementDueDay != nil {
		updates["statement_due_day"] = *req.StatementDueDay
	}
	if req.AlertDaysBefore != nil {
		updates["alert_days_before"] = *req.AlertDaysBefore
	}
	if req.Notes != nil {
		updates["notes"] = *req.Notes
	}
	if req.ParentBrand != nil {
		updates["parent_brand"] = *req.ParentBrand
	}
	if req.FranchiseNetwork != nil {
		updates["franchise_network"] = *req.FranchiseNetwork
	}
	if req.PaymentBehavior != nil {
		updates["payment_behavior"] = *req.PaymentBehavior
	}
	if req.PaymentCycleDays != nil {
		updates["payment_cycle_days"] = *req.PaymentCycleDays
	}
	if req.Name != nil {
		normalized := strings.ToLower(strings.TrimSpace(*req.Name))
		updates["normalized_name"] = normalized
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&vendor).Updates(updates).Error; err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update vendor", "error": err.Error()})
			return
		}
	}

	h.getVendorByID(c, vendorID)
}

func (h *VendorHandler) DeleteVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")

	result := h.GormDB.Delete(&models.Vendor{}, "id = ?", vendorID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete vendor", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Vendor deleted successfully"})
}

func (h *VendorHandler) getVendorByID(c *gin.Context, id string) {
	var vendor models.Vendor
	if err := h.GormDB.Preload("GLCode").First(&vendor, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query vendor", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, vendorToResponse(&vendor))
}

// LookupVendor handles GET /vendors/lookup?name=X — vendor lookup by name.
func (h *VendorHandler) LookupVendor(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "name query parameter required"})
		return
	}

	var vendor models.Vendor
	result := h.GormDB.Where("LOWER(name) = LOWER(?) OR LOWER(normalized_name) = LOWER(?)", name, name).First(&vendor)
	if result.Error != nil {
		c.JSON(http.StatusOK, gin.H{"found": false, "name": name})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"found":  true,
		"vendor": vendorToResponse(&vendor),
	})
}

type wfVendorRecord struct {
	ID                string  `json:"_id"`
	Vendor            string  `json:"vendor"`
	IsVerified        bool    `json:"isVerified"`
	IsPartsVendor     string  `json:"isPartsVendor"`
	IsStatementVendor bool    `json:"isStatementVendor"`
	StatementFreq     *string `json:"statementFrequency"`
	Ignored           bool    `json:"ignored"`
	IsCogs            bool    `json:"isCogs"`
	Category          *string `json:"category"`
	SubCategory       *string `json:"subCategory"`
	WickedFileVendor  *string `json:"wickedFileVendor"`
}

type importResult struct {
	Total   int      `json:"total"`
	Added   int      `json:"added"`
	Updated int      `json:"updated"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors"`
}

func (h *VendorHandler) ImportVendors(c *gin.Context) {
	h.GormDB.Exec(`ALTER TABLE vendors ADD COLUMN IF NOT EXISTS wf_id VARCHAR(100)`)
	h.GormDB.Exec(`ALTER TABLE vendors ADD COLUMN IF NOT EXISTS wf_category VARCHAR(100)`)

	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file field required"})
		return
	}
	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "could not open file"})
		return
	}
	defer f.Close()

	body, err := io.ReadAll(f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "could not read file"})
		return
	}

	var records []wfVendorRecord
	if err := json.Unmarshal(body, &records); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid JSON: " + err.Error()})
		return
	}

	result := importResult{Total: len(records), Errors: []string{}}

	for _, rec := range records {
		if rec.Ignored || strings.TrimSpace(rec.Vendor) == "" {
			result.Skipped++
			continue
		}
		normalized := strings.ToLower(strings.TrimSpace(rec.Vendor))
		isPartsVendor := strings.ToUpper(rec.IsPartsVendor)
		if isPartsVendor == "" {
			isPartsVendor = "NEVER"
		}

		var existing models.Vendor
		findErr := h.GormDB.Where("normalized_name = ?", normalized).First(&existing).Error

		if findErr == nil {
			updates := map[string]interface{}{
				"is_cogs_vendor":      rec.IsCogs,
				"is_statement_vendor": rec.IsStatementVendor,
				"is_parts_vendor":     isPartsVendor,
				"updated_at":          time.Now().UTC(),
			}
			if rec.StatementFreq != nil && *rec.StatementFreq != "" {
				updates["billing_frequency"] = *rec.StatementFreq
			}
			if rec.Category != nil && *rec.Category != "" {
				updates["category"] = *rec.Category
			}
			if err := h.GormDB.Model(&existing).Updates(updates).Error; err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("update %s: %v", rec.Vendor, err))
				continue
			}
			h.GormDB.Exec(`UPDATE vendors SET wf_id = $1, wf_category = $2 WHERE id = $3`, rec.ID, categoryStr(rec.Category), existing.ID)
			h.upsertAlias(existing.ID, rec.Vendor, "wf")
			if rec.WickedFileVendor != nil && strings.TrimSpace(*rec.WickedFileVendor) != "" {
				if strings.ToLower(strings.TrimSpace(*rec.WickedFileVendor)) != normalized {
					h.upsertAlias(existing.ID, strings.TrimSpace(*rec.WickedFileVendor), "wf")
				}
			}
			result.Updated++
		} else {
			isStmt := rec.IsStatementVendor
			isCogs := rec.IsCogs
			newVendor := models.Vendor{
				ID:                uuid.New().String(),
				Name:              rec.Vendor,
				NormalizedName:    &normalized,
				IsPartsVendor:     &isPartsVendor,
				IsCogsVendor:      &isCogs,
				IsStatementVendor: &isStmt,
				BillingFrequency:  rec.StatementFreq,
				Category:          rec.Category,
			}
			if err := h.GormDB.Create(&newVendor).Error; err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert %s: %v", rec.Vendor, err))
				continue
			}
			h.GormDB.Exec(`UPDATE vendors SET wf_id = $1, wf_category = $2 WHERE id = $3`, rec.ID, categoryStr(rec.Category), newVendor.ID)
			h.upsertAlias(newVendor.ID, rec.Vendor, "wf")
			if rec.WickedFileVendor != nil && strings.TrimSpace(*rec.WickedFileVendor) != "" {
				if strings.ToLower(strings.TrimSpace(*rec.WickedFileVendor)) != normalized {
					h.upsertAlias(newVendor.ID, strings.TrimSpace(*rec.WickedFileVendor), "wf")
				}
			}
			result.Added++
		}
	}

	c.JSON(http.StatusOK, result)
}

func (h *VendorHandler) upsertAlias(vendorID, alias, source string) {
	h.GormDB.Exec(`INSERT INTO vendor_aliases (id, vendor_id, alias, source) VALUES (gen_random_uuid(), $1, $2, $3) ON CONFLICT DO NOTHING`, vendorID, alias, source)
}

func categoryStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
