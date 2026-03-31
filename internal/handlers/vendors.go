package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type VendorHandler struct {
	DB *pgxpool.Pool
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
	CreatedAt         *time.Time  `json:"created_at"`
	UpdatedAt         *time.Time  `json:"updated_at"`
	GLCode            interface{} `json:"gl_code"`
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

	id := uuid.New().String()
	now := time.Now().UTC()

	shopName := ""
	if req.ShopName != nil {
		shopName = *req.ShopName
	}

	_, err := h.DB.Exec(context.Background(),
		`INSERT INTO vendors (id, name, category, vendor_type, shop_name, is_parts_vendor, is_cogs_vendor, is_statement_vendor, gl_code_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		id, req.Name, req.Category, req.VendorType, shopName, req.IsPartsVendor, req.IsCogsVendor, req.IsStatementVendor, req.GLCodeID, now, now,
	)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create vendor", "error": err.Error()})
		return
	}

	h.getVendorByID(c, id)
}

func (h *VendorHandler) ListVendors(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		`SELECT v.id, v.name, v.category, v.vendor_type, v.shop_name, v.is_parts_vendor, v.is_cogs_vendor, v.is_statement_vendor, v.gl_code_id, v.created_at, v.updated_at,
		        g.id, g.name, g.account_type, g.description
		 FROM vendors v
		 LEFT JOIN chart_of_accounts g ON v.gl_code_id = g.id`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query vendors", "error": err.Error()})
		return
	}
	defer rows.Close()

	vendors := []vendorResponse{}
	for rows.Next() {
		var v vendorResponse
		var glCodeID, gID, gName, gType, gDesc *string
		if err := rows.Scan(&v.ID, &v.Name, &v.Category, &v.VendorType, &v.ShopName,
			&v.IsPartsVendor, &v.IsCogsVendor, &v.IsStatementVendor, &glCodeID,
			&v.CreatedAt, &v.UpdatedAt,
			&gID, &gName, &gType, &gDesc); err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan vendor", "error": err.Error()})
			return
		}
		if gID != nil {
			v.GLCode = gin.H{
				"id": *gID, "name": gName, "account_type": gType,
				"description": gDesc,
			}
		}
		vendors = append(vendors, v)
	}

	c.JSON(http.StatusOK, vendors)
}

func (h *VendorHandler) GetVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")
	h.getVendorByID(c, vendorID)
}

func (h *VendorHandler) PatchVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")

	var exists bool
	err := h.DB.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM vendors WHERE id = $1)", vendorID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
		return
	}

	var req vendorUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	if req.Name != nil {
		setClauses = append(setClauses, fmt.Sprintf("name = $%d", argIdx))
		args = append(args, *req.Name)
		argIdx++
	}
	if req.Category != nil {
		setClauses = append(setClauses, fmt.Sprintf("category = $%d", argIdx))
		args = append(args, *req.Category)
		argIdx++
	}
	if req.VendorType != nil {
		setClauses = append(setClauses, fmt.Sprintf("vendor_type = $%d", argIdx))
		args = append(args, *req.VendorType)
		argIdx++
	}
	if req.ShopName != nil {
		setClauses = append(setClauses, fmt.Sprintf("shop_name = $%d", argIdx))
		args = append(args, *req.ShopName)
		argIdx++
	}
	if req.IsPartsVendor != nil {
		setClauses = append(setClauses, fmt.Sprintf("is_parts_vendor = $%d", argIdx))
		args = append(args, *req.IsPartsVendor)
		argIdx++
	}
	if req.IsCogsVendor != nil {
		setClauses = append(setClauses, fmt.Sprintf("is_cogs_vendor = $%d", argIdx))
		args = append(args, *req.IsCogsVendor)
		argIdx++
	}
	if req.IsStatementVendor != nil {
		setClauses = append(setClauses, fmt.Sprintf("is_statement_vendor = $%d", argIdx))
		args = append(args, *req.IsStatementVendor)
		argIdx++
	}
	if req.GLCodeID != nil {
		setClauses = append(setClauses, fmt.Sprintf("gl_code_id = $%d", argIdx))
		args = append(args, *req.GLCodeID)
		argIdx++
	}

	if len(setClauses) > 0 {
		setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
		args = append(args, time.Now().UTC())
		argIdx++

		args = append(args, vendorID)
		query := fmt.Sprintf("UPDATE vendors SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update vendor", "error": err.Error()})
			return
		}
	}

	h.getVendorByID(c, vendorID)
}

func (h *VendorHandler) DeleteVendor(c *gin.Context) {
	vendorID := c.Param("vendor_id")

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM vendors WHERE id = $1", vendorID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete vendor", "error": err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Vendor deleted successfully"})
}

func (h *VendorHandler) getVendorByID(c *gin.Context, id string) {
	var v vendorResponse
	var glCodeID, gID, gName, gType, gDesc *string

	err := h.DB.QueryRow(context.Background(),
		`SELECT v.id, v.name, v.category, v.vendor_type, v.shop_name, v.is_parts_vendor, v.is_cogs_vendor, v.is_statement_vendor, v.gl_code_id, v.created_at, v.updated_at,
		        g.id, g.name, g.account_type, g.description
		 FROM vendors v
		 LEFT JOIN chart_of_accounts g ON v.gl_code_id = g.id
		 WHERE v.id = $1`, id,
	).Scan(&v.ID, &v.Name, &v.Category, &v.VendorType, &v.ShopName,
		&v.IsPartsVendor, &v.IsCogsVendor, &v.IsStatementVendor, &glCodeID,
		&v.CreatedAt, &v.UpdatedAt,
		&gID, &gName, &gType, &gDesc)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Vendor not found"})
		return
	}

	if gID != nil {
		v.GLCode = gin.H{
			"id": *gID, "name": gName, "account_type": gType,
			"description": gDesc,
		}
	}

	c.JSON(http.StatusOK, v)
}
