package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LocationHandler struct {
	DB  *pgxpool.Pool
	Cfg interface{} // for future Tekmetric config
}

type locationCreateRequest struct {
	LocationName string  `json:"location_name" binding:"required"`
	AddressLine1 string  `json:"address_line_1" binding:"required"`
	AddressLine2 *string `json:"address_line_2"`
	City         string  `json:"city" binding:"required"`
	StateProvince string `json:"state_province" binding:"required"`
	PostalCode   string  `json:"postal_code" binding:"required"`
	Country      string  `json:"country" binding:"required"`
	ShopID       int     `json:"shop_id" binding:"required"`
}

type locationUpdateRequest struct {
	LocationName  *string `json:"location_name"`
	AddressLine1  *string `json:"address_line_1"`
	AddressLine2  *string `json:"address_line_2"`
	City          *string `json:"city"`
	StateProvince *string `json:"state_province"`
	PostalCode    *string `json:"postal_code"`
	Country       *string `json:"country"`
	ShopID        *int    `json:"shop_id"`
}

type locationResponse struct {
	ID              int        `json:"id"`
	LocationName    string     `json:"location_name"`
	AddressLine1    string     `json:"address_line_1"`
	AddressLine2    *string    `json:"address_line_2"`
	City            string     `json:"city"`
	StateProvince   string     `json:"state_province"`
	PostalCode      string     `json:"postal_code"`
	Country         string     `json:"country"`
	ShopID          *int       `json:"shop_id"`
	CreatedAt       *time.Time `json:"created_at"`
	UpdatedAt       *time.Time `json:"updated_at"`
}

type shopInfoCreateRequest struct {
	ShopName           string  `json:"shop_name" binding:"required"`
	ContactEmail       string  `json:"contact_email" binding:"required"`
	PDFForwardingEmail *string `json:"pdf_forwarding_email"`
}

type shopInfoUpdateRequest struct {
	ShopName           *string `json:"shop_name"`
	ContactEmail       *string `json:"contact_email"`
	PDFForwardingEmail *string `json:"pdf_forwarding_email"`
}

type shopInfoResponse struct {
	ID                 int        `json:"id"`
	ShopName           string     `json:"shop_name"`
	ContactEmail       string     `json:"contact_email"`
	PDFForwardingEmail *string    `json:"pdf_forwarding_email"`
	CreatedAt          *time.Time `json:"created_at"`
	UpdatedAt          *time.Time `json:"updated_at"`
}

func (h *LocationHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if fmt.Sprintf("%v", role) != "Admin" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Admin access required"})
		return false
	}
	return true
}

// POST /locations/
func (h *LocationHandler) CreateLocation(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	var req locationCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate shop_id
	var shopExists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM shop_info WHERE id = $1)", req.ShopID).Scan(&shopExists)
	if !shopExists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid shop_id"})
		return
	}

	now := time.Now().UTC()
	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO locations (location_name, address_line_1, address_line_2, city, state_province, postal_code, country, shop_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 RETURNING id`,
		req.LocationName, req.AddressLine1, req.AddressLine2, req.City, req.StateProvince, req.PostalCode, req.Country, req.ShopID, now, now,
	).Scan(&id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create location"})
		return
	}

	h.getLocationByID(c, id)
}

// GET /locations/
func (h *LocationHandler) GetAllLocations(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	rows, err := h.DB.Query(context.Background(),
		`SELECT id, location_name, address_line_1, address_line_2, city, state_province, postal_code, country, shop_id, created_at, updated_at
		 FROM locations OFFSET $1 LIMIT $2`, skip, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query locations"})
		return
	}
	defer rows.Close()

	locations := []locationResponse{}
	for rows.Next() {
		var loc locationResponse
		if err := rows.Scan(&loc.ID, &loc.LocationName, &loc.AddressLine1, &loc.AddressLine2,
			&loc.City, &loc.StateProvince, &loc.PostalCode, &loc.Country, &loc.ShopID,
			&loc.CreatedAt, &loc.UpdatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan location"})
			return
		}
		locations = append(locations, loc)
	}

	c.JSON(http.StatusOK, locations)
}

// GET /locations/:location_id
func (h *LocationHandler) GetLocation(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	locationID, err := strconv.Atoi(c.Param("location_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid location_id"})
		return
	}

	h.getLocationByID(c, locationID)
}

// PATCH /locations/:location_id
func (h *LocationHandler) UpdateLocation(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	locationID, err := strconv.Atoi(c.Param("location_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid location_id"})
		return
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM locations WHERE id = $1)", locationID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Location not found"})
		return
	}

	var req locationUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Validate shop_id if provided
	if req.ShopID != nil {
		var shopExists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM shop_info WHERE id = $1)", *req.ShopID).Scan(&shopExists)
		if !shopExists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid shop_id"})
			return
		}
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.LocationName != nil {
		addClause("location_name", *req.LocationName)
	}
	if req.AddressLine1 != nil {
		addClause("address_line_1", *req.AddressLine1)
	}
	if req.AddressLine2 != nil {
		addClause("address_line_2", *req.AddressLine2)
	}
	if req.City != nil {
		addClause("city", *req.City)
	}
	if req.StateProvince != nil {
		addClause("state_province", *req.StateProvince)
	}
	if req.PostalCode != nil {
		addClause("postal_code", *req.PostalCode)
	}
	if req.Country != nil {
		addClause("country", *req.Country)
	}
	if req.ShopID != nil {
		addClause("shop_id", *req.ShopID)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, locationID)
		query := fmt.Sprintf("UPDATE locations SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update location"})
			return
		}
	}

	h.getLocationByID(c, locationID)
}

// DELETE /locations/:location_id
func (h *LocationHandler) DeleteLocation(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	locationID, err := strconv.Atoi(c.Param("location_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid location_id"})
		return
	}

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM locations WHERE id = $1", locationID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete location"})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Location not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Location deleted successfully."})
}

func (h *LocationHandler) getLocationByID(c *gin.Context, id int) {
	var loc locationResponse
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, location_name, address_line_1, address_line_2, city, state_province, postal_code, country, shop_id, created_at, updated_at
		 FROM locations WHERE id = $1`, id,
	).Scan(&loc.ID, &loc.LocationName, &loc.AddressLine1, &loc.AddressLine2,
		&loc.City, &loc.StateProvince, &loc.PostalCode, &loc.Country, &loc.ShopID,
		&loc.CreatedAt, &loc.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Location not found"})
		return
	}

	c.JSON(http.StatusOK, loc)
}

// POST /locations/shop-info/
func (h *LocationHandler) CreateShopInfo(c *gin.Context) {
	var req shopInfoCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	now := time.Now().UTC()
	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO shop_info (shop_name, contact_email, pdf_forwarding_email, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		req.ShopName, req.ContactEmail, req.PDFForwardingEmail, now, now,
	).Scan(&id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create shop info"})
		return
	}

	h.getShopInfoByID(c, id)
}

// GET /locations/shop-info/:shop_info_id
func (h *LocationHandler) GetShopInfo(c *gin.Context) {
	shopInfoID, err := strconv.Atoi(c.Param("shop_info_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid shop_info_id"})
		return
	}

	h.getShopInfoByID(c, shopInfoID)
}

// GET /locations/shop-info/
func (h *LocationHandler) GetAllShopInfos(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	rows, err := h.DB.Query(context.Background(),
		`SELECT id, shop_name, contact_email, pdf_forwarding_email, created_at, updated_at
		 FROM shop_info OFFSET $1 LIMIT $2`, skip, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query shop infos"})
		return
	}
	defer rows.Close()

	infos := []shopInfoResponse{}
	for rows.Next() {
		var si shopInfoResponse
		if err := rows.Scan(&si.ID, &si.ShopName, &si.ContactEmail, &si.PDFForwardingEmail,
			&si.CreatedAt, &si.UpdatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan shop info"})
			return
		}
		infos = append(infos, si)
	}

	c.JSON(http.StatusOK, infos)
}

// PATCH /locations/shop-info/:shop_info_id
func (h *LocationHandler) UpdateShopInfo(c *gin.Context) {
	shopInfoID, err := strconv.Atoi(c.Param("shop_info_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid shop_info_id"})
		return
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM shop_info WHERE id = $1)", shopInfoID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
		return
	}

	var req shopInfoUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
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

	if req.ShopName != nil {
		addClause("shop_name", *req.ShopName)
	}
	if req.ContactEmail != nil {
		addClause("contact_email", *req.ContactEmail)
	}
	if req.PDFForwardingEmail != nil {
		addClause("pdf_forwarding_email", *req.PDFForwardingEmail)
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, shopInfoID)
		query := fmt.Sprintf("UPDATE shop_info SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update shop info"})
			return
		}
	}

	h.getShopInfoByID(c, shopInfoID)
}

// DELETE /locations/shop-info/:shop_info_id
func (h *LocationHandler) DeleteShopInfo(c *gin.Context) {
	shopInfoID, err := strconv.Atoi(c.Param("shop_info_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid shop_info_id"})
		return
	}

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM shop_info WHERE id = $1", shopInfoID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete shop info"})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Shop info deleted successfully!"})
}

func (h *LocationHandler) getShopInfoByID(c *gin.Context, id int) {
	var si shopInfoResponse
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, shop_name, contact_email, pdf_forwarding_email, created_at, updated_at
		 FROM shop_info WHERE id = $1`, id,
	).Scan(&si.ID, &si.ShopName, &si.ContactEmail, &si.PDFForwardingEmail,
		&si.CreatedAt, &si.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
		return
	}

	c.JSON(http.StatusOK, si)
}
