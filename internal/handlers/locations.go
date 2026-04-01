package handlers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type LocationHandler struct {
	GormDB *gorm.DB
}

type locationCreateRequest struct {
	LocationName  string  `json:"location_name" binding:"required"`
	AddressLine1  string  `json:"address_line_1" binding:"required"`
	AddressLine2  *string `json:"address_line_2"`
	City          string  `json:"city" binding:"required"`
	StateProvince string  `json:"state_province" binding:"required"`
	PostalCode    string  `json:"postal_code" binding:"required"`
	Country       string  `json:"country" binding:"required"`
	ShopID        int     `json:"shop_id" binding:"required"`
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
	ID            int        `json:"id"`
	LocationName  string     `json:"location_name"`
	AddressLine1  string     `json:"address_line_1"`
	AddressLine2  *string    `json:"address_line_2"`
	City          string     `json:"city"`
	StateProvince string     `json:"state_province"`
	PostalCode    string     `json:"postal_code"`
	Country       string     `json:"country"`
	ShopID        *int       `json:"shop_id"`
	CreatedAt     *time.Time `json:"created_at"`
	UpdatedAt     *time.Time `json:"updated_at"`
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

func locToResponse(loc *models.Location) locationResponse {
	resp := locationResponse{
		ID:           loc.ID,
		LocationName: loc.LocationName,
		ShopID:       loc.ShopID,
		CreatedAt:    &loc.CreatedAt,
		UpdatedAt:    &loc.UpdatedAt,
	}
	if loc.AddressLine1 != nil {
		resp.AddressLine1 = *loc.AddressLine1
	}
	resp.AddressLine2 = loc.AddressLine2
	if loc.City != nil {
		resp.City = *loc.City
	}
	if loc.StateProvince != nil {
		resp.StateProvince = *loc.StateProvince
	}
	if loc.PostalCode != nil {
		resp.PostalCode = *loc.PostalCode
	}
	if loc.Country != nil {
		resp.Country = *loc.Country
	}
	return resp
}

func shopToResponse(si *models.ShopInfo) shopInfoResponse {
	resp := shopInfoResponse{
		ID:                 si.ID,
		PDFForwardingEmail: si.PDFForwardingEmail,
		CreatedAt:          &si.CreatedAt,
		UpdatedAt:          &si.UpdatedAt,
	}
	if si.ShopName != nil {
		resp.ShopName = *si.ShopName
	}
	if si.ContactEmail != nil {
		resp.ContactEmail = *si.ContactEmail
	}
	return resp
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
	var count int64
	h.GormDB.Model(&models.ShopInfo{}).Where("id = ?", req.ShopID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid shop_id"})
		return
	}

	loc := models.Location{
		LocationName:  req.LocationName,
		AddressLine1:  &req.AddressLine1,
		AddressLine2:  req.AddressLine2,
		City:          &req.City,
		StateProvince: &req.StateProvince,
		PostalCode:    &req.PostalCode,
		Country:       &req.Country,
		ShopID:        &req.ShopID,
	}

	if err := h.GormDB.Create(&loc).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create location", "error": err.Error()})
		return
	}

	h.getLocationByID(c, loc.ID)
}

// GET /locations/
func (h *LocationHandler) GetAllLocations(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	var locs []models.Location
	if err := h.GormDB.Offset(skip).Limit(limit).Find(&locs).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query locations", "error": err.Error()})
		return
	}

	result := make([]locationResponse, len(locs))
	for i := range locs {
		result[i] = locToResponse(&locs[i])
	}

	c.JSON(http.StatusOK, result)
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

	var loc models.Location
	if err := h.GormDB.First(&loc, "id = ?", locationID).Error; err != nil {
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
		var count int64
		h.GormDB.Model(&models.ShopInfo{}).Where("id = ?", *req.ShopID).Count(&count)
		if count == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid shop_id"})
			return
		}
	}

	updates := map[string]interface{}{}
	if req.LocationName != nil {
		updates["location_name"] = *req.LocationName
	}
	if req.AddressLine1 != nil {
		updates["address_line_1"] = *req.AddressLine1
	}
	if req.AddressLine2 != nil {
		updates["address_line_2"] = *req.AddressLine2
	}
	if req.City != nil {
		updates["city"] = *req.City
	}
	if req.StateProvince != nil {
		updates["state_province"] = *req.StateProvince
	}
	if req.PostalCode != nil {
		updates["postal_code"] = *req.PostalCode
	}
	if req.Country != nil {
		updates["country"] = *req.Country
	}
	if req.ShopID != nil {
		updates["shop_id"] = *req.ShopID
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&loc).Updates(updates).Error; err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update location", "error": err.Error()})
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

	result := h.GormDB.Delete(&models.Location{}, "id = ?", locationID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete location", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Location not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Location deleted successfully."})
}

func (h *LocationHandler) getLocationByID(c *gin.Context, id int) {
	var loc models.Location
	if err := h.GormDB.First(&loc, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Location not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query location", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, locToResponse(&loc))
}

// POST /locations/shop-info/
func (h *LocationHandler) CreateShopInfo(c *gin.Context) {
	var req shopInfoCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	si := models.ShopInfo{
		ShopName:           &req.ShopName,
		ContactEmail:       &req.ContactEmail,
		PDFForwardingEmail: req.PDFForwardingEmail,
	}

	if err := h.GormDB.Create(&si).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create shop info", "error": err.Error()})
		return
	}

	h.getShopInfoByID(c, si.ID)
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

	var infos []models.ShopInfo
	if err := h.GormDB.Offset(skip).Limit(limit).Find(&infos).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query shop infos", "error": err.Error()})
		return
	}

	result := make([]shopInfoResponse, len(infos))
	for i := range infos {
		result[i] = shopToResponse(&infos[i])
	}

	c.JSON(http.StatusOK, result)
}

// PATCH /locations/shop-info/:shop_info_id
func (h *LocationHandler) UpdateShopInfo(c *gin.Context) {
	shopInfoID, err := strconv.Atoi(c.Param("shop_info_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid shop_info_id"})
		return
	}

	var si models.ShopInfo
	if err := h.GormDB.First(&si, "id = ?", shopInfoID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
		return
	}

	var req shopInfoUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	updates := map[string]interface{}{}
	if req.ShopName != nil {
		updates["shop_name"] = *req.ShopName
	}
	if req.ContactEmail != nil {
		updates["contact_email"] = *req.ContactEmail
	}
	if req.PDFForwardingEmail != nil {
		updates["pdf_forwarding_email"] = *req.PDFForwardingEmail
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&si).Updates(updates).Error; err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update shop info", "error": err.Error()})
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

	result := h.GormDB.Delete(&models.ShopInfo{}, "id = ?", shopInfoID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete shop info", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Shop info deleted successfully!"})
}

func (h *LocationHandler) getShopInfoByID(c *gin.Context, id int) {
	var si models.ShopInfo
	if err := h.GormDB.First(&si, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Shop info not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query shop info", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, shopToResponse(&si))
}
