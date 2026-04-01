package handlers

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type PermissionHandler struct {
	GormDB *gorm.DB
}

type permissionCreateRequest struct {
	Keyword     string  `json:"keyword" binding:"required"`
	Endpoint    string  `json:"endpoint" binding:"required"`
	Method      string  `json:"method" binding:"required"`
	Description *string `json:"description"`
}

type permissionResponse struct {
	ID          int     `json:"id"`
	Keyword     string  `json:"keyword"`
	Endpoint    string  `json:"endpoint"`
	Method      string  `json:"method"`
	Description *string `json:"description"`
	IsActive    bool    `json:"is_active"`
}

type roleCreateRequest struct {
	Name               string   `json:"name" binding:"required"`
	Description        *string  `json:"description"`
	PermissionKeywords []string `json:"permission_keywords"`
}

type roleResponse struct {
	ID          int                  `json:"id"`
	Name        string               `json:"name"`
	Description *string              `json:"description"`
	IsActive    bool                 `json:"is_active"`
	Permissions []permissionResponse `json:"permissions"`
}

type assignPermissionsRequest struct {
	PermissionKeywords []string `json:"permission_keywords" binding:"required"`
}

func permToResponse(p *models.Permission) permissionResponse {
	return permissionResponse{
		ID:          p.ID,
		Keyword:     p.Keyword,
		Endpoint:    p.Endpoint,
		Method:      p.Method,
		Description: &p.Description,
		IsActive:    p.IsActive,
	}
}

func (h *PermissionHandler) CreatePermission(c *gin.Context) {
	var req permissionCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check if keyword already exists
	var count int64
	h.GormDB.Model(&models.Permission{}).Where("keyword = ?", req.Keyword).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Permission keyword already exists"})
		return
	}

	desc := ""
	if req.Description != nil {
		desc = *req.Description
	}

	perm := models.Permission{
		Keyword:     req.Keyword,
		Endpoint:    req.Endpoint,
		Method:      req.Method,
		Description: desc,
		IsActive:    true,
	}

	if err := h.GormDB.Create(&perm).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create permission", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, permToResponse(&perm))
}

func (h *PermissionHandler) GetAllPermissions(c *gin.Context) {
	var perms []models.Permission
	if err := h.GormDB.Find(&perms).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query permissions", "error": err.Error()})
		return
	}

	result := make([]permissionResponse, len(perms))
	for i := range perms {
		result[i] = permToResponse(&perms[i])
	}

	c.JSON(http.StatusOK, result)
}

func (h *PermissionHandler) CreateRole(c *gin.Context) {
	var req roleCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check if role already exists
	var count int64
	h.GormDB.Model(&models.Role{}).Where("name = ?", req.Name).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Role already exists"})
		return
	}

	desc := ""
	if req.Description != nil {
		desc = *req.Description
	}

	err := h.GormDB.Transaction(func(tx *gorm.DB) error {
		role := models.Role{
			Name:        req.Name,
			Description: desc,
			IsActive:    true,
		}
		if err := tx.Create(&role).Error; err != nil {
			return err
		}

		for _, keyword := range req.PermissionKeywords {
			var perm models.Permission
			if err := tx.Where("keyword = ?", keyword).First(&perm).Error; err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Permission keyword not found: %s", keyword)})
				return err
			}
			if err := tx.Create(&models.RolePermission{RoleID: role.ID, PermissionID: perm.ID}).Error; err != nil {
				return err
			}
		}

		h.getRoleByID(c, role.ID)
		return nil
	})
	if err != nil && !c.Writer.Written() {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create role", "error": err.Error()})
	}
}

func (h *PermissionHandler) GetAllRoles(c *gin.Context) {
	var dbRoles []models.Role
	if err := h.GormDB.Find(&dbRoles).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query roles", "error": err.Error()})
		return
	}

	roles := make([]roleResponse, len(dbRoles))
	for i, r := range dbRoles {
		roles[i] = roleResponse{
			ID:          r.ID,
			Name:        r.Name,
			Description: &r.Description,
			IsActive:    r.IsActive,
			Permissions: []permissionResponse{},
		}
		perms, err := h.getPermissionsForRole(r.ID)
		if err == nil {
			roles[i].Permissions = perms
		}
	}

	c.JSON(http.StatusOK, roles)
}

func (h *PermissionHandler) GetRole(c *gin.Context) {
	var roleID int
	if _, err := fmt.Sscanf(c.Param("role_id"), "%d", &roleID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid role_id"})
		return
	}

	h.getRoleByID(c, roleID)
}

func (h *PermissionHandler) AssignPermissions(c *gin.Context) {
	var roleID int
	if _, err := fmt.Sscanf(c.Param("role_id"), "%d", &roleID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid role_id"})
		return
	}

	var req assignPermissionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Verify role exists
	var count int64
	h.GormDB.Model(&models.Role{}).Where("id = ?", roleID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Role not found"})
		return
	}

	err := h.GormDB.Transaction(func(tx *gorm.DB) error {
		// Clear existing permissions
		if err := tx.Where("role_id = ?", roleID).Delete(&models.RolePermission{}).Error; err != nil {
			return err
		}

		// Assign new permissions
		for _, keyword := range req.PermissionKeywords {
			var perm models.Permission
			if err := tx.Where("keyword = ?", keyword).First(&perm).Error; err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Permission keyword not found: %s", keyword)})
				return err
			}
			if err := tx.Create(&models.RolePermission{RoleID: roleID, PermissionID: perm.ID}).Error; err != nil {
				return err
			}
		}

		h.getRoleByID(c, roleID)
		return nil
	})
	if err != nil && !c.Writer.Written() {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to assign permissions", "error": err.Error()})
	}
}

func (h *PermissionHandler) getRoleByID(c *gin.Context, roleID int) {
	var role models.Role
	if err := h.GormDB.First(&role, "id = ?", roleID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Role not found"})
		return
	}

	perms, err := h.getPermissionsForRole(roleID)
	if err != nil {
		perms = []permissionResponse{}
	}

	c.JSON(http.StatusOK, roleResponse{
		ID:          role.ID,
		Name:        role.Name,
		Description: &role.Description,
		IsActive:    role.IsActive,
		Permissions: perms,
	})
}

func (h *PermissionHandler) getPermissionsForRole(roleID int) ([]permissionResponse, error) {
	var perms []models.Permission
	err := h.GormDB.
		Joins("JOIN role_permissions rp ON rp.permission_id = permissions.id").
		Where("rp.role_id = ?", roleID).
		Find(&perms).Error
	if err != nil {
		return nil, err
	}

	result := make([]permissionResponse, len(perms))
	for i := range perms {
		result[i] = permToResponse(&perms[i])
	}
	return result, nil
}
