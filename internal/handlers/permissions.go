package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PermissionHandler struct {
	DB *pgxpool.Pool
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

func (h *PermissionHandler) CreatePermission(c *gin.Context) {
	var req permissionCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check if keyword already exists
	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM permissions WHERE keyword = $1)", req.Keyword).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database error"})
		return
	}
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Permission keyword already exists"})
		return
	}

	var resp permissionResponse
	err = h.DB.QueryRow(context.Background(),
		`INSERT INTO permissions (keyword, endpoint, method, description)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, keyword, endpoint, method, description, is_active`,
		req.Keyword, req.Endpoint, req.Method, req.Description,
	).Scan(&resp.ID, &resp.Keyword, &resp.Endpoint, &resp.Method, &resp.Description, &resp.IsActive)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create permission"})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (h *PermissionHandler) GetAllPermissions(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		"SELECT id, keyword, endpoint, method, description, is_active FROM permissions")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query permissions"})
		return
	}
	defer rows.Close()

	permissions := []permissionResponse{}
	for rows.Next() {
		var p permissionResponse
		if err := rows.Scan(&p.ID, &p.Keyword, &p.Endpoint, &p.Method, &p.Description, &p.IsActive); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan permission"})
			return
		}
		permissions = append(permissions, p)
	}

	c.JSON(http.StatusOK, permissions)
}

func (h *PermissionHandler) CreateRole(c *gin.Context) {
	var req roleCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check if role already exists
	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM roles WHERE name = $1)", req.Name).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database error"})
		return
	}
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Role already exists"})
		return
	}

	tx, err := h.DB.Begin(context.Background())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to begin transaction"})
		return
	}
	defer tx.Rollback(context.Background())

	var roleID int
	var roleName string
	var roleDesc *string
	var roleActive bool
	err = tx.QueryRow(context.Background(),
		`INSERT INTO roles (name, description) VALUES ($1, $2)
		 RETURNING id, name, description, is_active`,
		req.Name, req.Description,
	).Scan(&roleID, &roleName, &roleDesc, &roleActive)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create role"})
		return
	}

	for _, keyword := range req.PermissionKeywords {
		var permID int
		err := tx.QueryRow(context.Background(),
			"SELECT id FROM permissions WHERE keyword = $1", keyword).Scan(&permID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Permission keyword not found: %s", keyword)})
			return
		}
		_, err = tx.Exec(context.Background(),
			"INSERT INTO role_permissions (role_id, permission_id) VALUES ($1, $2)", roleID, permID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to assign permission to role"})
			return
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to commit transaction"})
		return
	}

	h.getRoleByID(c, roleID)
}

func (h *PermissionHandler) GetAllRoles(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		"SELECT id, name, description, is_active FROM roles")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query roles"})
		return
	}
	defer rows.Close()

	roles := []roleResponse{}
	for rows.Next() {
		var r roleResponse
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.IsActive); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan role"})
			return
		}
		r.Permissions = []permissionResponse{}
		roles = append(roles, r)
	}

	// Load permissions for each role
	for i, r := range roles {
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
	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM roles WHERE id = $1)", roleID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Role not found"})
		return
	}

	tx, err := h.DB.Begin(context.Background())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to begin transaction"})
		return
	}
	defer tx.Rollback(context.Background())

	// Clear existing permissions
	_, err = tx.Exec(context.Background(), "DELETE FROM role_permissions WHERE role_id = $1", roleID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to clear permissions"})
		return
	}

	// Assign new permissions
	for _, keyword := range req.PermissionKeywords {
		var permID int
		err := tx.QueryRow(context.Background(),
			"SELECT id FROM permissions WHERE keyword = $1", keyword).Scan(&permID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Permission keyword not found: %s", keyword)})
			return
		}
		_, err = tx.Exec(context.Background(),
			"INSERT INTO role_permissions (role_id, permission_id) VALUES ($1, $2)", roleID, permID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to assign permission"})
			return
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to commit transaction"})
		return
	}

	h.getRoleByID(c, roleID)
}

func (h *PermissionHandler) getRoleByID(c *gin.Context, roleID int) {
	var r roleResponse
	err := h.DB.QueryRow(context.Background(),
		"SELECT id, name, description, is_active FROM roles WHERE id = $1", roleID,
	).Scan(&r.ID, &r.Name, &r.Description, &r.IsActive)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Role not found"})
		return
	}

	perms, err := h.getPermissionsForRole(roleID)
	if err != nil {
		r.Permissions = []permissionResponse{}
	} else {
		r.Permissions = perms
	}

	c.JSON(http.StatusOK, r)
}

func (h *PermissionHandler) getPermissionsForRole(roleID int) ([]permissionResponse, error) {
	rows, err := h.DB.Query(context.Background(),
		`SELECT p.id, p.keyword, p.endpoint, p.method, p.description, p.is_active
		 FROM permissions p
		 JOIN role_permissions rp ON p.id = rp.permission_id
		 WHERE rp.role_id = $1`, roleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	perms := []permissionResponse{}
	for rows.Next() {
		var p permissionResponse
		if err := rows.Scan(&p.ID, &p.Keyword, &p.Endpoint, &p.Method, &p.Description, &p.IsActive); err != nil {
			return nil, err
		}
		perms = append(perms, p)
	}
	return perms, nil
}
