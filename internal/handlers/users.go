package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type UserHandler struct {
	DB *pgxpool.Pool
}

type userCreateRequest struct {
	Email        string   `json:"email" binding:"required"`
	Password     string   `json:"password" binding:"required"`
	FirstName    string   `json:"first_name" binding:"required"`
	LastName     string   `json:"last_name" binding:"required"`
	MobileNumber string   `json:"mobile_number" binding:"required"`
	Role         *string  `json:"role"`
	LocationIDs  []string `json:"location_ids"`
}

type userUpdateRequest struct {
	Email        *string  `json:"email"`
	Password     *string  `json:"password"`
	FirstName    *string  `json:"first_name"`
	LastName     *string  `json:"last_name"`
	MobileNumber *string  `json:"mobile_number"`
	Role         *string  `json:"role"`
	IsActive     *bool    `json:"is_active"`
	LocationIDs  []string `json:"location_ids"`
}

type locationOut struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type userOut struct {
	ID           string        `json:"id"`
	Email        string        `json:"email"`
	FirstName    string        `json:"first_name"`
	LastName     string        `json:"last_name"`
	MobileNumber string        `json:"mobile_number"`
	IsActive     bool          `json:"is_active"`
	IsSuperuser  bool          `json:"is_superuser"`
	Role         string        `json:"role"`
	FullName     string        `json:"full_name"`
	Locations    []locationOut `json:"locations"`
}

func (h *UserHandler) scanUserOut(userID string) (*userOut, error) {
	var u userOut
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, email, first_name, last_name, mobile_number, is_active, is_superuser, role
		 FROM users WHERE id = $1`, userID,
	).Scan(&u.ID, &u.Email, &u.FirstName, &u.LastName, &u.MobileNumber, &u.IsActive, &u.IsSuperuser, &u.Role)
	if err != nil {
		return nil, err
	}
	u.FullName = u.FirstName + " " + u.LastName

	rows, err := h.DB.Query(context.Background(),
		`SELECT l.id, l.name FROM locations l
		 JOIN user_location_association ul ON l.id = ul.location_id
		 WHERE ul.user_id = $1`, userID)
	if err == nil {
		defer rows.Close()
		u.Locations = []locationOut{}
		for rows.Next() {
			var loc locationOut
			if err := rows.Scan(&loc.ID, &loc.Name); err == nil {
				u.Locations = append(u.Locations, loc)
			}
		}
	} else {
		u.Locations = []locationOut{}
	}

	return &u, nil
}

// GET /users/me
func (h *UserHandler) GetMyProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	u, err := h.scanUserOut(uid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, u)
}

// PATCH /users/me
func (h *UserHandler) EditMyProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var req userUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Users cannot update their own role or is_active
	req.Role = nil
	req.IsActive = nil

	if err := h.applyUserUpdate(uid, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	u, err := h.scanUserOut(uid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, u)
}

// GET /users/
func (h *UserHandler) ListUsers(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))

	rows, err := h.DB.Query(context.Background(),
		`SELECT id FROM users OFFSET $1 LIMIT $2`, skip, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query users"})
		return
	}
	defer rows.Close()

	users := []userOut{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		u, err := h.scanUserOut(id)
		if err == nil {
			users = append(users, *u)
		}
	}
	c.JSON(http.StatusOK, users)
}

// GET /users/:user_id
func (h *UserHandler) GetUser(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	uid := c.Param("user_id")
	u, err := h.scanUserOut(uid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, u)
}

// POST /users/
func (h *UserHandler) CreateUser(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	var req userCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check email uniqueness
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", req.Email).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Email already registered"})
		return
	}

	// Check mobile_number uniqueness
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE mobile_number = $1)", req.MobileNumber).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number already registered"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password"})
		return
	}

	role := "User"
	if req.Role != nil {
		role = *req.Role
	}

	id := uuid.New().String()
	now := time.Now().UTC()

	_, err = h.DB.Exec(context.Background(),
		`INSERT INTO users (id, email, hashed_password, first_name, last_name, mobile_number, role, is_active, is_superuser, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, true, false, $8)`,
		id, req.Email, string(hashedPassword), req.FirstName, req.LastName, req.MobileNumber, role, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create user"})
		return
	}

	// Assign locations
	if len(req.LocationIDs) > 0 {
		for _, locID := range req.LocationIDs {
			h.DB.Exec(context.Background(),
				"INSERT INTO user_location_association (user_id, location_id) VALUES ($1, $2)", id, locID)
		}
	}

	u, err := h.scanUserOut(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to read created user"})
		return
	}
	c.JSON(http.StatusCreated, u)
}

// PATCH /users/:user_id
func (h *UserHandler) PatchUser(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	uid := c.Param("user_id")

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", uid).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	var req userUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check email uniqueness
	if req.Email != nil {
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1 AND id != $2)", *req.Email, uid).Scan(&exists)
		if exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Email already registered"})
			return
		}
	}

	// Check mobile_number uniqueness
	if req.MobileNumber != nil {
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM users WHERE mobile_number = $1 AND id != $2)", *req.MobileNumber, uid).Scan(&exists)
		if exists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number already registered"})
			return
		}
	}

	if err := h.applyUserUpdate(uid, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	u, err := h.scanUserOut(uid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, u)
}

// DELETE /users/:user_id
func (h *UserHandler) DeleteUser(c *gin.Context) {
	if !h.requireAdmin(c) {
		return
	}

	uid := c.Param("user_id")

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM users WHERE id = $1", uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete user"})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *UserHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if fmt.Sprintf("%v", role) != "Admin" {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Admin access required"})
		return false
	}
	return true
}

func (h *UserHandler) applyUserUpdate(uid string, req *userUpdateRequest) error {
	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.Email != nil {
		addClause("email", *req.Email)
	}
	if req.FirstName != nil {
		addClause("first_name", *req.FirstName)
	}
	if req.LastName != nil {
		addClause("last_name", *req.LastName)
	}
	if req.MobileNumber != nil {
		addClause("mobile_number", *req.MobileNumber)
	}
	if req.Role != nil {
		addClause("role", *req.Role)
	}
	if req.IsActive != nil {
		addClause("is_active", *req.IsActive)
	}
	if req.Password != nil {
		hashed, err := bcrypt.GenerateFromPassword([]byte(*req.Password), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("failed to hash password")
		}
		addClause("hashed_password", string(hashed))
	}

	// Handle location_ids
	if req.LocationIDs != nil {
		// Clear existing
		h.DB.Exec(context.Background(),
			"DELETE FROM user_location_association WHERE user_id = $1", uid)
		for _, locID := range req.LocationIDs {
			h.DB.Exec(context.Background(),
				"INSERT INTO user_location_association (user_id, location_id) VALUES ($1, $2)", uid, locID)
		}
	}

	if len(setClauses) > 0 {
		args = append(args, uid)
		query := fmt.Sprintf("UPDATE users SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err := h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			return fmt.Errorf("failed to update user")
		}
	}

	return nil
}
