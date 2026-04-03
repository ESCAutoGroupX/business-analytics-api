package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type UserHandler struct {
	GormDB *gorm.DB
}

// FlexIDs accepts both ["1","2"] and [1,2] from JSON
type FlexIDs []string

func (f *FlexIDs) UnmarshalJSON(data []byte) error {
	var raw []interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	result := make([]string, len(raw))
	for i, v := range raw {
		result[i] = fmt.Sprintf("%v", v)
	}
	*f = result
	return nil
}

type userCreateRequest struct {
	Email        string   `json:"email" binding:"required"`
	Password     string   `json:"password" binding:"required"`
	FirstName    string   `json:"first_name" binding:"required"`
	LastName     string   `json:"last_name" binding:"required"`
	MobileNumber string   `json:"mobile_number" binding:"required"`
	Role         *string  `json:"role"`
	LocationIDs  FlexIDs `json:"location_ids"`
}

type userUpdateRequest struct {
	Email        *string  `json:"email"`
	Password     *string  `json:"password"`
	FirstName    *string  `json:"first_name"`
	LastName     *string  `json:"last_name"`
	MobileNumber *string  `json:"mobile_number"`
	Role         *string  `json:"role"`
	IsActive     *bool    `json:"is_active"`
	LocationIDs  FlexIDs `json:"location_ids"`
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
	log.Printf("scanUserOut: looking up user with ID=%s", userID)
	var user models.User
	result := h.GormDB.First(&user, "id = ?", userID)
	if result.Error != nil {
		log.Printf("scanUserOut: GORM error=%v, rows=%d", result.Error, result.RowsAffected)
		return nil, result.Error
	}
	log.Printf("scanUserOut: found user email=%s", user.Email)

	firstName := ""
	if user.FirstName != nil {
		firstName = *user.FirstName
	}
	lastName := ""
	if user.LastName != nil {
		lastName = *user.LastName
	}
	mobileNumber := ""
	if user.MobileNumber != nil {
		mobileNumber = *user.MobileNumber
	}
	role := ""
	if user.Role != nil {
		role = *user.Role
	}

	out := &userOut{
		ID:           user.ID,
		Email:        user.Email,
		FirstName:    firstName,
		LastName:     lastName,
		MobileNumber: mobileNumber,
		IsActive:     user.IsActive,
		IsSuperuser:  user.IsSuperuser,
		Role:         role,
		FullName:     firstName + " " + lastName,
		Locations:    []locationOut{},
	}

	for _, loc := range user.Locations {
		out.Locations = append(out.Locations, locationOut{
			ID:   fmt.Sprintf("%d", loc.ID),
			Name: loc.LocationName,
		})
	}

	return out, nil
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

	var userIDs []string
	if err := h.GormDB.Model(&models.User{}).Offset(skip).Limit(limit).Pluck("id", &userIDs).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query users", "error": err.Error()})
		return
	}

	users := []userOut{}
	for _, id := range userIDs {
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
	var count int64
	h.GormDB.Model(&models.User{}).Where("email = ?", req.Email).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Email already registered"})
		return
	}

	// Check mobile_number uniqueness
	h.GormDB.Model(&models.User{}).Where("mobile_number = ?", req.MobileNumber).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number already registered"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password", "error": err.Error()})
		return
	}

	role := "User"
	if req.Role != nil {
		role = *req.Role
	}

	user := models.User{
		ID:             uuid.New().String(),
		Email:          req.Email,
		HashedPassword: string(hashedPassword),
		FirstName:      &req.FirstName,
		LastName:       &req.LastName,
		MobileNumber:   &req.MobileNumber,
		Role:           &role,
		IsActive:       true,
		IsSuperuser:    false,
	}

	if err := h.GormDB.Create(&user).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create user", "error": err.Error()})
		return
	}

	// Assign locations
	for _, locID := range req.LocationIDs {
		h.GormDB.Create(&models.UserLocation{UserID: user.ID, LocationID: atoiSafe(locID)})
	}

	u, err := h.scanUserOut(user.ID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to read created user", "error": err.Error()})
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

	var count int64
	h.GormDB.Model(&models.User{}).Where("id = ?", uid).Count(&count)
	if count == 0 {
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
		h.GormDB.Model(&models.User{}).Where("email = ? AND id != ?", *req.Email, uid).Count(&count)
		if count > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Email already registered"})
			return
		}
	}

	// Check mobile_number uniqueness
	if req.MobileNumber != nil {
		h.GormDB.Model(&models.User{}).Where("mobile_number = ? AND id != ?", *req.MobileNumber, uid).Count(&count)
		if count > 0 {
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

	result := h.GormDB.Delete(&models.User{}, "id = ?", uid)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete user", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *UserHandler) requireAdmin(c *gin.Context) bool {
	role, _ := c.Get("role")
	if !strings.EqualFold(fmt.Sprintf("%v", role), "admin") {
		c.JSON(http.StatusForbidden, gin.H{"detail": "Admin access required"})
		return false
	}
	return true
}

func (h *UserHandler) applyUserUpdate(uid string, req *userUpdateRequest) error {
	updates := map[string]interface{}{}

	if req.Email != nil {
		updates["email"] = *req.Email
	}
	if req.FirstName != nil {
		updates["first_name"] = *req.FirstName
	}
	if req.LastName != nil {
		updates["last_name"] = *req.LastName
	}
	if req.MobileNumber != nil {
		updates["mobile_number"] = *req.MobileNumber
	}
	if req.Role != nil {
		updates["role"] = *req.Role
	}
	if req.IsActive != nil {
		updates["is_active"] = *req.IsActive
	}
	if req.Password != nil {
		hashed, err := bcrypt.GenerateFromPassword([]byte(*req.Password), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("failed to hash password")
		}
		updates["hashed_password"] = string(hashed)
	}

	// Handle location_ids
	if req.LocationIDs != nil {
		h.GormDB.Where("user_id = ?", uid).Delete(&models.UserLocation{})
		for _, locID := range req.LocationIDs {
			h.GormDB.Create(&models.UserLocation{UserID: uid, LocationID: atoiSafe(locID)})
		}
	}

	if len(updates) > 0 {
		if err := h.GormDB.Model(&models.User{}).Where("id = ?", uid).Updates(updates).Error; err != nil {
			return fmt.Errorf("failed to update user")
		}
	}

	return nil
}

func atoiSafe(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

// Ensure gorm.ErrRecordNotFound is referenced to avoid import error
var _ = errors.Is
var _ = gorm.ErrRecordNotFound
