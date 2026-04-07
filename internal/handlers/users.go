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
	MobileNumber string   `json:"mobile_number"`
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

	// Users cannot update their own role, is_active, or password via profile edit
	req.Role = nil
	req.IsActive = nil
	req.Password = nil

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

// POST /users/me/change-password
func (h *UserHandler) ChangePassword(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var req struct {
		CurrentPassword string `json:"current_password" binding:"required"`
		NewPassword     string `json:"new_password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "current_password and new_password are required"})
		return
	}

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", uid).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	log.Printf("Change password: user %s (%s) requesting password change", user.Email, uid)

	if err := bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(req.CurrentPassword)); err != nil {
		log.Printf("Change password: current password verification FAILED for %s", user.Email)
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Current password is incorrect"})
		return
	}
	log.Printf("Change password: current password verified for %s", user.Email)

	log.Printf("Change password: hashing new password for user %s", user.Email)
	hashed, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("Change password: hash generation FAILED for %s: %v", user.Email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password"})
		return
	}
	hashStr := string(hashed)
	log.Printf("Change password: hash result first 10 chars: %s", hashStr[:10])
	log.Printf("Change password: hash length: %d", len(hashStr))

	// Pre-save verification: confirm the hash matches the new password
	if err := bcrypt.CompareHashAndPassword(hashed, []byte(req.NewPassword)); err != nil {
		log.Printf("Change password: CRITICAL — hash does not match new password BEFORE save for %s: %v", user.Email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password hash verification failed before save"})
		return
	}
	log.Printf("Change password: pre-save hash verification passed")

	log.Printf("Change password: saving to DB for %s", user.Email)
	if err := h.GormDB.Model(&models.User{}).Where("id = ?", uid).Update("hashed_password", hashStr).Error; err != nil {
		log.Printf("Change password: DB update FAILED for %s: %v", user.Email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update password"})
		return
	}
	log.Printf("Change password: DB update succeeded for %s", user.Email)

	// Post-save verification: read back from DB and confirm
	var verify models.User
	if err := h.GormDB.First(&verify, "id = ?", uid).Error; err != nil {
		log.Printf("Change password: post-save read FAILED for %s: %v", user.Email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password save verification failed — could not read back"})
		return
	}
	log.Printf("Change password: read-back hash first 10 chars: %s", verify.HashedPassword[:10])
	if verify.HashedPassword != hashStr {
		log.Printf("Change password: CRITICAL — saved hash does not match for %s! wrote=%s… got=%s…",
			user.Email, hashStr[:20], verify.HashedPassword[:20])
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password save verification failed — hash mismatch after save"})
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(verify.HashedPassword), []byte(req.NewPassword)); err != nil {
		log.Printf("Change password: CRITICAL — read-back hash fails bcrypt compare for %s: %v", user.Email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password save verification failed"})
		return
	}
	log.Printf("Change password: post-save verification PASSED for %s", user.Email)

	c.JSON(http.StatusOK, gin.H{"detail": "Password changed successfully"})
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

	// Check mobile_number uniqueness (only if provided)
	if req.MobileNumber != "" {
		h.GormDB.Model(&models.User{}).Where("mobile_number = ?", req.MobileNumber).Count(&count)
		if count > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number already registered"})
			return
		}
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
	role = strings.ToUpper(role)

	user := models.User{
		ID:             uuid.New().String(),
		Email:          req.Email,
		HashedPassword: string(hashedPassword),
		FirstName:      &req.FirstName,
		LastName:       &req.LastName,
		MobileNumber:   nilIfEmpty(req.MobileNumber),
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

	var count int64
	h.GormDB.Model(&models.User{}).Where("id = ?", uid).Count(&count)
	if count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Try hard delete inside a transaction so junction cleanup is rolled back on failure
	txErr := h.GormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("user_id = ?", uid).Delete(&models.UserLocation{}).Error; err != nil {
			return err
		}
		return tx.Delete(&models.User{}, "id = ?", uid).Error
	})

	if txErr != nil {
		// FK constraint from other tables — soft delete instead
		log.Printf("Hard delete failed for user %s, deactivating: %v", uid, txErr)
		if err := h.GormDB.Model(&models.User{}).Where("id = ?", uid).Update("is_active", false).Error; err != nil {
			log.Printf("ERROR deactivating user %s: %v", uid, err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete user"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"detail": "Cannot delete user with existing records. User has been deactivated instead."})
		return
	}

	c.Status(http.StatusNoContent)
}

// POST /users/:user_id/reset-password
func (h *UserHandler) ResetUserPassword(c *gin.Context) {
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

	var req struct {
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "password is required"})
		return
	}

	log.Printf("Reset password: admin resetting password for user %s", uid)
	hashed, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("Reset password: hash generation FAILED for %s: %v", uid, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password"})
		return
	}
	hashStr := string(hashed)
	log.Printf("Reset password: hash first 10 chars: %s, length: %d", hashStr[:10], len(hashStr))

	if err := h.GormDB.Model(&models.User{}).Where("id = ?", uid).Update("hashed_password", hashStr).Error; err != nil {
		log.Printf("Reset password: DB update FAILED for %s: %v", uid, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to reset password"})
		return
	}

	// Post-save verification
	var verify models.User
	if err := h.GormDB.First(&verify, "id = ?", uid).Error; err != nil {
		log.Printf("Reset password: post-save read FAILED for %s: %v", uid, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password save verification failed"})
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(verify.HashedPassword), []byte(req.Password)); err != nil {
		log.Printf("Reset password: CRITICAL — read-back hash fails bcrypt compare for %s: %v", uid, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Password save verification failed"})
		return
	}
	log.Printf("Reset password: post-save verification PASSED for %s", uid)

	c.JSON(http.StatusOK, gin.H{"detail": "Password reset successfully"})
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
		updates["role"] = strings.ToUpper(*req.Role)
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

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Ensure gorm.ErrRecordNotFound is referenced to avoid import error
var _ = errors.Is
var _ = gorm.ErrRecordNotFound
