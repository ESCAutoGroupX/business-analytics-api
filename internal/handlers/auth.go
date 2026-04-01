package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type AuthHandler struct {
	GormDB    *gorm.DB
	SecretKey string
}

type signInRequest struct {
	Email    string `json:"email" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type signupRequest struct {
	Email        string  `json:"email" binding:"required"`
	Password     string  `json:"password" binding:"required"`
	FirstName    string  `json:"first_name" binding:"required"`
	LastName     string  `json:"last_name" binding:"required"`
	MobileNumber string  `json:"mobile_number" binding:"required"`
	Role         *string `json:"role"`
}

type loginRequest struct {
	Email    string `json:"email" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type forgotPasswordRequest struct {
	Email string `json:"email" binding:"required"`
}

type resetPasswordRequest struct {
	Token       string `json:"token" binding:"required"`
	NewPassword string `json:"new_password" binding:"required"`
}

func (h *AuthHandler) SignIn(c *gin.Context) {
	var req signInRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email and password are required"})
		return
	}

	var user models.User
	err := h.GormDB.Where("email = ?", req.Email).First(&user).Error
	found := err == nil
	log.Printf("SignIn: email=%s found=%v err=%v", req.Email, found, err)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email or password"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(req.Password)); err != nil {
		log.Printf("SignIn: bcrypt compare result: %v", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email or password"})
		return
	}
	log.Printf("SignIn: bcrypt compare result: %v", nil)

	role := ""
	if user.Role != nil {
		role = *user.Role
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": user.ID,
		"email":   user.Email,
		"role":    role,
		"exp":     time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"access_token": tokenString})
}

// POST /auth/signup
func (h *AuthHandler) Signup(c *gin.Context) {
	var req signupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check email uniqueness
	var count int64
	h.GormDB.Model(&models.User{}).Where("email = ?", req.Email).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Email is already registered"})
		return
	}

	// Check mobile_number uniqueness
	h.GormDB.Model(&models.User{}).Where("mobile_number = ?", req.MobileNumber).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number is already registered"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password", "error": err.Error()})
		return
	}

	role := "User"
	if req.Role != nil && *req.Role != "" {
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

	c.JSON(http.StatusOK, gin.H{
		"id":            user.ID,
		"email":         req.Email,
		"first_name":    req.FirstName,
		"last_name":     req.LastName,
		"mobile_number": req.MobileNumber,
		"role":          role,
		"message":       "User registered successfully. Please complete 2FA.",
	})
}

// POST /auth/login
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var user models.User
	if err := h.GormDB.Where("email = ?", req.Email).First(&user).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if !user.IsActive {
		c.JSON(http.StatusForbidden, gin.H{"detail": "User is inactive"})
		return
	}

	role := ""
	if user.Role != nil {
		role = *user.Role
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": user.ID,
		"email":   user.Email,
		"role":    role,
		"exp":     time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate token", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token": tokenString,
		"token_type":   "bearer",
		"user_id":      user.ID,
		"email":        user.Email,
		"role":         role,
	})
}

// POST /auth/login-direct
func (h *AuthHandler) LoginDirect(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	var user models.User
	if err := h.GormDB.Where("email = ?", req.Email).First(&user).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if !user.IsActive {
		c.JSON(http.StatusForbidden, gin.H{"detail": "User is inactive"})
		return
	}

	role := ""
	if user.Role != nil {
		role = *user.Role
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": user.ID,
		"email":   user.Email,
		"role":    role,
		"exp":     time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate token", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token": tokenString,
		"token_type":   "bearer",
		"user_id":      user.ID,
		"email":        user.Email,
		"role":         role,
	})
}

// POST /auth/forgot-password
func (h *AuthHandler) ForgotPassword(c *gin.Context) {
	email := c.Query("email")
	if email == "" {
		// Try JSON body
		var req forgotPasswordRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "email is required"})
			return
		}
		email = req.Email
	}

	var user models.User
	if err := h.GormDB.Where("email = ?", email).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Generate reset token
	tokenBytes := make([]byte, 32)
	rand.Read(tokenBytes)
	resetToken := hex.EncodeToString(tokenBytes)

	expiry := time.Now().UTC().Add(30 * time.Minute)

	if err := h.GormDB.Model(&user).Updates(map[string]interface{}{
		"reset_password_token":  resetToken,
		"reset_password_expiry": expiry,
	}).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate reset token", "error": err.Error()})
		return
	}

	// In production, send email here. For now, return success.
	c.JSON(http.StatusOK, gin.H{"message": "Reset password email sent"})
}

// POST /auth/reset-password
func (h *AuthHandler) ResetPassword(c *gin.Context) {
	token := c.Query("token")
	newPassword := c.Query("new_password")

	if token == "" || newPassword == "" {
		var req resetPasswordRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "token and new_password are required"})
			return
		}
		token = req.Token
		newPassword = req.NewPassword
	}

	var user models.User
	if err := h.GormDB.Where("reset_password_token = ?", token).First(&user).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid or expired token"})
		return
	}

	if user.ResetPasswordExpiry == nil || user.ResetPasswordExpiry.Before(time.Now().UTC()) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid or expired token"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password", "error": err.Error()})
		return
	}

	if err := h.GormDB.Model(&user).Updates(map[string]interface{}{
		"hashed_password":       string(hashedPassword),
		"reset_password_token":  nil,
		"reset_password_expiry": nil,
	}).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to reset password", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Password reset successfully"})
}
