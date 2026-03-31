package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type AuthHandler struct {
	DB        *pgxpool.Pool
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

	var userID int
	var email, passwordHash, role string

	err := h.DB.QueryRow(context.Background(),
		"SELECT id, email, hashed_password, role FROM users WHERE email = $1",
		req.Email,
	).Scan(&userID, &email, &passwordHash, &role)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email or password"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email or password"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": userID,
		"email":   email,
		"role":    role,
		"exp":     time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": tokenString})
}

// POST /auth/signup
func (h *AuthHandler) Signup(c *gin.Context) {
	var req signupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Check email uniqueness
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", req.Email).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Email is already registered"})
		return
	}

	// Check mobile_number uniqueness
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE mobile_number = $1)", req.MobileNumber).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Mobile number is already registered"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password"})
		return
	}

	role := "User"
	if req.Role != nil && *req.Role != "" {
		role = *req.Role
	}

	id := uuid.New().String()

	_, err = h.DB.Exec(context.Background(),
		`INSERT INTO users (id, email, hashed_password, first_name, last_name, mobile_number, role, is_active, is_superuser)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, true, false)`,
		id, req.Email, string(hashedPassword), req.FirstName, req.LastName, req.MobileNumber, role,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create user"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":            id,
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

	var userID, email, passwordHash, role string
	var isActive bool

	err := h.DB.QueryRow(context.Background(),
		"SELECT id, email, hashed_password, role, is_active FROM users WHERE email = $1",
		req.Email,
	).Scan(&userID, &email, &passwordHash, &role, &isActive)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if !isActive {
		c.JSON(http.StatusForbidden, gin.H{"detail": "User is inactive"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   userID,
		"email": email,
		"role":  role,
		"exp":   time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token": tokenString,
		"token_type":   "bearer",
		"user_id":      userID,
		"email":        email,
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

	var userID, email, passwordHash, role string
	var isActive bool

	err := h.DB.QueryRow(context.Background(),
		"SELECT id, email, hashed_password, role, is_active FROM users WHERE email = $1",
		req.Email,
	).Scan(&userID, &email, &passwordHash, &role, &isActive)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Invalid credentials"})
		return
	}

	if !isActive {
		c.JSON(http.StatusForbidden, gin.H{"detail": "User is inactive"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   userID,
		"email": email,
		"role":  role,
		"exp":   time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token": tokenString,
		"token_type":   "bearer",
		"user_id":      userID,
		"email":        email,
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

	var userID, firstName string
	err := h.DB.QueryRow(context.Background(),
		"SELECT id, first_name FROM users WHERE email = $1", email,
	).Scan(&userID, &firstName)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Generate reset token
	tokenBytes := make([]byte, 32)
	rand.Read(tokenBytes)
	resetToken := hex.EncodeToString(tokenBytes)

	expiry := time.Now().UTC().Add(30 * time.Minute)

	_, err = h.DB.Exec(context.Background(),
		"UPDATE users SET reset_password_token = $1, reset_password_expiry = $2 WHERE id = $3",
		resetToken, expiry, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate reset token"})
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

	var userID string
	var expiry *time.Time
	err := h.DB.QueryRow(context.Background(),
		"SELECT id, reset_password_expiry FROM users WHERE reset_password_token = $1", token,
	).Scan(&userID, &expiry)
	if err != nil || expiry == nil || expiry.Before(time.Now().UTC()) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid or expired token"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to hash password"})
		return
	}

	_, err = h.DB.Exec(context.Background(),
		"UPDATE users SET hashed_password = $1, reset_password_token = NULL, reset_password_expiry = NULL WHERE id = $2",
		string(hashedPassword), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": fmt.Sprintf("failed to reset password: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Password reset successfully"})
}
