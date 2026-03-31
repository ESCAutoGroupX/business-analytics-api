package handlers

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TwoFactorAuthHandler struct {
	DB        *pgxpool.Pool
	SecretKey string
}

type verifyOTPRequest struct {
	UserID string `json:"user_id" binding:"required"`
	OTP    string `json:"otp" binding:"required"`
}

type resendOTPRequest struct {
	UserID          string  `json:"user_id" binding:"required"`
	PreferredMethod *string `json:"preferred_method"`
}

func generateOTP() string {
	n, _ := rand.Int(rand.Reader, big.NewInt(900000))
	return fmt.Sprintf("%06d", n.Int64()+100000)
}

// POST /2fa/mobile-otp
func (h *TwoFactorAuthHandler) SendMobileOTP(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Update preferred_auth_method if not already mobile_otp
	h.DB.Exec(context.Background(),
		"UPDATE users SET preferred_auth_method = $1 WHERE id = $2 AND (preferred_auth_method IS NULL OR preferred_auth_method != $1)",
		"mobile_otp", userID)

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	h.DB.Exec(context.Background(),
		"UPDATE users SET otp = $1, otp_expiry = $2 WHERE id = $3",
		otp, expiry, userID)

	c.JSON(http.StatusOK, gin.H{"message": "OTP sent to your mobile number"})
}

// POST /2fa/email-verification
func (h *TwoFactorAuthHandler) SendEmailVerification(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	h.DB.Exec(context.Background(),
		"UPDATE users SET preferred_auth_method = $1 WHERE id = $2 AND (preferred_auth_method IS NULL OR preferred_auth_method != $1)",
		"email_otp", userID)

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	h.DB.Exec(context.Background(),
		"UPDATE users SET otp = $1, otp_expiry = $2 WHERE id = $3",
		otp, expiry, userID)

	c.JSON(http.StatusOK, gin.H{"message": "OTP sent to your email address"})
}

// POST /2fa/authenticator-setup
func (h *TwoFactorAuthHandler) SetupAuthenticator(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var exists bool
	err := h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	h.DB.Exec(context.Background(),
		"UPDATE users SET preferred_auth_method = $1 WHERE id = $2 AND (preferred_auth_method IS NULL OR preferred_auth_method != $1)",
		"authenticator_app", userID)

	// Generate a random base32-like secret
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	secret := make([]byte, 32)
	for i := range secret {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		secret[i] = chars[n.Int64()]
	}
	secretStr := string(secret)

	h.DB.Exec(context.Background(),
		"UPDATE users SET authenticator_secret = $1 WHERE id = $2",
		secretStr, userID)

	qrURL := fmt.Sprintf("/static/qrcodes/%s_qrcode.png", userID)

	c.JSON(http.StatusOK, gin.H{
		"message":      "Authenticator app setup complete",
		"qr_code_url":  qrURL,
	})
}

// POST /2fa/verify-otp
func (h *TwoFactorAuthHandler) VerifyOTP(c *gin.Context) {
	userID := c.Query("user_id")
	otpParam := c.Query("otp")

	if userID == "" || otpParam == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id and otp are required"})
		return
	}

	var storedOTP *string
	var otpExpiry *time.Time
	var uid string

	err := h.DB.QueryRow(context.Background(),
		"SELECT id, otp, otp_expiry FROM users WHERE id = $1", userID,
	).Scan(&uid, &storedOTP, &otpExpiry)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	if storedOTP == nil || *storedOTP != otpParam || otpExpiry == nil || otpExpiry.Before(time.Now().UTC()) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid or expired OTP"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uid,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(h.SecretKey))
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to generate token", "error": err.Error()})
		return
	}

	c.SetCookie("access_token", "Bearer "+tokenString, 86400, "/", "", false, true)

	c.JSON(http.StatusOK, gin.H{
		"message": "OTP verified successfully",
		"access_token": tokenString,
	})
}

// POST /2fa/resend-otp
func (h *TwoFactorAuthHandler) ResendOTP(c *gin.Context) {
	userID := c.Query("user_id")
	preferredMethod := c.Query("preferred_method")

	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var currentMethod *string
	err := h.DB.QueryRow(context.Background(),
		"SELECT preferred_auth_method FROM users WHERE id = $1", userID,
	).Scan(&currentMethod)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	if currentMethod == nil || *currentMethod == "" {
		if preferredMethod == "" {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Preferred authentication method is not set. Please provide one in the request (e.g., 'mobile_otp', 'email_otp', 'authenticator_app')."})
			return
		}
		validMethods := map[string]bool{"mobile_otp": true, "email_otp": true, "authenticator_app": true}
		if !validMethods[preferredMethod] {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid preferred authentication method. Allowed values are 'mobile_otp', 'email_otp', 'authenticator_app'."})
			return
		}
		h.DB.Exec(context.Background(),
			"UPDATE users SET preferred_auth_method = $1 WHERE id = $2", preferredMethod, userID)
		currentMethod = &preferredMethod
	}

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	switch *currentMethod {
	case "mobile_otp":
		h.DB.Exec(context.Background(),
			"UPDATE users SET otp = $1, otp_expiry = $2 WHERE id = $3", otp, expiry, userID)
		c.JSON(http.StatusOK, gin.H{"message": "OTP resent to your mobile number"})
	case "email_otp":
		h.DB.Exec(context.Background(),
			"UPDATE users SET otp = $1, otp_expiry = $2 WHERE id = $3", otp, expiry, userID)
		c.JSON(http.StatusOK, gin.H{"message": "OTP resent to your email address"})
	case "authenticator_app":
		c.JSON(http.StatusOK, gin.H{"message": "Authenticator app is set as the preferred method. Please use your authenticator app to generate the OTP."})
	default:
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid preferred authentication method."})
	}
}
