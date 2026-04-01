package handlers

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type TwoFactorAuthHandler struct {
	GormDB    *gorm.DB
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

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", userID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Update preferred_auth_method if not already mobile_otp
	method := "mobile_otp"
	if user.PreferredAuthMethod == nil || *user.PreferredAuthMethod != method {
		h.GormDB.Model(&user).Update("preferred_auth_method", method)
	}

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	h.GormDB.Model(&user).Updates(map[string]interface{}{
		"otp":        otp,
		"otp_expiry": expiry,
	})

	c.JSON(http.StatusOK, gin.H{"message": "OTP sent to your mobile number"})
}

// POST /2fa/email-verification
func (h *TwoFactorAuthHandler) SendEmailVerification(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", userID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	method := "email_otp"
	if user.PreferredAuthMethod == nil || *user.PreferredAuthMethod != method {
		h.GormDB.Model(&user).Update("preferred_auth_method", method)
	}

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	h.GormDB.Model(&user).Updates(map[string]interface{}{
		"otp":        otp,
		"otp_expiry": expiry,
	})

	c.JSON(http.StatusOK, gin.H{"message": "OTP sent to your email address"})
}

// POST /2fa/authenticator-setup
func (h *TwoFactorAuthHandler) SetupAuthenticator(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id is required"})
		return
	}

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", userID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	method := "authenticator_app"
	if user.PreferredAuthMethod == nil || *user.PreferredAuthMethod != method {
		h.GormDB.Model(&user).Update("preferred_auth_method", method)
	}

	// Generate a random base32-like secret
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	secret := make([]byte, 32)
	for i := range secret {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		secret[i] = chars[n.Int64()]
	}
	secretStr := string(secret)

	h.GormDB.Model(&user).Update("authenticator_secret", secretStr)

	qrURL := fmt.Sprintf("/static/qrcodes/%s_qrcode.png", userID)

	c.JSON(http.StatusOK, gin.H{
		"message":     "Authenticator app setup complete",
		"qr_code_url": qrURL,
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

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", userID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	if user.OTP == nil || *user.OTP != otpParam || user.OTPExpiry == nil || user.OTPExpiry.Before(time.Now().UTC()) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid or expired OTP"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": user.ID,
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
		"message":      "OTP verified successfully",
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

	var user models.User
	if err := h.GormDB.First(&user, "id = ?", userID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	currentMethod := user.PreferredAuthMethod

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
		h.GormDB.Model(&user).Update("preferred_auth_method", preferredMethod)
		currentMethod = &preferredMethod
	}

	otp := generateOTP()
	expiry := time.Now().UTC().Add(10 * time.Minute)

	switch *currentMethod {
	case "mobile_otp":
		h.GormDB.Model(&user).Updates(map[string]interface{}{"otp": otp, "otp_expiry": expiry})
		c.JSON(http.StatusOK, gin.H{"message": "OTP resent to your mobile number"})
	case "email_otp":
		h.GormDB.Model(&user).Updates(map[string]interface{}{"otp": otp, "otp_expiry": expiry})
		c.JSON(http.StatusOK, gin.H{"message": "OTP resent to your email address"})
	case "authenticator_app":
		c.JSON(http.StatusOK, gin.H{"message": "Authenticator app is set as the preferred method. Please use your authenticator app to generate the OTP."})
	default:
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid preferred authentication method."})
	}
}
