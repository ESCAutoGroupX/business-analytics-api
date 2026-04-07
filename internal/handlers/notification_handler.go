package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/notifications"
)

type NotificationHandler struct {
	Email *notifications.EmailSender
}

// POST /notifications/test-email
func (h *NotificationHandler) TestEmail(c *gin.Context) {
	var req struct {
		Type string `json:"type"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		req.Type = "test"
	}

	userEmail, _ := c.Get("email")
	email := fmt.Sprintf("%v", userEmail)

	switch req.Type {
	case "overdue_bills":
		go h.Email.CheckOverdueBills()
		c.JSON(http.StatusOK, gin.H{"status": "started", "alert": "overdue_bills"})
	case "reconciliation":
		go h.Email.CheckReconciliationAlert()
		c.JSON(http.StatusOK, gin.H{"status": "started", "alert": "reconciliation"})
	case "low_balance":
		go h.Email.CheckLowBankBalance()
		c.JSON(http.StatusOK, gin.H{"status": "started", "alert": "low_balance"})
	default:
		go h.Email.SendEmail(email, "ESC Business Analytics — Test Email",
			`<div style="font-family:Arial,sans-serif">
				<h2>Test Email</h2>
				<p>This is a test email from ESC Business Analytics.</p>
				<p>If you received this, email notifications are working correctly.</p>
			</div>`)
		c.JSON(http.StatusOK, gin.H{"status": "started", "alert": "test", "to": email})
	}
}
