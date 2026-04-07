package notifications

import (
	"fmt"
	"log"
	"net/smtp"
	"strings"

	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

type EmailSender struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

func (e *EmailSender) SendEmail(to, subject, htmlBody string) error {
	if e.Cfg.SMTPHost == "" || e.Cfg.SMTPPass == "" {
		log.Printf("EMAIL SKIPPED (SMTP not configured): to=%s subject=%s", to, subject)
		return fmt.Errorf("SMTP not configured")
	}

	from := e.Cfg.SMTPFrom
	if from == "" {
		from = e.Cfg.SMTPUser
	}

	port := e.Cfg.SMTPPort
	if port == "" {
		port = "587"
	}

	msg := strings.Join([]string{
		"From: " + from,
		"To: " + to,
		"Subject: " + subject,
		"MIME-Version: 1.0",
		"Content-Type: text/html; charset=\"UTF-8\"",
		"",
		htmlBody,
	}, "\r\n")

	auth := smtp.PlainAuth("", e.Cfg.SMTPUser, e.Cfg.SMTPPass, e.Cfg.SMTPHost)
	addr := e.Cfg.SMTPHost + ":" + port

	if err := smtp.SendMail(addr, auth, from, []string{to}, []byte(msg)); err != nil {
		log.Printf("EMAIL FAILED: to=%s subject=%s err=%v", to, subject, err)
		return err
	}

	log.Printf("EMAIL SENT: to=%s subject=%s", to, subject)
	return nil
}

func (e *EmailSender) SendEmailToAdmins(subject, htmlBody string) {
	var emails []string
	e.GormDB.Raw(`SELECT email FROM users WHERE UPPER(role) = 'ADMIN' AND is_active = true`).Scan(&emails)

	if len(emails) == 0 {
		log.Println("EMAIL: no active admin users found")
		return
	}

	for _, email := range emails {
		if err := e.SendEmail(email, subject, htmlBody); err != nil {
			log.Printf("EMAIL: failed to send to admin %s: %v", email, err)
		}
	}
}
