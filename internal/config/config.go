package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	SecretKey             string
	DatabaseURL           string
	Port                  string
	PlaidClientID         string
	PlaidSecret           string
	PlaidEnv              string
	TekmetricClientID     string
	TekmetricClientSecret string
	TekmetricBaseURL      string
	TekmetricBase64AuthKey string
	FrontendURL            string
	XeroClientID           string
	XeroClientSecret       string
	XeroRedirectURI        string
	SMTPHost               string
	SMTPPort               string
	SMTPUser               string
	SMTPPass               string
	SMTPFrom               string
}

func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from environment")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	return &Config{
		SecretKey:             os.Getenv("SECRET_KEY"),
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		Port:                  port,
		PlaidClientID:         os.Getenv("PLAID_CLIENT_ID"),
		PlaidSecret:           os.Getenv("PLAID_SECRET"),
		PlaidEnv:              os.Getenv("PLAID_ENV"),
		TekmetricClientID:     os.Getenv("TEKMETRIC_CLIENT_ID"),
		TekmetricClientSecret: os.Getenv("TEKMETRIC_CLIENT_SECRET"),
		TekmetricBaseURL:      os.Getenv("TEKMETRIC_BASE_URL"),
		TekmetricBase64AuthKey: os.Getenv("TEKMETRIC_BASE64_AUTH_KEY"),
		FrontendURL:            os.Getenv("FRONTEND_URL"),
		XeroClientID:           os.Getenv("XERO_CLIENT_ID"),
		XeroClientSecret:       os.Getenv("XERO_CLIENT_SECRET"),
		XeroRedirectURI:        os.Getenv("XERO_REDIRECT_URI"),
		SMTPHost:               os.Getenv("SMTP_HOST"),
		SMTPPort:               os.Getenv("SMTP_PORT"),
		SMTPUser:               os.Getenv("SMTP_USER"),
		SMTPPass:               os.Getenv("SMTP_PASS"),
		SMTPFrom:               os.Getenv("SMTP_FROM"),
	}
}
