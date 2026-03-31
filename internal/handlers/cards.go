package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CardHandler struct {
	DB *pgxpool.Pool
}

type cardCreateRequest struct {
	CardName        string `json:"card_name" binding:"required"`
	CardIDPlaid     string `json:"card_id_plaid" binding:"required"`
	BillingStartDay *int   `json:"billing_start_day"`
	BillingEndDay   *int   `json:"billing_end_day"`
	CycleType       string `json:"cycle_type" binding:"required"`
	LastFourDigits  string `json:"last_four_digits" binding:"required"`
	BankProvider    string `json:"bank_provider" binding:"required"`
}

type cardUpdateRequest struct {
	CardName        *string `json:"card_name"`
	CardIDPlaid     *string `json:"card_id_plaid"`
	BillingStartDay *int    `json:"billing_start_day"`
	BillingEndDay   *int    `json:"billing_end_day"`
	CycleType       *string `json:"cycle_type"`
	LastFourDigits  *string `json:"last_four_digits"`
	BankProvider    *string `json:"bank_provider"`
}

type cardResponse struct {
	ID              int        `json:"id"`
	CardName        string     `json:"card_name"`
	CardIDPlaid     string     `json:"card_id_plaid"`
	BillingStartDay *int       `json:"billing_start_day"`
	BillingEndDay   *int       `json:"billing_end_day"`
	CycleType       string     `json:"cycle_type"`
	LastFourDigits  string     `json:"last_four_digits"`
	BankProvider    string     `json:"bank_provider"`
	CreatedAt       *time.Time `json:"created_at"`
	UpdatedAt       *time.Time `json:"updated_at"`
}

var validCycleTypes = []string{"monthly", "quarterly", "biannual", "yearly", "custom"}
var validBankProviders = []string{"chase", "amex", "citi", "boa", "wells_fargo", "other"}

// POST /cards/
func (h *CardHandler) CreateCard(c *gin.Context) {
	var req cardCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	cycleType := strings.ToLower(req.CycleType)
	bankProvider := strings.ToLower(req.BankProvider)

	if !contains(validCycleTypes, cycleType) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid enum value: '%s' is not a valid CycleTypeEnum", req.CycleType)})
		return
	}
	if !contains(validBankProviders, bankProvider) {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid enum value: '%s' is not a valid BankProviderEnum", req.BankProvider)})
		return
	}

	// Check uniqueness of card_id_plaid
	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM cards WHERE card_id_plaid = $1)", req.CardIDPlaid).Scan(&exists)
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "This card_plaid_id is already associated with another card"})
		return
	}

	now := time.Now().UTC()
	var id int
	err := h.DB.QueryRow(context.Background(),
		`INSERT INTO cards (card_name, card_id_plaid, billing_start_day, billing_end_day, cycle_type, last_four_digits, bank_provider, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING id`,
		req.CardName, req.CardIDPlaid, req.BillingStartDay, req.BillingEndDay, cycleType, req.LastFourDigits, bankProvider, now, now,
	).Scan(&id)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "An unexpected error occurred while creating the card.", "error": err.Error()})
		return
	}

	h.getCardByID(c, id)
}

// GET /cards/:card_id
func (h *CardHandler) GetCard(c *gin.Context) {
	cardID, err := strconv.Atoi(c.Param("card_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid card_id"})
		return
	}
	h.getCardByID(c, cardID)
}

// GET /cards/
func (h *CardHandler) GetAllCards(c *gin.Context) {
	skip, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	rows, err := h.DB.Query(context.Background(),
		`SELECT id, card_name, card_id_plaid, billing_start_day, billing_end_day, cycle_type, last_four_digits, bank_provider, created_at, updated_at
		 FROM cards OFFSET $1 LIMIT $2`, skip, limit)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query cards", "error": err.Error()})
		return
	}
	defer rows.Close()

	cards := []cardResponse{}
	for rows.Next() {
		var card cardResponse
		if err := rows.Scan(&card.ID, &card.CardName, &card.CardIDPlaid, &card.BillingStartDay, &card.BillingEndDay,
			&card.CycleType, &card.LastFourDigits, &card.BankProvider, &card.CreatedAt, &card.UpdatedAt); err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan card", "error": err.Error()})
			return
		}
		cards = append(cards, card)
	}

	c.JSON(http.StatusOK, cards)
}

// PATCH /cards/:card_id
func (h *CardHandler) UpdateCard(c *gin.Context) {
	cardID, err := strconv.Atoi(c.Param("card_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid card_id"})
		return
	}

	var exists bool
	h.DB.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM cards WHERE id = $1)", cardID).Scan(&exists)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
		return
	}

	var req cardUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	addClause := func(col string, val interface{}) {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if req.CardName != nil {
		addClause("card_name", *req.CardName)
	}
	if req.CardIDPlaid != nil {
		// Check uniqueness
		var dupExists bool
		h.DB.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM cards WHERE card_id_plaid = $1 AND id != $2)", *req.CardIDPlaid, cardID).Scan(&dupExists)
		if dupExists {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "A card with this card_id_plaid already exists."})
			return
		}
		addClause("card_id_plaid", *req.CardIDPlaid)
	}
	if req.BillingStartDay != nil {
		addClause("billing_start_day", *req.BillingStartDay)
	}
	if req.BillingEndDay != nil {
		addClause("billing_end_day", *req.BillingEndDay)
	}
	if req.CycleType != nil {
		addClause("cycle_type", strings.ToLower(*req.CycleType))
	}
	if req.LastFourDigits != nil {
		addClause("last_four_digits", *req.LastFourDigits)
	}
	if req.BankProvider != nil {
		addClause("bank_provider", strings.ToLower(*req.BankProvider))
	}

	if len(setClauses) > 0 {
		addClause("updated_at", time.Now().UTC())
		args = append(args, cardID)
		query := fmt.Sprintf("UPDATE cards SET %s WHERE id = $%d", strings.Join(setClauses, ", "), argIdx)
		_, err = h.DB.Exec(context.Background(), query, args...)
		if err != nil {
			if strings.Contains(err.Error(), "card_id_plaid") {
				c.JSON(http.StatusBadRequest, gin.H{"detail": "A card with this card_id_plaid already exists."})
				return
			}
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "An unexpected error occurred while updating the card.", "error": err.Error()})
			return
		}
	}

	h.getCardByID(c, cardID)
}

// DELETE /cards/:card_id
func (h *CardHandler) DeleteCard(c *gin.Context) {
	cardID, err := strconv.Atoi(c.Param("card_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid card_id"})
		return
	}

	tag, err := h.DB.Exec(context.Background(), "DELETE FROM cards WHERE id = $1", cardID)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete card", "error": err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Card deleted successfully!"})
}

// GET /cards/custom-cycle
func (h *CardHandler) GetCustomCycleCards(c *gin.Context) {
	rows, err := h.DB.Query(context.Background(),
		`SELECT id, card_name, card_id_plaid, billing_start_day, billing_end_day, cycle_type, last_four_digits, bank_provider, created_at, updated_at
		 FROM cards WHERE cycle_type = 'custom'`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query cards", "error": err.Error()})
		return
	}
	defer rows.Close()

	cards := []cardResponse{}
	for rows.Next() {
		var card cardResponse
		if err := rows.Scan(&card.ID, &card.CardName, &card.CardIDPlaid, &card.BillingStartDay, &card.BillingEndDay,
			&card.CycleType, &card.LastFourDigits, &card.BankProvider, &card.CreatedAt, &card.UpdatedAt); err != nil {
			log.Printf("ERROR: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to scan card", "error": err.Error()})
			return
		}
		cards = append(cards, card)
	}

	c.JSON(http.StatusOK, cards)
}

func (h *CardHandler) getCardByID(c *gin.Context, id int) {
	var card cardResponse
	err := h.DB.QueryRow(context.Background(),
		`SELECT id, card_name, card_id_plaid, billing_start_day, billing_end_day, cycle_type, last_four_digits, bank_provider, created_at, updated_at
		 FROM cards WHERE id = $1`, id,
	).Scan(&card.ID, &card.CardName, &card.CardIDPlaid, &card.BillingStartDay, &card.BillingEndDay,
		&card.CycleType, &card.LastFourDigits, &card.BankProvider, &card.CreatedAt, &card.UpdatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
		return
	}

	c.JSON(http.StatusOK, card)
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
