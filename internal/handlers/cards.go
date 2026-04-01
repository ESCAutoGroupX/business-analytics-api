package handlers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type CardHandler struct {
	GormDB *gorm.DB
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

func cardToResponse(card *models.Card) cardResponse {
	cycleType := ""
	if card.CycleType != nil {
		cycleType = *card.CycleType
	}
	lastFour := ""
	if card.LastFourDigits != nil {
		lastFour = *card.LastFourDigits
	}
	bankProvider := ""
	if card.BankProvider != nil {
		bankProvider = *card.BankProvider
	}
	cardIDPlaid := ""
	if card.CardIDPlaid != nil {
		cardIDPlaid = *card.CardIDPlaid
	}
	return cardResponse{
		ID:              card.ID,
		CardName:        card.CardName,
		CardIDPlaid:     cardIDPlaid,
		BillingStartDay: card.BillingStartDay,
		BillingEndDay:   card.BillingEndDay,
		CycleType:       cycleType,
		LastFourDigits:  lastFour,
		BankProvider:    bankProvider,
		CreatedAt:       &card.CreatedAt,
		UpdatedAt:       &card.UpdatedAt,
	}
}

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
	var count int64
	h.GormDB.Model(&models.Card{}).Where("card_id_plaid = ?", req.CardIDPlaid).Count(&count)
	if count > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "This card_plaid_id is already associated with another card"})
		return
	}

	card := models.Card{
		CardName:        req.CardName,
		CardIDPlaid:     &req.CardIDPlaid,
		BillingStartDay: req.BillingStartDay,
		BillingEndDay:   req.BillingEndDay,
		CycleType:       &cycleType,
		LastFourDigits:  &req.LastFourDigits,
		BankProvider:    &bankProvider,
	}

	if err := h.GormDB.Create(&card).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "An unexpected error occurred while creating the card.", "error": err.Error()})
		return
	}

	h.getCardByID(c, card.ID)
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

	var cards []models.Card
	if err := h.GormDB.Offset(skip).Limit(limit).Find(&cards).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query cards", "error": err.Error()})
		return
	}

	result := make([]cardResponse, len(cards))
	for i := range cards {
		result[i] = cardToResponse(&cards[i])
	}

	c.JSON(http.StatusOK, result)
}

// PATCH /cards/:card_id
func (h *CardHandler) UpdateCard(c *gin.Context) {
	cardID, err := strconv.Atoi(c.Param("card_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid card_id"})
		return
	}

	var card models.Card
	if err := h.GormDB.First(&card, "id = ?", cardID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
		return
	}

	var req cardUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	updates := map[string]interface{}{}

	if req.CardName != nil {
		updates["card_name"] = *req.CardName
	}
	if req.CardIDPlaid != nil {
		// Check uniqueness
		var count int64
		h.GormDB.Model(&models.Card{}).Where("card_id_plaid = ? AND id != ?", *req.CardIDPlaid, cardID).Count(&count)
		if count > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "A card with this card_id_plaid already exists."})
			return
		}
		updates["card_id_plaid"] = *req.CardIDPlaid
	}
	if req.BillingStartDay != nil {
		updates["billing_start_day"] = *req.BillingStartDay
	}
	if req.BillingEndDay != nil {
		updates["billing_end_day"] = *req.BillingEndDay
	}
	if req.CycleType != nil {
		updates["cycle_type"] = strings.ToLower(*req.CycleType)
	}
	if req.LastFourDigits != nil {
		updates["last_four_digits"] = *req.LastFourDigits
	}
	if req.BankProvider != nil {
		updates["bank_provider"] = strings.ToLower(*req.BankProvider)
	}

	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := h.GormDB.Model(&card).Updates(updates).Error; err != nil {
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

	result := h.GormDB.Delete(&models.Card{}, "id = ?", cardID)
	if result.Error != nil {
		log.Printf("ERROR: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete card", "error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Card deleted successfully!"})
}

// GET /cards/custom-cycle
func (h *CardHandler) GetCustomCycleCards(c *gin.Context) {
	var cards []models.Card
	if err := h.GormDB.Where("cycle_type = ?", "custom").Find(&cards).Error; err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query cards", "error": err.Error()})
		return
	}

	result := make([]cardResponse, len(cards))
	for i := range cards {
		result[i] = cardToResponse(&cards[i])
	}

	c.JSON(http.StatusOK, result)
}

func (h *CardHandler) getCardByID(c *gin.Context, id int) {
	var card models.Card
	if err := h.GormDB.First(&card, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Card not found"})
			return
		}
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query card", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cardToResponse(&card))
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
