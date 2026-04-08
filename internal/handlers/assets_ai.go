package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type AssetAIHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

// ── POST /xero/assets/ai-classify ──────────────────────────────

type classifyRequest struct {
	AssetIDs []int `json:"asset_ids"`
	All      bool  `json:"all"`
}

type aiAssetResult struct {
	ID                   int     `json:"id"`
	Category             string  `json:"category"`
	UsefulLifeYears      int     `json:"useful_life_years"`
	DepreciationMethod   string  `json:"depreciation_method"`
	Confidence           float64 `json:"confidence"`
	Reasoning            string  `json:"reasoning"`
}

func (h *AssetAIHandler) ClassifyAssets(c *gin.Context) {
	var req classifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid request body"})
		return
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	keyLen := len(apiKey)
	prefix := apiKey
	if keyLen > 10 {
		prefix = apiKey[:10]
	}
	log.Printf("AI Classify: API key length=%d, prefix=%s", keyLen, prefix)

	if apiKey == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "ANTHROPIC_API_KEY not configured"})
		return
	}

	// Fetch assets to classify
	query := h.GormDB.Model(&models.XeroAsset{})
	if !req.All {
		if len(req.AssetIDs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "provide asset_ids or set all:true"})
			return
		}
		query = query.Where("id IN ?", req.AssetIDs)
	}

	var assets []models.XeroAsset
	if err := query.Find(&assets).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to fetch assets"})
		return
	}

	if len(assets) == 0 {
		c.JSON(http.StatusOK, gin.H{"classified": 0, "results": []interface{}{}})
		return
	}

	log.Printf("AI Classify: processing %d assets in batches of 20", len(assets))

	var allResults []aiAssetResult
	// Process in batches of 20
	for i := 0; i < len(assets); i += 20 {
		end := i + 20
		if end > len(assets) {
			end = len(assets)
		}
		batch := assets[i:end]

		results, err := h.classifyBatch(batch, apiKey)
		if err != nil {
			log.Printf("AI Classify batch error (assets %d-%d): %v", i, end-1, err)
			continue
		}
		allResults = append(allResults, results...)
	}

	// Save results to DB
	classified := 0
	for _, r := range allResults {
		cat := r.Category
		method := r.DepreciationMethod
		confidence := r.Confidence
		reasoning := r.Reasoning
		years := r.UsefulLifeYears

		err := h.GormDB.Model(&models.XeroAsset{}).Where("id = ?", r.ID).Updates(map[string]interface{}{
			"asset_category":               &cat,
			"useful_life_years_override":    &years,
			"depreciation_method_override":  &method,
			"ai_classified":                 true,
			"ai_confidence":                 &confidence,
			"ai_reasoning":                  &reasoning,
		}).Error
		if err != nil {
			log.Printf("AI Classify save error for asset %d: %v", r.ID, err)
			continue
		}
		classified++
	}

	log.Printf("AI Classify: classified %d assets", classified)

	c.JSON(http.StatusOK, gin.H{
		"classified": classified,
		"results":    allResults,
	})
}

func (h *AssetAIHandler) classifyBatch(assets []models.XeroAsset, apiKey string) ([]aiAssetResult, error) {
	// Build asset list for prompt
	type assetInput struct {
		ID            int     `json:"id"`
		Name          string  `json:"name"`
		PurchasePrice float64 `json:"purchase_price"`
	}

	var inputs []assetInput
	for _, a := range assets {
		price := 0.0
		if a.PurchasePrice != nil {
			price = *a.PurchasePrice
		}
		inputs = append(inputs, assetInput{
			ID:            a.ID,
			Name:          a.AssetName,
			PurchasePrice: price,
		})
	}

	inputJSON, _ := json.Marshal(inputs)

	userPrompt := fmt.Sprintf(`Classify these assets for depreciation purposes.
For each asset return: category, useful_life_years, depreciation_method, confidence (0-1), reasoning.

Categories: Equipment, Vehicle, Leasehold Improvement, Technology, Furniture, Other

Useful life guidelines:
- Equipment/Tools: 5-7 years
- Vehicles: 5 years
- Leasehold Improvements: 15 years
- Technology/Computers: 3-5 years
- Furniture/Fixtures: 7 years
- Other: 5 years

Depreciation methods: STRAIGHT_LINE or DIMINISHING

Assets: %s

Return JSON array only, no markdown: [{id, category, useful_life_years, depreciation_method, confidence, reasoning}]`, string(inputJSON))

	// Call Anthropic Messages API
	reqBody := map[string]interface{}{
		"model":      "claude-sonnet-4-20250514",
		"max_tokens": 4096,
		"system":     "You are an asset classification expert for auto repair shops. Classify each asset and return JSON only. No markdown fences.",
		"messages": []map[string]string{
			{"role": "user", "content": userPrompt},
		},
	}

	bodyBytes, _ := json.Marshal(reqBody)

	httpReq, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", apiKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic API call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("anthropic API returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response
	var apiResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse anthropic response: %w", err)
	}

	if len(apiResp.Content) == 0 {
		return nil, fmt.Errorf("empty response from anthropic")
	}

	text := apiResp.Content[0].Text

	// Strip markdown fences if present
	text = stripJSONFences(text)

	var results []aiAssetResult
	if err := json.Unmarshal([]byte(text), &results); err != nil {
		log.Printf("AI Classify: failed to parse AI output: %v\nRaw: %s", err, text)
		return nil, fmt.Errorf("failed to parse AI classification output: %w", err)
	}

	return results, nil
}

// stripJSONFences removes ```json ... ``` wrappers if present
func stripJSONFences(s string) string {
	// Find first [ or {
	start := -1
	for i, c := range s {
		if c == '[' || c == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return s
	}
	// Find last ] or }
	end := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ']' || s[i] == '}' {
			end = i + 1
			break
		}
	}
	if end < 0 {
		return s
	}
	return s[start:end]
}

// ── POST /xero/assets/calculate-depreciation ──────────────────

func (h *AssetAIHandler) CalculateDepreciation(c *gin.Context) {
	var assets []models.XeroAsset
	h.GormDB.Where("useful_life_years_override IS NOT NULL AND purchase_price IS NOT NULL").Find(&assets)

	if len(assets) == 0 {
		c.JSON(http.StatusOK, gin.H{"updated": 0})
		return
	}

	now := time.Now()
	updated := 0

	for _, a := range assets {
		if a.UsefulLifeYearsOverride == nil || a.PurchasePrice == nil {
			continue
		}

		usefulYears := *a.UsefulLifeYearsOverride
		if usefulYears <= 0 {
			continue
		}
		purchasePrice := *a.PurchasePrice

		// Determine purchase date
		purchaseDate := a.PurchaseDate
		if purchaseDate == nil {
			continue
		}

		// Calculate months since purchase
		months := monthsBetween(*purchaseDate, now)

		// Annual depreciation
		annualDep := purchasePrice / float64(usefulYears)

		// Accumulated depreciation = annual * (months/12), capped at purchase_price
		accumDep := annualDep * (float64(months) / 12.0)
		accumDep = math.Min(accumDep, purchasePrice)
		accumDep = math.Round(accumDep*100) / 100

		// Book value
		bookVal := purchasePrice - accumDep
		bookVal = math.Max(bookVal, 0)
		bookVal = math.Round(bookVal*100) / 100

		err := h.GormDB.Model(&models.XeroAsset{}).Where("id = ?", a.ID).Updates(map[string]interface{}{
			"current_accum_depreciation": accumDep,
			"book_value":                 bookVal,
		}).Error
		if err != nil {
			log.Printf("Depreciation calc error for asset %d: %v", a.ID, err)
			continue
		}
		updated++
	}

	log.Printf("Depreciation calculation: updated %d assets", updated)
	c.JSON(http.StatusOK, gin.H{"updated": updated})
}

func monthsBetween(from, to time.Time) float64 {
	years := to.Year() - from.Year()
	months := int(to.Month()) - int(from.Month())
	days := to.Day() - from.Day()
	total := float64(years*12+months) + float64(days)/30.0
	if total < 0 {
		return 0
	}
	return total
}

// ── PATCH /xero/assets/:id ─────────────────────────────────────

type assetPatchRequest struct {
	AssetName                  *string  `json:"asset_name"`
	PurchaseDate               *string  `json:"purchase_date"`
	PurchasePrice              *float64 `json:"purchase_price"`
	Location                   *string  `json:"location"`
	AssetTypeName              *string  `json:"asset_type"`
	Description                *string  `json:"description"`
	Status                     *string  `json:"status"`
	ResidualValue              *float64 `json:"residual_value"`
	UsefulLifeYearsOverride    *int     `json:"useful_life_years_override"`
	DepreciationMethodOverride *string  `json:"depreciation_method_override"`
	AssetCategory              *string  `json:"asset_category"`
	DepreciationRateOverride   *float64 `json:"depreciation_rate_override"`
}

func (h *AssetAIHandler) PatchAsset(c *gin.Context) {
	id := c.Param("id")

	var req assetPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid request body"})
		return
	}

	updates := map[string]interface{}{}
	if req.AssetName != nil {
		updates["asset_name"] = *req.AssetName
	}
	if req.PurchasePrice != nil {
		updates["purchase_price"] = req.PurchasePrice
	}
	if req.Location != nil {
		updates["location"] = req.Location
	}
	if req.Description != nil {
		updates["description"] = req.Description
	}
	if req.Status != nil {
		updates["status"] = req.Status
	}
	if req.AssetTypeName != nil {
		updates["asset_type_name"] = req.AssetTypeName
	}
	if req.ResidualValue != nil {
		updates["residual_value"] = req.ResidualValue
	}
	if req.UsefulLifeYearsOverride != nil {
		updates["useful_life_years_override"] = *req.UsefulLifeYearsOverride
	}
	if req.DepreciationMethodOverride != nil {
		updates["depreciation_method_override"] = *req.DepreciationMethodOverride
	}
	if req.AssetCategory != nil {
		updates["asset_category"] = *req.AssetCategory
	}
	if req.DepreciationRateOverride != nil {
		updates["depreciation_rate_override"] = *req.DepreciationRateOverride
	}
	if req.PurchaseDate != nil {
		if t, err := time.Parse("2006-01-02", *req.PurchaseDate); err == nil {
			updates["purchase_date"] = &t
		}
	}

	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "no fields to update"})
		return
	}

	result := h.GormDB.Model(&models.XeroAsset{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update asset"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "asset not found"})
		return
	}

	// Return updated asset
	var asset models.XeroAsset
	h.GormDB.First(&asset, id)
	c.JSON(http.StatusOK, asset)
}
