package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// ──────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────

// InvoicePartLine represents a line item from an invoice.
// RONumber and LineItemID are populated when the struct is reused for RO parts.
type InvoicePartLine struct {
	Index       int     `json:"index"`
	PartNumber  string  `json:"part_number"`
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	Total       float64 `json:"total"`
	RONumber    string  `json:"ro_number,omitempty"`
	LineItemID  string  `json:"line_item_id,omitempty"`
}

// ROPartLine represents a line item from a repair order.
type ROPartLine struct {
	RONumber    string
	LineItemID  string
	PartNumber  string
	Description string
	Quantity    int
	UnitPrice   float64
	Total       float64
}

// MatchResult holds the outcome of matching one invoice line item.
type MatchResult struct {
	LineItemIndex       int     `json:"line_item_index"`
	InvoicePart         string  `json:"invoice_part"`
	InvoiceDescription  string  `json:"invoice_description"`
	InvoiceAmount       float64 `json:"invoice_amount"`
	MatchedRONumber     string  `json:"matched_ro_number"`
	MatchedROLineItemID string  `json:"matched_ro_line_item_id"`
	MatchedPartNumber   string  `json:"matched_part_number"`
	MatchedDescription  string  `json:"matched_description"`
	Score               int     `json:"score"`
	Rule                string  `json:"rule"`
	Confidence          float64 `json:"confidence"`
	AITiebreakerUsed    bool    `json:"ai_tiebreaker_used"`
	AIReasoning         string  `json:"ai_reasoning,omitempty"`
}

// ──────────────────────────────────────────────
// Part Number Normalization
// ──────────────────────────────────────────────

var vendorPrefixes = []string{
	"ORIGINAL", "GENUINE", "REMAN", "REPL",
	"OEM", "OE", "GEN", "NEW", "ORIG", "ALT", "SC", "AF",
}

var nonAlphaNum = regexp.MustCompile(`[^A-Z0-9]`)

// ocrSubstitutions maps characters commonly mis-read by OCR to their numeric
// equivalents.
var ocrSubstitutions = map[byte]byte{
	'O': '0',
	'I': '1',
	'L': '1',
	'S': '5',
	'E': '3',
}

// NormalizePartNumber applies a four-phase pipeline to produce a canonical
// representation of a part number suitable for comparison.
//
// Phase 1: Uppercase + TrimSpace
// Phase 2: Strip known vendor prefixes
// Phase 3: Remove all non-alphanumeric characters
// Phase 4: OCR character substitution
func NormalizePartNumber(partNumber string, _ ...string) string {
	// Phase 1: uppercase + trim
	s := strings.TrimSpace(strings.ToUpper(partNumber))

	// Phase 2: strip vendor prefixes
	for _, prefix := range vendorPrefixes {
		if strings.HasPrefix(s, prefix) {
			s = strings.TrimSpace(s[len(prefix):])
			break
		}
	}

	// Phase 3: remove all non-alphanumeric characters
	s = nonAlphaNum.ReplaceAllString(s, "")

	// Phase 4: OCR substitution
	buf := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if replacement, ok := ocrSubstitutions[s[i]]; ok {
			buf[i] = replacement
		} else {
			buf[i] = s[i]
		}
	}
	return string(buf)
}

// ──────────────────────────────────────────────
// String Similarity Functions
// ──────────────────────────────────────────────

// levenshteinDistance computes the edit distance between two strings using
// standard dynamic programming.
func levenshteinDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	prev := make([]int, lb+1)
	curr := make([]int, lb+1)

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			ins := curr[j-1] + 1
			del := prev[j] + 1
			sub := prev[j-1] + cost

			m := ins
			if del < m {
				m = del
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

// StringSimilarity returns a value between 0 and 1 representing how similar
// two strings are, based on Levenshtein distance.  Returns 0 when both
// strings are empty.
func StringSimilarity(a, b string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	dist := levenshteinDistance(a, b)
	return 1.0 - float64(dist)/float64(maxLen)
}

// BidirectionalSimilarity returns the maximum similarity in both directions.
// Levenshtein is symmetric so the result is the same, but this keeps the API
// contract explicit.
func BidirectionalSimilarity(a, b string) float64 {
	s1 := StringSimilarity(a, b)
	s2 := StringSimilarity(b, a)
	if s2 > s1 {
		return s2
	}
	return s1
}

// ContainsMatch checks whether the normalized form of a contains b or vice
// versa, with a minimum length guard.  If minLen is 0 it defaults to 4.
func ContainsMatch(a, b string, minLen int) bool {
	if minLen == 0 {
		minLen = 4
	}
	na := NormalizePartNumber(a)
	nb := NormalizePartNumber(b)
	if len(na) < minLen || len(nb) < minLen {
		return false
	}
	return strings.Contains(na, nb) || strings.Contains(nb, na)
}

// CrossFieldSimilarity strips spaces from both inputs, uppercases them, and
// computes StringSimilarity.
func CrossFieldSimilarity(desc, code string) float64 {
	a := strings.ToUpper(strings.ReplaceAll(desc, " ", ""))
	b := strings.ToUpper(strings.ReplaceAll(code, " ", ""))
	return StringSimilarity(a, b)
}

// ──────────────────────────────────────────────
// Match Rules
// ──────────────────────────────────────────────

// MatchRule defines a single scoring rule applied during part matching.
type MatchRule struct {
	Name  string
	Score int
	Check func(inv, ro InvoicePartLine) bool
}

// amountEqual returns true when two amounts are within $0.01 of each other.
func amountEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.01
}

// amountWithinPct returns true when two amounts are within the given
// percentage of each other.
func amountWithinPct(a, b, pct float64) bool {
	denom := math.Max(math.Abs(a), 0.01)
	return math.Abs(a-b)/denom <= pct/100.0
}

// qtyEq returns true when two quantities are effectively equal.
func qtyEq(a, b float64) bool {
	return math.Abs(a-b) < 0.01
}

// norm is a convenience wrapper for NormalizePartNumber.
func norm(s string) string {
	return NormalizePartNumber(s)
}

var matchRules = []MatchRule{
	// 1. matchPartPerfect (100): exact normalized codes + exact amount + exact qty
	{Name: "matchPartPerfect", Score: 100, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return ni == nr && amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 2. matchPartPerfectContains (99): contains match + exact amount + exact qty
	{Name: "matchPartPerfectContains", Score: 99, Check: func(inv, ro InvoicePartLine) bool {
		return ContainsMatch(inv.PartNumber, ro.PartNumber, 4) &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 3. matchPart100 (95): similarity >= 1.0 + exact amount + exact qty
	{Name: "matchPart100", Score: 95, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 1.0 &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 4. matchPart90 (90): similarity >= 0.90 + exact amount + exact qty
	{Name: "matchPart90", Score: 90, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.90 &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 5. matchPart80 (85): similarity >= 0.80 + exact amount + exact qty
	{Name: "matchPart80", Score: 85, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.80 &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 6. matchPart70 (80): similarity >= 0.70 + exact amount + exact qty
	{Name: "matchPart70", Score: 80, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.70 &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 7. matchPart75NoLength (75): similarity >= 0.75, no amount/qty check
	{Name: "matchPart75NoLength", Score: 75, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.75
	}},
	// 8. matchPartDescriptionAmountQty (70): description similarity >= 0.95 + exact amount + exact qty
	{Name: "matchPartDescriptionAmountQty", Score: 70, Check: func(inv, ro InvoicePartLine) bool {
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.95 &&
			amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 9. matchPartAmountQuantity (65): exact amount + exact qty
	{Name: "matchPartAmountQuantity", Score: 65, Check: func(inv, ro InvoicePartLine) bool {
		return amountEqual(inv.Total, ro.Total) && qtyEq(inv.Quantity, ro.Quantity)
	}},
	// 10. matchPartAmount (63): exact amount only
	{Name: "matchPartAmount", Score: 63, Check: func(inv, ro InvoicePartLine) bool {
		return amountEqual(inv.Total, ro.Total)
	}},
	// 11. matchPartProductCodeExact (60): normalized codes exact match
	{Name: "matchPartProductCodeExact", Score: 60, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return ni == nr
	}},
	// 12. matchPartProductCode (55): similarity >= 0.85 on normalized codes
	{Name: "matchPartProductCode", Score: 55, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.85
	}},
	// 13. matchPartDescription (50): description similarity >= 0.90
	{Name: "matchPartDescription", Score: 50, Check: func(inv, ro InvoicePartLine) bool {
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.90
	}},
	// 14. matchSimilarDescAndAmount (40): description similarity >= 0.70 + amount within 10%
	{Name: "matchSimilarDescAndAmount", Score: 40, Check: func(inv, ro InvoicePartLine) bool {
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.70 &&
			amountWithinPct(inv.Total, ro.Total, 10)
	}},
	// 15. matchSimilarDescPartAndAmount (35): desc sim >= 0.60 + code sim >= 0.60 + amount within 15%
	{Name: "matchSimilarDescPartAndAmount", Score: 35, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.60 &&
			StringSimilarity(ni, nr) >= 0.60 &&
			amountWithinPct(inv.Total, ro.Total, 15)
	}},
	// 16. matchPortionOfProductCode (30): contains match on normalized codes (min 4 chars)
	{Name: "matchPortionOfProductCode", Score: 30, Check: func(inv, ro InvoicePartLine) bool {
		return ContainsMatch(inv.PartNumber, ro.PartNumber, 4)
	}},
	// 17. matchSimilarDescCodeAndAmount (25): desc sim >= 0.50 + code sim >= 0.50 + amount within 20%
	{Name: "matchSimilarDescCodeAndAmount", Score: 25, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.50 &&
			StringSimilarity(ni, nr) >= 0.50 &&
			amountWithinPct(inv.Total, ro.Total, 20)
	}},
	// 18. matchSimilarDescProductCode (20): desc sim >= 0.60 + code sim >= 0.50
	{Name: "matchSimilarDescProductCode", Score: 20, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.60 &&
			StringSimilarity(ni, nr) >= 0.50
	}},
	// 19. matchSimilarProductCode (15): code similarity >= 0.60
	{Name: "matchSimilarProductCode", Score: 15, Check: func(inv, ro InvoicePartLine) bool {
		ni, nr := norm(inv.PartNumber), norm(ro.PartNumber)
		return StringSimilarity(ni, nr) >= 0.60
	}},
	// 20. matchSimilarPartDescription (10): description similarity >= 0.50
	{Name: "matchSimilarPartDescription", Score: 10, Check: func(inv, ro InvoicePartLine) bool {
		return StringSimilarity(strings.ToUpper(inv.Description), strings.ToUpper(ro.Description)) >= 0.50
	}},
}

// ──────────────────────────────────────────────
// ComputeMatchScore
// ──────────────────────────────────────────────

// ComputeMatchScore iterates through the ordered rule set and returns the
// score and name of the first matching rule.  Returns (0, "") when no rule
// matches.
func ComputeMatchScore(invoicePart, roPart InvoicePartLine) (int, string) {
	for _, rule := range matchRules {
		if rule.Check(invoicePart, roPart) {
			return rule.Score, rule.Name
		}
	}
	return 0, ""
}

// ──────────────────────────────────────────────
// AI Tiebreaker
// ──────────────────────────────────────────────

// anthropicRequest is the request body sent to the Anthropic Messages API.
type anthropicRequest struct {
	Model    string             `json:"model"`
	MaxToks  int                `json:"max_tokens"`
	Messages []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// anthropicResponse mirrors the relevant subset of the Anthropic Messages API
// response.
type anthropicResponse struct {
	Content []struct {
		Text string `json:"text"`
	} `json:"content"`
}

// aiTiebreakerResult is the JSON structure we expect Claude to return inside
// its text response.
type aiTiebreakerResult struct {
	IsMatch    bool    `json:"is_match"`
	Confidence float64 `json:"confidence"`
	Reasoning  string  `json:"reasoning"`
}

// AITiebreaker uses Claude to decide whether two part lines refer to the same
// physical part when rule-based scoring is inconclusive (score 40-74).
// Returns (false, "", nil) for scores < 40 and (true, "", nil) for scores >= 75.
func AITiebreaker(ctx context.Context, cfg *config.Config, inv InvoicePartLine, ro InvoicePartLine, score int) (bool, string, error) {
	if score < 40 {
		return false, "", nil
	}
	if score >= 75 {
		return true, "", nil
	}

	prompt := fmt.Sprintf(`You are an automotive parts matching expert. Determine if these two part references are the same physical part.

Invoice part:
- Part number: %s
- Description: %s
- Quantity: %.0f
- Total: $%.2f

Repair order part:
- Part number: %s
- Description: %s
- Quantity: %.0f
- Total: $%.2f

Consider:
- Vendor prefixes (OEM, GENUINE, REMAN, etc.) that may differ
- OCR errors (O/0, I/1, L/1, S/5 confusion)
- Abbreviations and shorthand in descriptions
- Brand-equivalent parts from different manufacturers

Respond ONLY with JSON:
{"is_match": true/false, "confidence": 0.0-1.0, "reasoning": "brief explanation"}`,
		inv.PartNumber, inv.Description, inv.Quantity, inv.Total,
		ro.PartNumber, ro.Description, ro.Quantity, ro.Total,
	)

	reqBody := anthropicRequest{
		Model:   "claude-sonnet-4-20250514",
		MaxToks: 300,
		Messages: []anthropicMessage{
			{Role: "user", Content: prompt},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return false, "", fmt.Errorf("marshal anthropic request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	if err != nil {
		return false, "", fmt.Errorf("create anthropic request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", cfg.AnthropicAPIKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return false, "", fmt.Errorf("anthropic API call: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "", fmt.Errorf("read anthropic response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return false, "", fmt.Errorf("anthropic API returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var apiResp anthropicResponse
	if err := json.Unmarshal(respBytes, &apiResp); err != nil {
		return false, "", fmt.Errorf("unmarshal anthropic response: %w", err)
	}

	if len(apiResp.Content) == 0 {
		return false, "", fmt.Errorf("anthropic response contained no content blocks")
	}

	var result aiTiebreakerResult
	if err := json.Unmarshal([]byte(apiResp.Content[0].Text), &result); err != nil {
		return false, "", fmt.Errorf("parse AI tiebreaker JSON: %w", err)
	}

	return result.IsMatch, result.Reasoning, nil
}

// ──────────────────────────────────────────────
// Credit Matching
// ──────────────────────────────────────────────

// MatchCreditToReceivable finds the best receivable match for a vendor credit
// using a weighted scoring system.  Returns nil when no receivable reaches the
// minimum threshold of 195 points.
func MatchCreditToReceivable(credit models.VendorCredit, receivables []models.VendorReceivable) (*models.VendorReceivable, int) {
	const minScore = 195

	var bestMatch *models.VendorReceivable
	bestScore := 0

	for i := range receivables {
		r := &receivables[i]
		score := 0

		// Invoice number exact match: 250 pts
		if credit.ReferenceInvoiceNumber != nil && r.InvoiceNumber != nil {
			if *credit.ReferenceInvoiceNumber == *r.InvoiceNumber {
				score += 250
			} else {
				sim := StringSimilarity(
					strings.ToUpper(*credit.ReferenceInvoiceNumber),
					strings.ToUpper(*r.InvoiceNumber),
				)
				if sim >= 0.7 {
					score += int(150.0 * sim)
				}
			}
		}

		// PO number exact match: 100 pts — skip when either side is nil
		if credit.ReferencePONumber != nil && r.RONumber != nil {
			if *credit.ReferencePONumber == *r.RONumber {
				score += 100
			}
		}

		// Vendor name exact match: 100 pts
		if strings.EqualFold(credit.VendorName, r.VendorName) {
			score += 100
		}

		// Type bump for CORE_RETURN: +10 pts
		if r.ReceivableType == "CORE_RETURN" {
			score += 10
		}

		if score > bestScore {
			bestScore = score
			bestMatch = r
		}
	}

	if bestScore < minScore {
		return nil, bestScore
	}
	return bestMatch, bestScore
}

// ──────────────────────────────────────────────
// MatchInvoiceToROs
// ──────────────────────────────────────────────

// MatchInvoiceToROs matches each invoice part line against candidate RO part
// lines, optionally calling the AI tiebreaker for medium-confidence matches,
// and persists the results to the database.
//
// Both invoiceParts and roParts use InvoicePartLine so callers that flatten
// RO line items into that type (setting RONumber / LineItemID) can pass them
// directly.
func MatchInvoiceToROs(
	ctx context.Context,
	cfg *config.Config,
	db *gorm.DB,
	invoiceParts []InvoicePartLine,
	roParts []InvoicePartLine,
	documentID int,
	vendorName string,
) ([]MatchResult, error) {
	results := make([]MatchResult, 0, len(invoiceParts))

	for idx, inv := range invoiceParts {
		var mr MatchResult
		mr.LineItemIndex = idx
		mr.InvoicePart = inv.PartNumber
		mr.InvoiceDescription = inv.Description
		mr.InvoiceAmount = inv.Total

		// Step 1: check vendor_part_mappings for auto-match
		normalizedInv := NormalizePartNumber(inv.PartNumber)
		var mapping models.VendorPartMapping
		autoMatchFound := false

		findErr := db.WithContext(ctx).
			Where("vendor_name = ? AND vendor_part_normalized = ? AND auto_match_enabled = ?",
				vendorName, normalizedInv, true).
			First(&mapping).Error
		if findErr == nil {
			autoMatchFound = true
		}

		if autoMatchFound {
			// Step 2: auto-match result
			mr.Score = 100
			mr.Rule = "autoMatch"
			mr.Confidence = 1.0
			if mapping.InternalPartNumber != nil {
				mr.MatchedPartNumber = *mapping.InternalPartNumber
			}
			if mapping.Description != nil {
				mr.MatchedDescription = *mapping.Description
			}
		} else {
			// Step 3: rule-based scoring against all RO parts
			bestScore := 0
			bestRule := ""
			bestIdx := -1

			for j := range roParts {
				score, rule := ComputeMatchScore(inv, roParts[j])
				if score > bestScore {
					bestScore = score
					bestRule = rule
					bestIdx = j
				}
			}

			if bestIdx >= 0 {
				best := roParts[bestIdx]
				mr.MatchedRONumber = best.RONumber
				mr.MatchedROLineItemID = best.LineItemID
				mr.MatchedPartNumber = best.PartNumber
				mr.MatchedDescription = best.Description
				mr.Score = bestScore
				mr.Rule = bestRule

				// Step 4: AI tiebreaker for medium-confidence
				if bestScore >= 40 && bestScore <= 74 {
					isMatch, reasoning, aiErr := AITiebreaker(ctx, cfg, inv, best, bestScore)
					if aiErr == nil {
						mr.AITiebreakerUsed = true
						mr.AIReasoning = reasoning
						if isMatch {
							mr.Confidence = 0.85
						} else {
							mr.Confidence = 0.30
						}
					}
					// On AI error we fall through with the rule-based score.
				}
			}

			// Set confidence from score when AI was not used.
			if !mr.AITiebreakerUsed {
				if mr.Score >= 90 {
					mr.Confidence = 1.0
				} else if mr.Score >= 75 {
					mr.Confidence = 0.9
				} else if mr.Score >= 50 {
					mr.Confidence = 0.6
				} else if mr.Score > 0 {
					mr.Confidence = 0.3
				}
			}
		}

		// Step 5: determine status and persist PartMatchResult
		status := "unmatched"
		if mr.Score >= 100 {
			status = "confirmed"
		} else if mr.Score >= 75 {
			status = "pending"
		} else if mr.Score > 0 {
			status = "low_confidence"
		}

		docID := documentID
		lineIdx := idx
		vendorPartNorm := normalizedInv
		record := models.PartMatchResult{
			DocumentID:           &docID,
			LineItemIndex:        &lineIdx,
			VendorPartNumber:     strPtr(inv.PartNumber),
			VendorPartNormalized: strPtr(vendorPartNorm),
			MatchScore:           intPtr(mr.Score),
			MatchRule:            strPtr(mr.Rule),
			MatchConfidence:      float64Ptr(mr.Confidence),
			Status:               status,
			AITiebreakerUsed:     mr.AITiebreakerUsed,
			AIReasoning:          strPtr(mr.AIReasoning),
		}

		if mr.MatchedRONumber != "" {
			record.MatchedRONumber = strPtr(mr.MatchedRONumber)
		}
		if mr.MatchedROLineItemID != "" {
			record.MatchedROLineItemID = strPtr(mr.MatchedROLineItemID)
		}
		if mr.MatchedPartNumber != "" {
			record.MatchedPartNumber = strPtr(mr.MatchedPartNumber)
		}

		if err := db.WithContext(ctx).Create(&record).Error; err != nil {
			return nil, fmt.Errorf("save part match result: %w", err)
		}

		results = append(results, mr)
	}

	return results, nil
}

// ──────────────────────────────────────────────
// MatchInvoiceToRO (document-based orchestrator)
// ──────────────────────────────────────────────

// MatchInvoiceToRO is a higher-level orchestrator that loads the document,
// fetches ROs from the SMS API, and delegates to MatchInvoiceToROs.
func MatchInvoiceToRO(ctx context.Context, db *gorm.DB, cfg *config.Config,
	invoiceDocID int, shopID int, dateFrom, dateTo time.Time) ([]MatchResult, error) {

	// 1. Get invoice document and its line items
	var doc models.Document
	if err := db.First(&doc, invoiceDocID).Error; err != nil {
		return nil, fmt.Errorf("document not found: %w", err)
	}

	var invoiceItems []InvoicePartLine
	if doc.LineItems != nil {
		var rawItems []struct {
			PartNumber  string  `json:"part_number"`
			Description string  `json:"description"`
			Quantity    float64 `json:"quantity"`
			UnitPrice   float64 `json:"unit_price"`
			Total       float64 `json:"total"`
		}
		if err := json.Unmarshal([]byte(*doc.LineItems), &rawItems); err != nil {
			return nil, fmt.Errorf("failed to parse line items: %w", err)
		}
		for i, ri := range rawItems {
			invoiceItems = append(invoiceItems, InvoicePartLine{
				Index:       i,
				PartNumber:  ri.PartNumber,
				Description: ri.Description,
				Quantity:    ri.Quantity,
				UnitPrice:   ri.UnitPrice,
				Total:       ri.Total,
			})
		}
	}
	if len(invoiceItems) == 0 {
		return nil, fmt.Errorf("no line items found on document %d", invoiceDocID)
	}

	// 2. Get RO parts from SMS API
	smsClient := NewSMSClient(cfg)
	ros, err := smsClient.GetPostedROs(shopID, dateFrom, dateTo)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ROs: %w", err)
	}

	// Flatten RO line items
	var roParts []InvoicePartLine
	for _, ro := range ros {
		for _, li := range ro.LineItems {
			roParts = append(roParts, InvoicePartLine{
				PartNumber:  li.PartNumber,
				Description: li.Description,
				Quantity:    float64(li.Quantity),
				UnitPrice:   li.UnitPrice,
				Total:       li.Total,
				RONumber:    ro.RONumber,
				LineItemID:  li.ID,
			})
		}
	}

	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}

	return MatchInvoiceToROs(ctx, cfg, db, invoiceItems, roParts, invoiceDocID, vendorName)
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func intPtr(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}

func float64Ptr(v float64) *float64 {
	if v == 0 {
		return nil
	}
	return &v
}
