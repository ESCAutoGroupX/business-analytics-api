package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ── PDF Page Splitting ───────────────────────────────────────
// Uses Ghostscript to convert PDF pages to JPEG images, then
// Agent 0 classifies each page by invoice number.

type pageClassification struct {
	InvoiceNumber  string `json:"invoice_number"`
	PageNumber     *int   `json:"page_number"`
	TotalPages     *int   `json:"total_pages"`
	IsContinuation bool   `json:"is_continuation"`
}

// invoiceGroup represents pages that belong to the same invoice.
type invoiceGroup struct {
	InvoiceNumber string
	Pages         []int    // 1-based page numbers
	ImagePaths    []string // JPEG paths for each page
}

// splitPDFPages returns the number of pages in a PDF, or 0 if not a PDF / error.
func countPDFPages(pdfPath string) int {
	// Use Ghostscript to count pages
	out, err := exec.Command("gs", "-q", "-dNODISPLAY", "-dNOSAFER",
		"-c", fmt.Sprintf("(%s) (r) file runpdfbegin pdfpagecount = quit", pdfPath)).Output()
	if err != nil {
		log.Printf("splitter: gs page count failed: %v", err)
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return n
}

// pdfToPageImages converts a PDF into per-page JPEG images using Ghostscript.
// Returns a slice of file paths (1-indexed: page 1 → index 0).
func pdfToPageImages(pdfPath, outDir string, pageCount int) ([]string, error) {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}

	// Try batch conversion first
	outPattern := filepath.Join(outDir, "page_%d.jpg")
	cmd := exec.Command("gs",
		"-dBATCH", "-dNOPAUSE", "-dNOSAFER",
		"-sDEVICE=jpeg", "-r150", "-dJPEGQ=85",
		fmt.Sprintf("-sOutputFile=%s", outPattern),
		pdfPath,
	)
	log.Printf("splitter: running gs batch: %s", cmd.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("splitter: gs batch failed: %v\nOutput: %s", err, string(out))
		// Fallback: convert one page at a time
		log.Printf("splitter: trying per-page gs fallback for %d pages", pageCount)
		return pdfToPageImagesFallback(pdfPath, outDir, pageCount)
	}

	// Collect generated files
	var paths []string
	for i := 1; ; i++ {
		p := filepath.Join(outDir, fmt.Sprintf("page_%d.jpg", i))
		if _, err := os.Stat(p); os.IsNotExist(err) {
			break
		}
		paths = append(paths, p)
	}
	if len(paths) == 0 {
		log.Printf("splitter: gs batch produced 0 files, trying per-page fallback")
		return pdfToPageImagesFallback(pdfPath, outDir, pageCount)
	}
	log.Printf("splitter: gs batch produced %d page images", len(paths))
	return paths, nil
}

// pdfToPageImagesFallback converts each page individually using -dFirstPage/-dLastPage.
func pdfToPageImagesFallback(pdfPath, outDir string, pageCount int) ([]string, error) {
	var paths []string
	for i := 1; i <= pageCount; i++ {
		outPath := filepath.Join(outDir, fmt.Sprintf("page_%d.jpg", i))
		cmd := exec.Command("gs",
			"-dBATCH", "-dNOPAUSE", "-dNOSAFER",
			"-sDEVICE=jpeg", "-r150", "-dJPEGQ=85",
			fmt.Sprintf("-dFirstPage=%d", i),
			fmt.Sprintf("-dLastPage=%d", i),
			fmt.Sprintf("-sOutputFile=%s", outPath),
			pdfPath,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("splitter: gs page %d failed: %v — %s", i, err, string(out))
			continue
		}
		if _, err := os.Stat(outPath); err == nil {
			paths = append(paths, outPath)
		}
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("gs per-page fallback produced no output")
	}
	log.Printf("splitter: per-page fallback produced %d/%d page images", len(paths), pageCount)
	return paths, nil
}

// extractPDFPageRange uses Ghostscript to extract specific pages from a PDF.
func extractPDFPageRange(inputPDF, outputPDF string, pages []int) error {
	// Build page range string like "1,2,5"
	pageStrs := make([]string, len(pages))
	for i, p := range pages {
		pageStrs[i] = strconv.Itoa(p)
	}

	// Use Ghostscript -sPageList to extract specific pages
	cmd := exec.Command("gs",
		"-dBATCH", "-dNOPAUSE", "-dNOSAFER",
		"-sDEVICE=pdfwrite",
		fmt.Sprintf("-sPageList=%s", strings.Join(pageStrs, ",")),
		fmt.Sprintf("-sOutputFile=%s", outputPDF),
		inputPDF,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gs extract pages failed: %v: %s", err, string(out))
	}
	return nil
}

// classifyPages runs Agent 0 on each page image to determine which invoice it belongs to.
func (h *DocumentHandler) classifyPages(apiKey string, pageImages []string) ([]pageClassification, error) {
	results := make([]pageClassification, len(pageImages))

	for i, imgPath := range pageImages {
		imgBytes, err := os.ReadFile(imgPath)
		if err != nil {
			log.Printf("splitter: cannot read page image %s: %v", imgPath, err)
			results[i] = pageClassification{InvoiceNumber: "unknown"}
			continue
		}

		b64 := base64.StdEncoding.EncodeToString(imgBytes)
		text, err := h.callClaudeWithImage(apiKey, b64, "image/jpeg", "image",
			"You are analyzing a page from an automotive invoice PDF. Return JSON only.",
			`What invoice number and page number does this page belong to?
Return JSON:
{
  "invoice_number": "the invoice number visible on this page, or empty string if not visible",
  "page_number": null,
  "total_pages": null,
  "is_continuation": false
}
If this page shows a continuation of a previous invoice (no new header, just more line items), set is_continuation to true.
If no invoice number is visible, return invoice_number as empty string.
Return JSON only.`)
		if err != nil {
			log.Printf("splitter: agent0 failed for page %d: %v", i+1, err)
			results[i] = pageClassification{InvoiceNumber: "unknown"}
			continue
		}

		var pc pageClassification
		if err := json.Unmarshal([]byte(text), &pc); err != nil {
			log.Printf("splitter: agent0 parse failed for page %d: %v", i+1, err)
			results[i] = pageClassification{InvoiceNumber: "unknown"}
			continue
		}
		results[i] = pc
		log.Printf("splitter: page %d → invoice=%q continuation=%v", i+1, pc.InvoiceNumber, pc.IsContinuation)
	}

	return results, nil
}

// groupPagesByInvoice groups page classifications into invoice groups.
// Continuation pages are assigned to the most recent non-continuation invoice.
func groupPagesByInvoice(classifications []pageClassification) []invoiceGroup {
	if len(classifications) == 0 {
		return nil
	}

	// Assign each page to an invoice number.
	// Continuation pages or pages with empty/unknown invoice inherit from previous.
	assigned := make([]string, len(classifications))
	lastInvoice := ""
	for i, c := range classifications {
		inv := strings.TrimSpace(c.InvoiceNumber)
		if inv == "" || inv == "unknown" || c.IsContinuation {
			// Inherit from previous
			assigned[i] = lastInvoice
		} else {
			assigned[i] = inv
			lastInvoice = inv
		}
	}

	// If first pages had no invoice, assign them to the first known one
	if assigned[0] == "" {
		firstKnown := ""
		for _, a := range assigned {
			if a != "" {
				firstKnown = a
				break
			}
		}
		if firstKnown == "" {
			firstKnown = "unknown"
		}
		for i := range assigned {
			if assigned[i] == "" {
				assigned[i] = firstKnown
			} else {
				break
			}
		}
	}

	// Group by invoice number, preserving page order
	groupMap := map[string]*invoiceGroup{}
	var order []string
	for i, inv := range assigned {
		if _, ok := groupMap[inv]; !ok {
			groupMap[inv] = &invoiceGroup{InvoiceNumber: inv}
			order = append(order, inv)
		}
		groupMap[inv].Pages = append(groupMap[inv].Pages, i+1) // 1-based
	}

	var groups []invoiceGroup
	for _, inv := range order {
		groups = append(groups, *groupMap[inv])
	}

	return groups
}

// detectMultiInvoicePDF checks if a PDF has multiple invoices and returns groups.
// Returns nil if single invoice or not a PDF.
func (h *DocumentHandler) detectMultiInvoicePDF(apiKey, pdfPath, ext string) ([]invoiceGroup, string, error) {
	if ext != ".pdf" {
		return nil, "", nil
	}

	pageCount := countPDFPages(pdfPath)
	if pageCount <= 1 {
		return nil, "", nil // Single page, no splitting needed
	}

	log.Printf("splitter: PDF has %d pages, classifying...", pageCount)

	// Convert to page images
	tmpDir, err := os.MkdirTemp("", "docsplit-*")
	if err != nil {
		return nil, tmpDir, fmt.Errorf("create temp dir: %w", err)
	}

	pageImages, err := pdfToPageImages(pdfPath, tmpDir, pageCount)
	if err != nil {
		return nil, tmpDir, fmt.Errorf("pdf to images: %w", err)
	}

	// Classify each page
	classifications, err := h.classifyPages(apiKey, pageImages)
	if err != nil {
		return nil, tmpDir, fmt.Errorf("classify pages: %w", err)
	}

	// Group pages
	groups := groupPagesByInvoice(classifications)

	// Attach image paths
	for i := range groups {
		for _, pageNum := range groups[i].Pages {
			if pageNum-1 < len(pageImages) {
				groups[i].ImagePaths = append(groups[i].ImagePaths, pageImages[pageNum-1])
			}
		}
	}

	// If all pages belong to one invoice, no splitting needed
	if len(groups) <= 1 {
		log.Printf("splitter: all %d pages belong to one invoice", pageCount)
		return nil, tmpDir, nil
	}

	log.Printf("splitter: found %d distinct invoices across %d pages", len(groups), pageCount)
	for _, g := range groups {
		log.Printf("splitter: invoice %q → pages %v", g.InvoiceNumber, g.Pages)
	}

	// Sort pages within each group
	for i := range groups {
		sort.Ints(groups[i].Pages)
	}

	return groups, tmpDir, nil
}
