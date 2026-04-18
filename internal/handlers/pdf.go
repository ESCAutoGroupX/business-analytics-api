package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ServePDF serves the original PDF for a wf_document by looking up the file
// on disk under {PDFDir}/{location_name}/*{scan_suffix}.pdf.
func (h *DocumentHandler) ServePDF(c *gin.Context) {
	docID := c.Param("id")

	var scanID, locationName sql.NullString
	db, _ := h.GormDB.DB()
	err := db.QueryRow(`SELECT wf_scan_id, location_name FROM wf_documents WHERE id = $1`, docID).
		Scan(&scanID, &locationName)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}
	if !scanID.Valid || scanID.String == "" {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document has no scan id"})
		return
	}

	loc := ""
	if locationName.Valid {
		loc = locationName.String
	}

	path := FindPDFPath(h.Cfg.PDFDir, loc, scanID.String)
	if path == "" {
		if h.WFProxy != nil {
			_ = h.WFProxy.StreamPDF(c, scanID.String)
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"detail": "pdf file not found on disk"})
		return
	}

	c.Header("Content-Type", "application/pdf")
	c.Header("Content-Disposition", "inline")
	c.File(path)
}
