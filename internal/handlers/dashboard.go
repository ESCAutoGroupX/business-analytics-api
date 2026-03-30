package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func BankBalance(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "not yet implemented"})
}
