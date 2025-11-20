package data

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func InstallHandler(group *gin.RouterGroup, mgr *Manager) {
	group.POST("/tagNames", importTagNames(mgr))
	group.GET("/tagNames", getTagNames(mgr))
}

func importTagNames(mgr *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		file, err1 := c.FormFile("file")

		if err1 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": err1.Error()})
			return
		}
		if err := mgr.ImportTagNames(file); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}
		c.Status(http.StatusOK)
		return
	}
}

func getTagNames(mgr *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tagNames := mgr.ListTagNames()
		c.JSON(http.StatusOK, ResponseModel{TagNames: tagNames})
	}
}
