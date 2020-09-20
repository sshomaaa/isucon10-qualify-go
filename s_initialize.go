package main

import (
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"

	"github.com/labstack/echo"
)

func _initializeDB(connEnv *MySQLConnectionEnv, path string) error {
	sqlFile, _ := filepath.Abs(path)
	cmdStr := fmt.Sprintf("mysql -h %v -u %v -p%v -P %v %v < %v",
		connEnv.Host,
		connEnv.User,
		connEnv.Password,
		connEnv.Port,
		connEnv.DBName,
		sqlFile,
	)
	if err := exec.Command("bash", "-c", cmdStr).Run(); err != nil {
		return err
	}
	return nil
}

func initializeEstateDB(sqlDir string) error {
	paths := []string{
		filepath.Join(sqlDir, "0_Schema.sql"),
		filepath.Join(sqlDir, "1_DummyEstateData.sql"),
	}
	for _, p := range paths {
		if err := _initializeDB(dbeEnv, p); err != nil {
			return err
		}
	}
	return nil
}

func initializeChairDB(sqlDir string) error {
	paths := []string{
		filepath.Join(sqlDir, "0_Schema.sql"),
		filepath.Join(sqlDir, "2_DummyChairData.sql"),
	}
	for _, p := range paths {
		if err := _initializeDB(dbcEnv, p); err != nil {
			return err
		}
	}
	return nil
}

func initializeEstatePoint() error {
	_, err := dbe.Exec("INSERT INTO estate_point(id, point) SELECT id, POINT(latitude, longitude) FROM estate")
	return err
}

func initialize(c echo.Context) error {
	sqlDir := filepath.Join("..", "mysql", "db")

	// initialize db
	if err := initializeEstateDB(sqlDir); err != nil {
		c.Logger().Errorf("Initialize script error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if err := initializeChairDB(sqlDir); err != nil {
		c.Logger().Errorf("Initialize script error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if err := initializeEstatePoint(); err != nil {
		c.Logger().Errorf("Initialize script error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if err := initEstateCache(); err != nil {
		c.Logger().Errorf("Initialize script error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, InitializeResponse{
		Language: "go",
	})
}
