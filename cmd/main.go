package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/denis-gudim/moex-history-downloader/internal/history"
	"golang.org/x/sync/errgroup"
)

var (
	codes = []string{"H", "M", "U", "Z"}
)

// thirdFriday returns the third Friday of given year and month
func thirdFriday(year int, month int) time.Time {
	third := time.Date(year, time.Month(month), 15, 0, 0, 0, 0, time.UTC)
	weekday := third.Weekday()
	daysUntilFriday := (5 - weekday + 7) % 7
	return third.AddDate(0, 0, int(daysUntilFriday))
}

// createOrAppendFile creates a new file if it doesn't exist or appends to existing one
func createOrAppendFile(fileName string) (*os.File, error) {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return os.Create(fileName)
	}
	return os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
}

// ProcessContracts processes all contracts for given year range
func ProcessContracts(yearBegin, yearEnd int, contracts ...string) error {
	currentDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %w", err)
	}

	gr, _ := errgroup.WithContext(context.Background())
	gr.SetLimit(4)

	for _, contract := range contracts {
		gr.Go(func() error {
			// Create one file per contract
			fileName := filepath.Join(currentDir, fmt.Sprintf("%s.txt", contract))

			// Remove existing file to start fresh
			if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove existing file: %w", err)
			}

			// Create new file and write header
			file, err := os.Create(fileName)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}

			// Write header
			header := "<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n"
			if _, err := file.WriteString(header); err != nil {
				file.Close()
				return fmt.Errorf("failed to write header: %w", err)
			}
			file.Close()

			fetcher := &history.Fetcher{}
			for y := yearBegin; y < yearEnd; y++ {
				for i, code := range codes {
					m := i*3 + 3
					yBegin := y
					mBegin := m - 3

					if mBegin == 0 {
						mBegin = 12
						yBegin--
					}

					beginDate := thirdFriday(yBegin, mBegin).AddDate(0, 0, -1)
					endDate := thirdFriday(y, m).AddDate(0, 0, -2)
					ticker := fmt.Sprintf("%s%s%d", contract, code, y%10)

					data, err := fetcher.Fetch(context.Background(), "features", "forts", "RFUD", ticker, beginDate, endDate, 1)
					if err != nil {
						return fmt.Errorf("failed to get OHLC data for %s: %w", ticker, err)
					}

					// Append data to the contract file
					file, err := createOrAppendFile(fileName)
					if err != nil {
						return fmt.Errorf("failed to open file for appending: %w", err)
					}

					for _, ohlc := range data {
						line := fmt.Sprintf("%s,%s,%g,%g,%g,%g,%d\n",
							ohlc.Date.Format("20060102"),
							ohlc.Date.Format("15:04:05"),
							ohlc.Open, ohlc.High, ohlc.Low, ohlc.Close, ohlc.Volume)
						if _, err := file.WriteString(line); err != nil {
							file.Close()
							return fmt.Errorf("failed to write to file: %w", err)
						}
					}
					file.Close()
				}
			}
			return nil
		})

	}

	return gr.Wait()
}

func main() {
	futures := []string{
		"Si", "BR", "RI", "SR", "GZ", "LK", "MX", "GD", "RN", "VB", "MG", "SN", "NL", "MT", "GM", "TT", "PL", "CH", "YN", "AL", "ME", "FV", "PO", "PH", "TN", "AF", "NV", "PK", "RU", "HY",
	}
	if err := ProcessContracts(2016, 2026, futures...); err != nil {
		// if err := ProcessContracts(2016, 2026, "Si", "VB", "RI", "LK", "SR", "GZ"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
