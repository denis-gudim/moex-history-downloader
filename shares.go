package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	baseURL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards"
)

// OHLCV represents Open-High-Low-Close-Volume data
type OHLCV struct {
	Date   time.Time
	Ticker string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int64
}

// getOHLC fetches OHLC data from MOEX ISS
func getOHLC(board, ticker string, startDate, endDate time.Time, interval int) ([]OHLCV, error) {
	var result []OHLCV
	start := 0

	for {
		url := fmt.Sprintf("%s/%s/securities/%s/candles.csv?from=%s&till=%s&interval=%d&start=%d",
			baseURL, board, ticker,
			startDate.Format("2006-01-02"),
			endDate.Format("2006-01-02"),
			interval, start)

		fmt.Printf("Fetching %s data for %s - %s\n", ticker, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch data: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		reader := csv.NewReader(resp.Body)
		reader.Comma = ';'
		if _, err := reader.Read(); err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}

		reader.FieldsPerRecord = 0
		columns := make(map[string]int)
		column, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		for indx, name := range column {
			columns[name] = indx
		}

		var batchSize int
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read record: %w", err)
			}

			date, err := time.Parse("2006-01-02 15:04:05", record[columns["begin"]])
			if err != nil {
				return nil, fmt.Errorf("failed to parse date: %w", err)
			}

			open, err := strconv.ParseFloat(record[columns["open"]], 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse open price: %w", err)
			}

			high, err := strconv.ParseFloat(record[columns["high"]], 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse high price: %w", err)
			}

			low, err := strconv.ParseFloat(record[columns["low"]], 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse low price: %w", err)
			}

			close, err := strconv.ParseFloat(record[columns["close"]], 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse close price: %w", err)
			}

			volume, err := strconv.ParseInt(record[columns["volume"]], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse volume: %w", err)
			}

			result = append(result, OHLCV{
				Date:   date,
				Ticker: ticker,
				Open:   open,
				High:   high,
				Low:    low,
				Close:  close,
				Volume: volume,
			})
			batchSize++
		}

		if batchSize < 500 {
			break
		}
		start += batchSize
	}

	return result, nil
}

// ensureDir creates directory if it doesn't exist
func ensureDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

// createOrAppendFile creates a new file if it doesn't exist or appends to existing one
func createOrAppendFile(fileName string, writeHeader bool) (*os.File, error) {
	var file *os.File

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		file, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
		if writeHeader {
			header := "<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n"
			if _, err := file.WriteString(header); err != nil {
				file.Close()
				return nil, err
			}
		}
	} else {
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
	}

	return file, nil
}

// writeDataToFile writes OHLCV data to file
func writeDataToFile(file *os.File, data []OHLCV) error {
	for _, ohlc := range data {
		line := fmt.Sprintf("%s,%s,%g,%g,%g,%g,%d\n",
			ohlc.Date.Format("20060102"),
			ohlc.Date.Format("15:04:05"),
			ohlc.Open, ohlc.High, ohlc.Low, ohlc.Close, ohlc.Volume)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	return nil
}

// ProcessStocks processes all stocks for given year range
func ProcessStocks(yearStart, yearEnd int, stocks ...string) error {
	baseDir := "moex_data"
	if err := ensureDir(baseDir); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	gr, _ := errgroup.WithContext(context.Background())
	gr.SetLimit(4) // Limit concurrent requests

	for _, stock := range stocks {
		stock := stock // Create new variable for goroutine
		gr.Go(func() error {
			// Create or open file for the stock
			fileName := filepath.Join(baseDir, fmt.Sprintf("%s.txt", stock))
			file, err := createOrAppendFile(fileName, true)
			if err != nil {
				return fmt.Errorf("failed to create/open file for %s: %w", stock, err)
			}
			defer file.Close()

			for year := yearStart; year <= yearEnd; year++ {
				for month := 1; month <= 12; month++ {
					startDate := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
					endDate := startDate.AddDate(0, 1, -1)

					// Skip future months
					if startDate.After(time.Now()) {
						continue
					}

					data, err := getOHLC("TQBR", stock, startDate, endDate, 1)
					if err != nil {
						return fmt.Errorf("failed to get OHLC data for %s %d-%02d: %w", stock, year, month, err)
					}

					if len(data) > 0 {
						if err := writeDataToFile(file, data); err != nil {
							return fmt.Errorf("failed to write data to file: %w", err)
						}
						fmt.Printf("Successfully wrote %d records for %s %d-%02d\n", len(data), stock, year, month)
					} else {
						fmt.Printf("No data for %s %d-%02d\n", stock, year, month)
					}

					// Small delay to avoid overwhelming the API
					time.Sleep(100 * time.Millisecond)
				}
			}
			return nil
		})
	}

	return gr.Wait()
}

func main() {
	stocks := []string{
		"SBER", "GAZP", "LKOH", "GMKN",
	}

	if err := ProcessStocks(2010, 2026, stocks...); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
