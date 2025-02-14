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
	baseURL = "https://iss.moex.com/iss/engines/futures/markets/forts/boards"
)

var (
	codes = []string{"H", "M", "U", "Z"}
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

// thirdFriday returns the third Friday of given year and month
func thirdFriday(year int, month int) time.Time {
	third := time.Date(year, time.Month(month), 15, 0, 0, 0, 0, time.UTC)
	weekday := third.Weekday()
	daysUntilFriday := (5 - weekday + 7) % 7
	return third.AddDate(0, 0, int(daysUntilFriday))
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

		fmt.Println(url)

		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch data: %w", err)
		}
		defer resp.Body.Close()

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

					data, err := getOHLC("RFUD", ticker, beginDate, endDate, 1)
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
