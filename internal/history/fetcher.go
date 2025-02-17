package history

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type OHLCV struct {
	Date   time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int64
}

type Fetcher struct{}

func (f *Fetcher) Fetch(
	ctx context.Context, engine, market, board, ticker string, startDate, endDate time.Time, interval int,
) ([]OHLCV, error) {
	var result []OHLCV
	start := 0

	for {
		url := fmt.Sprintf(
			"https://iss.moex.com/iss/engines/%s/markets/%s/boards/%s/securities/%s/candles.csv?from=%s&till=%s&interval=%d&start=%d",
			engine, market, board, ticker,
			startDate.Format("2006-01-02"),
			endDate.Format("2006-01-02"),
			interval, start)

		fmt.Println(url)

		resp, err := http.Get(url)
		if err != nil {
			return nil, errors.Wrap(err, "http get")
		}
		defer resp.Body.Close()

		reader := csv.NewReader(resp.Body)
		reader.Comma = ';'
		if _, err := reader.Read(); err != nil {
			return nil, errors.Wrap(err, "skip csv header rows")
		}

		reader.FieldsPerRecord = 0
		columns := make(map[string]int)
		column, err := reader.Read()
		if err != nil {
			return nil, errors.Wrap(err, "read csv header columns")
		}
		for indx, name := range column {
			columns[name] = indx
		}

		var batchSize int
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, errors.Wrap(err, "read csv row")
			}

			date, err := time.Parse("2006-01-02 15:04:05", row[columns["begin"]])
			if err != nil {
				return nil, errors.Wrap(err, "parse date column")
			}

			open, err := strconv.ParseFloat(row[columns["open"]], 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse open column")
			}

			high, err := strconv.ParseFloat(row[columns["high"]], 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse high column")
			}

			low, err := strconv.ParseFloat(row[columns["low"]], 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse low column")
			}

			close, err := strconv.ParseFloat(row[columns["close"]], 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse close column")
			}

			volume, err := strconv.ParseInt(row[columns["volume"]], 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse volume column")
			}

			result = append(result, OHLCV{
				Date:   date,
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
