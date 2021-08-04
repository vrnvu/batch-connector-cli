package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/vrnvu/batch-connector/pkg/connector"
	"github.com/vrnvu/batch-connector/pkg/valuable"
)

func main() {

	var (
		timeout   int
		batchSize int64
	)

	timeout = 10
	batchSize = 100_000

	db_url := "postgres://postgres@localhost:5432/dbname?sslmode=disable"
	http_url := "https://datasets.imdbws.com/name.basics.tsv.gz"

	tableName := "names"
	columnNames := []string{"nconst", "primary_name", "birth_year", "death_year", "primary_professions", "known_for_titles"}

	conn := connector.New(timeout, batchSize, http_url, db_url)
	fmt.Println(conn.DB_URL)

	table := connector.Table{
		TableName:   tableName,
		ColumnNames: columnNames,
	}

	fmt.Println(table)

	conn.Run(table, transformer)

}

type name struct {
	NConst             string
	PrimaryName        string
	BirthYear          string
	DeathYear          string
	PrimaryProfessions []string
	KnownForTitles     []string
}

func (n *name) Values() []interface{} {
	v := make([]interface{}, 6)

	v[0] = n.NConst
	v[1] = n.PrimaryName
	v[2] = n.BirthYear
	v[3] = n.DeathYear
	v[4] = n.PrimaryProfessions
	v[5] = n.KnownForTitles

	return v
}

func transformer(ctx context.Context, outChannel chan valuable.Valuable, errChannel chan error, readCloser io.ReadCloser) {

	gzipReader, err := gzip.NewReader(readCloser)
	if err != nil {
		errChannel <- errors.Wrap(err, "instantiating gzip")
		return
	}
	defer gzipReader.Close()

	bufferReader := bufio.NewReader(gzipReader)

	skipHeader := true

	for {
		line, err := bufferReader.ReadString('\n')
		if err == io.EOF {
			return
		}

		if skipHeader {
			skipHeader = false
			continue
		}

		if err != nil {
			errChannel <- errors.Wrap(err, "reading gzip")
			return
		}

		record := strings.Split(strings.Trim(line, "\n"), "\t")
		n := &name{
			NConst:             record[0],
			PrimaryName:        record[1],
			BirthYear:          record[2],
			DeathYear:          record[3],
			PrimaryProfessions: strings.Split(record[4], ","),
			KnownForTitles:     strings.Split(record[5], ","),
		}

		// fmt.Println(n)

		select {
		case outChannel <- n:
		case <-ctx.Done():
			errChannel <- ctx.Err()
			return
		}
	}
}
