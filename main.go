package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	Name    string
	Columns []*Column
}

type Column struct {
	Name             string `db:"COLUMN_NAME"`
	Comment          string `db:"COLUMN_COMMENT"`
	DataType         string `db:"DATA_TYPE"`
	ClickhouseType   string
	NumericPrecision int    `db:"NUMERIC_PRECISION"`
	IsNullable       string `db:"IS_NULLABLE"`
}

var (
	db     *sqlx.DB
	conn   *string
	tables *string
)

func init() {
	conn = flag.String("conn", "", "eg. root:@tcp(localhost:3306)/test")
	tables = flag.String("tables", "", "eg. table1,table2")
}

func main() {
	flag.Parse()
	fmt.Println("conn: ", *conn)
	fmt.Println("tables: ", *tables)
	fmt.Println()

	if *conn == "" {
		fmt.Println("conn must be not empty!")
		return
	}
	if *tables == "" {
		fmt.Println("tables must be not empty!")
		return
	}
	var err error
	db, err = sqlx.Connect("mysql", *conn)
	if err != nil {
		fmt.Printf("conect db failed %+v", err)
		return
	}
	defer db.Close()
	tables := handleTable(strings.Split(*tables, ","))
	for _, t := range tables {
		ddl := fmt.Sprintf("CREATE TABLE `%s` (\n", t.Name)
		l := len(t.Columns)
		for i, column := range t.Columns {
			clickhouseType := column.ClickhouseType
			if column.IsNullable == "YES" {
				clickhouseType = fmt.Sprintf("Nullable(%s)", clickhouseType)
			}
			ddl += fmt.Sprintf("`%s` %s comment '%s'", column.Name, clickhouseType, column.Comment)
			if i != l-1 {
				ddl += fmt.Sprintf(", \n")
			}
		}
		ddl += "\n"
		ddl += ") Engine=ReplacingMergeTree(I_ID) order by I_ID"
		fmt.Println(ddl)
	}
}

var mysql2ClickhouseType = map[string]string{
	"bigint":   "UInt64",
	"tinyint":  "UInt8",
	"varchar":  "String",
	"int":      "Int32",
	"datetime": "DateTime",
}

func handleTable(tableNames []string) []*Table {
	var tables []*Table
	for _, name := range tableNames {
		tables = append(tables, &Table{
			Name:    name,
			Columns: handleColumn(name),
		})
	}
	return tables
}

func getIntTypeByLength(length int) string {
	switch {
	case length <= 8:
		return "Int8"
	case length <= 16:
		return "Int16"
	case length <= 32:
		return "Int32"
	case length <= 64:
		return "Int64"
	}
	return "Int64"
}

func handleColumn(tableName string) []*Column {
	var columns []*Column
	query := "select COLUMN_NAME,DATA_TYPE,IFNULL(COLUMN_COMMENT,'') as COLUMN_COMMENT,IFNULL(NUMERIC_PRECISION,0) as NUMERIC_PRECISION from information_schema.COLUMNS where TABLE_NAME=?"
	err := sqlx.Select(db, &columns, query, tableName)
	if err != nil {
		fmt.Printf("handleColumn %+v", err)
	}
	for i, column := range columns {
		if column.DataType == "int" {
			column.ClickhouseType = getIntTypeByLength(column.NumericPrecision)
		} else {
			column.ClickhouseType = mysql2ClickhouseType[column.DataType]
		}
		columns[i] = column
	}
	return columns
}
