package sqldbutils

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
)

var (
	// TextDatabaseTypeNames 문자형 데이터 타입
	// MYSQL, ORACLE, POSTGRESQL DATA TYPES NEEDS TO BE DEFINED
	TextDatabaseTypeNames = [...]string{
		"CHAR",
		"VARCHAR", "NVARCHAR",
		"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",

		"VARCHAR2", "NCHAR",
	}
)

// isTextDatabaseType 문자형 데이터베이스 데이터 타입 여부 확인
func isTextDatabaseType(typ string) bool {
	if typ == "" {
		return false
	}
	upperTyp := strings.ToUpper(typ)
	for _, str := range TextDatabaseTypeNames {
		if str == upperTyp {
			return true
		}
	}
	return false
}

func InitDB(dbConnStr string, dbDriver string, maxIdleConns int, maxOpenConns int, connMaxLifeTime int) (*sql.DB, error) {
	pdb, err := sql.Open(dbDriver, dbConnStr)
	if err != nil {
		return nil, err
	}
	pdb.SetMaxIdleConns(maxIdleConns)
	pdb.SetMaxOpenConns(maxOpenConns)
	pdb.SetConnMaxLifetime(time.Minute * time.Duration(connMaxLifeTime))

	return pdb, nil
}

// ScanUnknownColumns rowst의 컬럼 유형이나 수를 모르는 경우 사용한다.
func ScanUnknownColumns(rows *sql.Rows) ([][]interface{}, error) {
	if rows == nil {
		return nil, errors.New("nil rows are not allowed")
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var typeRawBytes sql.RawBytes
	var typeString sql.NullString
	result := make([][]interface{}, 0)
	for rows.Next() {
		columnPointers := make([]interface{}, len(colTypes))
		for i, ct := range colTypes {
			var o interface{}

			// t := ct.ScanType()
			// // if t.Name() == "RawBytes" { // THIS WORKS
			// if ct.DatabaseTypeName() == "VARCHAR" || ct.DatabaseTypeName() == "NVARCHAR" { // THIS WORKS TOO
			// 	o = reflect.New(reflect.TypeOf(typeString)).Interface()
			// } else {
			// 	o = reflect.New(ct.ScanType()).Interface()
			// }

			if ct.ScanType() == reflect.TypeOf(typeRawBytes) && isTextDatabaseType(ct.DatabaseTypeName()) {
				o = reflect.New(reflect.TypeOf(typeString)).Interface()
			} else {
				o = reflect.New(ct.ScanType()).Interface()
			}
			columnPointers[i] = o
		}
		err = rows.Scan(columnPointers...)
		if err != nil {
			rows.Close()
			return nil, err
		}

		result = append(result, columnPointers)
	}
	return result, nil
}

// QueryUnknownColumns sqlQry의 컬럼 수나 유형을 모를 때
func QueryUnknownColumns(db *sql.DB, sqlQry string) ([][]interface{}, []string, error) {
	var err error
	var rows *sql.Rows

	rows, err = db.Query(sqlQry)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	scanned, err := ScanUnknownColumns(rows)
	if err != nil {
		return nil, nil, err
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}
	rows.Close()

	if err != nil {
		return nil, nil, err
	}
	return scanned, cols, nil
}

func ExecuteUnknownColumns(db *sql.DB, sqlQry string, row []interface{}) (int64, error) {
	res, err := db.Exec(sqlQry, row...)
	if err != nil {
		return 0, err
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowCnt, nil
}
