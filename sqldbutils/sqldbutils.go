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
	NumericDatabaseTypeNames = [...]string{
		"NUMERIC", "NUMBER", "DECIMAL", "INT", "BIGINT",
	}
)

// isTextDatabaseType 문자형 데이터베이스 데이터 타입 여부 확인
func isTextDatabaseType(typ string) bool {
	if typ == "" {
		return false
	}
	upperTyp := strings.TrimSpace(strings.ToUpper(typ))
	for _, str := range TextDatabaseTypeNames {
		if str == upperTyp {
			return true
		}
	}
	return false
}
func isNumericDatabaseType(typ string) bool {
	if typ == "" {
		return false
	}
	upperTyp := strings.TrimSpace(strings.ToUpper(typ))
	for _, str := range NumericDatabaseTypeNames {
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

var (
	typeTime        time.Time
	typeRawBytes    sql.RawBytes
	typeNullString  sql.NullString
	typeNullFloat64 sql.NullFloat64
	typeNullInt32   sql.NullInt32
	typeNullInt64   sql.NullInt64
	typeNullTime    sql.NullTime
	typeNullBool    sql.NullBool
)

// scanUnknownColumns rowst의 컬럼 유형이나 수를 모르는 경우 사용한다.
func scanUnknownColumns(rows *sql.Rows) ([][]interface{}, error) {
	if rows == nil {
		return nil, errors.New("nil rows are not allowed")
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

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

			// if ct.ScanType() == reflect.TypeOf(typeRawBytes) && isTextDatabaseType(ct.DatabaseTypeName()) {
			// 	o = reflect.New(reflect.TypeOf(typeString)).Interface()
			// } else {
			// 	o = reflect.New(ct.ScanType()).Interface()
			// }

			switch ct.ScanType() {
			case reflect.TypeOf(typeRawBytes):
				if isTextDatabaseType(ct.DatabaseTypeName()) {
					o = reflect.New(reflect.TypeOf(typeNullString)).Interface()
				} else {
					o = reflect.New(ct.ScanType()).Interface()
				}
			case reflect.TypeOf((string)("")):
				o = reflect.New(reflect.TypeOf(typeNullString)).Interface()
			case reflect.TypeOf((float32)(0)):
				o = reflect.New(reflect.TypeOf(typeNullFloat64)).Interface()
			case reflect.TypeOf((float64)(0)):
				o = reflect.New(reflect.TypeOf(typeNullFloat64)).Interface()
			case reflect.TypeOf((bool)(true)):
				o = reflect.New(reflect.TypeOf(typeNullBool)).Interface()
			case reflect.TypeOf(typeTime):
				o = reflect.New(reflect.TypeOf(typeNullTime)).Interface()
			case reflect.TypeOf((int)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt64)).Interface()
			case reflect.TypeOf((int8)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((int16)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((int32)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((int64)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt64)).Interface()
			case reflect.TypeOf((uint)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt64)).Interface()
			case reflect.TypeOf((uint8)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((uint16)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((uint32)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt32)).Interface()
			case reflect.TypeOf((uint64)(0)):
				o = reflect.New(reflect.TypeOf(typeNullInt64)).Interface()
			default:
				if isNumericDatabaseType(ct.DatabaseTypeName()) {
					o = reflect.New(reflect.TypeOf(typeNullFloat64)).Interface()
				} else if isTextDatabaseType(ct.DatabaseTypeName()) {
					o = reflect.New(reflect.TypeOf(typeNullString)).Interface()
				} else {
					o = reflect.New(ct.ScanType()).Interface()
				}
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

	scanned, err := scanUnknownColumns(rows)
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
