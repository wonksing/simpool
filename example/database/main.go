package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/wonksing/simpool"
)

const (
	HostSrc  = "127.0.0.1"
	PortSrc  = 5432
	UserSrc  = "test"
	PwSrc    = "test123"
	DBNamSrc = "test"

	HostDst  = "127.0.0.1"
	PortDst  = 5432
	UserDst  = "test"
	PwDst    = "test123"
	DBNamDst = "test"
)

type InsertJob struct {
	db   *sql.DB
	col1 string
	col2 string
}

func NewInsertJob(db *sql.DB, col1, col2 string) *InsertJob {
	return &InsertJob{
		db:   db,
		col1: col1,
		col2: col2,
	}
}
func (s *InsertJob) Execute() {
	_, err := s.db.Exec("insert into dst(col1, col2) values($1, $2)", s.col1, s.col2)
	if err != nil {
		return
	}
}

type SomeModel struct {
	Col1 string
	Col2 string
}
type SomeModelList []SomeModel

func cleanupSrc(db *sql.DB) error {
	_, err := db.Exec("truncate table src")
	if err != nil {
		return err
	}

	return nil
}

func cleanupDst(db *sql.DB) error {
	_, err := db.Exec("truncate table dst")
	if err != nil {
		return err
	}

	return nil
}

func createTestData(db *sql.DB, numData int) error {
	for i := 0; i < numData; i++ {
		tmp := strconv.Itoa(i)
		qry := fmt.Sprintf("insert into src(col1, col2) values('%v', '%v')", tmp, tmp)
		_, err := db.Exec(qry)
		if err != nil {
			return err
		}
	}
	return nil
}

func selectFromSrc(db *sql.DB) (SomeModelList, error) {
	rows, err := db.Query("select col1, col2 from src")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make(SomeModelList, 0)
	var vCol1 sql.NullString
	var vCol2 sql.NullString
	for rows.Next() {
		err = rows.Scan(&vCol1, &vCol2)
		if err != nil {
			return nil, err
		}
		list = append(list, SomeModel{vCol1.String, vCol2.String})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return list, nil
}

var (
	numWorkers   int
	maxQueueSize int
	cleanup      bool
	numTests     int
)

func init() {
	flag.IntVar(&numWorkers, "w", 16, "number of workers")
	flag.IntVar(&maxQueueSize, "q", 320, "max queue size")
	flag.BoolVar(&cleanup, "c", true, "cleanup test db?")
	flag.IntVar(&numTests, "n", 10000, "number of tests")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	log.Printf("numWorkers: %v, maxQueueSize: %v, numCPU: %v", numWorkers, maxQueueSize, runtime.NumCPU())

	// connect to source database
	connStrSrc := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		HostSrc, PortSrc, UserSrc, PwSrc, DBNamSrc)

	DBSrc, err := sql.Open("postgres", connStrSrc)
	if err != nil {
		log.Printf("Error Opening: %v\n", err)
		return
	}
	defer DBSrc.Close()
	DBSrc.SetMaxIdleConns(1)
	DBSrc.SetMaxOpenConns(1)
	DBSrc.SetConnMaxLifetime(time.Minute * 5)

	// connect to destination database
	connStrDst := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		HostDst, PortDst, UserDst, PwDst, DBNamDst)

	DBDst, err := sql.Open("postgres", connStrDst)
	if err != nil {
		log.Printf("Error Opening: %v\n", err)
		return
	}
	defer DBDst.Close()
	DBDst.SetMaxIdleConns(numWorkers)
	DBDst.SetMaxOpenConns(numWorkers)
	DBDst.SetConnMaxLifetime(time.Minute * 5)

	// create and create test data
	if cleanup {
		cleanupSrc(DBSrc)
		cleanupDst(DBDst)
		start := time.Now()
		createTestData(DBSrc, numTests)
		elapsed := time.Since(start)
		log.Printf("Finished creating %v test data(%v)", numTests, elapsed)
	}

	// get data from source
	list, err := selectFromSrc(DBSrc)
	if err != nil {
		log.Printf("Error Selecting from source: %v\n", err)
		return
	}

	// insert into destination using simpool
	gp := simpool.NewPool(numWorkers, maxQueueSize)
	start := time.Now()
	for _, v := range list {
		job := NewInsertJob(DBDst, v.Col1, v.Col2)
		gp.Queue(job)
	}
	gp.Close() // wait for all jobs to finish and return

	elapsed := time.Since(start)
	en := int(elapsed / time.Second)
	log.Printf("Finished %v jobs(%v). %v tps\n", len(list), elapsed, len(list)/en)
}
