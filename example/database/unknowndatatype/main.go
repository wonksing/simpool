package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/wonksing/simpool"
	"github.com/wonksing/simpool/sqldbutils"
)

const (
	DriverSrc = "postgres"
	HostSrc   = "127.0.0.1"
	PortSrc   = 5432
	UserSrc   = "test"
	PwSrc     = "test123"
	DBNamSrc  = "test"

	DriverDst = "postgres"
	HostDst   = "127.0.0.1"
	PortDst   = 5432
	UserDst   = "test"
	PwDst     = "test123"
	DBNamDst  = "test"
)

func main() {

	// connect to source database
	connStrSrc := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		HostSrc, PortSrc, UserSrc, PwSrc, DBNamSrc)

	// connect to destination database
	connStrDst := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		HostDst, PortDst, UserDst, PwDst, DBNamDst)

	src, err := sqldbutils.InitDB(connStrSrc, DriverSrc, 1, 1, 5)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer src.Close()
	dst, err := sqldbutils.InitDB(connStrDst, DriverDst, 10, 10, 5)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dst.Close()

	numOfWorkers := 100
	numOfQueueSize := numOfWorkers + 100
	pool := simpool.NewPool(numOfWorkers, numOfQueueSize)
	defer pool.Close()

	log.Printf("Start selecting... %v\n", time.Now())
	rows, columns, err := sqldbutils.QueryUnknownColumns(src, "select col1, col2 from src")
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("Finished %v\n", time.Now())

	log.Printf("Columns: %v %v\n", columns, time.Now())
	for _, row := range rows {
		j := NewTransferJob(dst, row)
		pool.Queue(j)
	}
	log.Printf("Queueing %v\n", time.Now())
	pool.Wait()
	log.Printf("Finished transfer %v\n", time.Now())

}

//
type TransferJob struct {
	dstDB *sql.DB
	row   []interface{}
}

func NewTransferJob(dstDB *sql.DB, row []interface{}) *TransferJob {
	return &TransferJob{
		dstDB: dstDB,
		row:   row,
	}
}
func (s *TransferJob) Execute() *simpool.JobResult {
	res, err := sqldbutils.ExecuteUnknownColumns(s.dstDB, "insert into dst(col1, col2) values($1, $2)", s.row)
	if err != nil {
		fmt.Println(err)
	}
	// res := 1
	// var err error
	// fmt.Printf("a job finished, %d\n", res)
	return &simpool.JobResult{
		Res: res,
		Err: err,
	}
}
