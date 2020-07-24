package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/wonksing/simpool"
)

// PrintJob to push into the pool
// Your job should interfaces simpool.Job
type PrintJob struct {
	word string
}

// Execute your job here
func (s *PrintJob) Execute() *simpool.JobResult {
	// your code
	fmt.Println(s.word)

	// returns nil since we are just printing word
	return nil
}

var (
	numWorkers   int
	maxQueueSize int
	cleanup      bool
	numTests     int
)

func init() {
	flag.IntVar(&numWorkers, "w", 32, "number of workers")
	flag.IntVar(&maxQueueSize, "q", 100, "max queue size")
	flag.IntVar(&numTests, "n", 200000, "number of tests")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	log.Printf("numWorkers: %v, maxQueueSize: %v, numCPU: %v", numWorkers, maxQueueSize, runtime.NumCPU())

	start := time.Now()
	// create pool
	pool := simpool.NewPool(numWorkers, maxQueueSize)

	// create a job in each iteration and queue into the pool
	for i := 0; i < numTests; i++ {
		job := &PrintJob{strconv.Itoa(i)}
		pool.Queue(job) // non-blocking operation as long as the pool has remaining space
	}
	log.Printf("Finished queueing(%v)\n", time.Now())
	pool.Close() // wait for all jobs to finish and return

	elapsed := time.Since(start)
	log.Printf("Finished %v jobs(%v)\n", numTests, elapsed)
}
