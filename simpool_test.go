package simpool_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/wonksing/simpool"
)

var Validate chan string

type MyJob struct {
	name string
}

func NewMyJob(name string) *MyJob {
	return &MyJob{
		name: name,
	}
}
func (s *MyJob) Execute() *simpool.JobResult {
	rn := rand.Intn(100)
	time.Sleep(time.Millisecond * time.Duration(rn))
	Validate <- s.name
	// fmt.Printf("returning %v\n", s.name)
	return &simpool.JobResult{
		Res: s.name,
		Err: nil,
	}
}

func TestPoolWithWait(t *testing.T) {
	numTests := 1000
	Validate = make(chan string, numTests)
	noOfWorkers := 32
	maxQueueSize := 320

	gp := simpool.NewPool(noOfWorkers, maxQueueSize)

	var wg sync.WaitGroup
	for i := 0; i < numTests; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := NewMyJob(strconv.Itoa(n))

			r := gp.QueueAndWait(job)
			if r.Err != nil {
				// error handling
				return
			}
			fmt.Printf("MyJob Executed: %v %v\n", strconv.Itoa(n), r.Res.(string))
		}(i)
	}
	wg.Wait()
	gp.Close()

	close(Validate)
	cnt := 0
	for _ = range Validate {
		// fmt.Printf("MySimpleJob Executed: %v\n", v)
		cnt += 1
	}

	if cnt != numTests {
		t.FailNow()
	}
}

func TestPoolSimple(t *testing.T) {
	numTests := 1000
	Validate = make(chan string, numTests)
	noOfWorkers := 32
	maxQueueSize := 320

	gp := simpool.NewPool(noOfWorkers, maxQueueSize)
	for i := 0; i < numTests; i++ {
		job := NewMyJob(strconv.Itoa(i))
		gp.Queue(job)
		fmt.Printf("queued %v\n", strconv.Itoa(i))
	}
	gp.Close()
	fmt.Println("channel closed")

	close(Validate)
	cnt := 0
	for v := range Validate {
		fmt.Printf("MySimpleJob Executed: %v\n", v)
		cnt += 1
	}

	if cnt != numTests {
		t.FailNow()
	}
}
