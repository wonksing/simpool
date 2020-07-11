package simpool_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/wonksing/simpool"
)

var Validate chan string

type MyJob struct {
	name string
	res  chan *simpool.JobResult
}

func NewMyJob(name string) *MyJob {
	return &MyJob{
		name: name,
		res:  make(chan *simpool.JobResult),
	}
}
func (s *MyJob) Execute() {
	defer close(s.res)

	// rn := rand.Intn(100)
	// time.Sleep(time.Millisecond * time.Duration(rn))

	s.res <- &simpool.JobResult{s.name, nil}

	Validate <- s.name
}

func (s *MyJob) GetExecutedResult() *simpool.JobResult {
	return <-s.res
}

func TestPoolWithWait(t *testing.T) {
	numTests := 10000
	Validate = make(chan string, numTests)
	noOfWorkers := 8
	maxQueueSize := 100

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
			// fmt.Printf("MyJob Executed: %v\n", r.Res.(string))
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

type MySimpleJob struct {
	name string
}

func NewMySimpleJob(name string) *MySimpleJob {
	return &MySimpleJob{
		name: name,
	}
}
func (s *MySimpleJob) Execute() {
	// rn := rand.Intn(100)
	// time.Sleep(time.Millisecond * time.Duration(rn))
	Validate <- s.name
}

func (s *MySimpleJob) GetExecutedResult() *simpool.JobResult {
	return nil
}

func TestPoolSimple(t *testing.T) {
	numTests := 100000
	Validate = make(chan string, numTests)
	noOfWorkers := 8
	maxQueueSize := 100

	gp := simpool.NewPool(noOfWorkers, maxQueueSize)
	for i := 0; i < numTests; i++ {
		job := NewMySimpleJob(strconv.Itoa(i))
		gp.Queue(job)
	}
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
