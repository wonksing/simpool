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

type MyJob struct {
	name string
	res  chan string
}

func NewMyJob(name string) *MyJob {
	return &MyJob{
		name: name,
		res:  make(chan string),
	}
}
func (s *MyJob) Execute() interface{} {
	// fmt.Println(s.name)
	rn := rand.Intn(100)
	time.Sleep(time.Millisecond * time.Duration(rn))
	s.res <- s.name
	return s.name
}

func (s *MyJob) GetResult() interface{} {
	return <-s.res
}

func TestGoPool2(t *testing.T) {
	noOfWorkers := 4
	maxQueueSize := 100

	gp := simpool.NewPool(noOfWorkers, maxQueueSize)
	gp.Init()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			job := NewMyJob(strconv.Itoa(n))

			gp.Queue(job)
			r := job.GetResult().(string)
			fmt.Println(r)
			wg.Done()
		}(i)
	}
	wg.Wait()
	gp.Close()
}

type MyJobWithResult struct {
	name string
}

func NewMyJobWithResult(name string) simpool.Job {
	return &MyJobWithResult{
		name: name,
	}
}
func (s *MyJobWithResult) Execute() interface{} {
	return s.name
}
func (s *MyJobWithResult) SetResult(data interface{}) {
}
func (s *MyJobWithResult) GetResult() interface{} {
	return nil
}
func TestGoPool2WithResult(t *testing.T) {
	noOfWorkers := 4
	maxQueueSize := 100

	gp := simpool.NewPoolWithResult(noOfWorkers, maxQueueSize)
	gp.Init()
	for i := 0; i < 100; i++ {
		job := NewMyJobWithResult(strconv.Itoa(i))

		gp.Queue(job)
	}
	time.Sleep(time.Second * 1)
	gp.Close()

	done := make(chan bool)
	go func() {
		for r := range gp.ResChan {
			fmt.Println(r.(string))
		}
		close(done)
	}()
	<-done
}

// func TestChannel(t *testing.T) {
// 	c := make(chan string, 10)
// 	go func() {
// 		for i := 0; i < 20; i++ {
// 			c <- strconv.Itoa(i)
// 		}
// 		close(c)
// 	}()
// 	time.Sleep(2000)

// 	for val := range c {
// 		fmt.Println(val)
// 	}
// }
