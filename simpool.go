package simpool

import "sync"

type Job interface {
	Execute() interface{}
	GetResult() interface{}
}
type Pool struct {
	noOfWorkers  int
	maxQueueSize int
	wg           *sync.WaitGroup
	jobChan      chan Job
	ResChan      chan interface{}
	// wgResChan    *sync.WaitGroup
}

func NewPool(noOfWorkers int, maxQueueSize int) *Pool {
	var wg sync.WaitGroup
	jobChan := make(chan Job, maxQueueSize)
	p := &Pool{
		noOfWorkers:  noOfWorkers,
		maxQueueSize: maxQueueSize,
		wg:           &wg,
		jobChan:      jobChan,
	}
	return p
}

func NewPoolWithResult(noOfWorkers int, maxQueueSize int) *Pool {
	p := NewPool(noOfWorkers, maxQueueSize)
	p.ResChan = make(chan interface{}, maxQueueSize)
	return p
}

func (p *Pool) Init() {
	p.wg.Add(p.noOfWorkers)
	for i := 0; i < p.noOfWorkers; i++ {
		go p.startWorkers()
	}
}

func (p *Pool) startWorkers() {
	defer p.wg.Done()

	// it is a blocking operation.
	// wait until a task is received.
	// break when the channel is closed and empty.
	for job := range p.jobChan {
		if job != nil {
			res := job.Execute()
			if p.ResChan != nil {
				// p.wgResChan.Add(1)
				p.ResChan <- res
			}
		}
	}
}

// Queue a job into the Pool
func (p *Pool) Queue(job Job) {
	p.jobChan <- job
}

// Close workers
func (p *Pool) Close() {
	close(p.jobChan)
	p.wg.Wait()

	if p.ResChan != nil {
		close(p.ResChan)
		// p.wgResChan.Wait()
	}
}
