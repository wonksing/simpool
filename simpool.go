package simpool

import "sync"

// Job interface
type Job interface {
	Execute() interface{}
	GetResult() interface{}
}

// Pool struct
type Pool struct {
	noOfWorkers  int
	maxQueueSize int
	wg           *sync.WaitGroup
	jobChan      chan Job
	ResChan      chan interface{}
	// wgResChan    *sync.WaitGroup
}

// NewPool create pool object
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

// NewPoolWithResult create a pool with result channel
func NewPoolWithResult(noOfWorkers int, maxQueueSize int) *Pool {
	p := NewPool(noOfWorkers, maxQueueSize)
	p.ResChan = make(chan interface{}, maxQueueSize)
	return p
}

// Init initializes the pool
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
			_ = job.Execute()
			if p.ResChan != nil {
				p.ResChan <- job.GetResult()
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
	}
}
