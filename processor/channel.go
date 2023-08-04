package processor

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// defaultChanBuffer - Setting default buffer size for channel, pushing more items > chanBuffer is blocking operation
var defaultChanBuffer = 10
var defaultConcurrency = 10

type genericReq interface{}

type GenericResponse struct {
	Response interface{}
	Err      error
}

type ThreadSafeRoundRobinIterator int32
type GenericRequestChan chan genericReq
type GenericResponseChan chan GenericResponse

type handleFunc func(ctx context.Context, input interface{}) (output interface{}, err error)

type ChannelFactory struct {
	ctx           context.Context
	inputPipe     GenericRequestChan
	outPutPipe    GenericResponseChan
	concurrency   int
	closedWorkers ThreadSafeCounter
	execFunc      handleFunc
}

// IChannelFactory - Intentionally exposing via Interface, don't want caller to access internal components/functions
type IChannelFactory interface {
	Done()
	Push(requestParams interface{})
	GetOutputChannel() GenericResponseChan
	Pop() (GenericResponse, bool)
}

// Push - Adds request to Input channel for processing
func (cf *ChannelFactory) Push(reqParams interface{}) {
	cf.inputPipe <- reqParams
}

// Pop - Get response one by one, isPresent - false in case channel is closed
func (cf *ChannelFactory) Pop() (response GenericResponse, isPresent bool) {
	val, isOpen := <-cf.outPutPipe
	return val, isOpen
}

// GetOutputChannel - Returns Output channel which contains the processed responses
func (cf *ChannelFactory) GetOutputChannel() GenericResponseChan {
	return cf.outPutPipe
}

// Done - Gives a signal to close input Chan, when all requests are exhausted.
func (cf *ChannelFactory) Done() {
	close(cf.inputPipe)
}

// markConsensusToCloseOutPutChan - Method to mark consensus of closed child channels,
// Closes output when all child channels are closed.
func (cf *ChannelFactory) markConsensusToCloseOutPutChan() {
	cf.closedWorkers.Increment()
	if cf.closedWorkers.Get() == int32(cf.concurrency) {
		close(cf.outPutPipe)
	}
}

// concurrentPipeListener - Listener for child channels, does the method execution and pushes to Output chan.
func (cf *ChannelFactory) concurrentPipeListener() {
	defer cf.markConsensusToCloseOutPutChan()
	defer cf.recoverPipeGoRoutines()
	for req := range cf.inputPipe {
		response, err := cf.execFunc(cf.ctx, req)
		cf.outPutPipe <- GenericResponse{response, err}
	}
}

// recoverPipeGoRoutines - Recovery routine in case panic occurred, as this is run in separate go routine.
// Needed as this is not recovered under main go routine, and panic in this will lead to app shutdown.
func (cf *ChannelFactory) recoverPipeGoRoutines() {
	if r := recover(); r != nil {
		panic("implement this")
	}
}

// startListeners - Starts go routine listeners which are reading from newly created channels.
func (cf *ChannelFactory) startListeners() {
	for i := 0; i < cf.concurrency; i++ {
		go cf.concurrentPipeListener()
	}
	return
}

// NewChannelFactory - Initiates the Channels Factory for the process.
// concurrency - Desired parallel threads for the execFunc
// buffer - Buffer for interim channels. To be Set cautiously based on consuming pattern.
func NewChannelFactory(ctx context.Context, concurrency, buffer int, execFunc handleFunc) IChannelFactory {
	if concurrency == 0 {
		concurrency = defaultConcurrency
	}
	if buffer == 0 {
		buffer = defaultChanBuffer
	}
	inputPipe := make(GenericRequestChan, buffer)
	outputPipe := make(GenericResponseChan, buffer)
	cf := &ChannelFactory{
		ctx:           ctx,
		inputPipe:     inputPipe,
		outPutPipe:    outputPipe,
		concurrency:   concurrency,
		closedWorkers: ThreadSafeCounter(0),
		execFunc:      execFunc,
	}
	cf.startListeners()
	return cf
}
