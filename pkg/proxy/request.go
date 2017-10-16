// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Request struct {
	Multi []*redis.Resp
	Batch *sync.WaitGroup
	Group *sync.WaitGroup

	Broken *atomic2.Bool

	OpStr string
	OpFlag

	Database int32
	UnixNano int64

	*redis.Resp
	Err error

	Coalesce func() error
}

func (r *Request) IsBroken() bool {
	return r.Broken != nil && r.Broken.IsTrue()
}

func (r *Request) MakeSubRequest(n int) []Request {
	var sub = make([]Request, n)
	for i := range sub {
		x := &sub[i]
		x.Batch = r.Batch
		x.OpStr = r.OpStr
		x.OpFlag = r.OpFlag
		x.Broken = r.Broken
		x.Database = r.Database
		x.UnixNano = r.UnixNano
	}
	return sub
}

const GOLDEN_RATIO_PRIME_32 = 0x9e370001

// 每个Request都有一个编号，便于在分布式系统中统一log, 跟踪请求，debug, 性能分析
func (r *Request) Seed16() uint {
	h32 := uint32(r.UnixNano) + uint32(uintptr(unsafe.Pointer(r)))
	h32 *= GOLDEN_RATIO_PRIME_32
	return uint(h32 >> 16)
}

type RequestChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Request
	buff []*Request

	waits  int
	closed bool
}

const DefaultRequestChanBuffer = 128

func NewRequestChan() *RequestChan {
	return NewRequestChanBuffer(0)
}

func NewRequestChanBuffer(n int) *RequestChan {
	if n <= 0 {
		n = DefaultRequestChanBuffer
	}
	var ch = &RequestChan{
		buff: make([]*Request, n),
	}

	// condition是和lock关联的
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

func (c *RequestChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

func (c *RequestChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PushBack(r *Request) int {
	c.lock.Lock()
	n := c.lockedPushBack(r)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PopFront() (*Request, bool) {
	c.lock.Lock()
	r, ok := c.lockedPopFront()
	c.lock.Unlock()
	return r, ok
}

// lockedPushBack
// lockedPopFront
// 在内存管理时都不要自己管理lock, 统一交给外部函数来管理
//
func (c *RequestChan) lockedPushBack(r *Request) int {
	if c.closed {
		panic("send on closed chan")
	}

	// 通过Signal
	if c.waits != 0 {
		c.cond.Signal()
	}
	c.data = append(c.data, r)
	return len(c.data)
}

func (c *RequestChan) lockedPopFront() (*Request, bool) {
	for len(c.data) == 0 {
		if c.closed {
			return nil, false
		}
		// 如果没有数据，则立马做内存整理(调整），然后在等待
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait() // 这里在等待时，是否会出现: lockedPushBack 无法获取Lock呢?
		c.waits--
	}
	var r = c.data[0]
	c.data, c.data[0] = c.data[1:], nil
	return r, true
}

func (c *RequestChan) IsEmpty() bool {
	return c.Buffered() == 0
}

func (c *RequestChan) PopFrontAll(onRequest func(r *Request) error) error {
	for {
		r, ok := c.PopFront()
		if ok {
			// Request在同步处理模式下，先排队；
			// 在出队列时需要等待Response
			if err := onRequest(r); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (c *RequestChan) PopFrontAllVoid(onRequest func(r *Request)) {
	c.PopFrontAll(func(r *Request) error {
		onRequest(r)
		return nil
	})
}
