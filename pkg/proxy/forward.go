// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 如何将请求转发给后端redis服务器呢?
// 单连接?
// 多连接?
type forwardMethod interface {
	GetId() int
	Forward(s *Slot, r *Request, hkey []byte) error
}

var (
	ErrSlotIsNotReady = errors.New("slot is not ready, may be offline")
	ErrRespIsRequired = errors.New("resp is required")
)

// 同步
type forwardSync struct {
	forwardHelper
}

func (d *forwardSync) GetId() int {
	return models.ForwardSync
}

// 入口:
func (d *forwardSync) Forward(s *Slot, r *Request, hkey []byte) error {
	s.lock.RLock()
	// 在处理Request期间，Slot的信息是不允许修改的
	// 选择BackendConn
	//      这个地方可以定制不同的策略
	bc, err := d.process(s, r, hkey)
	s.lock.RUnlock()

	if err != nil {
		return err
	}
	// 交给BackendConn, 任务完成
	bc.PushBack(r)
	return nil
}

func (d *forwardSync) process(s *Slot, r *Request, hkey []byte) (*BackendConn, error) {
	if s.backend.bc == nil {
		log.Debugf("slot-%04d is not ready: hash key = '%s'",
			s.id, hkey)
		return nil, ErrSlotIsNotReady
	}

	// 正在迁移过程中，特殊处理
	// redis的内部处理速度 > 网络开销，即便内部性能下降50%， 应该对外部性能影响不大
	if s.migrate.bc != nil && len(hkey) != 0 {
		// 在Slot迁移期间，如果要访问该Slot中的某个key, 不管怎么样都暴力发送一个指令: migrate
		// 1. 由于: codis距离redis很近，这个网络开销不大
		// 2. redis本身的处理能力很强，这个负担不会太大
		// 3. redis本身是单线程的，slotsmgrt只有一次调用会生效
		// TODO:
		//   其他情况下我们的redis迁移是否可以可以考虑这个实习呢?
		if err := d.slotsmgrt(s, hkey, r.Database, r.Seed16()); err != nil {
			log.Debugf("slot-%04d migrate from = %s to %s failed: hash key = '%s', database = %d, error = %s",
				s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, r.Database, err)
			return nil, err
		}
	}
	r.Group = &s.refs
	r.Group.Add(1)
	return d.forward2(s, r), nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type forwardSemiAsync struct {
	forwardHelper
}

func (d *forwardSemiAsync) GetId() int {
	return models.ForwardSemiAsync
}

//
// 半异步实现?
//
func (d *forwardSemiAsync) Forward(s *Slot, r *Request, hkey []byte) error {
	var loop int
	for {
		// 等待结果
		s.lock.RLock()
		bc, retry, err := d.process(s, r, hkey)
		s.lock.RUnlock()

		// 是否Retry？
		switch {
		case err != nil:
			return err
		case !retry:
			if bc != nil {
				bc.PushBack(r)
			}
			return nil
		}

		var delay time.Duration
		switch {
		case loop < 5:
			delay = 0
		case loop < 20:
			delay = time.Millisecond * time.Duration(loop)
		default:
			delay = time.Millisecond * 20
		}
		time.Sleep(delay)

		if r.IsBroken() {
			return ErrRequestIsBroken
		}
		loop += 1
	}
}

func (d *forwardSemiAsync) process(s *Slot, r *Request, hkey []byte) (_ *BackendConn, retry bool, _ error) {
	if s.backend.bc == nil {
		log.Debugf("slot-%04d is not ready: hash key = '%s'",
			s.id, hkey)
		return nil, false, ErrSlotIsNotReady
	}

	// 什么时候retry有效呢?
	// 在迁移数据的时候有效
	// 很多啥情况下大的kv迁移非常耗时，也有可能短期内无法完成，因此将大的集合的迁移拆分成为小集合.....
	// 如何保证迁移的有效性?
	// https://github.com/CodisLabs/codis/issues/1094
	//
	if s.migrate.bc != nil && len(hkey) != 0 {
		resp, moved, err := d.slotsmgrtExecWrapper(s, hkey, r.Database, r.Seed16(), r.Multi)
		switch {
		case err != nil:
			log.Debugf("slot-%04d migrate from = %s to %s failed: hash key = '%s', error = %s",
				s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, err)
			return nil, false, err
		case !moved:
			switch {
			case resp != nil:
				r.Resp = resp
				return nil, false, nil
			}
			// 没有迁移，并且没有返回时, 等待?
			return nil, true, nil
		}
	}
	r.Group = &s.refs
	r.Group.Add(1)
	return d.forward2(s, r), false, nil
}

type forwardHelper struct {
}

func (d *forwardHelper) slotsmgrt(s *Slot, hkey []byte, database int32, seed uint) error {
	m := &Request{}
	m.Multi = []*redis.Resp{
		redis.NewBulkBytes([]byte("SLOTSMGRTTAGONE")), // Slots
		redis.NewBulkBytes(s.backend.bc.host),
		redis.NewBulkBytes(s.backend.bc.port),
		redis.NewBulkBytes([]byte("3000")),
		redis.NewBulkBytes(hkey),
	}
	m.Batch = &sync.WaitGroup{}

	s.migrate.bc.BackendConn(database, seed, true).PushBack(m)

	m.Batch.Wait()

	if err := m.Err; err != nil {
		return err
	}
	switch resp := m.Resp; {
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("bad slotsmgrt resp: %s", resp.Value)
	case resp.IsInt():
		log.Debugf("slot-%04d migrate from %s to %s: hash key = %s, database = %d, resp = %s",
			s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, database, resp.Value)
		return nil
	default:
		return fmt.Errorf("bad slotsmgrt resp: should be integer, but got %s", resp.Type)
	}
}

func (d *forwardHelper) slotsmgrtExecWrapper(s *Slot, hkey []byte, database int32, seed uint, multi []*redis.Resp) (_ *redis.Resp, moved bool, _ error) {
	m := &Request{}
	m.Multi = make([]*redis.Resp, 0, 2+len(multi))
	m.Multi = append(m.Multi,
		redis.NewBulkBytes([]byte("SLOTSMGRT-EXEC-WRAPPER")),
		redis.NewBulkBytes(hkey),
	)
	m.Multi = append(m.Multi, multi...)
	m.Batch = &sync.WaitGroup{}

	s.migrate.bc.BackendConn(database, seed, true).PushBack(m)

	m.Batch.Wait()

	if err := m.Err; err != nil {
		return nil, false, err
	}
	switch resp := m.Resp; {
	case resp == nil:
		return nil, false, ErrRespIsRequired
	case resp.IsError():
		return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: %s", resp.Value)
	case resp.IsArray():
		if len(resp.Array) != 2 {
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: array.len = %d",
				len(resp.Array))
		}
		if !resp.Array[0].IsInt() || len(resp.Array[0].Value) != 1 {
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: type(array[0]) = %s, len(array[0].value) = %d",
				resp.Array[0].Type, len(resp.Array[0].Value))
		}
		switch resp.Array[0].Value[0] - '0' {
		case 0:
			return nil, true, nil
		case 1:
			return nil, false, nil
		case 2:
			return resp.Array[1], false, nil
		default:
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: [%s] %s",
				resp.Array[0].Value, resp.Array[1].Value)
		}
	default:
		return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: should be integer, but got %s", resp.Type)
	}
}

func (d *forwardHelper) forward2(s *Slot, r *Request) *BackendConn {
	var database, seed = r.Database, r.Seed16()

	// 正常状态下的slot
	if s.migrate.bc == nil && r.IsReadOnly() && len(s.replicaGroups) != 0 {
		// 如果开启了replica, 那么直接获取readonly的replica
		for _, group := range s.replicaGroups {
			var i = seed
			// 随机选择一个Replica访问, bc == nil ??
			for _ = range group {
				i = (i + 1) % uint(len(group))
				if bc := group[i].BackendConn(database, seed, false); bc != nil {
					return bc
				}
			}
		}
	}
	return s.backend.bc.BackendConn(database, seed, true)
}
