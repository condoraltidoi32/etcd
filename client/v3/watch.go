// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Event struct {
	Type int32
	Kv   *KeyValue
}

type KeyValue struct {
	Key            []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Value          []byte
	Lease          int64
}

type WatchResponse struct {
	Header          ResponseHeader
	Events          []*Event
	CompactRevision int64
	Canceled        bool
	Created         bool
	closeErr        error
}

func (wr *WatchResponse) Err() error {
	if wr == nil {
		return nil
	}
	if wr.closeErr != nil {
		return wr.closeErr
	}
	if wr.Canceled {
		return errors.New("etcdserver: watch canceled")
	}
	return nil
}

type ResponseHeader struct {
	ClusterId uint64
	MemberId  uint64
	Revision  int64
	RaftTerm  uint64
}

type WatchChan <-chan WatchResponse

type Watcher interface {
	Watch(ctx context.Context, key string, opts ...OpOption) WatchChan
	Close() error
	RequestProgress(ctx context.Context) error
}

type OpOption func(*watchOption)

type watchOption struct {
	rev      int64
	prevKV   bool
	progress bool
	filters  []int32
}

func WithRev(rev int64) OpOption {
	return func(op *watchOption) {
		op.rev = rev
	}
}

func WithPrevKV() OpOption {
	return func(op *watchOption) {
		op.prevKV = true
	}
}

type watcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	streams map[*watchGrpcStream]struct{}
	closed  bool
}

type watcherStream struct {
	id       int64
	key      string
	opts     watchOption
	ctx      context.Context
	cancel   context.CancelFunc
	outc     chan WatchResponse
	recving  chan struct{}
	initReq  *watchRequest
	lastRev  int64
	resuming bool
	closed   bool
	mu       sync.Mutex
}

type watchRequest struct {
	ctx    context.Context
	key    string
	opts   watchOption
	stream *watcherStream
}

type watchGrpcStream struct {
	owner *watcher
	ctx   context.Context
	cancel context.CancelFunc

	substreams map[int64]*watcherStream
	reqc       chan *watchRequest
	respc      chan *WatchResponse

	mu        sync.RWMutex
	closed    bool
	resuming  bool
	closingc  chan struct{}
	done      chan struct{}
}

func NewWatcher(ctx context.Context) *watcher {
	wctx, cancel := context.WithCancel(ctx)
	w := &watcher{
		ctx:     wctx,
		cancel:  cancel,
		streams: make(map[*watchGrpcStream]struct{}),
	}
	return w
}

func (w *watcher) Watch(ctx context.Context, key string, opts ...OpOption) WatchChan {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		ch := make(chan WatchResponse, 1)
		ch <- WatchResponse{Canceled: true, closeErr: errors.New("watcher closed")}
		close(ch)
		return ch
	}

	var opt watchOption
	for _, o := range opts {
		o(&opt)
	}

	wsCtx, wsCancel := context.WithCancel(ctx)
	outc := make(chan WatchResponse)
	ws := &watcherStream{
		key:     key,
		opts:    opt,
		ctx:     wsCtx,
		cancel:  wsCancel,
		outc:    outc,
		recving: make(chan struct{}),
	}

	var stream *watchGrpcStream
	for s := range w.streams {
		stream = s
		break
	}
	if stream == nil {
		stream = w.newWatchGrpcStream()
		w.streams[stream] = struct{}{}
	}

	req := &watchRequest{
		ctx:    wsCtx,
		key:    key,
		opts:   opt,
		stream: ws,
	}
	ws.initReq = req

	go func() {
		select {
		case <-wsCtx.Done():
			stream.closeSubstream(ws, wsCtx.Err())
		case <-w.ctx.Done():
			stream.closeSubstream(ws, w.ctx.Err())
		case <-stream.done:
		}
	}()

	select {
	case stream.reqc <- req:
	case <-wsCtx.Done():
		stream.closeSubstream(ws, wsCtx.Err())
	case <-w.ctx.Done():
		stream.closeSubstream(ws, w.ctx.Err())
	}

	return outc
}

func (w *watcher) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.cancel()
	for s := range w.streams {
		s.close()
	}
	return nil
}

func (w *watcher) RequestProgress(ctx context.Context) error {
	return nil
}

func (w *watcher) newWatchGrpcStream() *watchGrpcStream {
	sCtx, sCancel := context.WithCancel(w.ctx)
	wgs := &watchGrpcStream{
		owner:      w,
		ctx:        sCtx,
		cancel:     sCancel,
		substreams: make(map[int64]*watcherStream),
		reqc:       make(chan *watchRequest, 100),
		respc:      make(chan *WatchResponse, 100),
		closingc:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	go wgs.run()
	return wgs
}

func (w *watchGrpcStream) run() {
	defer close(w.done)
	for {
		select {
		case req := <-w.reqc:
			if req == nil {
				continue
			}
			if req.ctx.Err() != nil {
				w.closeSubstream(req.stream, req.ctx.Err())
				continue
			}
			w.mu.Lock()
			w.substreams[int64(len(w.substreams)+1)] = req.stream
			w.mu.Unlock()
		case resp := <-w.respc:
			if resp == nil {
				continue
			}
			w.mu.RLock()
			for _, ws := range w.substreams {
				select {
				case ws.outc <- *resp:
				case <-ws.ctx.Done():
					go w.closeSubstream(ws, ws.ctx.Err())
				case <-w.ctx.Done():
					go w.closeSubstream(ws, w.ctx.Err())
				}
			}
			w.mu.RUnlock()
		case <-w.ctx.Done():
			w.mu.Lock()
			for _, ws := range w.substreams {
				w.closeSubstreamLocked(ws, w.ctx.Err())
			}
			w.mu.Unlock()
			return
		case <-w.closingc:
			return
		}
	}
}

func (w *watchGrpcStream) resume() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.ctx.Err() != nil {
		return
	}

	for id, ws := range w.substreams {
		// Check if the watch context has expired during disconnect/recovery
		if ws.ctx.Err() != nil {
			w.closeSubstreamLocked(ws, ws.ctx.Err())
			delete(w.substreams, id)
			continue
		}

		// Re-subscribe valid sub-watches with non-blocking checks
		req := &watchRequest{
			ctx:    ws.ctx,
			key:    ws.key,
			opts:   ws.opts,
			stream: ws,
		}

		select {
		case w.reqc <- req:
		case <-ws.ctx.Done():
			w.closeSubstreamLocked(ws, ws.ctx.Err())
			delete(w.substreams, id)
		case <-w.ctx.Done():
			return
		default:
			go func(subReq *watchRequest, subWs *watcherStream, subId int64) {
				select {
				case w.reqc <- subReq:
				case <-subWs.ctx.Done():
					w.closeSubstream(subWs, subWs.ctx.Err())
				case <-w.ctx.Done():
				}
			}(req, ws, id)
		}
	}
}

func (w *watchGrpcStream) closeSubstream(ws *watcherStream, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeSubstreamLocked(ws, err)
}

func (w *watchGrpcStream) closeSubstreamLocked(ws *watcherStream, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.closed {
		return
	}
	ws.closed = true
	ws.cancel()

	for id, s := range w.substreams {
		if s == ws {
			delete(w.substreams, id)
			break
		}
	}

	if err == nil {
		err = errors.New("watch stream closed")
	}

	select {
	case ws.outc <- WatchResponse{Canceled: true, closeErr: err}:
	case <-time.After(50 * time.Millisecond):
	default:
	}
	close(ws.outc)
}

func (w *watchGrpcStream) close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.cancel()
	close(w.closingc)
	for _, ws := range w.substreams {
		w.closeSubstreamLocked(ws, errors.New("watch grpc stream closed"))
	}
	w.mu.Unlock()
}
