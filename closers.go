package bmdb

import (
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

var (
	closers   []io.Closer
	listeners []func()
	l         sync.Mutex
	crashed   uint32
)

func closeOnCrash(c io.Closer) {
	l.Lock()
	closers = append(closers, c)
	l.Unlock()
}

func removeCloser(c io.Closer) {
	if atomic.LoadUint32(&crashed) == 1 {
		return
	}
	l.Lock()
	for i := len(closers) - 1; i >= 0; i-- {
		if c == closers[i] {
			closers = append(closers[:i], closers[i+1:]...)
			break
		}
	}
	l.Unlock()
}

func RunClosers() {
	if atomic.LoadUint32(&crashed) == 1 {
		return
	}
	atomic.StoreUint32(&crashed, 1)
	l.Lock()
	if len(closers) > 0 {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i].Close()
		}
	}
	closers = nil
	for _, fn := range listeners {
		fn()
	}
	listeners = nil
	l.Unlock()
}

func OnExit(fn func()) {
	l.Lock()
	listeners = append(listeners, fn)
	l.Unlock()
}

func init() {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-c
		RunClosers()
		os.Exit(1)
	}()
}
