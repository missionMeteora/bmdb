package bmdb

import (
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	closers   []io.Closer
	listeners []func()
	l         sync.Mutex
)

func closeOnCrash(c io.Closer) {
	l.Lock()
	closers = append(closers, c)
	l.Unlock() // no need for defer, it's slow and not needed
}

func removeCloser(c io.Closer) {
	l.Lock()
	for i := len(closers) - 1; i >= 0; i-- {
		if c == closers[i] {
			closers = append(closers[:i], closers[i+1:]...)
			break
		}
	}
	l.Unlock() // no need for defer, it's slow and not needed
}

func RunClosers() {
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

func OnCrash(fn func()) {
	l.Lock()
	listeners = append(listeners, fn)
	l.Unlock() // no need for defer, it's slow and not needed
}

func init() {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-c
		RunClosers()
	}()
}
