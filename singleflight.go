package rediflight

import (
	"fmt"
)

func (g *Group[T]) Do(key string, fn func() (T, error)) (v T, err error, shared bool) {
	lockKey := fmt.Sprintf("sf:lock:%s", key)
	channel := fmt.Sprintf("sf:chan:%s", key)
	// Attempt to acquire the lock.
	// If unsuccessful, subscribe and wait for the outcome.
	if !g.lock(lockKey) {
		shared = true
		v, err = g.subscribe(channel)
		return
	}
	defer g.unlock(lockKey)
	// Execute the function and publish the outcome if successful.
	v, err = fn()
	num, _ := g.numsub(channel)
	if num > 0 {
		_ = g.publish(channel, v, err)
	}
	return
}

func (g *Group[T]) DoChan(key string, fn func() (T, error)) <-chan Result[T] {
	ch := make(chan Result[T], 1)
	lockKey := fmt.Sprintf("sf:lock:%s", key)
	channel := fmt.Sprintf("sf:chan:%s", key)
	// Attempt to acquire the lock.
	// If unsuccessful, subscribe and wait for the outcome.
	if !g.lock(lockKey) {
		go func() {
			v, err := g.subscribe(channel)
			ch <- Result[T]{
				Val:  v,
				Err:    err,
				Shared: true,
			}
		}()
		return ch
	}
	go func() {
		defer g.unlock(lockKey)
		// Execute the function and publish the outcome if successful.
		v, err := fn()
		ch <- Result[T]{
			Val:  v,
			Err:    err,
			Shared: false,
		}
		num, _ := g.numsub(channel)
		if num > 0 {
			_ = g.publish(channel, v, err)
		}
	}()
	return ch
}
