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
