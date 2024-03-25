package rediflight

import (
	"fmt"
)

func (g *Group) Do(key string, fn func() (interface{}, error)) (v interface{}, err error, shared bool) {
	timeout := g.timeout
	lockKey := fmt.Sprintf("singleflight_lock_%s", key)
	channel := fmt.Sprintf("singleflight_result_%s", key)
	// Attempt to acquire the lock.
	// If unsuccessful, subscribe and wait for the outcome.
	if !g.lock(lockKey, timeout) {
		shared = true
		v, err = g.subscribe(channel)
		return
	}
	defer g.unlock(lockKey)
	// Execute the function and publish the outcome if successful.
	v, err = fn()
	_ = g.publish(channel, v, err)
	return
}
