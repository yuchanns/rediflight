package rediflight_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/yuchanns/rediflight"
)

var (
	pool *redis.Pool
)

func TestMain(m *testing.M) {
	s, _ := miniredis.Run()
	pool = &redis.Pool{
		MaxIdle:     10,
		MaxActive:   20,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr())
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	m.Run()
}

func TestDo(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	g := rediflight.NewGroup(pool, time.Second*20)
	expected := "bar"
	v, err, _ := g.Do("key", func() (interface{}, error) {
		return expected, nil
	})
	assert.Nil(err)
	assert.Equal(fmt.Sprintf("%v (%T)", expected, expected), fmt.Sprintf("%v (%T)", v, v))
}

func TestDoErr(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	g := rediflight.NewGroup(pool, time.Second*20)
	someErr := errors.New("Some error")
	v, err, _ := g.Do("key", func() (interface{}, error) {
		return nil, someErr
	})
	assert.NotNil(err)
	assert.Nil(v)
}

func TestDoDupSuppress(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	g := rediflight.NewGroup(pool, time.Second*20)

	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func() (interface{}, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for i := 0; i < n; i++ {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, err, _ := g.Do("key", fn)
			assert.Nil(err)
			assert.Equal("bar", v.(string))
		}()
	}
	wg1.Wait()
	c <- "bar"
	wg2.Wait()
	got := atomic.LoadInt32(&calls)
	assert.False(
		got <= 0 || got >= n,
		fmt.Sprintf("number of calls = %d; want over 0 and less than %d", got, n))
	close(c)
}
