package rediflight

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Group[T any] struct {
	timeout time.Duration
	pool    *redis.Pool
}

func NewGroup[T any](pool *redis.Pool, timeout time.Duration) *Group[T] {
	// TODO: use the adapter for various of client implementations.
	if timeout == 0 {
		timeout = time.Second * 20
	}
	return &Group[T]{pool: pool, timeout: timeout}
}

type result[T any] struct {
	Value     T
	ErrString string
}

func (g *Group[T]) invokeWithRedis(fn func(conn redis.Conn) error) error {
	conn := g.pool.Get()
	defer conn.Close()
	return fn(conn)
}

func (g *Group[T]) lock(key string) bool {
	var ok bool
	_ = g.invokeWithRedis(func(conn redis.Conn) error {
		reply, err := redis.String(
			conn.Do("SET", key, "1", "EX", int64(g.timeout.Seconds()), "NX"),
		)
		if err != nil {
			return err
		}
		ok = strings.ToUpper(reply) == "OK"
		if ok {
			conn.Flush()
		}
		return nil
	})
	return ok
}

func (g *Group[T]) unlock(key string) {
	_ = g.invokeWithRedis(func(conn redis.Conn) error {
		ok, _ := redis.Bool(conn.Do("EXISTS", key))
		if ok {
			conn.Do("DEL", key)
		}
		return nil
	})
}

func (g *Group[T]) numsub(channel string) (num int64, err error) {
	err = g.invokeWithRedis(func(conn redis.Conn) error {
		reply, err := redis.Values(conn.Do("PUBSUB", "NUMSUB", channel))
		if err != nil {
			return err
		}
		if len(reply) < 2 {
			return nil
		}
		num = reply[1].(int64)
		return nil
	})
	return
}

func (g *Group[T]) publish(channel string, value T, err error) error {
	r := result[T]{
		Value: value,
	}
	if err != nil {
		r.ErrString = err.Error()
	}
	buf, _ := json.Marshal(r)
	return g.invokeWithRedis(func(conn redis.Conn) error {
		_, err := conn.Do("PUBLISH", channel, buf)
		return err
	})
}

func (g *Group[T]) subscribe(channel string) (value T, err error) {
	err = g.invokeWithRedis(func(conn redis.Conn) error {
		ps := redis.PubSubConn{Conn: conn}
		ps.Subscribe(redis.Args{}.AddFlat(channel)...)
		defer ps.Unsubscribe()
		for {
			switch n := ps.ReceiveWithTimeout(g.timeout).(type) {
			case error:
				return n
			case redis.Subscription:
				continue
			case redis.Message:
				var r result[T]
				err = json.Unmarshal(n.Data, &r)
				if err != nil {
					return err
				}
				if r.ErrString != "" {
					return errors.New(r.ErrString)
				}
				value = r.Value
				return nil
			}
		}
	})
	return
}
