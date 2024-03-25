package rediflight

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Group struct {
	timeout time.Duration
	pool    *redis.Pool
}

func NewGroup(pool *redis.Pool, timeout time.Duration) *Group {
	if timeout == 0 {
		timeout = time.Second * 20
	}
	return &Group{pool: pool, timeout: timeout}
}

type result struct {
	Value     interface{}
	ErrString string
}

func (g *Group) invokeWithRedis(fn func(conn redis.Conn) error) error {
	conn := g.pool.Get()
	defer conn.Close()
	return fn(conn)
}

func (g *Group) lock(key string, ex time.Duration) bool {
	var ok bool
	_ = g.invokeWithRedis(func(conn redis.Conn) error {
		reply, err := redis.String(conn.Do("SET", key, "1", "EX", int64(ex.Seconds()), "NX"))
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

func (g *Group) unlock(key string) {
	_ = g.invokeWithRedis(func(conn redis.Conn) error {
		ok, _ := redis.Bool(conn.Do("EXISTS", key))
		if ok {
			conn.Do("DEL", key)
		}
		return nil
	})
}

func (g *Group) publish(channel string, value interface{}, err error) error {
	r := result{
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

func (g *Group) subscribe(channel string) (value interface{}, err error) {
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
				var r result
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
