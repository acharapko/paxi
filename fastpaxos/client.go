package fastpaxos

import (
	"errors"
	"strconv"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math/rand"
)

// Client overwrites read operation for Paxos
type Client struct {
	*paxi.HTTPClient
	ballot paxi.Ballot
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

func (c *Client) IncrementCID() int {
	if rand.Intn(100) >= paxi.GetConfig().Benchmark.CIDConflicts {
		c.HTTPClient.IncrementCID()
	}
	return c.CID
}

func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	val, _, err := c.RESTGetFastPaxos(key)
	return val, err
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	_, _, err := c.RESTPutFastPaxos(key, value)
	return err
}


func (c *Client) RESTPutFastPaxos(key paxi.Key, value paxi.Value) (paxi.Value, map[string]string, error) {
	leaderReply := make(chan bool)
	var val paxi.Value
	var metadata map[string]string
	var leaderErr error
	cid := c.IncrementCID()
	for id := range c.HTTP {
		log.Debugf("RestPut key %v to node %v with cid %d", key, id, cid)
		go func(to paxi.ID) {
			v, m, e := c.RESTPutWithCid(to, key, value, cid)
			if e != nil {
				log.Error(e)
			}
			b, berr := strconv.ParseBool(m[HTTPHeaderLeader])
			if berr != nil {
				log.Error(e)
			}
			if b {
				log.Debugf("Found Leader Reply for PUT key %v metadata: %v", key, m)
				val = v
				metadata = m
				leaderErr = e
				if m[HTTPLostSlot] == "true" {
					leaderErr = errors.New("command Lost and timeout")
					log.Errorf("Command Lost and timeout for key %v", key)
				}
				leaderReply <- true
			}
		}(id)
	}

	<-leaderReply
	log.Debugf("PUT DONE. key: %v, val: %v, err: %v", key, val, leaderErr)
	return val, metadata, leaderErr
}

func (c *Client) RESTGetFastPaxos(key paxi.Key) (paxi.Value, map[string]string, error) {
	leaderReply := make(chan bool)
	var val paxi.Value
	var metadata map[string]string
	var leaderErr error
	cid := c.IncrementCID()
	for id := range c.HTTP {
		log.Debugf("RestGet key %v to node %v with cid %d", key, id, cid)
		go func(to paxi.ID) {
			v, m, e := c.RESTGetWithCid(to, key, cid)
			if e != nil {
				log.Error(e)
			}
			b, berr := strconv.ParseBool(m[HTTPHeaderLeader])
			if berr != nil {
				log.Error(e)
			}
			if b {
				log.Debugf("Found Leader Reply %s for GET key %v, metadata: %v", v, key, m)
				val = v
				metadata = m
				leaderErr = e
				if m[HTTPLostSlot] == "true" {
					leaderErr = errors.New("command Lost and timeout")
					log.Errorf("Command Lost and timeout for key %v on GET", key)
				}
				leaderReply <- true
			}
		}(id)
	}

	<-leaderReply
	log.Debugf("GET DONE. key: %v, val: %v, err: %v", key, val, leaderErr)
	return val, metadata, leaderErr
}