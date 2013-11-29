// Go Riemann client
//
// TODO(cloudhead): pbEventsToEvents should parse Attributes field
package raidman

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	pb "code.google.com/p/goprotobuf/proto"
	"github.com/swdunlop/raidman/proto"
)

type network interface {
	Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error)
}

type tcp struct{}

type udp struct{}

// Client represents a connection to a Riemann server
type Client struct {
	sync.Mutex
	net        network
	connection net.Conn
}

// An Event represents a single Riemann event
type Event struct {
	Ttl         float32
	Time        int64
	Tags        []string
	Host        string // Defaults to os.Hostname()
	State       string
	Service     string
	Metric      interface{} // Could be Int, Float32, Float64
	Attributes  map[string]interface{}
	Description string
}

// Dial establishes a connection to a Riemann server at addr, on the network
// netwrk.
//
// Known networks are "tcp", "tcp4", "tcp6", "udp", "udp4", and "udp6".
func Dial(netwrk, addr string) (c *Client, err error) {
	c = new(Client)

	var cnet network
	switch netwrk {
	case "tcp", "tcp4", "tcp6":
		cnet = new(tcp)
	case "udp", "udp4", "udp6":
		cnet = new(udp)
	default:
		return nil, fmt.Errorf("dial %q: unsupported network %q", netwrk, netwrk)
	}

	c.net = cnet
	c.connection, err = net.Dial(netwrk, addr)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (network *tcp) Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
	msg := &proto.Msg{}
	data, err := pb.Marshal(message)
	if err != nil {
		return msg, err
	}
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
		return msg, err
	}
	if _, err = conn.Write(b.Bytes()); err != nil {
		return msg, err
	}
	if _, err = conn.Write(data); err != nil {
		return msg, err
	}
	var header uint32
	if err = binary.Read(conn, binary.BigEndian, &header); err != nil {
		return msg, err
	}
	response := make([]byte, header)
	if err = readFully(conn, response); err != nil {
		return msg, err
	}
	if err = pb.Unmarshal(response, msg); err != nil {
		return msg, err
	}
	if msg.GetOk() != true {
		return msg, errors.New(msg.GetError())
	}
	return msg, nil
}

func readFully(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}

func (network *udp) Send(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
	data, err := pb.Marshal(message)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write(data); err != nil {
		return nil, err
	}

	return nil, nil
}

func eventToPbEvent(event *Event) (*proto.Event, error) {
	var e proto.Event

	h := event.Host
	if event.Host == "" {
		h, _ = os.Hostname()
	}
	e.Host = &h

	if event.State != "" {
		e.State = &event.State
	}
	if event.Service != "" {
		e.Service = &event.Service
	}
	if event.Description != "" {
		e.Description = &event.Description
	}
	if event.Ttl != 0 {
		e.Ttl = &event.Ttl
	}
	if event.Time != 0 {
		e.Time = &event.Time
	}
	if len(event.Tags) > 0 {
		e.Tags = event.Tags
	}

	err := assignEventMetric(&e, event.Metric)
	if err != nil {
		return nil, err
	}

	attrs := make([]proto.Attribute, len(event.Attributes))
	i := 0
	for k, v := range event.Attributes {
		switch x := v.(type) {
		case string:
			attrs[i].Key = &k
			attrs[i].Value = &x
		case bool:
			if x {
				attrs[i].Key = &k
			}
		default:
			return nil, fmt.Errorf("Attribute %v has invalid type (type %T)", k, v)
		}
		i++
	}

	return &e, nil
}

func assignEventMetric(e *proto.Event, v interface{}) error {
	switch x := v.(type) {
	case nil:
		// do nothing; an event without a metric is legitimate
	case int:
		i := int64(x)
		e.MetricSint64 = &i
	case int32:
		i := int64(x)
		e.MetricSint64 = &i
	case int64:
		e.MetricSint64 = &x
	case float32:
		e.MetricF = &x
	case float64:
		e.MetricD = &x
	default:
		return fmt.Errorf("Metric of invalid type (type %T)", v)
	}
	return nil
}

func pbEventsToEvents(pbEvents []*proto.Event) []Event {
	var events []Event

	for _, event := range pbEvents {
		e := Event{
			State:       event.GetState(),
			Service:     event.GetService(),
			Host:        event.GetHost(),
			Description: event.GetDescription(),
			Ttl:         event.GetTtl(),
			Time:        event.GetTime(),
			Tags:        event.GetTags(),
		}
		if event.MetricF != nil {
			e.Metric = event.GetMetricF()
		} else if event.MetricD != nil {
			e.Metric = event.GetMetricD()
		} else {
			e.Metric = event.GetMetricSint64()
		}

		events = append(events, e)
	}

	return events
}

// Send sends an event to Riemann
func (c *Client) Send(event *Event) error {
	e, err := eventToPbEvent(event)
	if err != nil {
		return err
	}
	message := &proto.Msg{}
	message.Events = append(message.Events, e)
	c.Lock()
	defer c.Unlock()
	_, err = c.net.Send(message, c.connection)
	if err != nil {
		return err
	}

	return nil
}

// Query returns a list of events matched by query
func (c *Client) Query(q string) ([]Event, error) {
	switch c.net.(type) {
	case *udp:
		return nil, errors.New("Querying over UDP is not supported")
	}
	query := &proto.Query{}
	query.String_ = pb.String(q)
	message := &proto.Msg{}
	message.Query = query
	c.Lock()
	defer c.Unlock()
	response, err := c.net.Send(message, c.connection)
	if err != nil {
		return nil, err
	}
	return pbEventsToEvents(response.GetEvents()), nil
}

// Close closes the connection to Riemann
func (c *Client) Close() {
	c.Lock()
	c.connection.Close()
	c.Unlock()
}
