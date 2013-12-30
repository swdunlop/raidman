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

// Client represents a connection to a Riemann server
type Client struct {
	sync.Mutex
	transport Transport
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
func Dial(network, addr string) (*Client, error) {
	client := &Client{}
	rwc, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	switch network {
	case "udp", "udp4", "udp6":
		client.transport = &UdpTransport{rwc}
	default:
		client.transport = &TcpTransport{rwc}
	}

	return client, nil
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

// Send sends an event to Riemann and waits for an acknowledgement, if a TCP client was used.
func (c *Client) Send(event *Event) error {
	c.Lock()
	defer c.Unlock()

	evt, err := eventToPbEvent(event)
	if err != nil {
		return err
	}

	req := &proto.Msg{Events: []*proto.Event{evt}}
	err = c.transport.WriteMsg(req)
	switch {
	case err != nil:
		return err
	case c.isUsingUdp():
		return nil
	}

	_, err = c.transport.ReadMsg()
	return err
}

// Query returns a list of events matched by query; Query cannot be used with UDP clients.
func (c *Client) Query(q string) ([]Event, error) {
	c.Lock()
	defer c.Unlock()

	if c.isUsingUdp() {
		return nil, errors.New("Querying over UDP is not supported")
	}

	req := &proto.Msg{Query: &proto.Query{String_: pb.String(q)}}
	err := c.transport.WriteMsg(req)
	if err != nil {
		return nil, err
	}

	rsp, err := c.transport.ReadMsg()
	if err != nil {
		return nil, err
	}
	return pbEventsToEvents(rsp.GetEvents()), nil
}

func (c *Client) isUsingUdp() bool {
	_, yes := c.transport.(*UdpTransport)
	return yes
}

// Close closes the connection to Riemann
func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	return c.transport.Close()
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

type Transport interface {
	WriteMsg(msg *proto.Msg) error
	ReadMsg() (*proto.Msg, error)
	Close() error
}

type TcpTransport struct {
	rwc io.ReadWriteCloser
}

var _ Transport = &TcpTransport{}

func (tp *TcpTransport) WriteMsg(msg *proto.Msg) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return err
	}

	sz := len(data)
	buf := bytes.NewBuffer(make([]byte, 0, sz+4))
	binary.Write(buf, binary.BigEndian, uint32(sz))
	buf.Write(data)
	_, err = tp.rwc.Write(buf.Bytes())
	return err
}

func (tp *TcpTransport) ReadMsg() (*proto.Msg, error) {
	var sz uint32
	err := binary.Read(tp.rwc, binary.BigEndian, &sz)
	if err != nil {
		return nil, err
	}
	p := make([]byte, sz)
	err = readFully(tp.rwc, p)
	if err != nil {
		return nil, err
	}

	msg := new(proto.Msg)
	err = pb.Unmarshal(p, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (tp *TcpTransport) Close() error {
	return tp.rwc.Close()
}

type UdpTransport struct {
	rwc io.ReadWriteCloser
}

var _ Transport = &UdpTransport{}

func (tp *UdpTransport) WriteMsg(msg *proto.Msg) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tp.rwc.Write(data)
	return err
}

func (tp *UdpTransport) ReadMsg() (*proto.Msg, error) {
	p := make([]byte, 1<<16) // max udp data size is less than 64k
	n, err := tp.rwc.Read(p)
	if err != nil {
		return nil, err
	}
	p = p[:n]

	msg := new(proto.Msg)
	err = pb.Unmarshal(p, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (tp *UdpTransport) Close() error {
	return tp.rwc.Close()
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
