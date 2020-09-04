// Go Riemann client
//
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
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/swdunlop/raidman/proto"
)

// Wraps client state around a riemann Transport.
func WrapClient(transport Transport) *Client {
	return &Client{transport: transport}
}

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
	Attributes  map[string]string
	Description string
}

// Dial establishes a connection to a Riemann server using the specified network
// transport.  Supported transports: "tcp", "tcp4", "tcp6", "udp", "udp4", and "udp6".
func Dial(network, addr string) (*Client, error) {
	return DialTimeout(network, addr, 0)
}

// DialTimeout establishes a connection to a Riemann server, as described in Dial.  If
// timeout is nonzero, the attempt will give up earlier than the OS imposed limit.
func DialTimeout(network, addr string, timeout time.Duration) (*Client, error) {
	c := new(Client)

	switch network {
	case "udp", "udp4", "udp6":
		rwc, err := net.DialTimeout(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		c.transport = &UdpTransport{rwc}

	case "tcp", "tcp4", "tcp6":
		rwc, err := net.DialTimeout(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		c.transport = &TcpTransport{rwc}

	default:
		return nil, fmt.Errorf("dial %q: unsupported network %q", network, network)
	}

	return c, nil
}

// Provide a means for users who can't use Dial, or don't want a Client, to
// build their own Transport; TCP version.
func WrapTCPConn(conn *net.TCPConn) (TcpTransport) {
	return TcpTransport{conn}
}

// Provide a means for users who can't use Dial, or don't want a Client, to
// build their own Transport; UDP version.
func WrapUDPConn(conn *net.UDPConn) (UdpTransport) {
	return UdpTransport{conn}
}

// eventToPbEvent translates a raidman.Event into a lowlevel protocol Event.
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

	if len(event.Attributes) < 1 {
		return &e, nil
	}

	attrs := make([]*proto.Attribute, 0, len(event.Attributes))
	for k, v := range event.Attributes {
		attr := &proto.Attribute{Key: &k}
		if v != `` {
			attr.Value = &v
		}
		attrs = append(attrs, attr)
	}

	e.Attributes = attrs
	return &e, nil
}

// assignEventMetric updates the "Metric" fields of the underlying protobuf Event type based on the type of value provided
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

// isUsingUdp checks to see if the client is using a UDP transport; the UDP transport in Riemann is only partially functional, providing one way sends of events without waiting for acknowledgement
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

// pbEventsToEvents converts a slice of low level protobuf events into raidman events.
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
		e.Attributes = make(map[string]string)
		pbAttributes := event.GetAttributes()
		for _, pbAttribute := range pbAttributes {
			val := ``
			if pbAttribute.Value != nil {
				val = *pbAttribute.Value
			}
			e.Attributes[*pbAttribute.Key] = val
		}

		events = append(events, e)
	}

	return events
}

// A Transport provides a means of writing and reading messages to a Riemann client or server.  Transport is implemented by TcpTransport and UdpTransport, and wrapped by Clients.  Transports should not worry about synchronization -- Clients use a mutex for this purpose.
type Transport interface {
	WriteMsg(msg *proto.Msg) error
	ReadMsg() (*proto.Msg, error)
	Close() error
}

// TcpTransport implements Transport using an underlying TCP connection, with each protobuf message prefaced with a big-endian 32-bit length.
type TcpTransport struct {
	rwc io.ReadWriteCloser
}

var _ Transport = &TcpTransport{}

// WriteMsg is an implementation of Transport.
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

// ReadMsg is an implementation of Transport.
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

// Close is an implementation of io.Closer and Transport that forwards to the underlying connection.
func (tp *TcpTransport) Close() error {
	return tp.rwc.Close()
}

// UdpTransport implements Transport using an underlying UDP connection, with each protobuf message occuping one datagram.  Note that this limits the maximum message length to ~63k, due to the limit in UDP datagram sizes.  UdpTransports should only be used for cases of very high frequency metrics with limited descriptions and attributes.
type UdpTransport struct {
	rwc io.ReadWriteCloser
}

var _ Transport = &UdpTransport{}

// WriteMsg is an implementation of Transport.
func (tp *UdpTransport) WriteMsg(msg *proto.Msg) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tp.rwc.Write(data)
	return err
}

// ReadMsg is an implementation of Transport.
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

// Close is an implementation of io.Closer and Transport that forwards to the underlying socket.
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
