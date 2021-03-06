package raidman

import (
	"testing"
)

func TestTCP(t *testing.T) {
	c, err := Dial("tcp", "localhost:5555")
	if err != nil {
		t.Fatal(err.Error())
	}
	var event = &Event{
		State:      "success",
		Host:       "raidman",
		Service:    "tcp",
		Metric:     42,
		Ttl:        1,
		Tags:       []string{"tcp", "test", "raidman"},
		Attributes: map[string]string{"type": "test"},
	}

	err = c.Send(event)
	if err != nil {
		t.Error(err.Error())
	}

	events, err := c.Query("tagged \"test\"")
	if err != nil {
		t.Error(err.Error())
	}

	if len(events) < 1 {
		t.Error("Submitted event not found")
	}

	testAttributeExists := false
	for _, event := range events {
		if val, ok := event.Attributes["type"]; ok && val == "test" {
			testAttributeExists = true
		}
	}
	if !testAttributeExists {
		t.Error("Attribute \"type\" is missing")
	}

	c.Close()
}

func TestUDP(t *testing.T) {
	c, err := Dial("udp", "localhost:5555")
	if err != nil {
		t.Fatal(err.Error())
	}
	var event = &Event{
		State:   "warning",
		Host:    "raidman",
		Service: "udp",
		Metric:  3.4,
		Ttl:     10.7,
	}

	err = c.Send(event)
	if err != nil {
		t.Error(err.Error())
	}
	c.Close()
}

func TestTCPWithoutHost(t *testing.T) {
	c, err := Dial("tcp", "localhost:5555")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	var event = &Event{
		State:   "success",
		Service: "tcp-host-not-set",
		Ttl:     5,
	}

	err = c.Send(event)
	if err != nil {
		t.Error(err.Error())
	}

	events, err := c.Query("service = \"tcp-host-not-set\"")
	if err != nil {
		t.Error(err.Error())
	}

	if len(events) < 1 {
		t.Error("Submitted event not found")
	}

	for _, e := range events {
		if e.Host == "" {
			t.Error("Default host name is not set")
		}
	}
}

func BenchmarkTCP(b *testing.B) {
	c, err := Dial("tcp", "localhost:5555")

	var event = &Event{
		State:   "good",
		Host:    "raidman",
		Service: "benchmark",
	}

	if err == nil {
		for i := 0; i < b.N; i++ {
			c.Send(event)
		}
	}
	c.Close()
}

func BenchmarkUDP(b *testing.B) {
	c, err := Dial("udp", "localhost:5555")

	var event = &Event{
		State:   "good",
		Host:    "raidman",
		Service: "benchmark",
	}

	if err == nil {
		for i := 0; i < b.N; i++ {
			c.Send(event)
		}
	}
	c.Close()
}

func BenchmarkConcurrentTCP(b *testing.B) {
	c, err := Dial("tcp", "localhost:5555")

	var event = &Event{
		Host:    "raidman",
		Service: "tcp_concurrent",
		Tags:    []string{"concurrent", "tcp", "benchmark"},
	}

	ch := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		go func(metric int) {
			event.Metric = metric
			err = c.Send(event)
			ch <- i
		}(i)
	}
	<-ch

	c.Close()
}

func BenchmarkConcurrentUDP(b *testing.B) {
	c, err := Dial("udp", "localhost:5555")

	var event = &Event{
		Host:    "raidman",
		Service: "udp_concurrent",
		Tags:    []string{"concurrent", "udp", "benchmark"},
	}

	ch := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		go func(metric int) {
			event.Metric = metric
			err = c.Send(event)
			ch <- i
		}(i)
	}
	<-ch

	c.Close()
}
