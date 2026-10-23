// YAML configuration for the traffic generator.
//
// The generator runs in two deployment modes from the same binary:
//
//  1. in-cluster  : "discover" is set (or left empty for the built-in
//     default) and the UPF fabric is discovered through each node's
//     /api/teids control endpoint over the headless StatefulSet DNS.
//  2. external    : "upf" lists explicit endpoints (host may be a plain IP
//     plus NodePort, e.g. 192.0.2.10:32152). Discovery is skipped, so the
//     binary runs anywhere that can reach the fabric.
//
// Nothing in the file is cluster-specific: hosts, ports, rates and the
// RFC-style test profiles are all data.
package traffic

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// UpfSpec is one explicit UPF endpoint (external mode).
type UpfSpec struct {
	Host string `yaml:"host"` // FQDN or IP

	Node int `yaml:"node"` // node identity (0-255), maps into the F-TEID
	// Sessions is the node's session space as advertised by /api/teids
	// (defaults to MaxSessions). The node's top sequence is reserved for the
	// RFC 2544 bench session.
	Sessions int `yaml:"sessions"`
	// Control is the UPF's control-lite HTTP port on the same host (where
	// /api/teids and /api/dn-target live). Required for the RFC 2544 bench
	// in external mode; zero = bench unavailable on this node.
	Control int      `yaml:"control"`
	Ports   UpfPorts `yaml:"ports"`
}

// UpfPorts are the GTP-U data ports of one UPF endpoint. Zero = not offered.
type UpfPorts struct {
	N3 int `yaml:"n3"`
	N5 int `yaml:"n5"`
	N9 int `yaml:"n9"`
}

// DiscoverSpec points the generator at the in-cluster control endpoints.
type DiscoverSpec struct {
	DNS         string `yaml:"dns"`   // peer DNS template, {n} = ordinal
	Nodes       int    `yaml:"nodes"` // total UPF nodes
	MetricsPort int    `yaml:"metrics_port"`
}

// Config is the full generator configuration.
type Config struct {
	Name        string        `yaml:"name"`
	IntervalMS  int           `yaml:"interval_ms"`  // token-bucket tick
	PPS         float64       `yaml:"pps"`          // per-flow target pps at 100%
	Throttle    float64       `yaml:"throttle"`     // 0-100, initial output level
	MaxSessions int           `yaml:"max_sessions"` // per UPF node
	Ups         []UpfSpec     `yaml:"upf"`          // explicit endpoints (external mode)
	Discover    *DiscoverSpec `yaml:"discover"`     // in-cluster discovery (nil = default)
	Duration    string        `yaml:"duration"`     // "1h" / "" = run forever
	Bench       BenchProfile  `yaml:"bench"`        // RFC 2544 benchmark profile defaults
}

// Defaults are applied after unmarshalling so a minimal file works.
func (c *Config) setDefaults() {
	if c.Name == "" {
		c.Name = "trafficgen"
	}
	if c.IntervalMS <= 0 {
		c.IntervalMS = 20
	}
	if c.PPS <= 0 {
		c.PPS = 100
	}
	if c.Throttle <= 0 {
		c.Throttle = 100
	}
	if c.Throttle > 100 {
		c.Throttle = 100
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = 32
	}
	if c.Discover == nil {
		c.Discover = &DiscoverSpec{
			DNS:         "upf-{n}.upf-hs.telco.svc.cluster.local",
			Nodes:       2,
			MetricsPort: 8080,
		}
	}
	if c.Discover.DNS == "" {
		c.Discover.DNS = "upf-{n}" // same-network alias mode (podman local)
	}
	if c.Discover.Nodes <= 0 {
		c.Discover.Nodes = 2
	}
	if c.Discover.MetricsPort <= 0 {
		c.Discover.MetricsPort = 8080
	}
}

// Interval returns the tick period.
func (c *Config) Interval() time.Duration {
	return time.Duration(c.IntervalMS) * time.Millisecond
}

// ParseDuration parses the optional run duration ("" = forever).
func (c *Config) ParseDuration() (time.Duration, error) {
	if c.Duration == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Duration)
	if err != nil {
		return 0, fmt.Errorf("traffic: bad duration %q: %w", c.Duration, err)
	}
	return d, nil
}

// External reports whether explicit endpoints are configured (no discovery).
func (c *Config) External() bool { return len(c.Ups) > 0 }

// LoadConfig reads and validates one YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("traffic: read config: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("traffic: parse config: %w", err)
	}
	if len(c.Ups) == 0 && c.Discover == nil {
		return nil, fmt.Errorf("traffic: config must set either upf endpoints or discover")
	}
	c.setDefaults()
	if c.External() {
		nodes := 0
		for i := range c.Ups {
			u := &c.Ups[i]
			if u.Host == "" {
				return nil, fmt.Errorf("traffic: upf[%d]: empty host", i)
			}
			if u.Node >= 256 {
				return nil, fmt.Errorf("traffic: upf[%d]: node %d out of range 0-255", i, u.Node)
			}
			if u.Sessions <= 0 {
				u.Sessions = c.MaxSessions
			}
			if u.Node >= nodes {
				nodes = u.Node + 1
			}
		}
	} else if c.Discover.Nodes < 1 {
		return nil, fmt.Errorf("traffic: discover.nodes must be >= 1")
	}
	return c, nil
}
