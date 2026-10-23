// Policy enforcement for the user plane.
//
// A Policy is an ordered list of rules; the first rule that matches a
// packet decides its fate (Allow, Observe, Drop). No match implies Allow.
// Rules match on the SPI 5-tuple and the DPI application class. Policies
// are loaded from YAML so clusters can ship different enforcement profiles
// via ConfigMap-mounted files — nothing about a rule is cluster-specific.
package dpi

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
)

// Action is the verdict a policy rule assigns to a matched packet.
type Action int

const (
	Allow Action = iota
	Observe
	Drop
)

func (a Action) String() string {
	switch a {
	case Allow:
		return "allow"
	case Observe:
		return "observe"
	default:
		return "drop"
	}
}

func parseAction(s string) (Action, error) {
	switch s {
	case "allow", "":
		return Allow, nil
	case "observe":
		return Observe, nil
	case "drop":
		return Drop, nil
	}
	return Allow, fmt.Errorf("dpi: unknown policy action %q", s)
}

// PortRange is an inclusive L4 port range.
type PortRange struct {
	From uint16 `yaml:"from"`
	To   uint16 `yaml:"to"`
}

func (r PortRange) contains(p uint16) bool { return p >= r.From && p <= r.To }

// Rule is one policy entry. Empty App/Proto/Ports/Nets fields are wildcards.
type Rule struct {
	ID          string      `yaml:"id"`
	Description string      `yaml:"description"`
	Apps        []string    `yaml:"apps"`  // app names from AppByName
	Proto       string      `yaml:"proto"` // tcp | udp | icmp | any
	Ports       []PortRange `yaml:"ports"` // L4 port range on the inner DST
	Nets        []string    `yaml:"nets"`  // CIDRs applied to the inner DST
	Action      string      `yaml:"action"`

	apps     []AppID
	proto    uint8
	anyProto bool
	prefixes []netipPrefix
	action   Action
}

// netipPrefix is a parsed CIDR kept without importing net/netip twice.
type netipPrefix struct {
	network *net.IPNet
}

// compile resolves name-based fields into matchers. Called by LoadPolicy.
func (r *Rule) compile() error {
	for _, a := range r.Apps {
		id, ok := AppByName(a)
		if !ok {
			return fmt.Errorf("dpi: rule %q: unknown app %q", r.ID, a)
		}
		r.apps = append(r.apps, id)
	}
	switch r.Proto {
	case "", "any":
		r.anyProto = true
	case "tcp":
		r.proto = ProtoTCP
	case "udp":
		r.proto = ProtoUDP
	case "icmp":
		r.proto = ProtoICMP
	default:
		return fmt.Errorf("dpi: rule %q: unknown proto %q", r.ID, r.Proto)
	}
	for _, n := range r.Nets {
		_, ipnet, err := net.ParseCIDR(n)
		if err != nil {
			return fmt.Errorf("dpi: rule %q: bad CIDR %q: %v", r.ID, n, err)
		}
		r.prefixes = append(r.prefixes, netipPrefix{ipnet})
	}
	a, err := parseAction(r.Action)
	if err != nil {
		return err
	}
	r.action = a
	return nil
}

func (r *Rule) match(s SPI, app AppID) bool {
	if len(r.apps) > 0 {
		ok := false
		for _, a := range r.apps {
			if a == app {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if !r.anyProto && s.Proto != r.proto {
		return false
	}
	if len(r.Ports) > 0 {
		ok := false
		for _, pr := range r.Ports {
			if pr.contains(s.DstPort) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(r.prefixes) > 0 {
		ip := net.ParseIP(s.Dst)
		ok := false
		if ip != nil {
			for _, p := range r.prefixes {
				if p.network.Contains(ip) {
					ok = true
					break
				}
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// Policy is the ordered rule list evaluated by the engine.
type Policy struct {
	Rules []Rule `yaml:"rules"`
}

// LoadPolicy reads a policy file (YAML). An empty path yields an empty
// policy (allow-all), so inspection can run without any policy shipped.
func LoadPolicy(path string) (*Policy, error) {
	p := &Policy{}
	if path == "" {
		return p, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("dpi: read policy: %w", err)
	}
	if err := yaml.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("dpi: parse policy: %w", err)
	}
	for i := range p.Rules {
		if p.Rules[i].ID == "" {
			p.Rules[i].ID = fmt.Sprintf("rule-%d", i)
		}
		if p.Rules[i].Action == "" {
			p.Rules[i].Action = "observe" // default: log, do not touch
		}
		if err := p.Rules[i].compile(); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Evaluate returns the decision for one packet. The zero Policy allows all.
func (p *Policy) Evaluate(s SPI, app AppID) (rule *Rule, action Action) {
	if !s.Valid {
		return nil, Allow
	}
	for i := range p.Rules {
		if p.Rules[i].match(s, app) {
			return &p.Rules[i], p.Rules[i].action
		}
	}
	return nil, Allow
}
