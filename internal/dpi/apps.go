package dpi

// Deep Packet Inspection (DPI) application classification.
//
// The classifier is deliberately cheap: it inspects a bounded prefix of the
// L4 payload plus the 5-tuple SPI already computed. That is enough for a
// simulator plane whose job is to exercise and visualize the inspection
// pipeline (per-app counters, policy enforcement, CDR app splits, LI
// metadata), not to be a production NDR engine.

// AppID identifies a classified application.
type AppID uint16

const (
	AppUnknown AppID = iota
	AppOther
	AppICMP
	AppDNS
	AppHTTP
	AppHTTPS
	AppSSH
	AppQUIC
)

var appNames = map[AppID]string{
	AppUnknown: "unknown",
	AppOther:   "other",
	AppICMP:    "icmp",
	AppDNS:     "dns",
	AppHTTP:    "http",
	AppHTTPS:   "https",
	AppSSH:     "ssh",
	AppQUIC:    "quic",
}

// Name returns the display name of the application.
func (a AppID) Name() string {
	if n, ok := appNames[a]; ok {
		return n
	}
	return "unknown"
}

// AppByName resolves a YAML policy app name to its AppID.
func AppByName(name string) (AppID, bool) {
	for id, n := range appNames {
		if n == name {
			return id, true
		}
	}
	return AppUnknown, false
}

// classifyDepth is how many payload octets the classifier may look at.
const classifyDepth = 256

// Classify returns the application for the SPI/payload pair and a short
// human-readable detail (SNI, DNS QNAME, HTTP request line, ICMP type).
func Classify(s SPI, payload []byte) (AppID, string) {
	if !s.Valid {
		return AppUnknown, "malformed"
	}
	switch s.Proto {
	case ProtoICMP:
		return AppICMP, "type " + itoa8(s.ICMPType)
	case ProtoUDP:
		if s.DstPort == 53 || s.SrcPort == 53 {
			if q, ok := dnsQname(payload); ok {
				return AppDNS, q
			}
			return AppDNS, "dns"
		}
		if s.DstPort == 443 || s.DstPort > 1024 {
			if len(payload) > 0 && payload[0]&0xc0 == 0x40 {
				return AppQUIC, "long-header"
			}
		}
		return AppOther, "udp"
	case ProtoTCP:
		switch {
		case s.DstPort == 80 || s.SrcPort == 80:
			if line, ok := httpRequestLine(payload); ok {
				return AppHTTP, line
			}
			return AppHTTP, "http"
		case s.DstPort == 443 || s.SrcPort == 443:
			if sni, ok := extractSNI(payload); ok {
				return AppHTTPS, sni
			}
			return AppHTTPS, "tls"
		case s.DstPort == 22 || s.SrcPort == 22:
			return AppSSH, "ssh"
		}
		return AppOther, "tcp"
	}
	return AppOther, "other"
}

// dnsQname extracts the query name from a DNS message.
func dnsQname(p []byte) (string, bool) {
	if len(p) < 12 {
		return "", false
	}
	qd := binary_BE16(p, 4)
	if qd < 1 || len(p) < 12+qd*2 {
		return "", false
	}
	i := 12
	var labels []string
	for i < len(p) && len(p)-i >= 1 {
		l := int(p[i])
		if l == 0 {
			i++
			break
		}
		if l&0xc0 != 0 || i+l > len(p) {
			return "", false
		}
		labels = append(labels, string(p[i+1:i+1+l]))
		i += 1 + l
	}
	if len(labels) == 0 {
		return "", false
	}
	return joinLabels(labels, 64), true
}

// httpRequestLine pulls "METHOD PATH" from the start of an HTTP request.
func httpRequestLine(p []byte) (string, bool) {
	if len(p) < 4 {
		return "", false
	}
	verbs := []string{"GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "PATCH "}
	for _, v := range verbs {
		if len(p) >= len(v) && string(p[:len(v)]) == v {
			rest := p[len(v):]
			end := 0
			for end < len(rest) && rest[end] != ' ' && end < 64 {
				end++
			}
			if end > 0 {
				return string(p[:len(v)+end]), true
			}
			return v[:len(v)-1] + "*", true
		}
	}
	return "", false
}

// extractSNI finds the server_name extension in a bounded scan of the first
// classifyDepth octets of a TLS ClientHello.
func extractSNI(p []byte) (string, bool) {
	if len(p) < 6 || p[0] != 0x16 { // not a TLS handshake record
		return "", false
	}
	limit := len(p)
	if limit > classifyDepth {
		limit = classifyDepth
	}
	// Extension wire layout at offset t:
	//   t+0  t+1 : type 0x0002 (server_name)
	//   t+2  t+3 : extension length
	//   t+4  t+5 : server_name_list length
	//   t+6      : entry name type (0 = fully qualified)
	//   t+7  t+8 : name length (2 octets)
	//   t+9 ..   : the name
	for i := 0; i+9 < limit; i++ {
		if p[i] != 0x00 || p[i+1] != 0x02 {
			continue
		}
		extLen := binary_BE16(p, i+2)
		if extLen < 7 || extLen > 260 {
			continue
		}
		listLen := binary_BE16(p, i+4)
		if listLen < 5 || listLen > extLen-2 {
			continue
		}
		if p[i+6] != 0x00 {
			continue
		}
		nl := binary_BE16(p, i+7)
		if nl < 1 || i+9+nl > limit || 3+nl > listLen {
			continue
		}
		return string(p[i+9 : i+9+nl]), true
	}
	return "", false
}

func joinLabels(labels []string, max int) string {
	out := ""
	for _, l := range labels {
		if out != "" {
			out += "."
		}
		out += l
		if len(out) >= max {
			break
		}
	}
	return out
}

func itoa8(v uint8) string {
	if v == 0 {
		return "0"
	}
	var b [3]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// binary_BE16 reads a big-endian uint16 with explicit bounds, returned as int.
func binary_BE16(b []byte, i int) int {
	return int(b[i])<<8 | int(b[i+1])
}
