package web

import (
	"bytes"
	"strings"
	"testing"
)

// TestTemplateExecutes guards the embedded template: it must parse and
// execute, and its JS must not contain stray Go-template delimiters (which
// would break both rendering and the dashboard script).
func TestTemplateExecutes(t *testing.T) {
	s := New("", "")
	var buf bytes.Buffer
	if err := s.tpl.Execute(&buf, map[string]any{"UpfCount": 2, "TgCount": 1}); err != nil {
		t.Fatalf("template execute: %v", err)
	}
	out := buf.String()
	for _, need := range []string{
		"5G UPF FABRIC", `id="knob"`, "api/rfc2544", "api/knob", "api/cdrs",
	} {
		if !strings.Contains(out, need) {
			t.Fatalf("rendered page missing %q", need)
		}
	}
	if !strings.Contains(out, "2 UPF") || !strings.Contains(out, "1 TG") {
		t.Fatalf("header counts not rendered: %s", out[:200])
	}
	// No template delimiters may survive other than the ones we emitted.
	if idx := strings.Index(out, "{{"); idx >= 0 {
		t.Fatalf("unresolved template action at offset %d: %q", idx, out[idx:idx+60])
	}
}
