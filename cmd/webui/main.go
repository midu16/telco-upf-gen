// Command webui runs the fabric dashboard (templates + aggregation API) and,
// when DB_DSN is configured, the persistent observability store (metrics,
// SPI/DPI, CDR/LI, UI state).
package main

import (
	"flag"
	"log"
	"os"

	"github.com/midu/telco-upf-gen/internal/dbstore"
	"github.com/midu/telco-upf-gen/internal/web"
)

func main() {
	addr := flag.String("addr", envOr("ADDR", ":8080"), "listen address")
	upfs := flag.String("upfs", envOr("UPF_ENDPOINTS", ""),
		`UPF endpoints "name=http://host:port,..."`)
	tgs := flag.String("tgs", envOr("TG_ENDPOINTS", ""),
		`traffic-generator endpoints "name=http://host:port,..."`)
	flag.Parse()

	// Persistent store (PostgreSQL). Disabled by default; enabled by setting
	// DB_DSN (optionally DB_MAX_CONNS / DB_FLUSH_MS / DB_QUEUE). The packet
	// plane (UPF/TG) never touches the database — this process aggregates and
	// persists, so a database outage can only delay reporting, never data flow.
	opts, enabled := dbstore.FromEnv()
	if enabled {
		store, err := dbstore.Open(opts)
		if err != nil {
			log.Fatalf("webui: dbstore: %v", err)
		}
		defer store.Close()
		log.Printf("webui: dbstore enabled (flush every %s, queue %d, conns %d)",
			opts.FlushIntv, opts.QueueSize, opts.MaxConns)
	} else {
		log.Printf("webui: dbstore disabled (DB_DSN not set)")
	}

	srv := web.New(*upfs, *tgs)
	log.Printf("webui: listening on %s (%d UPF + %d TG endpoints)",
		*addr, srv.UpfCount(), srv.TgCount())
	if err := srv.Run(*addr); err != nil {
		log.Fatalf("webui: %v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
