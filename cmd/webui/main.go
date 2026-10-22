// Command webui runs the fabric dashboard (templates + aggregation API).
package main

import (
	"flag"
	"log"
	"os"

	"github.com/midu/telco-upf-gen/internal/web"
)

func main() {
	addr := flag.String("addr", envOr("ADDR", ":8080"), "listen address")
	upfs := flag.String("upfs", envOr("UPF_ENDPOINTS", ""),
		`UPF endpoints "name=http://host:port,..."`)
	tgs := flag.String("tgs", envOr("TG_ENDPOINTS", ""),
		`traffic-generator endpoints "name=http://host:port,..."`)
	flag.Parse()

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
