package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/logging"
	"github.com/lucas-clemente/quic-go/qlog"
	"github.com/mr-tron/base58"
	"k8s.io/klog/v2"
)

// QUIC_GO_LOG_LEVEL=DEBUG

var (
	flagDebug = flag.Bool("debug", false, "Enable debug logging")
	flagCount = flag.Int("count", 1, "Number of pings to send, -1 for infinite")
	flagDelay = flag.Duration("delay", 1*time.Second, "Delay between pings")
)

func init() {
	flag.Parse()
}

func main() {
	ctx := context.Background()

	if len(flag.Args()) != 1 {
		klog.Exitf("Usage: %s <node>:<port>", os.Args[0])
	}

	var (
		qconf quic.Config
		dbg   io.Writer
		err   error
	)
	if *flagDebug {
		dbg, err = os.OpenFile("keylog.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			klog.Exitf("Failed to open keylog file: %v", err)
		}
		qconf.Tracer = qlog.NewTracer(func(_ logging.Perspective, connID []byte) io.WriteCloser {
			filename := fmt.Sprintf("client_%x.qlog", connID)
			f, err := os.Create(filename)
			if err != nil {
				klog.Fatal(err)
			}
			log.Printf("Creating qlog file %s.\n", filename)
			return f
		})
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         nil,
		KeyLogWriter:       dbg,
	}

	c := 0

	for c < *flagCount || *flagCount == -1 {
		t := time.Now()
		ctx, cancel := context.WithTimeout(ctx, *flagDelay)
		conn, err := quic.DialAddrContext(ctx, flag.Args()[0], tlsConf, &qconf)
		if err != nil {
			klog.Errorf("Failed to dial: %v", err)
			cancel()
			continue
		}
		cancel()

		klog.Infof("Connected to %s (in %dms, %d/%d)",
			flag.Args()[0], time.Since(t).Milliseconds(),
			c+1, *flagCount)

		if klog.V(1).Enabled() {
			for _, cert := range conn.ConnectionState().TLS.PeerCertificates {
				klog.Infof("Certificate: %s", cert.Subject)
				klog.Infof("Public key: %s", base58.Encode(cert.PublicKey.(ed25519.PublicKey)))
			}
		}

		if err := conn.CloseWithError(0, ""); err != nil {
			klog.Exitf("Failed to close: %v", err)
		}

		time.Sleep(*flagDelay)

		c++
	}
}