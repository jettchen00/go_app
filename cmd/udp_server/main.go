package main

import (
	"flag"
	"fmt"
	"go_app/pkg/log"
	"net"
)

var (
	logFileName  string
	logFileSize  int = 4
	logFileCount int = 3
	logExpireDay int = 7
	logLevel     int
)

func parseArgs() {
	flag.StringVar(&logFileName, "log-file", "./log/udpsvr.log", "Path to the log file.")
	flag.IntVar(&logLevel, "log-level", -1, "log level.")
	flag.Parse()
}

func main() {
	parseArgs()
	log.InitLog(logFileName, logFileSize, logFileCount, logExpireDay, logLevel)
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 20001})
	if err != nil {
		fmt.Println("listen udp fail")
		log.Errorf(err, "listen udp fail")
		return
	}
	fmt.Println("listen udp succ")
	log.Info("listen udp succ, addr=%s", listener.LocalAddr().String())
	data := make([]byte, 4096)
	for {
		n, remoteAddr, err := listener.ReadFromUDP(data)
		if err != nil {
			log.Errorf(err, "read from udp fail")
			continue
		}
		response := fmt.Sprintf("recv msg, msg data is %s", string(data[:n]))
		log.Debug("recv msg, remoteAddr=%s, msg data is %s", remoteAddr, string(data[:n]))

		_, err = listener.WriteToUDP([]byte(response), remoteAddr)
		if err != nil {
			log.Errorf(err, "write to udp fail")
		}
	}
}
