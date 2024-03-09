/*
说明：封装一些通用函数
创建人 jettchen
创建时间 2023/06/15
*/
package utils

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"go_app/pkg/log"
)

var ErrNonTLSConfig = errors.New("empty cert/key/CA file")

// LoadClientTLS 加载客户端TLS
func LoadClientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, pool, err := loadTLS(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}

	c := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}
	return c, nil
}

// LoadServerTLS 加载服务端TLS
func LoadServerTLS(certFile, keyFile string) (*tls.Config, error) {
	cert, _, err := loadTLS(certFile, keyFile, "")
	if err != nil {
		return nil, err
	}

	c := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return c, nil
}

func loadTLS(certFile, keyFile, caFile string) (cert tls.Certificate, pool *x509.CertPool, err error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		err = ErrNonTLSConfig
		return
	}

	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		log.Error("KeyFile and CertFile must both be present[key:%v, cert:%v]", keyFile, certFile)
		return
	}

	if certFile != "" && keyFile != "" {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Error("load [key:%v, cert:%v] failed:%v", certFile, keyFile, err)
			return
		}
		log.Info("use [key:%v, cert:%v]", certFile, keyFile)
	}

	if caFile != "" {
		caData, err1 := os.ReadFile(caFile)
		if err1 != nil {
			log.Error("load [ca:%v] failed:%v", caFile, err1)
			err = err1
			return
		}
		pool = x509.NewCertPool()
		ok := pool.AppendCertsFromPEM(caData)
		if !ok {
			err = fmt.Errorf("add [ca:%v] failed", caFile)
			log.Error("%v", err.Error())
			return
		}
		log.Info("use [ca:%v]", caFile)
	}

	return
}
