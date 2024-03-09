/*
说明：提供一些基础功能的函数
创建人 jettchen
创建时间 2023/06/15
*/
package base

import (
	"fmt"
	"syscall"
	"time"

	"go_app/pkg/log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// 获取本机ip
func GetSelfIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "0.0.0.0"
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}

// 判断目标ip是否可以ping通
func IsPingSucc(ip string, val bool) bool {
	cmd := fmt.Sprintf("ping -c 1 %s > /dev/null && echo true || echo false", ip)
	output, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		return val
	}
	res := strings.TrimSpace(string(output))
	return (res == "true")
}

// 判断文件是否存在
func IsFileExist(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	log.Error("is file exis::err, filename=%s, err=%v", filename, err)
	return false, err
}

// 写pid文件
func WritePidFile(filename string) error {
	pid := os.Getpid()
	dir := filepath.Dir(filename)
	os.MkdirAll(dir, 0777)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	_, err = f.Write([]byte(fmt.Sprintf("%d", pid)))
	if err != nil {
		return err
	}
	return nil
}

// 向文件写入指定的内容，会覆盖旧内容
func WriteFile(filename string, content string) (bool, error) {
	needWrite := false
	oldContent, err := os.ReadFile(filename)
	if err != nil {
		log.Debug("file not exist, filename=%s, err=%v", filename, err)
		needWrite = true
	} else if string(oldContent) != content {
		// 文件内容不一样，也重新刷新下文件内容
		needWrite = true
	}
	if needWrite {
		err = os.WriteFile(filename, []byte(content), 0666)
		if err != nil {
			log.Error("write file fail, filename=%s, content=%s, err=%v", filename, content, err)
			return false, err
		}
		log.Debug("write file succ, filename=%s, content=%s", filename, content)
		return true, nil
	}
	log.Debug("file exist with same content, filename=%s, content=%s", filename, content)
	return false, nil
}

// 删除文件
func RemoveFile(filename string) {
	isExist, _ := IsFileExist(filename)
	if !isExist {
		return
	}
	err := os.Remove(filename)
	if err != nil {
		log.Error("remove file fail, filename=%s, err=%v", filename, err)
	} else {
		log.Info("remove file succ, filename=%s", filename)
	}
}

// 写文件，并且刷盘
func Write(filename string, b []byte) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(b)
	if err != nil {
		return err
	}
	err = f.Sync()
	return err
}

// 对文件加写锁
func WRLock(f *os.File) error {
	i := 0
	var err error
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		i++
		// 拥有一次重试机会
		if i > 1 {
			break
		}
		// 这里少量报错是可能的
		log.Error("retry because wrlock fail, filename=%s, err=%v", f.Name(), err)
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Error("wrlock still fail after retry, filename=%s, err=%v", f.Name(), err)
	} else {
		log.Debug("wrlock succ, filename=%s", f.Name())
	}
	return err
}

// 释放文件锁
func WRUnlock(f *os.File) error {
	i := 0
	var err error
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		if err == nil {
			break
		}
		i++
		// 拥有一次重试机会
		if i > 1 {
			break
		}
		log.Error("retry because unlock fail, filename=%s, err=%v", f.Name(), err)
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Error("unlock still fail after retry, filename=%s, err=%v", f.Name(), err)
	} else {
		log.Debug("unlocl succ, filename=%s", f.Name())
	}
	return err
}
