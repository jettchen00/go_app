/*
说明：提供一些文件读写功能的函数
创建人 jettchen
创建时间 2023/07/01
*/
package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// 判断文件是否存在
func IsFileExist(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
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
	oldContent, err := ioutil.ReadFile(filename)
	if err != nil {
		needWrite = true
	} else if string(oldContent) != content {
		// 文件内容不一样，也重新刷新下文件内容
		needWrite = true
	}
	if needWrite {
		err = ioutil.WriteFile(filename, []byte(content), 0666)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// 删除文件
func RemoveFile(filename string) error {
	isExist, _ := IsFileExist(filename)
	if !isExist {
		return nil
	}
	err := os.Remove(filename)
	if err != nil {
		return err
	}
	return nil
}
