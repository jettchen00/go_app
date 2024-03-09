/*
说明：提供一些通用的公共函数
创建人 jettchen
创建时间 2023/06/15
*/
package utils

import (
	"crypto/rand"
)

// 生成随机字符串
func RandomString(n int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes)
}
