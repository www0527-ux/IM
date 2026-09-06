package utils

import (
	"crypto/rand"
	"math/big"
)

func RandomNumber(length int) (string, error) {
	//[]byte 字节切片可以向string转换
	code := make([]byte, length)
	//big.Int可以用来表示任意大整数
	//max规定随机数的上限
	max := big.NewInt(10)
	//循环获得下标
	for i := range code {
		//rand.reader为随机数据的来源，会从操作系统提供的随机原读取
		number, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		code[i] = byte('0' + number.Int64())
	}
	return string(code), nil
}
