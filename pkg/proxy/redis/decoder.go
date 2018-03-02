// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"bytes"
	"io"
	"strconv"

	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

var (
	ErrBadCRLFEnd = errors.New("bad CRLF end")

	ErrBadArrayLen        = errors.New("bad array len")
	ErrBadArrayLenTooLong = errors.New("bad array len, too long")

	ErrBadBulkBytesLen        = errors.New("bad bulk bytes len")
	ErrBadBulkBytesLenTooLong = errors.New("bad bulk bytes len, too long")

	ErrBadMultiBulkLen     = errors.New("bad multi-bulk len")
	ErrBadMultiBulkContent = errors.New("bad multi-bulk content, should be bulkbytes")
)

const (
	MaxBulkBytesLen = 1024 * 1024 * 512
	MaxArrayLen     = 1024 * 1024
)

// byte to int64
func Btoi64(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errors.Trace(err)
	} else {
		return n, nil
	}
}

type Decoder struct {
	br *bufio2.Reader

	Err error
}

var ErrFailedDecoder = errors.New("use of failed decoder")

// 构造解码器
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderBuffer(bufio2.NewReaderSize(r, 8192))
}

func NewDecoderSize(r io.Reader, size int) *Decoder {
	return NewDecoderBuffer(bufio2.NewReaderSize(r, size))
}

func NewDecoderBuffer(br *bufio2.Reader) *Decoder {
	return &Decoder{br: br}
}

// 解析后端redis应答数据,backend调用
func (d *Decoder) Decode() (*Resp, error) {
	if d.Err != nil {
		return nil, errors.Trace(ErrFailedDecoder)
	}
	r, err := d.decodeResp()
	if err != nil {
		d.Err = err
	}
	return r, d.Err
}

// 解析请求数据，session调用
func (d *Decoder) DecodeMultiBulk() ([]*Resp, error) {
	if d.Err != nil {
		return nil, errors.Trace(ErrFailedDecoder)
	}
	m, err := d.decodeMultiBulk()
	if err != nil {
		d.Err = err
	}
	return m, err
}

func Decode(r io.Reader) (*Resp, error) {
	return NewDecoder(r).Decode()
}

func DecodeFromBytes(p []byte) (*Resp, error) {
	return NewDecoder(bytes.NewReader(p)).Decode()
}

func DecodeMultiBulkFromBytes(p []byte) ([]*Resp, error) {
	return NewDecoder(bytes.NewReader(p)).DecodeMultiBulk()
}

// 解析请求、应答数据
// @请求数据格式统一为: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
// @单行回复(状态回复): "+OK\r\n"
// @错误回复: "-ERR unknown command 'foobar'\r\n"
// @整型恢复: ":1\r\n"
// @批量回复: "$6\r\nfoobar\r\n"
// @多个批量回复(同请求数据格式): "*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nhello\r\n$5\r\nWorld\r\n"
func (d *Decoder) decodeResp() (*Resp, error) {
	b, err := d.br.ReadByte()
	if err != nil {
		return nil, errors.Trace(err)
	}
	r := &Resp{}
	r.Type = RespType(b)
	switch r.Type {
	default:
		return nil, errors.Errorf("bad resp type %s", r.Type)
	case TypeString, TypeError, TypeInt:
		r.Value, err = d.decodeTextBytes()
	case TypeBulkBytes:
		r.Value, err = d.decodeBulkBytes()
	case TypeArray:
		r.Array, err = d.decodeArray()
	}
	return r, err
}

// 解析TypeString, TypeError, TypeInt应答
func (d *Decoder) decodeTextBytes() ([]byte, error) {
	b, err := d.br.ReadBytes('\n')
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errors.Trace(ErrBadCRLFEnd)
	} else {
		return b[:n], nil
	}
}

// 解析批量回复、多个批量回复数字部分
func (d *Decoder) decodeInt() (int64, error) {
	b, err := d.br.ReadSlice('\n')
	if err != nil {
		return 0, errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return 0, errors.Trace(ErrBadCRLFEnd)
	} else {
		return Btoi64(b[:n])
	}
}

// 解析批量回复
func (d *Decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errors.Trace(ErrBadBulkBytesLen)
	case n > MaxBulkBytesLen:
		return nil, errors.Trace(ErrBadBulkBytesLenTooLong)
	case n == -1:
		return nil, nil
	}
	b, err := d.br.ReadFull(int(n) + 2)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.Trace(ErrBadCRLFEnd)
	}
	return b[:n], nil
}

func (d *Decoder) decodeArray() ([]*Resp, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errors.Trace(ErrBadArrayLen)
	case n > MaxArrayLen:
		return nil, errors.Trace(ErrBadArrayLenTooLong)
	case n == -1:
		return nil, nil
	}
	array := make([]*Resp, n)
	for i := range array {
		r, err := d.decodeResp()
		if err != nil {
			return nil, err
		}
		array[i] = r
	}
	return array, nil
}

// TODO 单个批量请求?
func (d *Decoder) decodeSingleLineMultiBulk() ([]*Resp, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return nil, err
	}
	multi := make([]*Resp, 0, 8)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				multi = append(multi, NewBulkBytes(b[l:r]))
			}
			l = r + 1
		}
	}
	if len(multi) == 0 {
		return nil, errors.Trace(ErrBadMultiBulkLen)
	}
	return multi, nil
}

// 解析多个批量报文，作为对外函数单独使用
func (d *Decoder) decodeMultiBulk() ([]*Resp, error) {
	b, err := d.br.PeekByte()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if RespType(b) != TypeArray {
		return d.decodeSingleLineMultiBulk()
	}
	if _, err := d.br.ReadByte(); err != nil {
		return nil, errors.Trace(err)
	}
	n, err := d.decodeInt()
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch {
	case n <= 0:
		return nil, errors.Trace(ErrBadArrayLen)
	case n > MaxArrayLen:
		return nil, errors.Trace(ErrBadArrayLenTooLong)
	}
	multi := make([]*Resp, n)
	for i := range multi {
		r, err := d.decodeResp()
		if err != nil {
			return nil, err
		}
		if r.Type != TypeBulkBytes {
			return nil, errors.Trace(ErrBadMultiBulkContent)
		}
		multi[i] = r
	}
	return multi, nil
}
