// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
// bufio2相对原生bufio可以减少内存碎片

package bufio2

import (
	"bufio"
	"bytes"
	"io"
)

const DefaultBufferSize = 1024

type Reader struct {
	err error
	buf []byte

	rd   io.Reader
	rpos int
	wpos int

	slice sliceAlloc
}

// 根据默认size分配、构造读缓冲区
func NewReader(rd io.Reader) *Reader {
	return NewReaderSize(rd, DefaultBufferSize)
}

// 根据size分配、构造读缓冲区
func NewReaderSize(rd io.Reader, size int) *Reader {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Reader{rd: rd, buf: make([]byte, size)}
}

// 根据已经分配的buffer，构造读缓冲区
func NewReaderBuffer(rd io.Reader, buf []byte) *Reader {
	if len(buf) == 0 {
		buf = make([]byte, DefaultBufferSize)
	}
	return &Reader{rd: rd, buf: buf}
}

// 从rd读取数据并填充到buf的wpos位置之后
func (b *Reader) fill() error {
	if b.err != nil {
		return b.err
	}
	// 释放空间
	if b.rpos > 0 {
		n := copy(b.buf, b.buf[b.rpos:b.wpos])
		b.rpos = 0
		b.wpos = n
	}
	n, err := b.rd.Read(b.buf[b.wpos:])
	if err != nil {
		b.err = err
	} else if n == 0 {
		b.err = io.ErrNoProgress
	} else {
		b.wpos += n
	}
	return b.err
}

// 当前填充数据长度
func (b *Reader) buffered() int {
	return b.wpos - b.rpos
}

func (b *Reader) Read(p []byte) (int, error) {
	if b.err != nil || len(p) == 0 {
		return 0, b.err
	}
	// 当前buf没有数据
	if b.buffered() == 0 {
		if len(p) >= len(b.buf) { // 超过buf长度，直接读取到p中
			n, err := b.rd.Read(p)
			if err != nil {
				b.err = err
			}
			return n, b.err
		}
		// 未超过buf长度
		if b.fill() != nil {
			return 0, b.err
		}
	}
	// 从buf中读取、copy数据
	n := copy(p, b.buf[b.rpos:b.wpos])
	b.rpos += n
	return n, nil
}

// 读取一个字节
func (b *Reader) ReadByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	// 当前buf数据为空，读取并填充buf
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	b.rpos += 1
	return c, nil
}

// 只读取不移除所读取数据
func (b *Reader) PeekByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	return c, nil
}

// 读取delim分割位置之前的字节数组
func (b *Reader) ReadSlice(delim byte) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	for {
		var index = bytes.IndexByte(b.buf[b.rpos:b.wpos], delim)
		// 查找到分割位置
		if index >= 0 {
			limit := b.rpos + index + 1
			slice := b.buf[b.rpos:limit]
			b.rpos = limit
			return slice, nil
		}
		// buf满
		if b.buffered() == len(b.buf) {
			b.rpos = b.wpos
			return b.buf, bufio.ErrBufferFull
		}
		// 读取填充
		if b.fill() != nil {
			return nil, b.err
		}
	}
}

func (b *Reader) ReadBytes(delim byte) ([]byte, error) {
	var full [][]byte
	var last []byte
	var size int
	for last == nil {
		f, err := b.ReadSlice(delim)
		if err != nil {
			if err != bufio.ErrBufferFull {
				return nil, b.err
			}
			dup := b.slice.Make(len(f))
			copy(dup, f)
			full = append(full, dup)
		} else {
			last = f
		}
		size += len(f)
	}
	var n int
	// 如果size小于512，复用slice的buf
	var buf = b.slice.Make(size)
	for _, frag := range full {
		n += copy(buf[n:], frag)
	}
	copy(buf[n:], last)
	return buf, nil
}

func (b *Reader) ReadFull(n int) ([]byte, error) {
	if b.err != nil || n == 0 {
		return nil, b.err
	}
	var buf = b.slice.Make(n)
	if _, err := io.ReadFull(b, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

type Writer struct {
	err error
	buf []byte

	wr   io.Writer
	wpos int // buf[0:wpos]为所存储数据
}

// 根据默认size分配、构造写缓冲区
func NewWriter(wr io.Writer) *Writer {
	return NewWriterSize(wr, DefaultBufferSize)
}

// 根据size分配、构造写缓冲区
func NewWriterSize(wr io.Writer, size int) *Writer {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Writer{wr: wr, buf: make([]byte, size)}
}

// 根据已经分配的buffer，构造写缓冲区
func NewWriterBuffer(wr io.Writer, buf []byte) *Writer {
	if len(buf) == 0 {
		buf = make([]byte, DefaultBufferSize)
	}
	return &Writer{wr: wr, buf: buf}
}

func (b *Writer) Flush() error {
	return b.flush()
}

func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	// 没有数据可写
	if b.wpos == 0 {
		return nil
	}
	// 写buf[:b.wpos]
	n, err := b.wr.Write(b.buf[:b.wpos])
	if err != nil {
		b.err = err
	} else if n < b.wpos {
		b.err = io.ErrShortWrite
	} else {
		b.wpos = 0
	}
	return b.err
}

// 可用写buf空间
func (b *Writer) available() int {
	return len(b.buf) - b.wpos
}

func (b *Writer) Write(p []byte) (nn int, err error) {
	// 要写的p大于buf可用空间
	for b.err == nil && len(p) > b.available() {
		var n int
		// 大于整个buf的空间
		if b.wpos == 0 {
			n, b.err = b.wr.Write(p)
		} else {
			// 根据可用空间，复制、冲刷数据
			n = copy(b.buf[b.wpos:], p)
			b.wpos += n
			b.flush()
		}
		nn, p = nn+n, p[n:]
	}
	if b.err != nil || len(p) == 0 {
		return nn, b.err
	}
	// 把最后剩余数据复制、刷盘
	n := copy(b.buf[b.wpos:], p)
	b.wpos += n
	return nn + n, nil
}

// 写一个字节、不刷数据
func (b *Writer) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.available() == 0 && b.flush() != nil {
		return b.err
	}
	b.buf[b.wpos] = c
	b.wpos += 1
	return nil
}

func (b *Writer) WriteString(s string) (nn int, err error) {
	// s长度大于buf可用长度
	for b.err == nil && len(s) > b.available() {
		n := copy(b.buf[b.wpos:], s)
		b.wpos += n
		b.flush()
		nn, s = nn+n, s[n:]
	}
	if b.err != nil || len(s) == 0 {
		return nn, b.err
	}
	// 复制剩余空间
	n := copy(b.buf[b.wpos:], s)
	b.wpos += n
	return nn + n, nil
}
