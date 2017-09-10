package gohbase2

import (
	"context"
	"io"
	"net"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/taozle/gohbase2/hbaseproto"
)

const (
	transportBufferSize = 4096
)

type Option func(c *connConfig)

type connConfig struct {
	// Whether use framed transport.
	framed bool

	// Whether use compact protocol.
	compact bool
}

func (c *connConfig) applyOption(opts ...Option) {
	for _, opt := range opts {
		opt(c)
	}
}

func Framed() Option {
	return func(c *connConfig) {
		c.framed = true
	}
}

func Compact() Option {
	return func(c *connConfig) {
		c.compact = true
	}
}

// Conn represents a connection to hbase thrift server.
// It is not goroutine safe.
type Conn struct {
	transport thrift.TTransport
	protocol  thrift.TProtocol
	client    hbaseproto.THBaseService
}

func Dial(addr string, opts ...Option) *Conn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}

	return NewConn(conn, opts...)
}

func NewConn(rw io.ReadWriter, opts ...Option) *Conn {
	cfg := &connConfig{
		framed:  false,
		compact: false,
	}
	cfg.applyOption(opts...)

	var transport thrift.TTransport
	transport = thrift.NewStreamTransportRW(rw)
	if cfg.framed {
		transport = thrift.NewTFramedTransport(transport)
	}
	transport = thrift.NewTBufferedTransport(transport, transportBufferSize)

	var protocol thrift.TProtocol
	protocol = thrift.NewTBinaryProtocolTransport(transport)
	if cfg.compact {
		protocol = thrift.NewTCompactProtocol(transport)
	}

	return &Conn{
		transport: transport,
		protocol:  protocol,
		client:    hbaseproto.NewTHBaseServiceClientProtocol(transport, protocol, protocol),
	}
}

// Simple wrapper for hbaseproto.THBaseService
func (c *Conn) Exists(ctx context.Context, table []byte, tget *TGet) (r bool, err error)                                                                                                           { return c.client.Exists(ctx, table, tget) }
func (c *Conn) ExistsAll(ctx context.Context, table []byte, tgets []*TGet) (r []bool, err error)                                                                                                   { return c.client.ExistsAll(ctx, table, tgets) }
func (c *Conn) Get(ctx context.Context, table []byte, tget *TGet) (r *TResult, err error)                                                                                                          { return c.client.Get(ctx, table, tget) }
func (c *Conn) GetMulti(ctx context.Context, table []byte, tgets []*TGet) (r []*TResult, err error)                                                                                                { return c.client.GetMultiple(ctx, table, tgets) }
func (c *Conn) Put(ctx context.Context, table []byte, tput *TPut) error                                                                                                                            { return c.client.Put(ctx, table, tput) }
func (c *Conn) CheckAndPut(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, value []byte, tput *TPut) (bool, error)                                                 { return c.client.CheckAndPut(ctx, table, row, family, qualifier, value, tput) }
func (c *Conn) PutMulti(ctx context.Context, table []byte, tputs []*TPut) error                                                                                                                    { return c.client.PutMultiple(ctx, table, tputs) }
func (c *Conn) DeleteSingle(ctx context.Context, table []byte, tdel *TDelete) error                                                                                                                { return c.client.DeleteSingle(ctx, table, tdel) }
func (c *Conn) DeleteMulti(ctx context.Context, table []byte, tdels []*TDelete) (r []*TDelete, err error)                                                                                          { return c.client.DeleteMultiple(ctx, table, tdels) }
func (c *Conn) CheckAndDelete(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, value []byte, tdel *TDelete) (bool, error)                                           { return c.client.CheckAndDelete(ctx, table, row, family, qualifier, value, tdel) }
func (c *Conn) Increment(ctx context.Context, table []byte, tincrement *TIncrement) (r *TResult, err error)                                                                                        { return c.client.Increment(ctx, table, tincrement) }
func (c *Conn) Append(ctx context.Context, table []byte, tappend *TAppend) (r *TResult, err error)                                                                                                 { return c.client.Append(ctx, table, tappend) }
func (c *Conn) OpenScanner(ctx context.Context, table []byte, tscan *TScan) (r int32, err error)                                                                                                   { return c.client.OpenScanner(ctx, table, tscan) }
func (c *Conn) GetScannerRows(ctx context.Context, scannerId int32, numRows int32) (r []*TResult, err error)                                                                                       { return c.client.GetScannerRows(ctx, scannerId, numRows) }
func (c *Conn) CloseScanner(ctx context.Context, scannerId int32) (err error)                                                                                                                      { return c.client.CloseScanner(ctx, scannerId) }
func (c *Conn) MutateRow(ctx context.Context, table []byte, trowMutations *TRowMutations) (err error)                                                                                              { return c.client.MutateRow(ctx, table, trowMutations) }
func (c *Conn) GetScannerResults(ctx context.Context, table []byte, tscan *TScan, numRows int32) (r []*TResult, err error)                                                                         { return c.client.GetScannerResults(ctx, table, tscan, numRows) }
func (c *Conn) GetRegionLocation(ctx context.Context, table []byte, row []byte, reload bool) (r *THRegionLocation, err error)                                                                      { return c.client.GetRegionLocation(ctx, table, row, reload) }
func (c *Conn) GetAllRegionLocations(ctx context.Context, table []byte) (r []*THRegionLocation, err error)                                                                                         { return c.client.GetAllRegionLocations(ctx, table) }
func (c *Conn) CheckAndMutate(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, compareOp TCompareOp, value []byte, rowMutations *TRowMutations) (r bool, err error) { return c.client.CheckAndMutate(ctx, table, row, family, qualifier, compareOp, value, rowMutations) }

// Extended
func (c *Conn) Close() { c.transport.Close() }
