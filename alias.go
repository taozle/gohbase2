package gohbase2

import "github.com/taozle/gohbase2/hbaseproto"

type (
	// Common
	TColumn = hbaseproto.TColumn
	TColumnValue = hbaseproto.TColumnValue
	TTimeRange = hbaseproto.TTimeRange
	TAuthorization = hbaseproto.TAuthorization
	TDurability = hbaseproto.TDurability
	TCellVisibility = hbaseproto.TCellVisibility
	TDeleteType = hbaseproto.TDeleteType
	TResult = hbaseproto.TResult_

	// Param
	TIncrement = hbaseproto.TIncrement
	TAppend = hbaseproto.TAppend
	TScan = hbaseproto.TScan
	TRowMutations = hbaseproto.TRowMutations
	THRegionLocation = hbaseproto.THRegionLocation
	TCompareOp = hbaseproto.TCompareOp

	// Operation
	TGet = hbaseproto.TGet
	TPut = hbaseproto.TPut
	TDelete = hbaseproto.TDelete
)
