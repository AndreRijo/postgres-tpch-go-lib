package tpch

import (
	"time"

	"postgres_tpch_go_lib/src/proto"

	"github.com/uptrace/bun"
)

//Defines structs used for queries and views

type Q1Result struct {
	bun.BaseModel
	L_returnflag   string
	L_linestatus   string
	Sum_qty        int
	Sum_base_price float64
	Sum_disc_price float64
	Sum_charge     float64
	Avg_qty        float64
	Avg_price      float64
	Avg_disc       float64
	Count_order    int
}

type Q3Result struct {
	bun.BaseModel
	Revenue        float64
	L_orderkey     int32
	O_orderdate    time.Time
	O_shippriority string
}

type Q5Result struct {
	bun.BaseModel
	N_name  string
	Revenue float64
}

type Q6Result struct {
	bun.BaseModel
	Revenue float64
}

type Q11BaseResult struct {
	bun.BaseModel
	Ps_partkey int32
	Value      float64
}

type Q11NationResult struct {
	bun.BaseModel
	N_nationkey int8
	Factor      float64
}

type Q14Result struct {
	bun.BaseModel
	Promo_revenue float64
}

type Q15Result struct {
	bun.BaseModel
	S_suppkey     string
	S_name        string
	S_address     string
	S_phone       string
	Total_revenue float64
}

type Q18Result struct {
	bun.BaseModel
	C_name        string
	C_custkey     int32
	O_orderkey    int32
	O_orderdate   time.Time
	O_totalprice  float64
	O_sumquantity int
}

// Query args for redirect client and server
type Q1Args struct {
	FirstFrom, SecondFrom              string
	FirstOrderByOne, FirstOrderByTwo   string
	SecondOrderByOne, SecondOrderByTwo string
}

type Q3Args struct {
	From, FirstOrderBy, SecondOrderBy string
	Limit                             int
}

type Q5Args struct {
	From, OrderBy string
}

type Q6Args struct {
	From string
}

type Q11Args struct {
	FirstFrom, SecondFrom string
	FirstOrderBy          string
	FirstLimit            int
}

type Q14Args struct {
	From string
}

type Q15Args struct {
	FromOne, FromTwo string
	Columns          []string
	Where            string
	OrderBy          string
}

type Q18Args struct {
	From                   string
	OrderByOne, OrderByTwo string
	Limit                  int
}

func (q1 Q1Args) FromProtobuf(protobuf *proto.Query) Q1Args {
	firstQ, secondQ := protobuf.Queries[0], protobuf.Queries[1]
	q1.FirstFrom, q1.SecondFrom = firstQ.GetFrom()[0], secondQ.GetFrom()[0]
	q1.FirstOrderByOne, q1.FirstOrderByTwo = firstQ.GetOrderBy()[0], firstQ.GetOrderBy()[1]
	q1.SecondOrderByOne, q1.SecondOrderByTwo = secondQ.GetOrderBy()[0], secondQ.GetOrderBy()[1]
	return q1
}

func (q1 Q1Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(1)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q1.FirstFrom}, OrderBy: []string{q1.FirstOrderByOne, q1.FirstOrderByTwo}}, {From: []string{q1.SecondFrom}, OrderBy: []string{q1.SecondOrderByOne, q1.SecondOrderByTwo}}}
	return
}

func (q3 Q3Args) FromProtobuf(protobuf *proto.Query) Q3Args {
	firstQ := protobuf.Queries[0]
	q3.From, q3.Limit = firstQ.GetFrom()[0], int(firstQ.GetLimit())
	q3.FirstOrderBy, q3.SecondOrderBy = firstQ.GetOrderBy()[0], firstQ.GetOrderBy()[1]
	return q3
}

func (q3 Q3Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(3)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q3.From}, OrderBy: []string{q3.FirstOrderBy, q3.SecondOrderBy}, Limit: getInt32P(int32(q3.Limit))}}
	return
}

func (q5 Q5Args) FromProtobuf(protobuf *proto.Query) Q5Args {
	firstQ := protobuf.Queries[0]
	q5.From, q5.OrderBy = firstQ.GetFrom()[0], firstQ.GetOrderBy()[0]
	return q5
}

func (q5 Q5Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(5)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q5.From}, OrderBy: []string{q5.OrderBy}}}
	return
}

func (q6 Q6Args) FromProtobuf(protobuf *proto.Query) Q6Args {
	firstQ := protobuf.Queries[0]
	q6.From = firstQ.GetFrom()[0]
	return q6
}

func (q6 Q6Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(6)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q6.From}}}
	return
}

func (q11 Q11Args) FromProtobuf(protobuf *proto.Query) Q11Args {
	firstQ, secondQ := protobuf.Queries[0], protobuf.Queries[1]
	q11.FirstFrom, q11.SecondFrom = firstQ.GetFrom()[0], secondQ.GetFrom()[0]
	q11.FirstOrderBy, q11.FirstLimit = firstQ.GetOrderBy()[0], int(firstQ.GetLimit())
	return q11
}

func (q11 Q11Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(11)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q11.FirstFrom}, OrderBy: []string{q11.FirstOrderBy}, Limit: getInt32P(int32(q11.FirstLimit))}, {From: []string{q11.SecondFrom}}}
	return
}

func (q14 Q14Args) FromProtobuf(protobuf *proto.Query) Q14Args {
	firstQ := protobuf.Queries[0]
	q14.From = firstQ.GetFrom()[0]
	return q14
}

func (q14 Q14Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(14)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q14.From}}}
	return
}

func (q15 Q15Args) FromProtobuf(protobuf *proto.Query) Q15Args {
	firstQ := protobuf.Queries[0]
	q15.FromOne, q15.FromTwo = firstQ.GetFrom()[0], firstQ.GetFrom()[1]
	q15.Columns, q15.Where = firstQ.GetColumn(), firstQ.GetWhere()
	q15.OrderBy = firstQ.GetOrderBy()[0]
	return q15
}

func (q15 Q15Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(15)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q15.FromOne, q15.FromTwo}, Column: q15.Columns, Where: &q15.Where, OrderBy: []string{q15.OrderBy}}}
	return
}

func (q18 Q18Args) FromProtobuf(protobuf *proto.Query) Q18Args {
	firstQ := protobuf.Queries[0]
	q18.From, q18.Limit = firstQ.GetFrom()[0], int(firstQ.GetLimit())
	q18.OrderByOne, q18.OrderByTwo = firstQ.GetOrderBy()[0], firstQ.GetOrderBy()[1]
	return q18
}

func (q18 Q18Args) ToProtobuf() (protobuf *proto.Query) {
	protobuf = &proto.Query{QueryId: getInt32P(18)}
	protobuf.Queries = []*proto.QueryInfo{{From: []string{q18.From}, OrderBy: []string{q18.OrderByOne, q18.OrderByTwo}, Limit: getInt32P(int32(q18.Limit))}}
	return
}

func getInt32P(i int32) *int32 {
	return &i
}
