package tpch

import (
	"encoding/binary"
	"fmt"
	"io"
	"postgres_tpch_go_lib/src/proto"
	"strconv"
	"time"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

const (
	//Requests
	PB_QUERY              = 1
	PB_INSERT_LINEITEM    = 2
	PB_INSERT_ORDER       = 3
	PB_INSERT_PARTSUPP    = 4
	PB_INSERT_SUPPLIER    = 5
	PB_INSERT_CUSTOMER    = 6
	PB_INSERT_PART        = 7
	PB_INSERT_NATION      = 8
	PB_INSERT_REGION      = 9
	PB_INSERT             = 10
	PB_BULK_INSERT        = 11
	PB_INSERT_ORDER_ITEMS = 12
	PB_INSERT_TPCH        = 13
	PB_MULTI_INSERT_TPCH  = 14
	PB_MULTI_QUERY        = 15
	PB_DELETE             = 16
	PB_CREATE_TABLE       = 17
	PB_CREATE_VIEW        = 18
	PB_DROP_TABLE         = 19
	PB_DROP_VIEW          = 20
	PB_CLOSE_CONNECTION   = 254

	//Replies
	PB_QUERY_REPLY             = 129
	PB_INSERT_REPLY            = 130
	PB_INSERT_TPCH_REPLY       = 141
	PB_MULTI_INSERT_TPCH_REPLY = 142
	PB_MULTI_QUERY_REPLY       = 143
	PB_DELETE_REPLY            = 144
	PB_CREATE_TABLE_REPLY      = 145
	PB_CREATE_VIEW_REPLY       = 146
	PB_DROP_TABLE_REPLY        = 147
	PB_DROP_VIEW_REPLY         = 148
)

func ReceiveProto(in io.Reader) (protoType byte, protobuf pb.Message, err error) {
	msgType, msgBuf, err := readProtoFromNetwork(in)
	if err != nil {
		fmt.Printf("[WARNING]Returning error on ReceiveProto. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		return msgType, nil, err
	}
	protobuf = unmarshallProto(msgType, msgBuf)
	return msgType, protobuf, err
}

func readProtoFromNetwork(in io.Reader) (msgType byte, msgData []byte, err error) {
	sizeBuf := make([]byte, 4)
	n := 0
	for nRead := 0; nRead < 4; {
		n, err = in.Read(sizeBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}

	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf))
	msgBuf := make([]byte, msgSize)
	for nRead := 0; nRead < msgSize; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}

	msgType = msgBuf[0]
	msgData = msgBuf[1:]
	return
}

func unmarshallProto(msgType byte, msgData []byte) (protobuf pb.Message) {
	switch msgType {
	case PB_QUERY:
		protobuf = &proto.Query{}
	case PB_MULTI_QUERY:
		protobuf = &proto.MultiQuery{}
	case PB_INSERT_TPCH:
		protobuf = &proto.TpchUpdate{}
	case PB_MULTI_INSERT_TPCH:
		protobuf = &proto.MultiTpchUpdate{}
	case PB_QUERY_REPLY:
		protobuf = &proto.QueryResp{}
	case PB_MULTI_QUERY_REPLY:
		protobuf = &proto.MultiQueryResp{}
	case PB_INSERT_TPCH_REPLY:
		protobuf = &proto.TpchUpdateResp{}
	case PB_MULTI_INSERT_TPCH_REPLY:
		protobuf = &proto.MultiTpchUpdateResp{}

	case PB_INSERT_ORDER_ITEMS:
		protobuf = &proto.InsertOrderItems{}
	case PB_BULK_INSERT:
		protobuf = &proto.BulkInsert{}

	case PB_INSERT_LINEITEM:
		protobuf = &proto.InsertLineItem{}
	case PB_INSERT_ORDER:
		protobuf = &proto.InsertOrder{}
	case PB_DELETE:
		protobuf = &proto.Delete{}
	case PB_INSERT_REPLY:
		protobuf = &proto.InsertResp{}
	case PB_DELETE_REPLY:
		protobuf = &proto.Delete{}

	case PB_INSERT_PARTSUPP:
		protobuf = &proto.InsertPartSupp{}
	case PB_INSERT_SUPPLIER:
		protobuf = &proto.InsertSupplier{}
	case PB_INSERT_CUSTOMER:
		protobuf = &proto.InsertCustomer{}
	case PB_INSERT_PART:
		protobuf = &proto.InsertPart{}
	case PB_INSERT_NATION:
		protobuf = &proto.InsertNation{}
	case PB_INSERT_REGION:
		protobuf = &proto.InsertRegion{}
	case PB_INSERT:
		protobuf = &proto.Insert{}
	case PB_CREATE_VIEW:
		protobuf = &proto.CreateView{}
	case PB_CREATE_TABLE:
		protobuf = &proto.CreateTable{}
	case PB_DROP_TABLE:
		protobuf = &proto.DropTable{}
	case PB_DROP_VIEW:
		protobuf = &proto.DropView{}
	case PB_CREATE_VIEW_REPLY:
		protobuf = &proto.CreateViewResp{}
	case PB_CREATE_TABLE_REPLY:
		protobuf = &proto.CreateTableResp{}
	case PB_DROP_TABLE_REPLY:
		protobuf = &proto.DropTableResp{}
	case PB_DROP_VIEW_REPLY:
		protobuf = &proto.DropViewResp{}
	case PB_CLOSE_CONNECTION:
		protobuf = &proto.CloseConnection{}
	default:
		fmt.Printf("[WARNING]Unknown message type: %v\n", msgType)
	}
	err := pb.Unmarshal(msgData, protobuf)
	if err != nil {
		fmt.Printf("[ERROR]Error unmarshalling protobuf: %s\n", err)
	}
	return

}

func SendProto(code byte, protobuf pb.Message, writer io.Writer) (err error) {
	toSend, err := pb.Marshal(protobuf)
	if err != nil {
		fmt.Printf("[ERROR]Error marshalling protobuf: %s\n", err)
		return
	}
	protoSize := len(toSend)
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], toSend)
	_, err = writer.Write(buffer)
	return
}

func (l *LineItem) ToProto() *proto.InsertLineItem {
	return &proto.InsertLineItem{
		OrderKey:      &l.L_ORDERKEY,
		PartKey:       &l.L_PARTKEY,
		SuppKey:       &l.L_SUPPKEY,
		LineNumber:    pb.Int32(int32(l.L_LINENUMBER)),
		Quantity:      pb.Int32(int32(l.L_QUANTITY)),
		ExtendedPrice: &l.L_EXTENDEDPRICE,
		Discount:      &l.L_DISCOUNT,
		Tax:           &l.L_TAX,
		ReturnFlag:    &l.L_RETURNFLAG,
		LineStatus:    &l.L_LINESTATUS,
		ShipDate:      TimeToDateProto(l.L_SHIPDATE),
		CommitDate:    TimeToDateProto(l.L_COMMITDATE),
		ReceiptDate:   TimeToDateProto(l.L_RECEIPTDATE),
		ShipInstruct:  &l.L_SHIPINSTRUCT,
		ShipMode:      &l.L_SHIPMODE,
		Comment:       &l.L_COMMENT,
	}
}

func (l *LineItem) FromProto(p *proto.InsertLineItem) *LineItem {
	l.L_ORDERKEY = *p.OrderKey
	l.L_PARTKEY = *p.PartKey
	l.L_SUPPKEY = *p.SuppKey
	l.L_LINENUMBER = int8(*p.LineNumber)
	l.L_QUANTITY = int8(*p.Quantity)
	l.L_EXTENDEDPRICE = *p.ExtendedPrice
	l.L_DISCOUNT = *p.Discount
	l.L_TAX = *p.Tax
	l.L_RETURNFLAG = *p.ReturnFlag
	l.L_LINESTATUS = *p.LineStatus
	l.L_SHIPDATE = DateProtoToTime(p.ShipDate)
	l.L_COMMITDATE = DateProtoToTime(p.CommitDate)
	l.L_RECEIPTDATE = DateProtoToTime(p.ReceiptDate)
	l.L_SHIPINSTRUCT = *p.ShipInstruct
	l.L_SHIPMODE = *p.ShipMode
	l.L_COMMENT = *p.Comment
	return l
}

func (o *Orders) ToProto() *proto.InsertOrder {
	return &proto.InsertOrder{
		OrderKey:      &o.O_ORDERKEY,
		CustKey:       &o.O_CUSTKEY,
		OrderStatus:   &o.O_ORDERSTATUS,
		TotalPrice:    &o.O_TOTALPRICE,
		OrderDate:     TimeToDateProto(o.O_ORDERDATE),
		OrderPriority: &o.O_ORDERPRIORITY,
		Clerk:         &o.O_CLERK,
		ShipPriority:  &o.O_SHIPPRIORITY,
		Comment:       &o.O_COMMENT,
	}
}

func (o *Orders) FromProto(p *proto.InsertOrder) *Orders {
	o.O_ORDERKEY = *p.OrderKey
	o.O_CUSTKEY = *p.CustKey
	o.O_ORDERSTATUS = *p.OrderStatus
	o.O_TOTALPRICE = *p.TotalPrice
	o.O_ORDERDATE = DateProtoToTime(p.OrderDate)
	o.O_ORDERPRIORITY = *p.OrderPriority
	o.O_CLERK = *p.Clerk
	o.O_SHIPPRIORITY = *p.ShipPriority
	o.O_COMMENT = *p.Comment
	return o
}

func (ps *PartSupp) ToProto() *proto.InsertPartSupp {
	return &proto.InsertPartSupp{
		PartKey:    &ps.PS_PARTKEY,
		SuppKey:    &ps.PS_SUPPKEY,
		AvailQty:   &ps.PS_AVAILQTY,
		SupplyCost: &ps.PS_SUPPLYCOST,
		Comment:    &ps.PS_COMMENT,
	}
}

func (ps *PartSupp) FromProto(p *proto.InsertPartSupp) *PartSupp {
	ps.PS_PARTKEY = *p.PartKey
	ps.PS_SUPPKEY = *p.SuppKey
	ps.PS_AVAILQTY = *p.AvailQty
	ps.PS_SUPPLYCOST = *p.SupplyCost
	ps.PS_COMMENT = *p.Comment
	return ps
}

func (s *Supplier) ToProto() *proto.InsertSupplier {
	return &proto.InsertSupplier{
		SuppKey:   &s.S_SUPPKEY,
		Name:      &s.S_NAME,
		Address:   &s.S_ADDRESS,
		NationKey: pb.Int32(int32(s.S_NATIONKEY)),
		Phone:     &s.S_PHONE,
		AcctBal:   &s.S_ACCTBAL,
		Comment:   &s.S_COMMENT,
	}
}

func (s *Supplier) FromProto(p *proto.InsertSupplier) *Supplier {
	s.S_SUPPKEY = *p.SuppKey
	s.S_NAME = *p.Name
	s.S_ADDRESS = *p.Address
	s.S_NATIONKEY = int8(*p.NationKey)
	s.S_PHONE = *p.Phone
	s.S_ACCTBAL = *p.AcctBal
	s.S_COMMENT = *p.Comment
	return s
}

func (p *Part) ToProto() *proto.InsertPart {
	return &proto.InsertPart{
		PartKey:     &p.P_PARTKEY,
		Name:        &p.P_NAME,
		Mfgr:        &p.P_MFGR,
		Brand:       &p.P_BRAND,
		Type:        &p.P_TYPE,
		Size:        &p.P_SIZE,
		Container:   &p.P_CONTAINER,
		RetailPrice: &p.P_RETAILPRICE,
		Comment:     &p.P_COMMENT,
	}
}

func (p *Part) FromProto(proto *proto.InsertPart) *Part {
	p.P_PARTKEY = *proto.PartKey
	p.P_NAME = *proto.Name
	p.P_MFGR = *proto.Mfgr
	p.P_BRAND = *proto.Brand
	p.P_TYPE = *proto.Type
	p.P_SIZE = *proto.Size
	p.P_CONTAINER = *proto.Container
	p.P_RETAILPRICE = *proto.RetailPrice
	p.P_COMMENT = *proto.Comment
	return p
}

func (c *Customer) ToProto() *proto.InsertCustomer {
	return &proto.InsertCustomer{
		CustKey:    &c.C_CUSTKEY,
		Name:       &c.C_NAME,
		Address:    &c.C_ADDRESS,
		NationKey:  pb.Int32(int32(c.C_NATIONKEY)),
		Phone:      &c.C_PHONE,
		AcctBal:    &c.C_ACCTBAL,
		MktSegment: &c.C_MKTSEGMENT,
		Comment:    &c.C_COMMENT,
	}
}

func (c *Customer) FromProto(p *proto.InsertCustomer) *Customer {
	c.C_CUSTKEY = *p.CustKey
	c.C_NAME = *p.Name
	c.C_ADDRESS = *p.Address
	c.C_NATIONKEY = int8(*p.NationKey)
	c.C_PHONE = *p.Phone
	c.C_ACCTBAL = *p.AcctBal
	c.C_MKTSEGMENT = *p.MktSegment
	c.C_COMMENT = *p.Comment
	return c
}

func (n *Nation) ToProto() *proto.InsertNation {
	return &proto.InsertNation{
		NationKey: pb.Int32(int32(n.N_NATIONKEY)),
		Name:      &n.N_NAME,
		RegionKey: pb.Int32(int32(n.N_REGIONKEY)),
		Comment:   &n.N_COMMENT,
	}
}

func (n *Nation) FromProto(p *proto.InsertNation) *Nation {
	n.N_NATIONKEY = int8(*p.NationKey)
	n.N_NAME = *p.Name
	n.N_REGIONKEY = int8(*p.RegionKey)
	n.N_COMMENT = *p.Comment
	return n
}

func (r *Region) ToProto() *proto.InsertRegion {
	return &proto.InsertRegion{
		RegionKey: pb.Int32(int32(r.R_REGIONKEY)),
		Name:      &r.R_NAME,
		Comment:   &r.R_COMMENT,
	}
}

func (r *Region) FromProto(p *proto.InsertRegion) *Region {
	r.R_REGIONKEY = int8(*p.RegionKey)
	r.R_NAME = *p.Name
	r.R_COMMENT = *p.Comment
	return r
}

func TimeToDateProto(t time.Time) *proto.Date {
	return &proto.Date{Year: pb.Int32(int32(t.Year())), Month: pb.Int32(int32(t.Month())), Day: pb.Int32(int32(t.Day()))}
}

func DateProtoToTime(d *proto.Date) time.Time {
	return time.Date(int(*d.Year), time.Month(*d.Month), int(*d.Day), 0, 0, 0, 0, time.UTC)
}

func FromLineItemsSliceToProto(items []*LineItem) []*proto.InsertLineItem {
	protoSlice := make([]*proto.InsertLineItem, len(items))
	for i, v := range items {
		protoSlice[i] = v.ToProto()
	}
	return protoSlice
}

func FromLineItemsProtoToSlice(items []*proto.InsertLineItem) []*LineItem {
	slice := make([]*LineItem, len(items))
	for i, v := range items {
		slice[i] = (&LineItem{}).FromProto(v)
	}
	return slice
}

func GetStringType(msgType byte) string {
	switch msgType {
	case PB_QUERY:
		return "PB_QUERY"
	case PB_MULTI_QUERY:
		return "PB_MULTI_QUERY"
	case PB_INSERT_TPCH:
		return "PB_INSERT_TPCH"
	case PB_MULTI_INSERT_TPCH:
		return "PB_MULTI_INSERT_TPCH"
	case PB_QUERY_REPLY:
		return "PB_QUERY_REPLY"
	case PB_MULTI_QUERY_REPLY:
		return "PB_MULTI_QUERY_REPLY"
	case PB_INSERT_TPCH_REPLY:
		return "PB_INSERT_TPCH_REPLY"
	case PB_MULTI_INSERT_TPCH_REPLY:
		return "PB_MULTI_INSERT_TPCH_REPLY"
	case PB_INSERT_ORDER_ITEMS:
		return "PB_INSERT_ORDER_ITEMS"
	case PB_BULK_INSERT:
		return "PB_BULK_INSERT"
	case PB_INSERT_LINEITEM:
		return "PB_INSERT_LINEITEM"
	case PB_INSERT_ORDER:
		return "PB_INSERT_ORDER"
	case PB_DELETE:
		return "PB_DELETE"
	case PB_INSERT_REPLY:
		return "PB_INSERT_REPLY"
	case PB_DELETE_REPLY:
		return "PB_DELETE_REPLY"
	case PB_INSERT_PARTSUPP:
		return "PB_INSERT_PARTSUPP"
	case PB_INSERT_SUPPLIER:
		return "PB_INSERT_SUPPLIER"
	case PB_INSERT_CUSTOMER:
		return "PB_INSERT_CUSTOMER"
	case PB_INSERT_PART:
		return "PB_INSERT_PART"
	case PB_INSERT_NATION:
		return "PB_INSERT_NATION"
	case PB_INSERT_REGION:
		return "PB_INSERT_REGION"
	case PB_INSERT:
		return "PB_INSERT"
	case PB_CREATE_VIEW:
		return "PB_CREATE_VIEW"
	case PB_CREATE_TABLE:
		return "PB_CREATE_TABLE"
	case PB_DROP_TABLE:
		return "PB_DROP_TABLE"
	case PB_DROP_VIEW:
		return "PB_DROP_VIEW"
	case PB_CREATE_VIEW_REPLY:
		return "PB_CREATE_VIEW_REPLY"
	case PB_CREATE_TABLE_REPLY:
		return "PB_CREATE_TABLE_REPLY"
	case PB_DROP_TABLE_REPLY:
		return "PB_DROP_TABLE_REPLY"
	case PB_DROP_VIEW_REPLY:
		return "PB_DROP_VIEW_REPLY"
	case PB_CLOSE_CONNECTION:
		return "PB_CLOSE_CONNECTION"
	default:
		return "[WARNING]Unknown message type: " + strconv.Itoa(int(msgType))
	}
}
