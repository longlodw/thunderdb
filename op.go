package thunder

const (
	OpEq = OpType(0b0010)
	OpNe = OpType(0b0101)
	OpGt = OpType(0b0100)
	OpLt = OpType(0b0001)
	OpGe = OpType(0b0110)
	OpLe = OpType(0b0011)
)

type OpType uint8

type Op struct {
	Field string
	Value any
	Type  OpType
}

func Eq(value any) Op {
	return Op{
		Value: value,
		Type:  OpEq,
	}
}

func Ne(value any) Op {
	return Op{
		Value: value,
		Type:  OpNe,
	}
}

func Gt(value any) Op {
	return Op{
		Value: value,
		Type:  OpGt,
	}
}

func Lt(value any) Op {
	return Op{
		Value: value,
		Type:  OpLt,
	}
}

func Ge(value any) Op {
	return Op{
		Value: value,
		Type:  OpGe,
	}
}

func Le(value any) Op {
	return Op{
		Value: value,
		Type:  OpLe,
	}
}
