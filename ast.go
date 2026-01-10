package thunderdb

type QueryPart interface {
	Project(fields []int) QueryPart
	Join(other QueryPart, conditions []JoinOn) QueryPart
}

type Head struct {
	bodies []QueryPart
}

func (h *Head) Project(fields []int) QueryPart {
	return &ProjectedBody{
		child:  h,
		fields: fields,
	}
}

func (h *Head) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       h,
		right:      other,
		conditions: conditions,
	}
}

func (h *Head) Bind(bodies []QueryPart) {
	h.bodies = bodies
}

type ProjectedBody struct {
	child  QueryPart
	fields []int
}

func (ph *ProjectedBody) Project(fields []int) QueryPart {
	return &ProjectedBody{
		child:  ph.child,
		fields: fields,
	}
}

func (ph *ProjectedBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       ph,
		right:      other,
		conditions: conditions,
	}
}

type JoinedBody struct {
	left       QueryPart
	right      QueryPart
	conditions []JoinOn
}

func (jh *JoinedBody) Project(fields []int) QueryPart {
	return &ProjectedBody{
		child:  jh,
		fields: fields,
	}
}

func (jh *JoinedBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       jh,
		right:      other,
		conditions: conditions,
	}
}

const (
	EQ = Op(iota)
	NEQ
	LT
	LTE
	GT
	GTE
)

type Op int

type JoinOn struct {
	leftField  int
	rightField int
	operator   Op
}

type StoredBody string

func (ph StoredBody) Project(fields []int) QueryPart {
	return &ProjectedBody{
		child:  ph,
		fields: fields,
	}
}

func (ph StoredBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       ph,
		right:      other,
		conditions: conditions,
	}
}
