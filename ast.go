package thunderdb

type QueryPart interface {
	Project(cols []int) (QueryPart, error)
	Join(other QueryPart, conditions []JoinOn) (QueryPart, error)
	Metadata() *Metadata
}

type Head struct {
	bodies   []QueryPart
	metadata Metadata
}

func NewHead(colsCount int, indexInfos []IndexInfo) (*Head, error) {
	result := &Head{}
	if err := initStoredMetadata(&result.metadata, colsCount, indexInfos); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *Head) Project(cols []int) (QueryPart, error) {
	return newProjectedBody(h, cols)
}

func (h *Head) Join(other QueryPart, conditions []JoinOn) (QueryPart, error) {
	return newJoinedBody(h, other, conditions)
}

func (h *Head) Metadata() *Metadata {
	return &h.metadata
}

func (h *Head) Bind(bodies []QueryPart) {
	h.bodies = bodies
}

type ProjectedBody struct {
	child    QueryPart
	cols     []int
	metadata Metadata
}

func newProjectedBody(
	child QueryPart,
	cols []int,
) (*ProjectedBody, error) {
	result := &ProjectedBody{
		child: child,
		cols:  cols,
	}
	if err := initProjectedMetadata(&result.metadata, child.Metadata(), cols); err != nil {
		return nil, err
	}
	return result, nil
}

func (ph *ProjectedBody) Project(cols []int) (QueryPart, error) {
	return newProjectedBody(ph, cols)
}

func (ph *ProjectedBody) Join(other QueryPart, conditions []JoinOn) (QueryPart, error) {
	return newJoinedBody(ph, other, conditions)
}

func (ph *ProjectedBody) Metadata() *Metadata {
	return &ph.metadata
}

type JoinedBody struct {
	left       QueryPart
	right      QueryPart
	conditions []JoinOn
	metadata   Metadata
}

func newJoinedBody(
	left QueryPart,
	right QueryPart,
	conditions []JoinOn,
) (*JoinedBody, error) {
	result := &JoinedBody{
		left:       left,
		right:      right,
		conditions: conditions,
	}
	if err := initJoinedMetadata(&result.metadata, left.Metadata(), right.Metadata()); err != nil {
		return nil, err
	}
	return result, nil
}

func (jh *JoinedBody) Project(cols []int) (QueryPart, error) {
	return newProjectedBody(jh, cols)
}

func (jh *JoinedBody) Join(other QueryPart, conditions []JoinOn) (QueryPart, error) {
	return newJoinedBody(jh, other, conditions)
}

func (jh *JoinedBody) Metadata() *Metadata {
	return &jh.metadata
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

type StoredBody struct {
	storageName string
	metadata    Metadata
}

func (ph *StoredBody) Project(cols []int) (QueryPart, error) {
	return newProjectedBody(ph, cols)
}

func (ph *StoredBody) Join(other QueryPart, conditions []JoinOn) (QueryPart, error) {
	return newJoinedBody(ph, other, conditions)
}

func (ph *StoredBody) Metadata() *Metadata {
	return &ph.metadata
}

type IndexInfo struct {
	ReferencedCols []int
	IsUnique       bool
}
