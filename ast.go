package thunderdb

type Query interface {
	Project(cols []int) (Query, error)
	Join(other Query, conditions []JoinOn) (Query, error)
	Metadata() *Metadata
}

type HeadQuery struct {
	bodies   []Query
	metadata Metadata
}

func NewHeadQuery(colsCount int, indexInfos []IndexInfo) (*HeadQuery, error) {
	result := &HeadQuery{}
	if err := initStoredMetadata(&result.metadata, colsCount, indexInfos); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *HeadQuery) Project(cols []int) (Query, error) {
	return newProjectedQuery(h, cols)
}

func (h *HeadQuery) Join(other Query, conditions []JoinOn) (Query, error) {
	return newJoinedQuery(h, other, conditions)
}

func (h *HeadQuery) Metadata() *Metadata {
	return &h.metadata
}

func (h *HeadQuery) Bind(bodies []Query) {
	h.bodies = bodies
}

type ProjectedQuery struct {
	child    Query
	cols     []int
	metadata Metadata
}

func newProjectedQuery(
	child Query,
	cols []int,
) (*ProjectedQuery, error) {
	result := &ProjectedQuery{
		child: child,
		cols:  cols,
	}
	if err := initProjectedMetadata(&result.metadata, child.Metadata(), cols); err != nil {
		return nil, err
	}
	return result, nil
}

func (ph *ProjectedQuery) Project(cols []int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

func (ph *ProjectedQuery) Join(other Query, conditions []JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

func (ph *ProjectedQuery) Metadata() *Metadata {
	return &ph.metadata
}

type JoinedQuery struct {
	left       Query
	right      Query
	conditions []JoinOn
	metadata   Metadata
}

func newJoinedQuery(
	left Query,
	right Query,
	conditions []JoinOn,
) (*JoinedQuery, error) {
	result := &JoinedQuery{
		left:       left,
		right:      right,
		conditions: conditions,
	}
	if err := initJoinedMetadata(&result.metadata, left.Metadata(), right.Metadata()); err != nil {
		return nil, err
	}
	return result, nil
}

func (jh *JoinedQuery) Project(cols []int) (Query, error) {
	return newProjectedQuery(jh, cols)
}

func (jh *JoinedQuery) Join(other Query, conditions []JoinOn) (Query, error) {
	return newJoinedQuery(jh, other, conditions)
}

func (jh *JoinedQuery) Metadata() *Metadata {
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

type StoredQuery struct {
	storageName string
	metadata    Metadata
}

func (ph *StoredQuery) Project(cols []int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

func (ph *StoredQuery) Join(other Query, conditions []JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

func (ph *StoredQuery) Metadata() *Metadata {
	return &ph.metadata
}

type IndexInfo struct {
	ReferencedCols []int
	IsUnique       bool
}
