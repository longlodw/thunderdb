package thunderdb

type QueryPart interface {
	Project(cols []int, computedCols []int) QueryPart
	Join(other QueryPart, conditions []JoinOn) QueryPart
	ColumnSpecs() []ColumnSpec
	ComputedColumnSpecs() []ComputedColumnSpec
}

type Head struct {
	bodies   []QueryPart
	metadata storageMetadata
}

func NewHead(
	collumnSpecs []ColumnSpec,
	computedColumnSpecs []ComputedColumnSpec,
) *Head {
	allColsRef := make([]int, len(collumnSpecs))
	for i := range collumnSpecs {
		allColsRef[i] = i
	}
	computedColumnSpecs = append(computedColumnSpecs, ComputedColumnSpec{
		FieldRefs: allColsRef,
		IsUnique:  true,
	})
	return &Head{
		metadata: storageMetadata{
			ColumnSpecs:         collumnSpecs,
			ComputedColumnSpecs: computedColumnSpecs,
		},
	}
}

func (h *Head) Project(cols []int, computedCols []int) QueryPart {
	return &ProjectedBody{
		child:        h,
		cols:         cols,
		computedCols: computedCols,
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

func (h *Head) ColumnSpecs() []ColumnSpec {
	return h.metadata.ColumnSpecs
}

func (h *Head) ComputedColumnSpecs() []ComputedColumnSpec {
	return h.metadata.ComputedColumnSpecs
}

type ProjectedBody struct {
	child        QueryPart
	cols         []int
	computedCols []int
	metadata     storageMetadata
}

func newProjectedBody(
	child QueryPart,
	cols []int,
	computedCols []int,
) *ProjectedBody {
	result := &ProjectedBody{
		child:        child,
		cols:         cols,
		computedCols: computedCols,
	}
	for _, c := range cols {
		result.metadata.ColumnSpecs = append(result.metadata.ColumnSpecs, child.ColumnSpecs()[c])
	}
	for _, cc := range computedCols {
		result.metadata.ComputedColumnSpecs = append(result.metadata.ComputedColumnSpecs, child.ComputedColumnSpecs()[cc])
	}
	return result
}

func (ph *ProjectedBody) Project(cols []int, computedCols []int) QueryPart {
	return &ProjectedBody{
		child:        ph.child,
		cols:         cols,
		computedCols: computedCols,
	}
}

func (ph *ProjectedBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       ph,
		right:      other,
		conditions: conditions,
	}
}

func (ph *ProjectedBody) ColumnSpecs() []ColumnSpec {
	return ph.metadata.ColumnSpecs
}

func (ph *ProjectedBody) ComputedColumnSpecs() []ComputedColumnSpec {
	return ph.metadata.ComputedColumnSpecs
}

type JoinedBody struct {
	left       QueryPart
	right      QueryPart
	conditions []JoinOn
	metadata   storageMetadata
}

func newJoinedBody(
	left QueryPart,
	right QueryPart,
	conditions []JoinOn,
) *JoinedBody {
	result := &JoinedBody{
		left:       left,
		right:      right,
		conditions: conditions,
	}
	result.metadata.ColumnSpecs = append(result.metadata.ColumnSpecs, left.ColumnSpecs()...)
	result.metadata.ColumnSpecs = append(result.metadata.ColumnSpecs, right.ColumnSpecs()...)
	result.metadata.ComputedColumnSpecs = append(result.metadata.ComputedColumnSpecs, left.ComputedColumnSpecs()...)
	result.metadata.ComputedColumnSpecs = append(result.metadata.ComputedColumnSpecs, right.ComputedColumnSpecs()...)
	return result
}

func (jh *JoinedBody) Project(cols []int, computedCols []int) QueryPart {
	return &ProjectedBody{
		child:        jh,
		cols:         cols,
		computedCols: computedCols,
	}
}

func (jh *JoinedBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       jh,
		right:      other,
		conditions: conditions,
	}
}

func (jh *JoinedBody) ColumnSpecs() []ColumnSpec {
	return jh.metadata.ColumnSpecs
}

func (jh *JoinedBody) ComputedColumnSpecs() []ComputedColumnSpec {
	return jh.metadata.ComputedColumnSpecs
}

func (n *JoinedBody) splitRanges(ranges map[int]*BytesRange) (map[int]*BytesRange, map[int]*BytesRange) {
	leftRanges := make(map[int]*BytesRange)
	rightRanges := make(map[int]*BytesRange)
	for field, r := range ranges {
		if field < len(n.left.ColumnSpecs()) {
			leftRanges[field] = r
		} else if field < len(n.left.ColumnSpecs())+len(n.right.ColumnSpecs()) {
			rightRanges[field-len(n.left.ColumnSpecs())] = r
		} else if field < len(n.left.ColumnSpecs())+len(n.right.ColumnSpecs())+len(n.left.ComputedColumnSpecs()) {
			leftRanges[field-len(n.left.ColumnSpecs())-len(n.right.ColumnSpecs())] = r
		} else {
			rightRanges[field-len(n.left.ColumnSpecs())-len(n.right.ColumnSpecs())-len(n.left.ComputedColumnSpecs())] = r
		}
	}
	return leftRanges, rightRanges
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
	metadata    storageMetadata
}

func (ph StoredBody) Project(cols []int, computedCols []int) QueryPart {
	return &ProjectedBody{
		child:        ph,
		cols:         cols,
		computedCols: computedCols,
	}
}

func (ph StoredBody) Join(other QueryPart, conditions []JoinOn) QueryPart {
	return &JoinedBody{
		left:       ph,
		right:      other,
		conditions: conditions,
	}
}

func (ph StoredBody) ColumnSpecs() []ColumnSpec {
	return ph.metadata.ColumnSpecs
}

func (ph StoredBody) ComputedColumnSpecs() []ComputedColumnSpec {
	return ph.metadata.ComputedColumnSpecs
}
