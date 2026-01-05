package thunderdb

import "iter"

type Selector interface {
	Select(ranges map[string]*BytesRange) (iter.Seq2[Row, error], error)
	Columns() []string
	Fields() map[string]ColumnSpec
	Project(mapping map[string]string) Selector
	IsRecursive() bool
	Join(bodies ...Selector) Selector
	selectEval(ranges map[string]*BytesRange, noEval bool) (iter.Seq2[Row, error], error)
}

type linkedSelector interface {
	Selector
	addParent(parent *queryParent)
	parents() []*queryParent
}
