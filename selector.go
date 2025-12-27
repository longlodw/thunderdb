package thunder

import "iter"

type Selector interface {
	Select(ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error)
	Columns() []string
	Project(mapping map[string]string) Selector
	IsRecursive() bool
	Join(bodies ...Selector) Selector
}

type linkedSelector interface {
	Selector
	addParent(parent *queryParent)
	parents() []*queryParent
}
