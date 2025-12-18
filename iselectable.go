package thunder

import "iter"

type ISelectable interface {
	Select(ops ...Op) (iter.Seq2[map[string]any, error], error)
	Columns() []string
	Project(mapping map[string]string) (*Projection, error)
}
