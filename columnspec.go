package thunderdb

type ColumnSpec struct {
	ReferenceCols []string
	Unique        bool
	Indexed       bool
}
