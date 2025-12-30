package thunderdb

type Row interface {
	Get(field string) (any, error)
	ToMap() (map[string]any, error)
}
