package thunder

import (
	"errors"
	"fmt"
)

var (
	// Persistent / Query errors
	ErrObjectFieldCountMismatch = errors.New("object has incorrect number of fields")
	ErrObjectMissingField       = func(col string) error { return fmt.Errorf("object is missing field %s", col) }
	ErrIndexMetadataNotFound    = func(idxName string) error { return fmt.Errorf("index metadata not found for index %s", idxName) }
	ErrFieldNotFoundInColumns   = func(field string) error { return fmt.Errorf("field %s not found in columns", field) }
	ErrOperationValueComposite  = func(field string) error {
		return fmt.Errorf("operation value must be a slice for composite index %s", field)
	}
	ErrTypeMismatch         = func(got, want any) error { return fmt.Errorf("type mismatch: %T vs %T", got, want) }
	ErrUnsupportedOperator  = func(op OpType) error { return fmt.Errorf("unsupported operator: %d", op) }
	ErrMarshalComparison    = func(err error) error { return fmt.Errorf("failed to marshal value for comparison: %v", err) }
	ErrBodyMissingColumn    = func(col string) error { return fmt.Errorf("body missing required column %s", col) }
	ErrUnsupportedSelector  = func(node any) error { return fmt.Errorf("unsupported selector type in query body: %T", node) }
	ErrProjectionMissingCol = func(baseCol string) error { return fmt.Errorf("column %s not found in base columns", baseCol) }
	ErrProjectionMissingFld = func(field string) error { return fmt.Errorf("field %s not found in projection", field) }
	ErrIndexColNotFound     = func(col string) error { return fmt.Errorf("index column %s not found in columns", col) }
	ErrIndexNotFound        = func(idxName string) error { return fmt.Errorf("index %s not found", idxName) }
	ErrUniqueColNotFound    = func(col string) error { return fmt.Errorf("unique column %s not found in columns", col) }
	ErrUniqueConstraint     = func(idxName string) error {
		return fmt.Errorf("unique constraint violation on index %s", idxName)
	}
	ErrUniqueIndexValueCount = func(field string, expected, got int) error {
		return fmt.Errorf("unique index %s requires %d values, got %d", field, expected, got)
	}
	ErrIndexValueCount = func(field string, expected, got int) error {
		return fmt.Errorf("index %s requires %d values, got %d", field, expected, got)
	}
	ErrFieldNotFoundInObject = func(field string) error { return fmt.Errorf("field %s not found in object", field) }
	ErrCannotMarshal         = func(v any) error { return fmt.Errorf("cannot marshal value '%v' of type %T", v, v) }
	ErrUnsupportedType       = func(v any) error { return fmt.Errorf("unsupported comparison type %T", v) }
)
