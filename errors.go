package thunderdb

import "fmt"

const (
	ErrCodeFieldCountMismatch = iota
	ErrCodeFieldNotFound
	ErrCodeFieldNotFoundInColumns
	ErrCodeUnsupportedOperator
	ErrCodeUnsupportedSelector
	ErrCodeIndexNotFound
	ErrCodeUniqueConstraint
	ErrCodeCannotMarshal
	ErrCodeCannotUnmarshal
	ErrCodeMetaDataNotFound
	ErrCodeCorruptedIndexEntry
	ErrCodeCorruptedMetaDataEntry
	ErrCodeCannotEvaluateExpression
	ErrCodeRecursionDepthExceeded
	ErrCodeColumnCountExceeded64
	ErrCodeInvalidColumnReference
)

type ThunderError struct {
	Code    int
	Message string
}

func (e *ThunderError) Error() string {
	return e.Message
}

func ErrFieldCountMismatch(expected, got int) error {
	return &ThunderError{
		Code:    ErrCodeFieldCountMismatch,
		Message: fmt.Sprintf("object field count mismatch: expected %d, got %d", expected, got),
	}
}

func ErrUnsupportedOperator(op Op) error {
	return &ThunderError{
		Code:    ErrCodeUnsupportedOperator,
		Message: fmt.Sprintf("unsupported operator: %d", op),
	}
}

func ErrFieldNotFound(field string) error {
	return &ThunderError{
		Code:    ErrCodeFieldNotFound,
		Message: fmt.Sprintf("field not found: %s", field),
	}
}

func ErrUnsupportedSelector() error {
	return &ThunderError{
		Code:    ErrCodeUnsupportedSelector,
		Message: "unsupported selector",
	}
}

func ErrIndexNotFound(indexName string) error {
	return &ThunderError{
		Code:    ErrCodeIndexNotFound,
		Message: fmt.Sprintf("index not found: %s", indexName),
	}
}

func ErrUniqueConstraint(indexName string, value any) error {
	return &ThunderError{
		Code:    ErrCodeUniqueConstraint,
		Message: fmt.Sprintf("unique constraint violation on index %s for value %v", indexName, value),
	}
}

func ErrCannotMarshal(v any) error {
	return &ThunderError{
		Code:    ErrCodeCannotMarshal,
		Message: fmt.Sprintf("cannot marshal object: %v", v),
	}
}

func ErrCannotUnmarshal(v any) error {
	return &ThunderError{
		Code:    ErrCodeCannotUnmarshal,
		Message: fmt.Sprintf("cannot unmarshal into object: %v", v),
	}
}

func ErrMetaDataNotFound(relation string) error {
	return &ThunderError{
		Code:    ErrCodeMetaDataNotFound,
		Message: fmt.Sprintf("meta data not found for relation: %s", relation),
	}
}

func ErrCorruptedIndexEntry(indexName string) error {
	return &ThunderError{
		Code:    ErrCodeCorruptedIndexEntry,
		Message: fmt.Sprintf("corrupted index entry in index %s", indexName),
	}
}

func ErrCorruptedMetaDataEntry(relation, metaName string) error {
	return &ThunderError{
		Code:    ErrCodeCorruptedMetaDataEntry,
		Message: fmt.Sprintf("corrupted meta data entry %s in relation %s", metaName, relation),
	}
}

func ErrRecursionDepthExceeded(depth int) error {
	return &ThunderError{
		Code:    ErrCodeRecursionDepthExceeded,
		Message: fmt.Sprintf("recursion depth exceeded: %d", depth),
	}
}

func ErrColumnCountExceeded64(count int) error {
	return &ThunderError{
		Code:    ErrCodeColumnCountExceeded64,
		Message: fmt.Sprintf("column count exceeded maximum of 64: %d", count),
	}
}

func ErrInvalidColumnReference(relation string, col int) error {
	return &ThunderError{
		Code:    ErrCodeInvalidColumnReference,
		Message: fmt.Sprintf("invalid column index %d for storage %s", col, relation),
	}
}
