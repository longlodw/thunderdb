package thunderdb

import (
	"errors"
	"fmt"
)

type ErrorCode int

const (
	ErrCodeFieldCountMismatch ErrorCode = iota
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

func (e ErrorCode) Error() string {
	switch e {
	case ErrCodeFieldCountMismatch:
		return "field count mismatch"
	case ErrCodeFieldNotFound:
		return "field not found"
	case ErrCodeFieldNotFoundInColumns:
		return "field not found in columns"
	case ErrCodeUnsupportedOperator:
		return "unsupported operator"
	case ErrCodeUnsupportedSelector:
		return "unsupported selector"
	case ErrCodeIndexNotFound:
		return "index not found"
	case ErrCodeUniqueConstraint:
		return "unique constraint violation"
	case ErrCodeCannotMarshal:
		return "cannot marshal"
	case ErrCodeCannotUnmarshal:
		return "cannot unmarshal"
	case ErrCodeMetaDataNotFound:
		return "metadata not found"
	case ErrCodeCorruptedIndexEntry:
		return "corrupted index entry"
	case ErrCodeCorruptedMetaDataEntry:
		return "corrupted metadata entry"
	case ErrCodeCannotEvaluateExpression:
		return "cannot evaluate expression"
	case ErrCodeRecursionDepthExceeded:
		return "recursion depth exceeded"
	case ErrCodeColumnCountExceeded64:
		return "column count exceeded maximum of 64"
	case ErrCodeInvalidColumnReference:
		return "invalid column reference"
	default:
		return fmt.Sprintf("unknown error code: %d", int(e))
	}
}

type ThunderError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *ThunderError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *ThunderError) Unwrap() error {
	return e.Cause
}

func (e *ThunderError) Is(target error) bool {
	if t, ok := target.(ErrorCode); ok {
		return e.Code == t
	}
	return false
}

// NewThunderError creates a new ThunderError.
// This is a helper to standardize creation if we want to export it later,
// but for now we keep the specific constructors.

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

func ErrIndexNotFound(relation string, indexBitmap uint64) error {
	return &ThunderError{
		Code:    ErrCodeIndexNotFound,
		Message: fmt.Sprintf("index not found for relation %s with bitmap %b", relation, indexBitmap),
	}
}

func ErrUniqueConstraint(relation string, indexBitmap uint64, value any) error {
	return &ThunderError{
		Code:    ErrCodeUniqueConstraint,
		Message: fmt.Sprintf("unique constraint violation on relation %s index %b for value %v", relation, indexBitmap, value),
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

func ErrCorruptedIndexEntry(relation string, indexBitmap uint64) error {
	return &ThunderError{
		Code:    ErrCodeCorruptedIndexEntry,
		Message: fmt.Sprintf("corrupted index entry in relation %s index %b", relation, indexBitmap),
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

// Wrap allows wrapping an existing error with a ThunderError code
func Wrap(code ErrorCode, msg string, err error) error {
	return &ThunderError{
		Code:    code,
		Message: msg,
		Cause:   err,
	}
}

// IsErrorCode checks if the error corresponds to a specific ThunderDB ErrorCode
func IsErrorCode(err error, code ErrorCode) bool {
	return errors.Is(err, code)
}
