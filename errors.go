package thunderdb

import (
	"errors"
	"fmt"
)

// ErrorCode represents a category of errors that can occur in ThunderDB.
// Error codes can be used with errors.Is() to check error types.
//
// Example:
//
//	if errors.Is(err, thunderdb.ErrCodeUniqueConstraint) {
//	    // Handle unique constraint violation
//	}
type ErrorCode int

const (
	// ErrCodeFieldCountMismatch indicates a mismatch between expected and actual field counts.
	ErrCodeFieldCountMismatch ErrorCode = iota
	// ErrCodeFieldNotFound indicates a requested field/column does not exist.
	ErrCodeFieldNotFound
	// ErrCodeFieldNotFoundInColumns indicates a field reference is invalid for the column set.
	ErrCodeFieldNotFoundInColumns
	// ErrCodeUnsupportedOperator indicates an invalid comparison operator was used.
	ErrCodeUnsupportedOperator
	// ErrCodeUnsupportedSelector indicates an invalid selector was used.
	ErrCodeUnsupportedSelector
	// ErrCodeIndexNotFound indicates a required index does not exist.
	ErrCodeIndexNotFound
	// ErrCodeUniqueConstraint indicates an insert/update would violate a unique constraint.
	ErrCodeUniqueConstraint
	// ErrCodeCannotMarshal indicates a value could not be serialized.
	ErrCodeCannotMarshal
	// ErrCodeCannotUnmarshal indicates a value could not be deserialized.
	ErrCodeCannotUnmarshal
	// ErrCodeMetaDataNotFound indicates relation metadata was not found.
	ErrCodeMetaDataNotFound
	// ErrCodeCorruptedIndexEntry indicates an index entry is corrupted.
	ErrCodeCorruptedIndexEntry
	// ErrCodeCorruptedMetaDataEntry indicates metadata is corrupted.
	ErrCodeCorruptedMetaDataEntry
	// ErrCodeCannotEvaluateExpression indicates an expression could not be evaluated.
	ErrCodeCannotEvaluateExpression
	// ErrCodeRecursionDepthExceeded indicates a recursive query exceeded the depth limit.
	ErrCodeRecursionDepthExceeded
	// ErrCodeColumnCountExceeded64 indicates the column count exceeds the maximum of 64.
	ErrCodeColumnCountExceeded64
	// ErrCodeInvalidColumnReference indicates a column reference is out of bounds.
	ErrCodeInvalidColumnReference
	// ErrCodeInvalidEncoding indicates data has invalid encoding.
	ErrCodeInvalidEncoding
)

// Error returns a human-readable description of the error code.
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
	case ErrCodeInvalidEncoding:
		return "invalid encoding"
	default:
		return fmt.Sprintf("unknown error code: %d", int(e))
	}
}

// ThunderError is the primary error type returned by ThunderDB operations.
// It includes an error code, message, and optional cause for error chaining.
//
// Use errors.Is() to check for specific error codes:
//
//	if errors.Is(err, thunderdb.ErrCodeUniqueConstraint) {
//	    // Handle unique constraint violation
//	}
type ThunderError struct {
	Code    ErrorCode // The category of error
	Message string    // Human-readable error description
	Cause   error     // Underlying error, if any
}

// Error returns the error message, including the cause if present.
func (e *ThunderError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap returns the underlying cause of the error for error chain traversal.
func (e *ThunderError) Unwrap() error {
	return e.Cause
}

// Is reports whether the error matches a target ErrorCode.
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

func ErrInvalidEncoding(detail string) error {
	return &ThunderError{
		Code:    ErrCodeInvalidEncoding,
		Message: fmt.Sprintf("invalid encoding: %s", detail),
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
