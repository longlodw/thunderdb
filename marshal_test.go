package thunderdb

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

// TestOrderedCodec_LexicographicalOrder tests that the marshaled bytes
// maintain the same lexicographical order as the original values.
func TestOrderedCodec_LexicographicalOrder(t *testing.T) {
	tests := []struct {
		name   string
		values []any
	}{
		{
			name:   "nil values",
			values: []any{nil, nil},
		},
		{
			name:   "booleans",
			values: []any{false, true},
		},
		{
			name:   "positive integers",
			values: []any{int64(0), int64(1), int64(100), int64(1000), int64(math.MaxInt64)},
		},
		{
			name:   "negative integers",
			values: []any{int64(math.MinInt64), int64(-1000), int64(-100), int64(-1), int64(0)},
		},
		{
			name:   "mixed sign integers",
			values: []any{int64(-100), int64(-10), int64(-1), int64(0), int64(1), int64(10), int64(100)},
		},
		{
			name:   "unsigned integers",
			values: []any{uint64(0), uint64(1), uint64(100), uint64(1000), uint64(math.MaxUint64)},
		},
		{
			name:   "positive floats",
			values: []any{float64(0.0), float64(0.1), float64(1.0), float64(10.5), float64(100.0)},
		},
		{
			name:   "negative floats",
			values: []any{float64(-100.0), float64(-10.5), float64(-1.0), float64(-0.1), float64(0.0)},
		},
		{
			name:   "mixed sign floats",
			values: []any{float64(-100.0), float64(-1.0), float64(0.0), float64(1.0), float64(100.0)},
		},
		{
			name:   "special floats",
			values: []any{math.Inf(-1), float64(-1.0), float64(0.0), float64(1.0), math.Inf(1)},
		},
		{
			name:   "strings",
			values: []any{"", "a", "aa", "ab", "b", "ba", "bb", "z"},
		},
		{
			name:   "strings with special characters",
			values: []any{"", " ", "!", "a", "z", "~"},
		},
		{
			name:   "strings with embedded nulls",
			values: []any{"a", "a\x00", "a\x00b", "b"},
		},
		{
			name:   "byte slices",
			values: []any{[]byte{}, []byte{0x00}, []byte{0x00, 0x01}, []byte{0x01}, []byte{0xFF}},
		},
		{
			name: "times",
			values: []any{
				time.Unix(0, 0),
				time.Unix(1000, 0),
				time.Unix(10000, 0),
				time.Unix(100000, 0),
			},
		},
		{
			name: "cross-type ordering (type tags)",
			values: []any{
				nil,
				false,
				true,
				int64(-100),
				int64(0),
				int64(100),
				uint64(0),
				uint64(100),
				float64(0.0),
				float64(100.0),
				"",
				"test",
				[]byte{},
				[]byte{0x01},
				time.Unix(0, 0),
				// Tuples sort by their content lexicographically
				// Empty tuple now correctly sorts before non-empty tuples
				[]any{},
				[]any{int64(1)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal all values
			marshaled := make([][]byte, len(tt.values))
			for i, v := range tt.values {
				data, err := orderedMaUn.Marshal(v)
				if err != nil {
					t.Fatalf("failed to marshal value %v: %v", v, err)
				}
				marshaled[i] = data
			}

			// Verify that marshaled bytes maintain order
			for i := 0; i < len(marshaled)-1; i++ {
				cmp := bytes.Compare(marshaled[i], marshaled[i+1])
				if cmp > 0 {
					t.Errorf("order violated: marshaled[%d] > marshaled[%d]\n  values: %v > %v\n  bytes: %x > %x",
						i, i+1, tt.values[i], tt.values[i+1], marshaled[i], marshaled[i+1])
				}
			}
		})
	}
}

// TestOrderedCodec_StringsCompareConsistency tests that string marshaling
// is consistent with strings.Compare.
func TestOrderedCodec_StringsCompareConsistency(t *testing.T) {
	testStrings := []string{
		"",
		"a",
		"aa",
		"aaa",
		"ab",
		"abc",
		"b",
		"ba",
		"z",
		"za",
		"zz",
		// With special characters
		" ",
		"!",
		"~",
		// With embedded nulls
		"a\x00",
		"a\x00a",
		"a\x00b",
		"b\x00",
	}

	// Test all pairs
	for i := range len(testStrings) {
		for j := range len(testStrings) {
			s1, s2 := testStrings[i], testStrings[j]

			// Get comparison of original strings
			strCmp := strings.Compare(s1, s2)

			// Marshal both strings
			data1, err1 := orderedMaUn.Marshal(s1)
			data2, err2 := orderedMaUn.Marshal(s2)

			if err1 != nil || err2 != nil {
				t.Fatalf("failed to marshal strings: %v, %v", err1, err2)
			}

			// Get comparison of marshaled bytes
			bytesCmp := bytes.Compare(data1, data2)

			// Verify consistency: sign(strCmp) == sign(bytesCmp)
			if !sameSign(strCmp, bytesCmp) {
				t.Errorf("inconsistent comparison for %q vs %q:\n  strings.Compare = %d\n  bytes.Compare = %d",
					s1, s2, strCmp, bytesCmp)
			}
		}
	}
}

// TestOrderedCodec_BytesCompareConsistency tests that byte slice marshaling
// is consistent with bytes.Compare.
func TestOrderedCodec_BytesCompareConsistency(t *testing.T) {
	testBytes := [][]byte{
		{},
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0x01, 0x00},
		{0x01, 0x01},
		{0x7F},
		{0x80},
		{0xFF},
		{0xFF, 0x00},
		{0xFF, 0xFF},
	}

	// Test all pairs
	for i := range len(testBytes) {
		for j := range len(testBytes) {
			b1, b2 := testBytes[i], testBytes[j]

			// Get comparison of original byte slices
			origCmp := bytes.Compare(b1, b2)

			// Marshal both byte slices
			data1, err1 := orderedMaUn.Marshal(b1)
			data2, err2 := orderedMaUn.Marshal(b2)

			if err1 != nil || err2 != nil {
				t.Fatalf("failed to marshal byte slices: %v, %v", err1, err2)
			}

			// Get comparison of marshaled bytes
			marshaledCmp := bytes.Compare(data1, data2)

			// Verify consistency: sign(origCmp) == sign(marshaledCmp)
			if !sameSign(origCmp, marshaledCmp) {
				t.Errorf("inconsistent comparison for %x vs %x:\n  original bytes.Compare = %d\n  marshaled bytes.Compare = %d",
					b1, b2, origCmp, marshaledCmp)
			}
		}
	}
}

// TestOrderedCodec_StringBytesWithNulls tests that string and byte encoding
// with embedded null bytes (0x00) preserves lexicographic order.
// The encoding escapes 0x00 as [0x00, 0xFF] and uses [0x00, 0x00] as terminator.
func TestOrderedCodec_StringBytesWithNulls(t *testing.T) {
	t.Run("strings with embedded nulls", func(t *testing.T) {
		// Critical edge cases for null handling
		testStrings := []string{
			"",
			"\x00",         // single null
			"\x00\x00",     // double null
			"\x00\x00\x01", // double null + byte
			"\x00\x01",     // null + byte
			"\x00\xFF",     // null + 0xFF (escape sequence!)
			"\x01",         // 0x01
			"a",            // regular char
			"a\x00",        // regular + null
			"a\x00\x00",    // regular + double null
			"a\x00\x00b",   // regular + double null + regular
			"a\x00b",       // regular + null + regular
			"b",            // next char
		}

		// Marshal all strings
		marshaled := make([][]byte, len(testStrings))
		for i, s := range testStrings {
			data, err := orderedMaUn.Marshal(s)
			if err != nil {
				t.Fatalf("failed to marshal string %q: %v", s, err)
			}
			marshaled[i] = data
		}

		// Verify order is preserved
		for i := 0; i < len(marshaled)-1; i++ {
			origCmp := strings.Compare(testStrings[i], testStrings[i+1])
			encCmp := bytes.Compare(marshaled[i], marshaled[i+1])

			if !sameSign(origCmp, encCmp) {
				t.Errorf("order violated for strings:\n  %q vs %q\n  original: %d, encoded: %d\n  bytes: %x vs %x",
					testStrings[i], testStrings[i+1], origCmp, encCmp,
					marshaled[i], marshaled[i+1])
			}
		}
	})

	t.Run("bytes with embedded nulls", func(t *testing.T) {
		// Critical edge cases including escape sequence
		testBytes := [][]byte{
			{},
			{0x00},             // single null
			{0x00, 0x00},       // double null (looks like terminator!)
			{0x00, 0x00, 0x01}, // double null + byte
			{0x00, 0x01},       // null + byte
			{0x00, 0xFF},       // null + 0xFF (escape sequence itself!)
			{0x00, 0xFF, 0x00}, // escape sequence + null
			{0x00, 0xFF, 0xFF}, // escape sequence + 0xFF
			{0x01},             // 0x01
			{0x01, 0x00},       // 0x01 + null
			{0xFF},             // 0xFF
			{0xFF, 0x00},       // 0xFF + null
		}

		// Marshal all byte slices
		marshaled := make([][]byte, len(testBytes))
		for i, b := range testBytes {
			data, err := orderedMaUn.Marshal(b)
			if err != nil {
				t.Fatalf("failed to marshal bytes %x: %v", b, err)
			}
			marshaled[i] = data
		}

		// Verify order is preserved
		for i := 0; i < len(marshaled)-1; i++ {
			origCmp := bytes.Compare(testBytes[i], testBytes[i+1])
			encCmp := bytes.Compare(marshaled[i], marshaled[i+1])

			if !sameSign(origCmp, encCmp) {
				t.Errorf("order violated for bytes:\n  %x vs %x\n  original: %d, encoded: %d\n  marshaled: %x vs %x",
					testBytes[i], testBytes[i+1], origCmp, encCmp,
					marshaled[i], marshaled[i+1])
			}
		}

		// Round-trip test to ensure decoding works correctly
		for i, b := range testBytes {
			var decoded []byte
			err := orderedMaUn.Unmarshal(marshaled[i], &decoded)
			if err != nil {
				t.Fatalf("failed to unmarshal bytes %x: %v", b, err)
			}
			if !bytes.Equal(b, decoded) {
				t.Errorf("round-trip failed for %x: got %x", b, decoded)
			}
		}
	})
}

// TestOrderedCodec_IntegerOrder tests that integer encoding maintains order
// across different integer types and signs.
func TestOrderedCodec_IntegerOrder(t *testing.T) {
	// Test int64
	int64Values := []int64{
		math.MinInt64,
		-1000000,
		-1000,
		-100,
		-10,
		-1,
		0,
		1,
		10,
		100,
		1000,
		1000000,
		math.MaxInt64,
	}

	t.Run("int64", func(t *testing.T) {
		marshaled := make([][]byte, len(int64Values))
		for i, v := range int64Values {
			data, err := orderedMaUn.Marshal(v)
			if err != nil {
				t.Fatalf("failed to marshal int64 %d: %v", v, err)
			}
			marshaled[i] = data
		}

		for i := 0; i < len(marshaled)-1; i++ {
			if bytes.Compare(marshaled[i], marshaled[i+1]) >= 0 {
				t.Errorf("int64 order violated: %d >= %d", int64Values[i], int64Values[i+1])
			}
		}
	})

	// Test uint64
	uint64Values := []uint64{
		0,
		1,
		10,
		100,
		1000,
		1000000,
		math.MaxUint64,
	}

	t.Run("uint64", func(t *testing.T) {
		marshaled := make([][]byte, len(uint64Values))
		for i, v := range uint64Values {
			data, err := orderedMaUn.Marshal(v)
			if err != nil {
				t.Fatalf("failed to marshal uint64 %d: %v", v, err)
			}
			marshaled[i] = data
		}

		for i := 0; i < len(marshaled)-1; i++ {
			if bytes.Compare(marshaled[i], marshaled[i+1]) >= 0 {
				t.Errorf("uint64 order violated: %d >= %d", uint64Values[i], uint64Values[i+1])
			}
		}
	})
}

// TestOrderedCodec_FloatOrder tests that float encoding maintains order,
// including special values like -Inf, NaN, and +Inf.
func TestOrderedCodec_FloatOrder(t *testing.T) {
	floatValues := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1000000.0,
		-1000.5,
		-100.5,
		-10.5,
		-1.5,
		-1.0,
		-0.5,
		-0.1,
		// Note: -0.0 and 0.0 are equal in float comparison but have different bit patterns
		// Only test one of them to avoid false positives
		0.0,
		0.1,
		0.5,
		1.0,
		1.5,
		10.5,
		100.5,
		1000.5,
		1000000.0,
		math.MaxFloat64,
		math.Inf(1),
	}

	marshaled := make([][]byte, len(floatValues))
	for i, v := range floatValues {
		data, err := orderedMaUn.Marshal(v)
		if err != nil {
			t.Fatalf("failed to marshal float64 %f: %v", v, err)
		}
		marshaled[i] = data
	}

	for i := 0; i < len(marshaled)-1; i++ {
		if bytes.Compare(marshaled[i], marshaled[i+1]) >= 0 {
			t.Errorf("float64 order violated: %f >= %f", floatValues[i], floatValues[i+1])
		}
	}
}

// TestOrderedCodec_TimeOrder tests that time encoding maintains chronological order.
func TestOrderedCodec_TimeOrder(t *testing.T) {
	timeValues := []time.Time{
		time.Unix(-1000000, 0),
		time.Unix(-1000, 0),
		time.Unix(-1, 0),
		time.Unix(0, 0),
		time.Unix(1, 0),
		time.Unix(1000, 0),
		time.Unix(1000000, 0),
		time.Now(),
		time.Now().Add(time.Hour),
	}

	marshaled := make([][]byte, len(timeValues))
	for i, v := range timeValues {
		data, err := orderedMaUn.Marshal(v)
		if err != nil {
			t.Fatalf("failed to marshal time %v: %v", v, err)
		}
		marshaled[i] = data
	}

	for i := 0; i < len(marshaled)-1; i++ {
		if bytes.Compare(marshaled[i], marshaled[i+1]) >= 0 {
			t.Errorf("time order violated: %v >= %v", timeValues[i], timeValues[i+1])
		}
	}
}

// TestOrderedCodec_TupleOrder tests that tuple encoding maintains order
// based on lexicographic comparison of elements.
func TestOrderedCodec_TupleOrder(t *testing.T) {
	// Tuple encoding now uses correct lexicographic ordering:
	// [tagTuple][elements...][tagTupleEnd]
	// - tagTuple = 0x0A, tagTupleEnd = 0x00
	// - Empty tuple: [0x0A][0x00]
	// - Tuple [0]:   [0x0A][0x04][int64 bytes][0x00]
	// Since 0x00 (tagTupleEnd) < element tags (0x01+), empty tuples sort before non-empty
	// Shorter tuples sort before longer ones with the same prefix: [] < [0] < [0,0] < [0,1]
	// This matches standard lexicographic ordering (like "" < "a" < "aa" in strings)
	tupleValues := [][]any{
		{},                   // 0x0A 0x00 (empty, sorts first)
		{int64(0)},           // 0x0A 0x04 ... 0x00
		{int64(0), int64(0)}, // 0x0A 0x04 ... 0x04 ... 0x00
		{int64(0), int64(1)}, // 0x0A 0x04 ... 0x04 ... 0x00
		{int64(1)},           // 0x0A 0x04 ... 0x00
		{int64(1), int64(0)}, // 0x0A 0x04 ... 0x04 ... 0x00
		{int64(1), int64(1)}, // 0x0A 0x04 ... 0x04 ... 0x00
		{"a"},                // 0x0A 0x07 ... 0x00
		{"a", "a"},           // 0x0A 0x07 ... 0x07 ... 0x00
		{"a", "b"},           // 0x0A 0x07 ... 0x07 ... 0x00
		{"b"},                // 0x0A 0x07 ... 0x00
		{"b", "a"},           // 0x0A 0x07 ... 0x07 ... 0x00
	}

	marshaled := make([][]byte, len(tupleValues))
	for i, v := range tupleValues {
		data, err := orderedMaUn.Marshal(v)
		if err != nil {
			t.Fatalf("failed to marshal tuple %v: %v", v, err)
		}
		marshaled[i] = data
	}

	for i := 0; i < len(marshaled)-1; i++ {
		cmp := bytes.Compare(marshaled[i], marshaled[i+1])
		if cmp > 0 {
			t.Errorf("tuple order violated: %v > %v\n  bytes: %x > %x",
				tupleValues[i], tupleValues[i+1], marshaled[i], marshaled[i+1])
		}
	}
}

// TestOrderedCodec_EmptyTupleOrdering verifies that empty tuples now correctly
// sort before non-empty tuples, following standard lexicographic ordering.
func TestOrderedCodec_EmptyTupleOrdering(t *testing.T) {
	emptyTuple := []any{}
	nonEmptyTuple := []any{int64(0)}

	emptyData, err := orderedMaUn.Marshal(emptyTuple)
	if err != nil {
		t.Fatalf("failed to marshal empty tuple: %v", err)
	}

	nonEmptyData, err := orderedMaUn.Marshal(nonEmptyTuple)
	if err != nil {
		t.Fatalf("failed to marshal non-empty tuple: %v", err)
	}

	cmp := bytes.Compare(emptyData, nonEmptyData)

	t.Logf("Empty tuple:     %x", emptyData)
	t.Logf("Tuple [0]:       %x", nonEmptyData)
	t.Logf("bytes.Compare:   %d", cmp)

	// Verify correct behavior: [] < [0] (like "" < "a" in strings)
	if cmp >= 0 {
		t.Errorf("Empty tuple should sort before non-empty tuple")
		t.Errorf("Expected: [] < [0] (like \"\" < \"a\" in strings)")
		t.Errorf("Actual:   [] %s [0]", map[int]string{-1: "<", 0: "==", 1: ">"}[cmp])
	} else {
		t.Logf("CORRECT: Empty tuple sorts before non-empty tuple")
		t.Logf("This matches standard lexicographic ordering")
	}
}

// TestOrderedCodec_RoundTrip tests that values can be marshaled and
// unmarshaled correctly.
func TestOrderedCodec_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"false", false},
		{"true", true},
		{"int64 zero", int64(0)},
		{"int64 positive", int64(12345)},
		{"int64 negative", int64(-12345)},
		{"uint64 zero", uint64(0)},
		{"uint64 positive", uint64(12345)},
		{"float64 zero", float64(0.0)},
		{"float64 positive", float64(123.45)},
		{"float64 negative", float64(-123.45)},
		{"string empty", ""},
		{"string", "hello world"},
		{"string with null", "hello\x00world"},
		{"bytes empty", []byte{}},
		{"bytes", []byte{1, 2, 3, 4, 5}},
		{"bytes with null", []byte{1, 0, 2, 0, 3}},
		{"time", time.Unix(1234567890, 0)},
		{"tuple empty", []any{}},
		{"tuple single", []any{int64(42)}},
		{"tuple multi", []any{int64(42), "test", []byte{1, 2, 3}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := orderedMaUn.Marshal(tt.value)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			// Unmarshal based on type
			var result any
			switch tt.value.(type) {
			case []any:
				var decoded []any
				err = orderedMaUn.Unmarshal(data, &decoded)
				result = decoded
			default:
				err = orderedMaUn.Unmarshal(data, &result)
			}

			if err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			// Compare results
			if !deepEqual(tt.value, result) {
				t.Errorf("round trip failed:\n  original: %v (%T)\n  result:   %v (%T)",
					tt.value, tt.value, result, result)
			}
		})
	}
}

// TestOrderedCodec_TypedUnmarshal tests unmarshaling into typed pointers.
func TestOrderedCodec_TypedUnmarshal(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		data, _ := orderedMaUn.Marshal("hello")
		var result string
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("expected 'hello', got %q", result)
		}
	})

	t.Run("int64", func(t *testing.T) {
		data, _ := orderedMaUn.Marshal(int64(42))
		var result int64
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if result != 42 {
			t.Errorf("expected 42, got %d", result)
		}
	})

	t.Run("float64", func(t *testing.T) {
		data, _ := orderedMaUn.Marshal(float64(3.14))
		var result float64
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if result != 3.14 {
			t.Errorf("expected 3.14, got %f", result)
		}
	})

	t.Run("bool", func(t *testing.T) {
		data, _ := orderedMaUn.Marshal(true)
		var result bool
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("[]byte", func(t *testing.T) {
		data, _ := orderedMaUn.Marshal([]byte{1, 2, 3})
		var result []byte
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if !bytes.Equal(result, []byte{1, 2, 3}) {
			t.Errorf("expected [1 2 3], got %v", result)
		}
	})

	t.Run("time.Time", func(t *testing.T) {
		original := time.Unix(1234567890, 0).UTC()
		data, _ := orderedMaUn.Marshal(original)
		var result time.Time
		err := orderedMaUn.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if !result.Equal(original) {
			t.Errorf("expected %v, got %v", original, result)
		}
	})
}

// Helper functions

// sameSign returns true if both integers have the same sign or are both zero.
func sameSign(a, b int) bool {
	if a == 0 && b == 0 {
		return true
	}
	if a < 0 && b < 0 {
		return true
	}
	if a > 0 && b > 0 {
		return true
	}
	return false
}

// deepEqual compares two values for equality, handling special cases.
func deepEqual(a, b any) bool {
	// Handle nil
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle byte slices
	if ba, ok := a.([]byte); ok {
		if bb, ok := b.([]byte); ok {
			return bytes.Equal(ba, bb)
		}
		return false
	}

	// Handle time.Time
	if ta, ok := a.(time.Time); ok {
		if tb, ok := b.(time.Time); ok {
			return ta.Equal(tb)
		}
		return false
	}

	// Handle slices
	if sa, ok := a.([]any); ok {
		if sb, ok := b.([]any); ok {
			if len(sa) != len(sb) {
				return false
			}
			for i := range sa {
				if !deepEqual(sa[i], sb[i]) {
					return false
				}
			}
			return true
		}
		return false
	}

	// Default comparison
	return a == b
}
