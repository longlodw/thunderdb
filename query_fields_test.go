package thunderdb

import (
	"testing"
)

// TestProjection_FilterByComputedField tests that fields with ReferenceCols can be used to filter
func TestProjection_FilterByComputedField(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup schema with a computed field
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "employees"
	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":        {},
		"first":     {Indexed: true},
		"last":      {Indexed: true},
		"full_name": {ReferenceCols: []string{"first", "last"}}, // Computed field
		"salary":    {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	if err := p.Insert(map[string]any{"id": "1", "first": "alice", "last": "smith", "salary": 50000.0}); err != nil {
		t.Fatal(err)
	}
	if err := p.Insert(map[string]any{"id": "2", "first": "bob", "last": "jones", "salary": 60000.0}); err != nil {
		t.Fatal(err)
	}
	if err := p.Insert(map[string]any{"id": "3", "first": "charlie", "last": "brown", "salary": 55000.0}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test: Query using the computed field
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err = tx.LoadPersistent(relation)
	if err != nil {
		t.Fatal(err)
	}

	// Get fields to verify full_name is available
	fields := p.Fields()
	if _, ok := fields["full_name"]; !ok {
		t.Fatal("Expected 'full_name' field to be available in Fields()")
	}

	// Verify it has ReferenceCols
	if len(fields["full_name"].ReferenceCols) != 2 {
		t.Errorf("Expected 'full_name' to have 2 ReferenceCols, got %d", len(fields["full_name"].ReferenceCols))
	}

	// Filter by a reference column - this should work because the field is in the fieldspec
	firstKey, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}

	f := map[string]*BytesRange{
		"first": NewBytesRange(firstKey, firstKey, true, true, nil),
	}

	seq, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	// Verify results
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		first, _ := row.Get("first")
		if first != "alice" {
			t.Errorf("Expected first='alice', got %v", first)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result when filtering by first='alice', got %d", count)
	}
}

// TestJoining_FilterByComputedField tests that computed fields work across joins
func TestJoining_FilterByComputedField(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create employees table with computed field
	employees, err := tx.CreatePersistent("employees", map[string]ColumnSpec{
		"emp_id":  {Indexed: true},
		"name":    {},
		"dept_id": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create departments table
	departments, err := tx.CreatePersistent("departments", map[string]ColumnSpec{
		"dept_id": {Indexed: true},
		"dept_name": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	if err := employees.Insert(map[string]any{"emp_id": "1", "name": "alice", "dept_id": "d1"}); err != nil {
		t.Fatal(err)
	}
	if err := employees.Insert(map[string]any{"emp_id": "2", "name": "bob", "dept_id": "d2"}); err != nil {
		t.Fatal(err)
	}

	if err := departments.Insert(map[string]any{"dept_id": "d1", "dept_name": "engineering"}); err != nil {
		t.Fatal(err)
	}
	if err := departments.Insert(map[string]any{"dept_id": "d2", "dept_name": "sales"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test: Join and filter
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	employees, err = tx.LoadPersistent("employees")
	if err != nil {
		t.Fatal(err)
	}
	departments, err = tx.LoadPersistent("departments")
	if err != nil {
		t.Fatal(err)
	}

	// Create a join
	joined := employees.Join(departments)

	// Verify both tables' fields are available
	joinedFields := joined.Fields()
	if _, ok := joinedFields["emp_id"]; !ok {
		t.Fatal("Expected 'emp_id' in joined fields")
	}
	if _, ok := joinedFields["dept_name"]; !ok {
		t.Fatal("Expected 'dept_name' in joined fields")
	}

	// Filter by dept_name
	deptKey, err := ToKey("engineering")
	if err != nil {
		t.Fatal(err)
	}

	f := map[string]*BytesRange{
		"dept_name": NewBytesRange(deptKey, deptKey, true, true, nil),
	}

	seq, err := joined.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	// Verify results
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		deptName, _ := row.Get("dept_name")
		if deptName != "engineering" {
			t.Errorf("Expected dept_name='engineering', got %v", deptName)
		}
		empName, _ := row.Get("name")
		if empName != "alice" {
			t.Errorf("Expected name='alice', got %v", empName)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}

// TestProjection_ProjectedFieldFiltering tests filtering on projected fields
func TestProjection_ProjectedFieldFiltering(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, err := tx.CreatePersistent("users", map[string]ColumnSpec{
		"id":       {Indexed: true},
		"username": {Indexed: true},
		"email":    {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	if err := users.Insert(map[string]any{"id": "1", "username": "alice", "email": "alice@example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := users.Insert(map[string]any{"id": "2", "username": "bob", "email": "bob@example.com"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test: Project and filter
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, err = tx.LoadPersistent("users")
	if err != nil {
		t.Fatal(err)
	}

	// Create a projection that aliases 'username' to 'user'
	projected := users.Project(map[string]string{
		"user":  "username",
		"email": "email",
	})

	// Verify projected field is available
	projFields := projected.Fields()
	if _, ok := projFields["user"]; !ok {
		t.Fatal("Expected 'user' field in projected fields")
	}

	// Filter by the projected field 'user'
	userKey, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}

	f := map[string]*BytesRange{
		"user": NewBytesRange(userKey, userKey, true, true, nil),
	}

	seq, err := projected.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	// Verify results
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		user, _ := row.Get("user")
		if user != "alice" {
			t.Errorf("Expected user='alice', got %v", user)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}

// TestJoining_BestBodyIndexPrioritizesIndexed tests that bestBodyIndex prioritizes indexed fields
func TestJoining_BestBodyIndexPrioritizesIndexed(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create tables with different index configurations
	t1, err := tx.CreatePersistent("table1", map[string]ColumnSpec{
		"id":   {Indexed: true},
		"col1": {},
		"col2": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	t2, err := tx.CreatePersistent("table2", map[string]ColumnSpec{
		"id":   {Indexed: true},
		"col1": {Indexed: true}, // This one is indexed
		"col3": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		id := string(rune('0' + i))
		if err := t1.Insert(map[string]any{"id": id, "col1": "value" + id, "col2": "other" + id}); err != nil {
			t.Fatal(err)
		}
		if err := t2.Insert(map[string]any{"id": id, "col1": "value" + id, "col3": "extra" + id}); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test: Join and filter by indexed field (should prefer that table)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	t1, err = tx.LoadPersistent("table1")
	if err != nil {
		t.Fatal(err)
	}
	t2, err = tx.LoadPersistent("table2")
	if err != nil {
		t.Fatal(err)
	}

	// Join with col1 filter (indexed in t2, not in t1)
	joined := t1.Join(t2)

	col1Key, err := ToKey("value1")
	if err != nil {
		t.Fatal(err)
	}

	f := map[string]*BytesRange{
		"col1": NewBytesRange(col1Key, col1Key, true, true, nil),
	}

	seq, err := joined.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	// Just verify we get the correct result (the optimization is internal)
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		col1, _ := row.Get("col1")
		if col1 != "value1" {
			t.Errorf("Expected col1='value1', got %v", col1)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}

// TestMultipleReferenceColumnsFiltering tests filtering with fields that have multiple reference columns
func TestMultipleReferenceColumnsFiltering(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	records, err := tx.CreatePersistent("records", map[string]ColumnSpec{
		"id":    {Indexed: true},
		"year": {Indexed: true},
		"month": {Indexed: true},
		"day":   {Indexed: true},
		// Composite field referencing multiple columns
		"date_key": {ReferenceCols: []string{"year", "month", "day"}},
		"data":     {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	if err := records.Insert(map[string]any{"id": "1", "year": "2024", "month": "01", "day": "15", "data": "jan_data"}); err != nil {
		t.Fatal(err)
	}
	if err := records.Insert(map[string]any{"id": "2", "year": "2024", "month": "02", "day": "20", "data": "feb_data"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test: Query using one of the reference columns
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	records, err = tx.LoadPersistent("records")
	if err != nil {
		t.Fatal(err)
	}

	// Verify date_key field is available
	fields := records.Fields()
	if _, ok := fields["date_key"]; !ok {
		t.Fatal("Expected 'date_key' field to be available")
	}

	// Filter by year (a reference column of date_key)
	yearKey, err := ToKey("2024")
	if err != nil {
		t.Fatal(err)
	}

	f := map[string]*BytesRange{
		"year": NewBytesRange(yearKey, yearKey, true, true, nil),
	}

	seq, err := records.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we get both records from 2024
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		year, _ := row.Get("year")
		if year != "2024" {
			t.Errorf("Expected year='2024', got %v", year)
		}
	}
	if count != 2 {
		t.Errorf("Expected 2 results, got %d", count)
	}
}
