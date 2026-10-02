//go:build integration

package metadata

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// FindFKCascades names each cascading constraint child → parent with its rules,
// in a fixed order, and covers a self-referencing table and an ON UPDATE-only
// rule. A RESTRICT foreign key and a cascade in a schema outside the scope are
// not reported.
func TestFindFKCascades_namesChildAndParent(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	otherDB, _ := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE customers (id INT PRIMARY KEY)`)
	testutil.MustExec(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL,
		CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE)`)
	testutil.MustExec(t, db, `CREATE TABLE categories (id INT PRIMARY KEY)`)
	testutil.MustExec(t, db, `CREATE TABLE products (id INT PRIMARY KEY, category_id INT NOT NULL,
		CONSTRAINT fk_products_cat FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE)`)
	testutil.MustExec(t, db, `CREATE TABLE employees (id INT PRIMARY KEY, manager_id INT NULL,
		CONSTRAINT fk_emp_manager FOREIGN KEY (manager_id) REFERENCES employees(id) ON DELETE CASCADE)`)
	testutil.MustExec(t, db, `CREATE TABLE notes (id INT PRIMARY KEY, customer_id INT NOT NULL,
		CONSTRAINT fk_notes_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE RESTRICT)`)
	testutil.MustExec(t, otherDB, `CREATE TABLE p (id INT PRIMARY KEY)`)
	testutil.MustExec(t, otherDB, `CREATE TABLE c (id INT PRIMARY KEY, p_id INT NOT NULL,
		CONSTRAINT fk_c FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)`)

	found, err := FindFKCascades(db, []string{dbName})
	if err != nil {
		t.Fatal(err)
	}
	got := DescribeFKCascades(found, FKCascadeListLimit)
	want := dbName + ".employees → " + dbName + ".employees, ON DELETE CASCADE; " +
		dbName + ".orders → " + dbName + ".customers, ON DELETE CASCADE; " +
		dbName + ".products → " + dbName + ".categories, ON UPDATE CASCADE"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	verr := ValidateNoFKCascades(db, []string{dbName})
	var ferr *FKCascadesError
	if !errors.As(verr, &ferr) || len(ferr.Found) != 3 {
		t.Fatalf("ValidateNoFKCascades = %v, want an FKCascadesError with 3 constraints", verr)
	}
}
