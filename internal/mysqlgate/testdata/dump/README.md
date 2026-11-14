This dump is written by hand in the shape mydumper 1.0.3 writes (compare
`internal/baseline/testdata`). It is the fixed input of the MySQL regression
gate; see the package's `doc.go`.

`shop.odd.00000.sql` holds one byte that is not UTF-8 on purpose (0xFF in a
`varbinary`), so an editor that rewrites the file as UTF-8 changes the fixture.
