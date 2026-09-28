A real dump, kept as a test fixture. Do not edit the files by hand.

Source: MySQL 8.0.46. Tool: mydumper v1.0.3-1 with
`--complete-insert --trx-tables=0 --regex '^(edge|onlyviews)\.'`.

Schema `edge`:

| Object | Kind | Rows | Files are named |
|---|---|---|---|
| `orders-view` | table | 2 | `edge.orders-view` |
| `items` | table | 1 | `edge.items` |
| `items-schema-view` | table | 1 | `edge.items-schema-view` |
| `log-schema` | table | 1 | `edge.log-schema` |
| `Mixed.Case` | table | 1 | `edge.mydumper_0` |
| `empty_real` | table | 0 | `edge.empty_real` |
| `mem_real` | table, ENGINE=MEMORY | 0 | `edge.mem_real` |
| `sales` | table | 1 | `edge.sales` |
| `sale` | view | | `edge.sale` |
| `sales_recent` | view | | `edge.sales_recent` |
| `Totals.By-schema` | view | | `edge.mydumper_1` |

Schema `onlyviews` holds one view, `v1`, and no table.
