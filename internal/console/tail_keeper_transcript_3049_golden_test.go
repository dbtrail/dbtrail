package console

// keeperTranscript3049 is what TestTailKeeper_anotherSourceIsDrivenAsBefore_3049
// printed on the commit before #3049's step was added to the keeper
// (95955154).
const keeperTranscript3049 = `the first statement starts both tails
  source Newest -> event 6 at binlog.000003:600, <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  table items: Cut -> binlog.000003:1 after event 0, "" over 1+0
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+0
  table items: Start -> "", <nil> over 1+0
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+0
  table items: Fill up to 6 -> "", <nil> over 1+0
  table orders: Cut -> binlog.000003:1 after event 0, "" over 1+0
  table orders: BaseFile -> "file /snap/1/shop/orders.parquet" over 1+0
  table orders: Start -> "", <nil> over 1+0
  table orders: BaseFile -> "file /snap/1/shop/orders.parquet" over 1+0
  table orders: Fill up to 6 -> "", <nil> over 1+0
  tail items#1: Apply events 4(a) 5(b) -> <nil>
  tail orders#1: Apply events 1(1) 2(2) -> <nil>
  tail orders#1: Apply events 3(3) -> <nil>
  statement: no tail
the next one reads them
  source Newest -> event 6 at binlog.000003:600, <nil>
  source Holds event 6 -> true, <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail items#1: WriteFile 2-shop.items.parquet -> 2 rows, <nil>, keys a,b
  tail orders#1: WriteFile 1-shop.orders.parquet -> 3 rows, <nil>, keys 1,2,3
  statement: shop.items reads [2-shop.items.parquet] by [id] through 12:06 (event 6)
  statement: shop.orders reads [1-shop.orders.parquet] by [id] through 12:06 (event 6)
changes are folded and written as a delta
  source Newest -> event 9 at binlog.000003:900, <nil>
  source Holds event 6 -> true, <nil>
  source Since (6, 9] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail items#1: Apply events 9(a) -> <nil>
  tail items#1: WriteDelta 4-shop.items.parquet -> 1 rows, <nil>, keys a
  tail orders#1: Apply events 7(4) 8(1) -> <nil>
  tail orders#1: WriteDelta 3-shop.orders.parquet -> 2 rows, <nil>, keys 1,4
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:09 (event 9)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet] by [id] through 12:09 (event 9)
nothing new
  source Newest -> event 9 at binlog.000003:900, <nil>
  source Holds event 9 -> true, <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:09 (event 9)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet] by [id] through 12:09 (event 9)
one change, write 1 of a chain
  source Newest -> event 10 at binlog.000003:1000, <nil>
  source Holds event 9 -> true, <nil>
  source Since (9, 10] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 10(2) -> <nil>
  tail orders#1: WriteDelta 5-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:10 (event 10)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet 5-shop.orders.parquet] by [id] through 12:10 (event 10)
one change, write 2 of a chain
  source Newest -> event 11 at binlog.000003:1100, <nil>
  source Holds event 10 -> true, <nil>
  source Since (10, 11] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 11(2) -> <nil>
  tail orders#1: WriteDelta 6-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:11 (event 11)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet 5-shop.orders.parquet 6-shop.orders.parquet] by [id] through 12:11 (event 11)
one change, write 3 of a chain
  source Newest -> event 12 at binlog.000003:1200, <nil>
  source Holds event 11 -> true, <nil>
  source Since (11, 12] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 12(2) -> <nil>
  tail orders#1: WriteDelta 7-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:12 (event 12)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet 5-shop.orders.parquet 6-shop.orders.parquet 7-shop.orders.parquet] by [id] through 12:12 (event 12)
one change, write 4 of a chain
  source Newest -> event 13 at binlog.000003:1300, <nil>
  source Holds event 12 -> true, <nil>
  source Since (12, 13] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 13(2) -> <nil>
  tail orders#1: WriteDelta 8-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:13 (event 13)
  statement: shop.orders reads [1-shop.orders.parquet 3-shop.orders.parquet 5-shop.orders.parquet 6-shop.orders.parquet 7-shop.orders.parquet 8-shop.orders.parquet] by [id] through 12:13 (event 13)
one change, write 5 of a chain
  source Newest -> event 14 at binlog.000003:1400, <nil>
  source Holds event 13 -> true, <nil>
  source Since (13, 14] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 14(2) -> <nil>
  tail orders#1: WriteFile 9-shop.orders.parquet -> 4 rows, <nil>, keys 1,2,3,4
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:14 (event 14)
  statement: shop.orders reads [9-shop.orders.parquet] by [id] through 12:14 (event 14)
one change, write 6 of a chain
  source Newest -> event 15 at binlog.000003:1500, <nil>
  source Holds event 14 -> true, <nil>
  source Since (14, 15] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 15(2) -> <nil>
  tail orders#1: WriteDelta 10-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:15 (event 15)
  statement: shop.orders reads [9-shop.orders.parquet 10-shop.orders.parquet] by [id] through 12:15 (event 15)
one change, write 7 of a chain
  source Newest -> event 16 at binlog.000003:1600, <nil>
  source Holds event 15 -> true, <nil>
  source Since (15, 16] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 16(2) -> <nil>
  tail orders#1: WriteDelta 11-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:16 (event 16)
  statement: shop.orders reads [9-shop.orders.parquet 10-shop.orders.parquet 11-shop.orders.parquet] by [id] through 12:16 (event 16)
one change, write 8 of a chain
  source Newest -> event 17 at binlog.000003:1700, <nil>
  source Holds event 16 -> true, <nil>
  source Since (16, 17] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 17(2) -> <nil>
  tail orders#1: WriteDelta 12-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:17 (event 17)
  statement: shop.orders reads [9-shop.orders.parquet 10-shop.orders.parquet 11-shop.orders.parquet 12-shop.orders.parquet] by [id] through 12:17 (event 17)
one change, write 9 of a chain
  source Newest -> event 18 at binlog.000003:1800, <nil>
  source Holds event 17 -> true, <nil>
  source Since (17, 18] of items@1+0,orders@1+0 -> <nil>
  source Broken of items@1+0,orders@1+0 -> 0 broken, <nil>
  tail orders#1: Apply events 18(2) -> <nil>
  tail orders#1: WriteDelta 13-shop.orders.parquet -> 1 rows, <nil>, keys 2
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:18 (event 18)
  statement: shop.orders reads [9-shop.orders.parquet 10-shop.orders.parquet 11-shop.orders.parquet 12-shop.orders.parquet 13-shop.orders.parquet] by [id] through 12:18 (event 18)
a newer cut of one table is pruned to
  source Newest -> event 18 at binlog.000003:1800, <nil>
  source Holds event 18 -> true, <nil>
  source Broken of items@1+0,orders@1+1 -> 0 broken, <nil>
  table orders: BaseFile -> "file /snap/1/shop/orders.parquet" over 1+1
  table orders: Cut -> binlog.000003:501 after event 5, "" over 1+1
  tail orders#1: PruneThrough binlog.000003:501 after event 5 -> 1 dropped, <nil>
  tail orders#1: WriteFile 14-shop.orders.parquet -> 3 rows, <nil>, keys 1,2,4
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:18 (event 18)
  statement: shop.orders reads [14-shop.orders.parquet] by [id] through 12:18 (event 18)
and its tail goes on over it
  source Newest -> event 19 at binlog.000003:1900, <nil>
  source Holds event 18 -> true, <nil>
  source Since (18, 19] of items@1+0,orders@1+1 -> <nil>
  source Broken of items@1+0,orders@1+1 -> 0 broken, <nil>
  tail orders#1: Apply events 19(5) -> <nil>
  tail orders#1: WriteDelta 15-shop.orders.parquet -> 1 rows, <nil>, keys 5
  statement: shop.items reads [2-shop.items.parquet 4-shop.items.parquet] by [id] through 12:19 (event 19)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:19 (event 19)
a cut that is no later point of the same copy starts a build
  source Newest -> event 19 at binlog.000003:1900, <nil>
  source Holds event 19 -> true, <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Cut -> binlog.000003:1 after event 1, "" over 1+1
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Cut -> binlog.000003:1 after event 1, "" over 1+1
  table items: Cut -> binlog.000003:1 after event 1, "" over 1+1
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Start -> "", <nil> over 1+1
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Fill up to 19 -> "", <nil> over 1+1
  tail items#2: Apply events 4(a) 5(b) -> <nil>
  tail items#2: Apply events 9(a) -> <nil>
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:19 (event 19)
which is read once it is in
  source Newest -> event 19 at binlog.000003:1900, <nil>
  source Holds event 19 -> true, <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  tail items#2: WriteFile 16-shop.items.parquet -> 2 rows, <nil>, keys a,b
  statement: shop.items reads [16-shop.items.parquet] by [id] through 12:19 (event 19)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:19 (event 19)
a write that fails
  source Newest -> event 20 at binlog.000003:2000, <nil>
  source Holds event 19 -> true, <nil>
  source Since (19, 20] of items@1+1,orders@1+1 -> <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  tail items#2: Apply events 20(c) -> <nil>
  tail items#2: WriteDelta 17-shop.items.parquet -> 0 rows, the disk is full, not written
  statement: shop.items reads [16-shop.items.parquet] by [id] through 12:19 (event 19)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:20 (event 20)
and one that works again
  source Newest -> event 20 at binlog.000003:2000, <nil>
  source Holds event 20 -> true, <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  tail items#2: WriteDelta 18-shop.items.parquet -> 1 rows, <nil>, keys c
  statement: shop.items reads [16-shop.items.parquet 18-shop.items.parquet] by [id] through 12:20 (event 20)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:20 (event 20)
a tail over its budget of keys stops
  source Newest -> event 25 at binlog.000003:2500, <nil>
  source Holds event 20 -> true, <nil>
  source Since (20, 25] of items@1+1,orders@1+1 -> <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  tail orders#1: Apply events 21(6) 22(7) -> <nil>
  tail orders#1: Apply events 23(8) 24(9) -> <nil>
  statement: shop.items reads [16-shop.items.parquet 18-shop.items.parquet] by [id] through 12:25 (event 25)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:20 (event 20)
and stays as it is while the other goes on
  source Newest -> event 27 at binlog.000003:2700, <nil>
  source Holds event 25 -> true, <nil>
  source Since (25, 27] of items@1+1 -> <nil>
  source Broken of items@1+1,orders@1+1 -> 0 broken, <nil>
  tail items#2: Apply events 27(d) -> <nil>
  tail items#2: WriteDelta 19-shop.items.parquet -> 1 rows, <nil>, keys d
  statement: shop.items reads [16-shop.items.parquet 18-shop.items.parquet 19-shop.items.parquet] by [id] through 12:27 (event 27)
  statement: shop.orders reads [14-shop.orders.parquet 15-shop.orders.parquet] by [id] through 12:20 (event 20)
a table that can have no tail
  source Newest -> event 27 at binlog.000003:2700, <nil>
  source Holds event 27 -> true, <nil>
  source Broken of items@1+1,users@1+0 -> 0 broken, <nil>
  table users: Cut -> binlog.000003:1 after event 0, "" over 1+0
  table users: BaseFile -> "file /snap/1/shop/users.parquet" over 1+0
  table users: Start -> "its key is not made of integers", <nil> over 1+0
  table users: BaseFile -> "file /snap/1/shop/users.parquet" over 1+0
  statement: shop.items reads [16-shop.items.parquet 18-shop.items.parquet 19-shop.items.parquet] by [id] through 12:27 (event 27)
is not started again
  source Newest -> event 27 at binlog.000003:2700, <nil>
  source Holds event 27 -> true, <nil>
  source Broken of items@1+1,users@1+0 -> 0 broken, <nil>
  statement: shop.items reads [16-shop.items.parquet 18-shop.items.parquet 19-shop.items.parquet] by [id] through 12:27 (event 27)
a table whose definition changed loses its tail
  source Newest -> event 27 at binlog.000003:2700, <nil>
  source Holds event 27 -> true, <nil>
  source Broken of items@1+1 -> 1 broken, <nil>
  statement: no tail
a source whose log started again
  source Newest -> event 28 at binlog.000001:4, <nil>
  source Broken of items@1+1 -> 0 broken, <nil>
  table items: Cut -> binlog.000003:1 after event 1, "" over 1+1
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Start -> "", <nil> over 1+1
  table items: BaseFile -> "file /snap/1/shop/items.parquet" over 1+1
  table items: Fill up to 28 -> "", <nil> over 1+1
  tail items#3: Apply events 4(a) 5(b) -> <nil>
  tail items#3: Apply events 9(a) 20(c) -> <nil>
  tail items#3: Apply events 27(d) 28(z) -> <nil>
  statement: no tail
is read again from its cut
  source Newest -> event 28 at binlog.000001:4, <nil>
  source Holds event 28 -> true, <nil>
  source Broken of items@1+1 -> 0 broken, <nil>
  tail items#3: WriteFile 20-shop.items.parquet -> 5 rows, <nil>, keys a,b,c,d,z
  statement: shop.items reads [20-shop.items.parquet] by [id] through 12:28 (event 28)
`
