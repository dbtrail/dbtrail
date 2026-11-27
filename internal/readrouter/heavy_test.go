package readrouter

import "testing"

// portOpts are the CopyOptions the routing port reads every statement under
// (shim's Handler.copyOpts), LIKE switch off.
var portOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true, MySQLDateAdd: true, MySQLTimestampDiff: true, MySQLAvg: true, MySQLLength: true}

// An expensive statement is let past the vetoes that are only how the copy
// computes a value, and the copy can be sent its text.
func TestHardVetoOpts_relaxed(t *testing.T) {
	for _, stmt := range []string{
		"SELECT a FROM t WHERE b LIKE 'x%'",
		"SELECT a FROM t WHERE b like 'x%'",
		"SELECT a FROM t WHERE b NOT LIKE '%x'",
		"SELECT a FROM t WHERE b LIKE c",
		"SELECT WEEK(d) FROM t",
		"SELECT YEARWEEK(d) FROM t",
		"SELECT EXTRACT(WEEK FROM d) FROM t",
		"SELECT INSTR(a, 'x') FROM t",
		"SELECT a FROM t UNION SELECT a FROM u",
		"SELECT a FROM t INTERSECT SELECT a FROM u",
		"SELECT CONCAT(a, b) FROM t",
		"SELECT GREATEST(a, b) FROM t",
		"SELECT SUBSTRING(a, 0, 2) FROM t",
		"SELECT SUBSTRING(a FROM n FOR 2) FROM t",
		"SELECT LEFT(a, n) FROM t",
		"SELECT SUM(a) / 2.0 FROM t",
		"SELECT MEDIAN(a) FROM t",
		"SELECT 9223372036854775808 FROM t",
		"SELECT YEAR(d) + 1 FROM t",
		// Several relaxed vetoes at once.
		"SELECT CONCAT(a, b) FROM t WHERE c LIKE 'x%' GROUP BY 1 HAVING SUM(n) / 2 > 1",
	} {
		if v := VetoOpts(stmt, portOpts); v == "" {
			t.Errorf("%s: no veto at all; the case tests nothing", stmt)
			continue
		}
		if hard := HardVetoOpts(stmt, portOpts); hard != "" {
			t.Errorf("%s: held to %q, want it let past for an expensive plan", stmt, hard)
		}
		if _, why := ForCopyOpts(stmt, portOpts); why != "" {
			t.Errorf("%s: the copy cannot be sent its text (%s): it would be refused, not answered", stmt, why)
		}
	}
}

// What the copy would read as another statement, or cannot answer as MySQL
// means it, or needs the live server for, keeps the statement on MySQL
// whatever its plan.
func TestHardVetoOpts_held(t *testing.T) {
	for _, stmt := range []string{
		"SELECT /*+ NO_INDEX(t) */ a FROM t",
		"SELECT /*!80000 a */ FROM t",
		`SELECT "x" FROM t`,
		`SELECT 'a\\b' FROM t`,
		"SELECT a || b FROM t",
		"SELECT a ^ b FROM t",
		"SELECT 5--3 FROM t",
		"SELECT #2\n a FROM t",
		"SELECT @x FROM t",
		"SELECT @@version FROM t",
		"SELECT a FROM t FOR UPDATE",
		"SELECT a FROM t LOCK IN SHARE MODE",
		"SELECT a INTO OUTFILE '/tmp/x' FROM t",
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM mysql.user",
		"SELECT FOUND_ROWS()",
		"SELECT USER()",
		"SELECT DATABASE()",
		"SELECT CURRENT_USER",
		"SELECT RAND() FROM t",
		"SELECT UUID() FROM t",
		"SELECT JSON_EXTRACT(j, '$.a') FROM t",
		"SELECT j->'$.a' FROM t",
		"WITH RECURSIVE r AS (SELECT 1 n UNION ALL SELECT n + 1 FROM r) SELECT n FROM r",
		"SELECT a FROM t WHERE MATCH(b) AGAINST ('x')",
		"SELECT a FROM t WHERE b REGEXP 'x'",
		"SELECT a FROM t WHERE b SOUNDS LIKE 'x'",
		"SELECT GROUP_CONCAT(a) FROM t",
		"SELECT GROUP_CONCAT(a, b) FROM t",
		"SELECT GROUP_CONCAT(a SEPARATOR ';') FROM t",
		"SELECT a FROM t WHERE b RLIKE 'x' AND c LIKE 'y%'",
		"SELECT a FROM t WHERE BINARY a = 'x'",
		"SELECT a FROM t WHERE a COLLATE utf8mb4_bin = 'x'",
		"SELECT CAST(a AS UNSIGNED) FROM t",
		"SELECT a DIV 2 FROM t",
		"SELECT STR_TO_DATE(a, '%Y') FROM t",
		"SELECT TIMEDIFF(a, b) FROM t",
		"SELECT SYSDATE() FROM t",
		"SELECT NOW(6) FROM t",
		"SELECT UNIX_TIMESTAMP() FROM t",
		"SELECT _bintrail_date_sub(a) FROM t",
		"SELECT a AS $x$ FROM t",
	} {
		v, hard := VetoOpts(stmt, portOpts), HardVetoOpts(stmt, portOpts)
		if v == "" {
			t.Errorf("%q: no veto at all; the case tests nothing", stmt)
			continue
		}
		if hard != v {
			t.Errorf("%q: held to %q, want %q (VetoOpts's own)", stmt, hard, v)
		}
	}
}

// A statement with a veto that is relaxed and one that is not is held by
// the second, whichever VetoOpts names first.
func TestHardVetoOpts_mixed(t *testing.T) {
	for _, c := range []struct{ stmt, want string }{
		// The LIKE comes first on the list, the || after it.
		{"SELECT a || b FROM t WHERE c LIKE 'x%'", "|| (string concatenation on the copy, logical OR on MySQL)"},
		{"SELECT a FROM t WHERE c LIKE 'x%' AND d = @v", "user or system variable"},
		// The relaxed checks after the list, before a veto of the list.
		{"SELECT CONCAT(a, b) FROM t FOR UPDATE", "locking read"},
		{"SELECT SUBSTRING(a, 0, 2) FROM t WHERE b = @v", "user or system variable"},
		{"SELECT SUM(a) / 2 FROM t WHERE c LIKE 'x%' AND RAND() > 0.5", "RAND/UUID"},
	} {
		if v := VetoOpts(c.stmt, portOpts); v == "" {
			t.Errorf("%s: no veto at all", c.stmt)
		}
		if got := HardVetoOpts(c.stmt, portOpts); got != c.want {
			t.Errorf("%s: held to %q, want %q", c.stmt, got, c.want)
		}
	}
}

// With no veto, neither says anything; with the LIKE switch on, a LIKE is
// no veto to begin with and one with a pattern from a column is relaxed.
func TestHardVetoOpts_noneAndLikeSwitch(t *testing.T) {
	for _, stmt := range []string{"SELECT a FROM t WHERE b = 1", "SELECT COUNT(*) FROM t", ""} {
		if v, hard := VetoOpts(stmt, portOpts), HardVetoOpts(stmt, portOpts); v != "" || hard != "" {
			t.Errorf("%q: veto %q, hard %q; want neither", stmt, v, hard)
		}
	}
	on := portOpts
	on.LikeIgnoresCase = true
	if v := VetoOpts("SELECT a FROM t WHERE b LIKE c", on); v != likeVeto {
		t.Fatalf("a LIKE with its pattern from a column under the switch: veto %q, want %q", v, likeVeto)
	}
	if hard := HardVetoOpts("SELECT a FROM t WHERE b LIKE c", on); hard != "" {
		t.Errorf("a LIKE with its pattern from a column under the switch: held to %q, want it let past", hard)
	}
	if hard := HardVetoOpts("SELECT a FROM t WHERE b REGEXP 'x'", on); hard != likeVeto {
		t.Errorf("REGEXP under the switch: held to %q, want %q", hard, likeVeto)
	}
}

// Every name heavyRelaxes lets past is a name on the veto list: a name that
// drifts from the list would relax nothing, silently.
func TestHeavyRelaxes_namesOnTheList(t *testing.T) {
	on := map[string]bool{}
	for _, v := range vetoes {
		on[v.name] = true
	}
	for name := range heavyRelaxes {
		if !on[name] {
			t.Errorf("heavyRelaxes names %q, which is not on the veto list", name)
		}
	}
}

// The 22 CH-benCHmark reports as run against MySQL: none holds a veto an
// expensive plan does not let past, so each one's plan decides.
func TestHardVetoOpts_chReports(t *testing.T) {
	for name, stmt := range chReports {
		if hard := HardVetoOpts(stmt, portOpts); hard != "" {
			t.Errorf("%s: held to %q", name, hard)
		}
		if _, why := ForCopyOpts(stmt, portOpts); why != "" {
			t.Errorf("%s: the copy cannot be sent its text: %s", name, why)
		}
	}
}

var chReports = map[string]string{
	"Q1":  "SELECT ol_number, sum(ol_quantity) AS sum_qty, sum(ol_amount) AS sum_amount, avg(ol_quantity) AS avg_qty, avg(ol_amount) AS avg_amount, count(*) AS count_order FROM order_line WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' GROUP BY ol_number ORDER BY ol_number",
	"Q2":  "SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment FROM item, supplier, stock, nation, region, (SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity FROM stock, supplier, nation, region WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey AND su_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name LIKE 'Europ%' GROUP BY s_i_id) m WHERE i_id = s_i_id AND MOD((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND i_data LIKE '%b' AND r_name LIKE 'Europ%' AND i_id=m_i_id AND s_quantity = m_s_quantity ORDER BY n_name, su_name, i_id LIMIT 100",
	"Q3":  "SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) AS revenue, o_entry_d FROM customer, new_order, orders, order_line WHERE c_state LIKE 'A%' AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND no_w_id = o_w_id AND no_d_id = o_d_id AND no_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d > '2007-01-02 00:00:00.000000' GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d ORDER BY revenue DESC , o_entry_d LIMIT 10",
	"Q4":  "SELECT o_ol_cnt, count(*) AS order_count FROM orders WHERE exists (SELECT * FROM order_line WHERE o_id = ol_o_id AND o_w_id = ol_w_id AND o_d_id = ol_d_id AND ol_delivery_d >= o_entry_d) GROUP BY o_ol_cnt ORDER BY o_ol_cnt",
	"Q5":  "SELECT n_name, sum(ol_amount) AS revenue FROM customer, orders, order_line, stock, supplier, nation, region WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id=o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND MOD((s_w_id * s_i_id), 10000) = su_suppkey AND ascii(substring(c_state from 1 for 1)) = su_nationkey AND su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'Europe' AND o_entry_d >= '2007-01-02 00:00:00.000000' GROUP BY n_name ORDER BY revenue DESC",
	"Q6":  "SELECT sum(ol_amount) AS revenue FROM order_line WHERE ol_delivery_d >= '1999-01-01 00:00:00.000000' AND ol_delivery_d < '2032-01-01 00:00:00.000000' AND ol_quantity BETWEEN 1 AND 100000",
	"Q7":  "SELECT su_nationkey AS supp_nation, substring(c_state from 1 for 1) AS cust_nation, extract(YEAR FROM o_entry_d) AS l_year, sum(ol_amount) AS revenue FROM supplier, stock, order_line, orders, customer, nation n1, nation n2 WHERE ol_supply_w_id = s_w_id AND ol_i_id = s_i_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND su_nationkey = n1.n_nationkey AND ascii(substring(c_state from 1 for 1)) = n2.n_nationkey AND ((n1.n_name = 'Germany' AND n2.n_name = 'Cambodia') OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany')) GROUP BY su_nationkey, cust_nation, l_year ORDER BY su_nationkey, cust_nation, l_year",
	"Q8":  "SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year",
	"Q9":  "SELECT n_name, extract(YEAR FROM o_entry_d) AS l_year, sum(ol_amount) AS sum_profit FROM item, stock, supplier, order_line, orders, nation WHERE ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND ol_i_id = i_id AND su_nationkey = n_nationkey AND i_data LIKE '%bb' GROUP BY n_name, l_year ORDER BY n_name, l_year DESC",
	"Q10": "SELECT c_id, c_last, sum(ol_amount) AS revenue, c_city, c_phone, n_name FROM customer, orders, order_line, nation WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d >= '2007-01-02 00:00:00.000000' AND o_entry_d <= ol_delivery_d AND n_nationkey = ascii(substring(c_state from 1 for 1)) GROUP BY c_id, c_last, c_city, c_phone, n_name ORDER BY revenue DESC LIMIT 20",
	"Q11": "SELECT s_i_id, sum(s_order_cnt) AS ordercount FROM stock, supplier, nation WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY s_i_id HAVING sum(s_order_cnt) > (SELECT sum(s_order_cnt) * .005 FROM stock, supplier, nation WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_name = 'Germany') ORDER BY ordercount DESC",
	"Q12": "SELECT o_ol_cnt, sum(CASE WHEN o_carrier_id = 1 OR o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, sum(CASE WHEN o_carrier_id <> 1 AND o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count FROM orders, order_line WHERE ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d <= ol_delivery_d AND ol_delivery_d < '2032-01-01 00:00:00.000000' GROUP BY o_ol_cnt ORDER BY o_ol_cnt",
	"Q13": "SELECT c_count, count(*) AS custdist FROM (SELECT c_id, count(o_id) AS c_count FROM customer LEFT OUTER JOIN orders ON (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id AND o_carrier_id > 8) GROUP BY c_id) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC",
	"Q14": "SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM order_line, item WHERE ol_i_id = i_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' AND ol_delivery_d < '2032-01-02 00:00:00.000000'",
	"Q15": "WITH revenue0 (supplier_no, total_revenue) AS (SELECT mod((s_w_id * s_i_id),10000) as supplier_no, sum(ol_amount) as total_revenue FROM order_line, stock WHERE ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' GROUP BY supplier_no) SELECT su_suppkey, su_name, su_address, su_phone, total_revenue FROM supplier, revenue0 WHERE su_suppkey = supplier_no AND total_revenue = (select max(total_revenue) from revenue0) ORDER BY su_suppkey",
	"Q16": "SELECT i_name, substring(i_data from 1 for 3) AS brand, i_price, count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt FROM stock, item WHERE i_id = s_i_id AND i_data NOT LIKE 'zz%' AND (mod((s_w_id * s_i_id),10000) NOT IN (SELECT su_suppkey FROM supplier WHERE su_comment LIKE '%bad%')) GROUP BY i_name, brand, i_price ORDER BY supplier_cnt DESC",
	"Q17": "SELECT SUM(ol_amount) / 2.0 AS avg_yearly FROM order_line, (SELECT i_id, AVG (ol_quantity) AS a FROM item, order_line WHERE i_data LIKE '%b' AND ol_i_id = i_id GROUP BY i_id) t WHERE ol_i_id = t.i_id AND ol_quantity < t.a",
	"Q18": "SELECT c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount) AS amount_sum FROM customer, orders, order_line WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id GROUP BY o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt HAVING sum(ol_amount) > 200 ORDER BY amount_sum DESC, o_entry_d LIMIT 100",
	"Q19": "SELECT sum(ol_amount) AS revenue FROM order_line, item WHERE (ol_i_id = i_id AND i_data LIKE '%a' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 3)) OR (ol_i_id = i_id AND i_data LIKE '%b' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 4)) OR (ol_i_id = i_id AND i_data LIKE '%c' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 5, 3))",
	"Q20": "SELECT su_name, su_address FROM supplier, nation WHERE su_suppkey IN (SELECT mod(s_i_id * s_w_id, 10000) FROM stock INNER JOIN item ON i_id = s_i_id INNER JOIN order_line ON ol_i_id = s_i_id WHERE ol_delivery_d > '2010-05-23 12:00:00' AND i_data LIKE 'co%' GROUP BY s_i_id, s_w_id, s_quantity HAVING 2*s_quantity > sum(ol_quantity)) AND su_nationkey = n_nationkey AND n_name = 'Germany' ORDER BY su_name",
	"Q21": "SELECT su_name, count(*) AS numwait FROM supplier, order_line l1, orders, stock, nation WHERE ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND mod((s_w_id * s_i_id),10000) = su_suppkey AND l1.ol_delivery_d > o_entry_d AND NOT EXISTS (SELECT * FROM order_line l2 WHERE l2.ol_o_id = l1.ol_o_id AND l2.ol_w_id = l1.ol_w_id AND l2.ol_d_id = l1.ol_d_id AND l2.ol_delivery_d > l1.ol_delivery_d) AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY su_name ORDER BY numwait DESC, su_name LIMIT 100",
	"Q22": "SELECT substring(c_state from 1 for 1) AS country, count(*) AS numcust, sum(c_balance) AS totacctbal FROM customer WHERE substring(c_phone from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7') AND c_balance > (SELECT avg(c_balance) FROM customer WHERE c_balance > 0.00 AND substring(c_phone from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7')) AND NOT EXISTS (SELECT * FROM orders WHERE o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id) GROUP BY substring(c_state from 1 for 1) ORDER BY substring(c_state,1,1)",
}
