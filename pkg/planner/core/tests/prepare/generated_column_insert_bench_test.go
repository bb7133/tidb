package prepare_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func buildVirtualGeneratedInsertSelectDDL(totalCols, generatedCols int) string {
	var sb strings.Builder
	sb.WriteString("create table dst (id int")
	for i := 0; i < totalCols-generatedCols-1; i++ {
		fmt.Fprintf(&sb, ", c%d int", i)
	}
	for i := 0; i < generatedCols; i++ {
		fmt.Fprintf(&sb, ", vg%d int as (null) virtual", i)
	}
	sb.WriteString(")")
	return sb.String()
}

func buildInsertSelectSQL(totalCols, generatedCols int) string {
	baseCols := totalCols - generatedCols
	colNames := make([]string, 0, baseCols)
	selectExprs := make([]string, 0, baseCols)
	colNames = append(colNames, "id")
	selectExprs = append(selectExprs, "id")
	for i := 0; i < baseCols-1; i++ {
		colNames = append(colNames, fmt.Sprintf("c%d", i))
		selectExprs = append(selectExprs, fmt.Sprintf("c%d", i))
	}
	return fmt.Sprintf("insert into dst (%s) select %s from src", strings.Join(colNames, ","), strings.Join(selectExprs, ","))
}

func setupInsertSelectVirtualGeneratedBenchmark(t testing.TB, totalCols, generatedCols int) (*testkit.TestKit, string) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists src, dst")
	tk.MustExec(buildVirtualGeneratedInsertSelectDDL(totalCols, generatedCols))

	var src strings.Builder
	src.WriteString("create table src (id int")
	for i := 0; i < totalCols-generatedCols-1; i++ {
		fmt.Fprintf(&src, ", c%d int", i)
	}
	src.WriteString(")")
	tk.MustExec(src.String())
	tk.MustExec("insert into src values (1" + strings.Repeat(", 1", totalCols-generatedCols-1) + ")")
	return tk, buildInsertSelectSQL(totalCols, generatedCols)
}

func TestPrepareInsertSelectWithManyVirtualGeneratedColumns(t *testing.T) {
	tk, sql := setupInsertSelectVirtualGeneratedBenchmark(t, 240, 120)
	stmtID, _, _, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	defer func() { require.NoError(t, tk.Session().DropPreparedStmt(stmtID)) }()

	_, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtID, expression.Args2Expressions4Test())
	require.NoError(t, err)
	tk.MustQuery("select count(*) from dst").Check(testkit.Rows("1"))
}

func BenchmarkPrepareInsertSelectWithManyVirtualGeneratedColumns(b *testing.B) {
	tk, sql := setupInsertSelectVirtualGeneratedBenchmark(b, 500, 150)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmtID, _, _, err := tk.Session().PrepareStmt(sql)
		if err != nil {
			b.Fatal(err)
		}
		if err := tk.Session().DropPreparedStmt(stmtID); err != nil {
			b.Fatal(err)
		}
	}
}
