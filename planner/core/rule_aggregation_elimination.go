// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"

	"github.com/sirupsen/logrus"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type aggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
}

type aggregationPattern struct {
	outter *LogicalAggregation
	proj   *LogicalProjection
	inner  *LogicalAggregation
}

// aggMapping is a inner/outter mapping
var (
	aggMapping = []struct {
		outter string
		inner  string
		result string
	}{
		{ast.AggFuncFirstRow, ast.AggFuncFirstRow, ast.AggFuncFirstRow},
		{ast.AggFuncSum, ast.AggFuncSum, ast.AggFuncSum},
		{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncCount},
		{ast.AggFuncMax, ast.AggFuncMax, ast.AggFuncMax},
		{ast.AggFuncMax, ast.AggFuncFirstRow, ast.AggFuncMax},
		{ast.AggFuncMin, ast.AggFuncMin, ast.AggFuncMin},
		{ast.AggFuncMin, ast.AggFuncFirstRow, ast.AggFuncMin},
	}
	temp = make(map[string]map[string]string)
)

func init() {
	for _, m := range aggMapping {
		if _, ok := temp[m.outter]; !ok {
			temp[m.outter] = make(map[string]string)
		}
		temp[m.outter][m.inner] = m.result
	}
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *LogicalAggregation) *LogicalProjection {
	schemaByGroupby := expression.NewSchema(agg.groupByCols...)
	coveredByUniqueKey := false
	for _, key := range agg.children[0].Schema().Keys {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			coveredByUniqueKey = true
			break
		}
	}
	if coveredByUniqueKey {
		// GroupByCols has unique key, so this aggregation can be removed.
		proj := a.convertAggToProj(agg)
		proj.SetChildren(agg.children[0])
		return proj
	}
	return nil
}

func (a *aggregationEliminateChecker) convertAggToProj(agg *LogicalAggregation) *LogicalProjection {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx)
	for _, fun := range agg.AggFuncs {
		expr := a.rewriteExpr(agg.ctx, fun)
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(agg.schema.Clone())
	return proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (a *aggregationEliminateChecker) rewriteExpr(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc) expression.Expression {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode {
			return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
		}
		return a.rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		panic("Unsupported function")
	}
}

func (a *aggregationEliminateChecker) rewriteCount(ctx sessionctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		if mysql.HasNotNullFlag(expr.GetType().Flag) {
			isNullExprs = append(isNullExprs, expression.Zero)
		} else {
			isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
			isNullExprs = append(isNullExprs, isNullExpr)
		}
	}

	innerExpr := expression.ComposeDNFCondition(ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(ctx, ast.If, targetTp, innerExpr, expression.Zero, expression.One)
	return newExpr
}

func (a *aggregationEliminateChecker) rewriteBitFunc(ctx sessionctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg)
	outerCast := a.wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.Zero.Clone())
	} else {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(), outerCast, &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: targetTp})
	}
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func (a *aggregationEliminateChecker) wrapCastFunction(ctx sessionctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType() == targetTp {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

func (a *aggregationEliminator) tryToEliminateAggregationEnhanced(la *LogicalAggregation) *LogicalAggregation {
	ptn := &aggregationPattern{outter: la}
	collectNestedGroupByPattern(la, ptn)

	if ptn.inner == nil || ptn.proj == nil {
		return nil
	}
	exprMap, aggMap := equalityMaps(la.context(), ptn)
	_, items := exprSubstitute(la.context(), exprMap, ptn.outter.GroupByItems)
	_, items = aggSubstitute(la.context(), aggMap, items)
	logrus.Infof("NNNNNNN new items: %v", items)

	for idx, item := range items {
		logrus.Infof("MMMMMMMM searching equality for: %v, replaced as: %v", ptn.outter.GroupByItems[idx], item)
		found := false
		for _, subItem := range ptn.inner.GroupByItems {
			if subItem.Equal(la.context(), item) {
				logrus.Infof("MMMMMMMM found equality: %v, %v", ptn.outter.GroupByItems[idx], subItem)
				found = true
				break
			}
		}
		if !found {
			logrus.Info("MMMMMMMM item not found")
			return nil
		}
	}
	// Now GroupByItems in outter aggregation is a subset of those in inner aggregation.
	// It is possible to combine aggregation
	logrus.Infof("================= GB Items: %s all contained in its child: %s, can try to prune", ptn.outter.GroupByItems, ptn.inner.GroupByItems)

	var newAggs []*aggregation.AggFuncDesc
	for _, agg := range ptn.outter.AggFuncs {
		_, exprs := exprSubstitute(la.context(), exprMap, agg.Args)
		expr := exprs[0]
		cols := expression.ExtractColumns(expr)
		if len(cols) != 1 {
			// we have a expr that refs to more than 1 col like `.. (sum(b) + count(c)) as a ...`, we do not optimize such cases
			return nil
		}
		col := cols[0]
		colAgg := aggMap[string(col.HashCode(nil))]
		logrus.Infof("VVVVVVVVVVV colAgg: %v", colAgg)

		if _, ok := expr.(*expression.Column); !ok {
			found := false
			for _, item := range ptn.inner.GroupByItems {
				if item.Equal(la.context(), expr) {
					found = true
					logrus.Infof("WWWWWWW found proj relation: expr - %v, gb item - %v", expr, item)
					break
				}
			}
			if !found {
				logrus.Infof("WWWWWWW cannot project expr: %v", expr)
				return nil
			}
		} else {
			logrus.Infof("WWWWWWW expr: %v is a trivial column", expr)
		}

		_, es := aggSubstitute(la.context(), aggMap, exprs)
		newAgg := tryToCombineAggFunc(agg, colAgg, es[0])
		if newAgg == nil {
			logrus.Infof("=========== Unreplaceable agg pair: %v and %v %v", agg, colAgg, es)
			return nil
		}
		newAggs = append(newAggs, newAgg)
	}
	la.AggFuncs = newAggs
	la.GroupByItems = items
	la.collectGroupByColumns()
	la.SetChildren(ptn.inner.Children()...)

	///////////////// Print Debug Info ////////////////////
	logrus.Infof("VVVVVVVVVVV Replaced")
	printDebugInfo(la)

	return la
}

func (a *aggregationEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.optimize(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return p, nil
	}
	// if proj := a.tryToEliminateAggregation(agg); proj != nil {
	// 	return proj, nil
	// }
	if proj := a.tryToEliminateAggregationEnhanced(agg); proj != nil {
		return proj, nil
	}

	return p, nil
}

func tryToCombineAggFunc(outter, inner *aggregation.AggFuncDesc, expr expression.Expression) *aggregation.AggFuncDesc {
	v, ok := temp[outter.Name]
	if !ok {
		return nil
	}
	t, ok1 := v[inner.Name]
	if !ok1 {
		return nil
	}
	r := outter.Clone()
	r.Name = t
	r.Args[0] = expr
	return r
}

func collectNestedGroupByPattern(lp LogicalPlan, ptn *aggregationPattern) {
	if len(lp.Children()) != 1 {
		return
	}
	child := lp.Children()[0]
	switch v := child.(type) {
	case *LogicalAggregation:
		ptn.inner = v
		return
	case *LogicalProjection:
		if ptn.proj != nil {
			return
		}
		ptn.proj = v
	case *LogicalLimit, *LogicalSelection:
		return
	default:
		// Do nothing
	}
	collectNestedGroupByPattern(child, ptn)
}

func equalityMaps(ctx sessionctx.Context, ptn *aggregationPattern) (map[string]expression.Expression, map[string]*aggregation.AggFuncDesc) {
	exprMap := make(map[string]expression.Expression)
	for _, col := range ptn.proj.Schema().Columns {
		idx := ptn.proj.Schema().ColumnIndex(col)
		if idx == -1 {
			panic("Cannot find column, should never happen")
		}
		exprMap[string(col.HashCode(nil))] = ptn.proj.Exprs[idx]
	}
	gbItemMap := make(map[string]*aggregation.AggFuncDesc)
	for _, col := range ptn.inner.Schema().Columns {
		idx := ptn.inner.Schema().ColumnIndex(col)
		if idx == -1 {
			panic("Cannot find column, should never happen")
		}
		gbItemMap[string(col.HashCode(nil))] = ptn.inner.AggFuncs[idx]
	}

	return exprMap, gbItemMap
}

// exprSubstitute tries to substitutes expressions occured in given `exprs` with their equalities defined in `exprMap`
func exprSubstitute(ctx sessionctx.Context, exprMap map[string]expression.Expression, exprs []expression.Expression) (bool, []expression.Expression) {
	var newExprs []expression.Expression
	replaced := false
	for idx, expr := range exprs {
		if e, ok := exprMap[string(expr.HashCode(nil))]; ok {
			replaced = true
			if newExprs == nil {
				newExprs = make([]expression.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[idx] = e
			continue
		}
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		if subReplaced, subExprs := exprSubstitute(ctx, exprMap, sf.GetArgs()); subReplaced {
			replaced = true
			newSf := expression.NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), subExprs...)
			if newExprs == nil {
				newExprs = make([]expression.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[idx] = newSf
		}
	}
	if replaced {
		return true, newExprs
	}
	return false, exprs
}

// aggSubstitute tries to substitutes expressions
func aggSubstitute(
	ctx sessionctx.Context,
	exprMap map[string]*aggregation.AggFuncDesc,
	exprs []expression.Expression,
) (bool, []expression.Expression) {
	var newExprs []expression.Expression
	replaced := false
	for idx, expr := range exprs {
		if agg, ok := exprMap[string(expr.HashCode(nil))]; ok {
			replaced = true
			if newExprs == nil {
				newExprs = make([]expression.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[idx] = agg.Args[0]
			continue
		}
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		if subReplaced, subExprs := aggSubstitute(ctx, exprMap, sf.GetArgs()); subReplaced {
			replaced = true
			newSf := expression.NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), subExprs...)
			if newExprs == nil {
				newExprs = make([]expression.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[idx] = newSf
		}
	}
	if replaced {
		return true, newExprs
	}
	return false, exprs
}

func printDebugInfo(lp LogicalPlan) {
	logrus.Info("*********** plan *************")
	switch p := lp.(type) {
	case *LogicalAggregation:
		logrus.Infof("====== Result LA: %v, schema: %v, gb items: %v, gb cols: %v", p.AggFuncs, p.Schema(), p.GroupByItems, p.groupByCols)
		for _, agg := range p.AggFuncs {
			for _, col := range expression.ExtractColumns(agg.Args[0]) {
				logrus.Infof("====== LA ref columns: %v, index: %v", col, col.UniqueID)
			}
		}
	case *LogicalProjection:
		logrus.Infof("====== Result LP: %v, schema: %v", p.Exprs, p.Schema())
		for _, expr := range p.Exprs {
			for _, col := range expression.ExtractColumns(expr) {
				logrus.Infof("====== LP ref columns: %v, index: %v", col, col.UniqueID)
			}
		}
	case *LogicalSelection:
		logrus.Infof("====== Result LS: %v, schema: %v", p.Conditions, p.Schema())
		for _, expr := range p.Conditions {
			for _, col := range expression.ExtractColumns(expr) {
				logrus.Infof("====== LP ref columns: %v, index: %v", col, col.UniqueID)
			}
		}
	}
	for _, col := range lp.Schema().Columns {
		logrus.Infof("====== schema col: %v, uniq id: %v", col, col.UniqueID)
	}
	if len(lp.Children()) == 0 {
		return
	}
	printDebugInfo(lp.Children()[0])
}
