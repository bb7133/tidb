// Copyright 2016 PingCAP, Inc.
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
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
)

type groupPruner struct {
}

type subAggInfo struct {
	agg     *LogicalAggregation
	subAgg  *LogicalAggregation
	subProj *LogicalProjection
}

func (s *groupPruner) optimize(lp LogicalPlan) (LogicalPlan, error) {
	lp.PruneGroupBy()
	return lp, nil
}

// PruneGroupBy implements LogicalPlan interface.
// If any expression has SetVar functions, we do not prune it.
func (p *LogicalProjection) PruneGroupBy() {
	logrus.Infof("XXXXX LP PruneGBK: %v", lpString(p))
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

func equalityMapsOri(ctx sessionctx.Context, subInfo *subAggInfo) (map[string]expression.Expression, map[string]*aggregation.AggFuncDesc) {
	exprMap := make(map[string]expression.Expression)
	for _, col := range subInfo.subProj.Schema().Columns {
		idx := subInfo.subProj.Schema().ColumnIndex(col)
		if idx == -1 {
			panic("Cannot find column, should never happen")
		}
		exprMap[string(col.HashCode(nil))] = subInfo.subProj.Exprs[idx]
	}
	gbItemMap := make(map[string]*aggregation.AggFuncDesc)
	for _, col := range subInfo.subAgg.Schema().Columns {
		idx := subInfo.subAgg.Schema().ColumnIndex(col)
		if idx == -1 {
			panic("Cannot find column, should never happen")
		}
		gbItemMap[string(col.HashCode(nil))] = subInfo.subAgg.AggFuncs[idx]
	}

	return exprMap, gbItemMap
}

// exprSubstituteOri tries to substitutes expressions occured in given `exprs` with their equalities defined in `exprMap`
func exprSubstituteOri(ctx sessionctx.Context, exprMap map[string]expression.Expression, exprs ...expression.Expression) (bool, []expression.Expression) {
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
		if subReplaced, subExprs := exprSubstituteOri(ctx, exprMap, sf.GetArgs()...); subReplaced {
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

// aggSubstituteOri tries to substitutes expressions
func aggSubstituteOri(ctx sessionctx.Context, exprMap map[string]*aggregation.AggFuncDesc, exprs ...expression.Expression) (bool, []expression.Expression) {
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
		if subReplaced, subExprs := aggSubstituteOri(ctx, exprMap, sf.GetArgs()...); subReplaced {
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

func printDebugInfoOri(lp LogicalPlan) {
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
	printDebugInfoOri(lp.Children()[0])
}

func collectGroupByInfo(lp LogicalPlan, info *subAggInfo) {
	if len(lp.Children()) != 1 {
		return
	}
	child := lp.Children()[0]
	switch v := child.(type) {
	case *LogicalAggregation:
		info.subAgg = v
		return
	case *LogicalProjection:
		if info.subProj != nil {
			return
		}
		info.subProj = v
	case *LogicalLimit, *LogicalSelection:
		return
	default:
		// Do nothing
	}
	collectGroupByInfo(child, info)
}

func (info *subAggInfo) String() string {
	return fmt.Sprintf("Agg: %v, SubAgg: %v, Proj: %v", laString(info.agg), laString(info.subAgg), lpString(info.subProj))
}

func laString(la *LogicalAggregation) string {
	if la == nil {
		return "LA - nil"
	}
	return fmt.Sprintf("LA - aggFuncs: %v, gb exprs: %v, schema: %v", la.AggFuncs, la.GroupByItems, la.Schema())
}

func lpString(lp *LogicalProjection) string {
	if lp == nil {
		return "LP - nil"
	}
	return fmt.Sprintf("LP - exprs: %v, schema: %v", lp.Exprs, lp.Schema())
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalSelection) PruneGroupBy() {
	logrus.Infof("XXXXXX LS PruneGBK, conditions: %v, schema: %v", p.Conditions, p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (la *LogicalAggregation) PruneGroupBy() {
	logrus.Infof("XXXXXX LA PruneGBK: %v", laString(la))
	for _, child := range la.Children() {
		child.PruneGroupBy()
	}

	///////
	return
	///////

	subInfo := &subAggInfo{agg: la}
	collectGroupByInfo(la, subInfo)
	if subInfo.subAgg == nil || subInfo.subProj == nil {
		return
	}
	exprMap, aggMap := equalityMapsOri(la.context(), subInfo)
	// logrus.Infof("NNNNN exprMap: %v", exprMap)
	// logrus.Infof("NNNNN aggMap: %v", aggMap)
	_, s1 := exprSubstituteOri(la.context(), exprMap, subInfo.agg.GroupByItems...)
	logrus.Infof("NNNNNNN s1: %v", s1)
	_, s2 := aggSubstituteOri(la.context(), aggMap, s1...)
	logrus.Infof("NNNNNNN s2: %v", s2)

	for idx, item := range s2 {
		logrus.Infof("MMMMMMMM searching equality for: %v, replaced as: %v", subInfo.agg.GroupByItems[idx], item)
		found := false
		for _, subItem := range subInfo.subAgg.GroupByItems {
			if subItem.Equal(la.context(), item) {
				logrus.Infof("MMMMMMMM found equality: %v, %v", subInfo.agg.GroupByItems[idx], subItem)
				found = true
				continue
			}
		}
		if !found {
			logrus.Info("MMMMMMMM item not found")
			return
		}
	}
	logrus.Infof("================= GB Items: %s all contained in its child: %s, can try to prune", subInfo.agg.GroupByItems, subInfo.subAgg.GroupByItems)
	logrus.Infof("================= GB schema: %s, child: schema: %s", la.Schema(), subInfo.subProj.Schema())

	var newAggs []*aggregation.AggFuncDesc
	for _, agg := range subInfo.agg.AggFuncs {
		_, exprs := exprSubstituteOri(la.context(), exprMap, agg.Args...)
		expr := exprs[0]
		cols := expression.ExtractColumns(expr)
		if len(cols) != 1 {
			// we have a expr that refs to more than 1 col, such like (sum(b) + count(c)) as a, we do not optimize such cases
			return
		}
		col := cols[0]
		subAgg := aggMap[string(col.HashCode(nil))]
		logrus.Infof("VVVVVVVVVVV subAgg: %v", subAgg)

		if _, ok := expr.(*expression.Column); !ok {
			found := false
			for _, i := range subInfo.subAgg.GroupByItems {
				if i.Equal(la.context(), expr) {
					found = true
					logrus.Infof("WWWWWWW found proj relation: expr - %v, gb item - %v", expr, i)
					break
				}
			}
			if !found {
				logrus.Infof("WWWWWWW cannot project expr: %v", expr)
				return
			}
		} else {
			logrus.Infof("WWWWWWW expr: %v is a trivial column", expr)
		}

		_, es := aggSubstituteOri(la.context(), aggMap, expr)

		newAgg := agg.Clone()
		switch {
		case newAgg.Name == ast.AggFuncSum && subAgg.Name == ast.AggFuncCount:
			newAgg.Args[0] = es[0]
			newAgg.Name = ast.AggFuncCount
		case newAgg.Name == ast.AggFuncSum && subAgg.Name == ast.AggFuncSum:
			newAgg.Args[0] = es[0]
		case newAgg.Name == ast.AggFuncMax && subAgg.Name == ast.AggFuncMax:
			newAgg.Args[0] = es[0]
		case newAgg.Name == ast.AggFuncMin && subAgg.Name == ast.AggFuncMin:
			newAgg.Args[0] = es[0]
		case newAgg.Name == ast.AggFuncMax && subAgg.Name == ast.AggFuncFirstRow:
			newAgg.Args[0] = es[0]
		case newAgg.Name == ast.AggFuncMin && subAgg.Name == ast.AggFuncFirstRow:
			newAgg.Args[0] = es[0]
		default:
			logrus.Infof("=========== Unreplaceable agg pair: %v and %v %v", agg, subAgg, es)
			return
		}
		newAggs = append(newAggs, newAgg)
	}
	la.AggFuncs = newAggs
	la.GroupByItems = s2
	la.collectGroupByColumns()
	la.SetChildren(subInfo.subAgg.Children()...)

	///////////////// Print Debug Info ////////////////////
	logrus.Infof("VVVVVVVVVVV Replaced")
	printDebugInfoOri(la)
}

// PruneGroupBy implements LogicalPlan interface.
func (ls *LogicalSort) PruneGroupBy() {
	logrus.Infof("XXXXXX LogicalSort PruneGBK, by items: %v, schema: %v", ls.ByItems, ls.Schema())
	for _, child := range ls.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneGroupBy() {
	logrus.Infof("XXXXXX LUA PruneGBK, schema: %v", p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneGroupBy() {
	logrus.Infof("XXXXXX LUS PruneGBK, schema: %v", p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (ds *DataSource) PruneGroupBy() {
	logrus.Infof("XXXXXX DS PruneGBK, schema: %v", ds.Schema())
	for _, child := range ds.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalTableDual) PruneGroupBy() {
	logrus.Infof("XXXXXX LTD PruneGBK, schema: %v", p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalJoin) PruneGroupBy() {
	logrus.Infof("XXXXXX LJ PruneGBK, schema: %v", p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (ll *LogicalLimit) PruneGroupBy() {
	logrus.Infof("XXXXXX LogicalLimit PruneGBK, schema: %v", ll.Schema())
	for _, child := range ll.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (la *LogicalApply) PruneGroupBy() {
	logrus.Infof("XXXXXX LogicalApply PruneGBK, schema: %v", la.Schema())
	for _, child := range la.Children() {
		child.PruneGroupBy()
	}
}

// PruneGroupBy implements LogicalPlan interface.
func (p *LogicalLock) PruneGroupBy() {
	logrus.Infof("XXXXXX LL PruneGBK, schema: %v", p.Schema())
	for _, child := range p.Children() {
		child.PruneGroupBy()
	}
}
