/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// Copyright (C) 2004-2005 SAS Institute, Inc.
// All Rights Reserved.
*/
package mondrian.olap.fun;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil.MeasureVisitor;
import mondrian.rolap.PreEvalFailedException;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapMeasureGroup;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapStoredMeasure;


/**
 * Definition of the <code>NonEmptyCrossJoin</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 *
 * author 16 December, 2004
 */
public class NonEmptyCrossJoinXLFunDef extends CrossJoinFunDef {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
        "NonEmptyCrossJoinXL",
            "NonEmptyCrossJoinXL(<Set1>, <Set2>)",
            "Returns the cross product of two sets, excluding empty tuples, tuples without associated fact table data and aggregates not used by Excel.",
            new String[]{"fxxx"},
            NonEmptyCrossJoinXLFunDef.class);

    public NonEmptyCrossJoinXLFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        return new AbstractListCalc(
            call, new Calc[] {listCalc1, listCalc2}, false)
        {
            public TupleList evaluateList(Evaluator evaluator) {
                SchemaReader schemaReader = evaluator.getSchemaReader();

                // Evaluate the arguments in non empty mode, but remove from
                // the slicer any members that will be overridden by args to
                // the NonEmptyCrossjoin function. For example, in
                //
                //   SELECT NonEmptyCrossJoin(
                //       [Store].[USA].Children,
                //       [Product].[Beer].Children)
                //    FROM [Sales]
                //    WHERE [Store].[Mexico]
                //
                // we want all beers, not just those sold in Mexico.
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(true);
                    for (Member member
                        : ((RolapEvaluator) evaluator).getSlicerMembers())
                    {
                        if (getType().getElementType().usesHierarchy(
                                member.getHierarchy(), true))
                        {
                            evaluator.setContext(
                                member.getHierarchy().getAllMember());
                        }
                    }

                    NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        return
                            (TupleList) nativeEvaluator.execute(
                                ResultStyle.LIST);
                    }
                    final TupleList list1 = listCalc1.evaluateList(evaluator);
                    if (list1.isEmpty()) {
                        return list1;
                    }
                    final TupleList list2 = listCalc2.evaluateList(evaluator);
                    TupleList result = null;
                    if (((RolapEvaluator)evaluator).isPreEvaluation()) {
                        while (true) {
                            int arity = list1.getArity() + list2.getArity();
                            List<Member> firstTuple = new ArrayList<Member>();
                            for (int i = 0; i < list1.getArity(); i++) {
                                Member m = list1.get(0).get(i);
                                if (m.isMeasure()) {
                                    arity--;
                                } else if (!m.isAll()) {
                                    firstTuple.add(m);
                                } else {
                                    Member firstChild = evaluator.getSchemaReader().getMemberChildren(m).get(0);
                                    firstTuple.add(firstChild);
                                }
                            }
                            for (int i = 0; i < list2.getArity(); i++) {
                                Member m = list2.get(0).get(i);
                                if (m.isMeasure()) {
                                    arity--;
                                } else if (!m.isAll()) {
                                    firstTuple.add(m);
                                } else {
                                    Member firstChild = evaluator.getSchemaReader().getMemberChildren(m).get(0);
                                    firstTuple.add(firstChild);
                                }
                            }
                            result = TupleCollections.createList(arity);
                            result.add(firstTuple);
                            //we need measure to get measuregroup which is needed to get starcolumn
                            RolapStoredMeasure measure = null;
                            Member[] members = evaluator.getNonAllMembers();
                            if (members.length > 0 && members[0] instanceof RolapStoredMeasure) {
                                measure = (RolapStoredMeasure) members[0];
                            } else {
                                boolean storedMeasureFound = false;
                                Set<Member> queryMeasureSet = evaluator.getQuery().getMeasuresMembers();
                                Set<Member> measureSet = new HashSet<Member>();
                                MeasureVisitor visitor = new MeasureVisitor(measureSet, call);
                                for (Member m : queryMeasureSet) {
                                    if (m.isCalculated()) {
                                        Exp exp = m.getExpression();
                                        exp.accept(visitor);
                                    } else {
                                        measureSet.add(m);
                                    }
                                    if (measureSet.size() > 0) {
                                    	storedMeasureFound = true;
                                    	measure = (RolapStoredMeasure)measureSet.iterator().next();
                                    	break;
                                    }
                                }
                                if (!storedMeasureFound) {
                                	throw PreEvalFailedException.INSTANCE;
                                }
                            }
                            
                            final RolapMeasureGroup measureGroup = measure.getMeasureGroup();
                            Set<RolapStar.Column> preEvalOptimizedColumns = new HashSet<RolapStar.Column>();
                            for (Member member : firstTuple) {
                                final RolapMember rmember = (RolapMember) member;
                                final RolapCubeLevel level = rmember.getLevel();
                                final List<RolapStar.Column> starColumns = level.getLevelReader().getStarKeyColumns(rmember,
                                        measureGroup);
                                preEvalOptimizedColumns.addAll(starColumns);
                            }
                            for (int i = 0; i < arity; i++) {
                                List<Member> tuple = new ArrayList<Member>(arity);
                                for (int j = 0; j < arity - i - 1; j++) {
                                    tuple.add(firstTuple.get(j));
                                }
                                for (int j = arity - i - 1; j < arity; j++) {
                                    tuple.add(evaluator.getSchemaReader()
                                            .getHierarchyRootMembers(firstTuple.get(j).getHierarchy()).get(0));

                                }
                                result.add(tuple);
                            }
                            ((RolapEvaluator) evaluator).setPreEvalOptimizedColumns(preEvalOptimizedColumns);
                            break;
                        }
                    }
                    else {
                    	result = mutableCrossJoinXL(list1, list2);
                    }
                    
                    // remove any remaining empty crossings from the result
                    result = nonEmptyList(evaluator, result, call, ctag);
                    return result;
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                if (super.dependsOn(hierarchy)) {
                    return true;
                }
                // Member calculations generate members, which mask the actual
                // expression from the inherited context.
                if (listCalc1.getType().usesHierarchy(hierarchy, true)) {
                    return false;
                }
                if (listCalc2.getType().usesHierarchy(hierarchy, true)) {
                    return false;
                }
                // The implicit value expression, executed to figure out
                // whether a given tuple is empty, depends upon all dimensions.
                return true;
            }
        };
    }

}

// End NonEmptyCrossJoinFunDef.java
