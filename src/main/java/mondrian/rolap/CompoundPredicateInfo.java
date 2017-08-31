/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
//
*/
package mondrian.rolap;


import mondrian.calc.TupleIterable;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.olap.fun.VisualTotalsFunDef.VisualTotalMember;
import mondrian.olap.type.SetType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.ListPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.PredicateColumn;
import mondrian.rolap.agg.Predicates;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
/**
 * Constructs a Pair<BitKey, StarPredicate> based on an tuple list and
 * measure, along with the string representation of the predicate.
 * Also sets the isSatisfiable flag based on whether a predicate
 * is compatible with the measure.
 *
 * This logic was extracted from RolapAggregationManager and AggregationKey.
 */
public class CompoundPredicateInfo {

    private final Pair<BitKey, StarPredicate> predicate;
    private String predicateString;
    private final RolapMeasure measure;
    private boolean satisfiable = true;

    public CompoundPredicateInfo(
        List<List<Member>> tupleList, RolapMeasure measure, Evaluator evaluator)
    {
        this.measure = measure;
        this.predicate = predicateFromTupleList(tupleList, measure, evaluator);
        this.predicateString = getPredicateString(
            getStar(measure), getPredicate());
        assert measure != null;
    }

    public StarPredicate getPredicate() {
        return predicate == null ? null : predicate.right;
    }

    public BitKey getBitKey() {
        return predicate == null ? null : predicate.left;
    }

    public String getPredicateString() {
        return predicateString;
    }

    public boolean isSatisfiable() {
        return satisfiable;
    }

    public RolapCube getCube() {
        return measure.isCalculated() ? null
            : ((RolapStoredMeasure)measure).getCube();
    }
    
    RolapMeasureGroup getMeasureGroup() {
        return ((RolapStoredMeasure)measure).getMeasureGroup();    
    }
    
    public void andInPlace(CompoundPredicateInfo cpi) {
        if (!this.predicate.getKey().equals(cpi.predicate.getKey())) {
            throw new UnsupportedOperationException();
        }
        this.predicate.right = this.predicate.right.and(cpi.predicate.right);
        this.predicateString = getPredicateString(
                getStar(measure), getPredicate());
    }
    
    /**
     * Returns a string representation of the predicate
     */
    public static String getPredicateString(
        RolapStar star, StarPredicate predicate)
    {
        if (star == null || predicate == null) {
            return null;
        }
        final StringBuilder buf = new StringBuilder();
        buf.setLength(0);
        predicate.toSql(star.getSqlQueryDialect(), buf);
        return buf.toString();
    }

    private static RolapStar getStar(RolapMeasure measure) {
        if (measure.isCalculated()) {
            return null;
        }
        final RolapStoredMeasure storedMeasure =
            (RolapStoredMeasure) measure;
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) storedMeasure.getStarMeasure();
        assert starMeasure != null;
        return starMeasure.getStar();
    }

    private Pair<BitKey, StarPredicate> predicateFromTupleList(
        List<List<Member>> tupleList, RolapMeasure measure, Evaluator evaluator)
    {
        if (measure.isCalculated()) {
            // need a base measure to build predicates
            return null;
        }
        RolapMeasureGroup measureGroup = ((RolapStoredMeasure)measure).getMeasureGroup();

        BitKey compoundBitKey;
        StarPredicate compoundPredicate;
        Map<BitKey, List<RolapMember[]>> compoundGroupMap;
        boolean unsatisfiable;
        int starColumnCount = getStar(measure).getColumnCount();

        compoundBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        compoundBitKey.clear();
        compoundGroupMap =
            new LinkedHashMap<BitKey, List<RolapMember[]>>();
        unsatisfiable =
            makeCompoundGroup(
                starColumnCount,
                measureGroup,
                tupleList,
                compoundGroupMap);

        if (unsatisfiable) {
            satisfiable = false;
            return null;
        }
        compoundPredicate =
            makeCompoundPredicate(compoundGroupMap, measureGroup);
        if (compoundPredicate != null) {
            for (BitKey bitKey : compoundGroupMap.keySet()) {
                compoundBitKey = compoundBitKey.or(bitKey);
            }
        }
        if (compoundBitKey.cardinality() == 1) {
            List<StarColumnPredicate> starColPredList = new ArrayList<StarColumnPredicate>();
            if (compoundPredicate instanceof ValueColumnPredicate) {
                starColPredList.add((StarColumnPredicate)compoundPredicate);
            } else {
                for (StarPredicate starPredicate : ((ListPredicate)compoundPredicate).getChildren()) {
                    starColPredList.add((StarColumnPredicate)starPredicate);
                }
            }
            compoundPredicate = new ListColumnPredicate(compoundPredicate.getColumnList().get(0),starColPredList);
        }
        return  Pair.of(compoundBitKey, compoundPredicate);
    }

    /**
     * Groups members (or tuples) from the same compound (i.e. hierarchy) into
     * groups that are constrained by the same set of columns.
     *
     * <p>For example:</p>
     *
     * <pre>
     * Members
     *     [USA].[CA],
     *     [Canada].[BC],
     *     [USA].[CA].[San Francisco],
     *     [USA].[OR].[Portland]
     * </pre>
     *
     * will be grouped into
     *
     * <pre>
     * Group 1:
     *     {[USA].[CA], [Canada].[BC]}
     * Group 2:
     *     {[USA].[CA].[San Francisco], [USA].[OR].[Portland]}
     * </pre>
     *
     * <p>This helps with generating optimal form of sql.
     *
     * <p>In case of aggregating over a list of tuples, similar logic also
     * applies.
     *
     * <p>For example:</p>
     *
     * <pre>
     * Tuples:
     *     ([Gender].[M], [Store].[USA].[CA])
     *     ([Gender].[F], [Store].[USA].[CA])
     *     ([Gender].[M], [Store].[USA])
     *     ([Gender].[F], [Store].[Canada])
     * </pre>
     *
     * <p>will be grouped into</p>
     *
     * <pre>
     * Group 1:
     *     {([Gender].[M], [Store].[USA].[CA]),
     *      ([Gender].[F], [Store].[USA].[CA])}
     * Group 2:
     *     {([Gender].[M], [Store].[USA]),
     *      ([Gender].[F], [Store].[Canada])}
     * </pre>
     *
     * <p>This function returns a boolean value indicating if any constraint
     * can be created from the aggregationList. It is possible that only part
     * of the aggregationList can be applied, which still leads to a (partial)
     * constraint that is represented by the {@code compoundGroupMap}
     * parameter.</p>
     */
    private boolean makeCompoundGroup(
        int starColumnCount,
        RolapMeasureGroup measureGroup,
        List<List<Member>> aggregationList,
        Map<BitKey, List<RolapMember[]>> compoundGroupMap)
    {
        // The more generalized aggregation as aggregating over tuples.
        // The special case is a tuple defined by only one member.
        int unsatisfiableTupleCount = 0;
        for (List<Member> aggregation : aggregationList) {
            if (aggregation.size() == 0) {
                // not a tuple
                ++unsatisfiableTupleCount;
                continue;
            }

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            RolapMember[] tuple = new RolapMember[aggregation.size()];
            int i = 0;
            for (Member member : aggregation) {
                if (member instanceof VisualTotalMember) {
                    tuple[i] = ((VisualTotalMember) member).getMember();
                } else {
                    tuple[i] = (RolapMember)member;
                }
                i++;
            }

            boolean tupleUnsatisfiable = false;
            for (RolapMember member : tuple) {
                // Tuple cannot be constrained if any of the member cannot be.
                tupleUnsatisfiable =
                    makeCompoundGroupForMember(
                        member,
                        measureGroup,
                        bitKey);
                if (tupleUnsatisfiable) {
                    // If this tuple is unsatisfiable, skip it and try to
                    // constrain the next tuple.
                    ++unsatisfiableTupleCount;
                    break;
                }
            }

            if (!tupleUnsatisfiable && !bitKey.isEmpty()) {
                // Found tuple(columns) to constrain,
                // now add it to the compoundGroupMap
                addTupleToCompoundGroupMap(tuple, bitKey, compoundGroupMap);
            }
        }

        return unsatisfiableTupleCount == aggregationList.size();
    }

    private void addTupleToCompoundGroupMap(
            RolapMember[] tuple,
            BitKey bitKey,
            Map<BitKey, List<RolapMember[]>> compoundGroupMap)
        {
            List<RolapMember[]> compoundGroup = compoundGroupMap.get(bitKey);
            if (compoundGroup == null) {
                compoundGroup = new ArrayList<RolapMember[]>();
                compoundGroupMap.put(bitKey, compoundGroup);
            }
            compoundGroup.add(tuple);
        }

    private boolean makeCompoundGroupForMember(
            RolapMember member,
            RolapMeasureGroup measureGroup,
            BitKey bitKey)
        {
            assert measureGroup != null;
            final RolapCubeLevel level = member.getLevel();
            for (RolapSchema.PhysColumn key : level.getAttribute().getKeyList()) {
                final RolapStar.Column column =
                    measureGroup.getRolapStarColumn(
                        level.getDimension(),
                        key);
                if (column == null) {
                    // request is unsatisfiable
                    return true;
                }
                bitKey.set(column.getBitPosition());
            }
            return false;
        }

    /**
     * Translates a Map&lt;BitKey, List&lt;RolapMember&gt;&gt; of the same
     * compound member into {@link ListPredicate} by traversing a list of
     * members or tuples.
     *
     * <p>1. The example below is for list of tuples
     *
     * <blockquote>
     * group 1: [Gender].[M], [Store].[USA].[CA]<br/>
     * group 2: [Gender].[F], [Store].[USA].[CA]
     * </blockquote>
     *
     * is translated into
     *
     * <blockquote>
     * (Gender = 'M' AND Store_State = 'CA' AND Store_Country = 'USA')<br/>
     * OR<br/>
     * (Gender = 'F' AND Store_State = 'CA' AND Store_Country = 'USA')
     * </blockquote>
     *
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form as
     *
     * <blockquote>
     * WHERE (gender = 'M'<br/>
     *        AND Store_State = 'CA'<br/>
     *        AND Store_Country = 'USA')<br/>
     *     OR (Gender = 'F'<br/>
     *         AND Store_State = 'CA'<br/>
     *         AND Store_Country = 'USA')
     * </blockquote>
     *
     * <p>2. The example below for a list of members
     *
     * <blockquote>
     * group 1: [USA].[CA], [Canada].[BC]<br/>
     * group 2: [USA].[CA].[San Francisco], [USA].[OR].[Portland]
     * </blockquote>
     *
     * is translated into:
     *
     * <blockquote>
     * (Country = 'USA' AND State = 'CA')<br/>
     * OR (Country = 'Canada' AND State = 'BC')<br/>
     * OR (Country = 'USA' AND State = 'CA' AND City = 'San Francisco')<br/>
     * OR (Country = 'USA' AND State = 'OR' AND City = 'Portland')
     * </pre>
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form. For exmaple, if the underlying DB supports multi
     * value IN-list, the second group will turn into this predicate:
     * <pre>
     * where (country, state, city) IN (('USA', 'CA', 'San Francisco'),
     *                                      ('USA', 'OR', 'Portland'))
     * </blockquote>
     *
     * or, if the DB does not support multi-value IN list:
     *
     * <blockquote>
     * WHERE country = 'USA' AND<br/>
     *           ((state = 'CA' AND city = 'San Francisco') OR<br/>
     *            (state = 'OR' AND city = 'Portland'))
     * </blockquote>
     *
     * @param compoundGroupMap Map from dimensionality to groups
     * @param measureGroup Measure group
     * @return compound predicate for a tuple or a member
     */
    private StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapMember[]>> compoundGroupMap,
        RolapMeasureGroup measureGroup)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate>();
        final List<RolapSchema.PhysRouter> routers =
            new ArrayList<RolapSchema.PhysRouter>();
        int count = -1;
        for (List<RolapMember[]> group : compoundGroupMap.values()) {
            // e.g.
            // {[USA].[CA], [Canada].[BC]}
            // or
            // {
            ++count;
            StarPredicate compoundGroupPredicate = null;
            for (RolapMember[] tuple : group) {
                // [USA].[CA]
                StarPredicate tuplePredicate = null;

                for (int i = 0; i < tuple.length; i++) {
                    RolapMember member = tuple[i];
                    final RolapSchema.PhysRouter router;
                    if (count == 0) {
                        router = new RolapSchema.CubeRouter(
                            measureGroup, member.getDimension());
                        routers.add(router);
                    } else {
                        router = routers.get(i);
                    }
                    tuplePredicate = makeCompoundPredicateForMember(
                        router, member, tuplePredicate);
                }
                if (tuplePredicate != null) {
                    if (compoundGroupPredicate == null) {
                        compoundGroupPredicate = tuplePredicate;
                    } else {
                        compoundGroupPredicate =
                            compoundGroupPredicate.or(tuplePredicate);
                    }
                }
            }

            if (compoundGroupPredicate != null) {
                // Sometimes the compound member list does not constrain any
                // columns; for example, if only AllLevel is present.
                compoundPredicateList.add(compoundGroupPredicate);
            }
        }

        return Predicates.or(compoundPredicateList);
    }

    private StarPredicate makeCompoundPredicateForMember(
        RolapSchema.PhysRouter router,
        RolapMember member,
        StarPredicate memberPredicate)
    {
        final RolapCubeLevel level = member.getLevel();
        final List<RolapSchema.PhysColumn> keyList =
            level.attribute.getKeyList();
        final List<Comparable> valueList = member.getKeyAsList();
        for (Pair<RolapSchema.PhysColumn, Comparable> pair
            : Pair.iterate(keyList, valueList))
        {
            final ValueColumnPredicate predicate =
                new ValueColumnPredicate(
                    new PredicateColumn(
                        router,
                        pair.left),
                    pair.right);
            if (memberPredicate == null) {
                memberPredicate = predicate;
            } else {
                memberPredicate = memberPredicate.and(predicate);
            }
        }
        return memberPredicate;
    }

   /* private StarPredicate makeCalculatedMemberPredicate(
        RolapMember member, RolapCube baseCube, Evaluator evaluator)
    {
        assert member.getExpression() instanceof ResolvedFunCall;

        ResolvedFunCall fun = (ResolvedFunCall) member.getExpression();

        final Exp exp = fun.getArg(0);
        final Type type = exp.getType();

        if (type instanceof SetType) {
            return makeSetPredicate(exp, evaluator);
        } else if (type.getArity() == 1) {
            return makeUnaryPredicate(member, baseCube, evaluator);
        } else {
            throw MondrianResource.instance()
                .UnsupportedCalculatedMember.ex(member.getName(), null);
        }
    }

    private StarPredicate makeUnaryPredicate(
        RolapMember member, RolapCube baseCube, Evaluator evaluator)
    {
      TupleConstraintStruct constraint = new TupleConstraintStruct();
      SqlConstraintUtils
          .expandSupportedCalculatedMember(member, evaluator, constraint);
      List<Member> expandedMemberList = constraint.getMembers();
      for (Member checkMember : expandedMemberList) {
          if (checkMember == null
              || checkMember.isCalculated()
              || !(checkMember instanceof RolapMember))
          {
              throw MondrianResource.instance()
                  .UnsupportedCalculatedMember.ex(member.getName(), null);
          }
      }
      List<StarPredicate> predicates =
          new ArrayList<StarPredicate>(expandedMemberList.size());
      for (Member iMember : expandedMemberList) {
          RolapMember iCubeMember = ((RolapMember)iMember);
          RolapCubeLevel iLevel = iCubeMember.getLevel();
          RolapStar.Column iColumn = iLevel.getBaseStarKeyColumn(baseCube);
          Object iKey = iCubeMember.getKey();
          StarPredicate iPredicate = new ValueColumnPredicate(iColumn, iKey);
          predicates.add(iPredicate);
      }
      StarPredicate r = null;
      if (predicates.size() == 1) {
          r = predicates.get(0);
      } else {
          r = new OrPredicate(predicates);
      }
      return r;
    }

    private StarPredicate makeSetPredicate(
        final Exp exp, Evaluator evaluator)
    {
      TupleIterable evaluatedSet =
          evaluator.getSetEvaluator(
              exp, true).evaluateTupleIterable();
      ArrayList<StarPredicate> orList = new ArrayList<StarPredicate>();
      OrPredicate orPredicate = null;
      for (List<Member> complexSetItem : evaluatedSet) {
          List<StarPredicate> andList = new ArrayList<StarPredicate>();
          for (Member singleSetItem : complexSetItem) {
              final List<List<Member>> singleItemList =
                  Collections.singletonList(
                      Collections.singletonList(singleSetItem));
              StarPredicate singlePredicate = predicateFromTupleList(
                  singleItemList,
                  measure, evaluator).getValue();
              andList.add(singlePredicate);
          }
          AndPredicate andPredicate = new AndPredicate(andList);
          orList.add(andPredicate);
          orPredicate  = new OrPredicate(orList);
      }
      return orPredicate;
    }*/
}

// End CompoundPredicateInfo.java
