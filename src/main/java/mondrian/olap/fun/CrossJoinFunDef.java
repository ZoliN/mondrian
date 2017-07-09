/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapEvaluator;
import mondrian.util.CartesianProductList;

import java.util.*;

/**
 * Definition of the <code>CrossJoin</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class CrossJoinFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Crossjoin",
            "Crossjoin(<Set1>, <Set2>)",
            "Returns the cross product of two sets.",
            new String[]{"fxxx"},
            CrossJoinFunDef.class);

    static final StarCrossJoinResolver StarResolver =
        new StarCrossJoinResolver();
    
    static final ParanthesisResolver ParanthesisResolver =
        new ParanthesisResolver();

    

    // used to tell the difference between crossjoin expressions.
    static protected final int ctag = FunUtil.counterTag++;

    public CrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
        List<MemberType> list = new ArrayList<MemberType>();
        for (Exp arg : args) {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                addTypes(type, list);
            } else if (getName().equals("*")) {
                // The "*" form of CrossJoin is lenient: args can be either
                // members/tuples or sets.
                addTypes(type, list);
            } else {
                throw Util.newInternal("arg to crossjoin must be a set");
            }
        }
        final MemberType[] types = list.toArray(new MemberType[list.size()]);
        TupleType.checkHierarchies(types);
        final TupleType tupleType = new TupleType(types);
        return new SetType(tupleType);
    }

    /**
     * Adds a type to a list of types. If type is a {@link TupleType}, does so
     * recursively.
     *
     * @param type Type to add to list
     * @param list List of types to add to
     */
    private static void addTypes(final Type type, List<MemberType> list) {
        if (type instanceof SetType) {
            SetType setType = (SetType) type;
            addTypes(setType.getElementType(), list);
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            for (Type elementType : tupleType.elementTypes) {
                addTypes(elementType, list);
            }
        } else if (type instanceof MemberType) {
            list.add((MemberType) type);
        } else {
            throw Util.newInternal("Unexpected type: " + type);
        }
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // What is the desired return type?
        for (ResultStyle r : compiler.getAcceptableResultStyles()) {
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY
                    return compileCallIterable(call, compiler);
            case LIST:
                // Consumer wants (immutable) LIST
                if (call.getArgCount()>2) throw Util.newInternal("Unimplemented cross join feature: list style result with more than 2 arguments.");
                return compileCallImmutableList(call, compiler);
            case MUTABLE_LIST:
                // Consumer MUTABLE_LIST
                if (call.getArgCount()>2) throw Util.newInternal("Unimplemented cross join feature: list style result with more than 2 arguments.");
                return compileCallMutableList(call, compiler);
            }
        }
        throw ResultStyleException.generate(
            ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
            compiler.getAcceptableResultStyles());
    }

    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    // Iterable
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        Calc[] calcs = new Calc[call.getArgCount()];
        for (int i = 0; i < calcs.length; i++) {
            calcs[i]=toIter(compiler, call.getArg(i));
            // Check returned calc ResultStyles
            checkIterListResultStyles(calcs[i]);
        }
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: ITERABLE, LIST or MUTABLE_LIST, but
        // LIST and MUTABLE_LIST are treated the same; so
        // there are 16 possible combinations - sweet.

        return new CrossJoinIterCalc(call, calcs);
    }

    private Calc toIter(ExpCompiler compiler, final Exp exp) {
        // Want iterable, immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            // this can return an IterCalc or ListCalc
            return compiler.compileAs(
                exp,
                null,
                ResultStyle.ITERABLE_LIST_MUTABLELIST);
        } else {
            // this always returns an IterCalc
            return new SetFunDef.ExprIterCalc(
                new DummyExp(new SetType(type)),
                new Exp[] {exp},
                compiler,
                ResultStyle.ITERABLE_LIST_MUTABLELIST);
        }
    }

    class CrossJoinIterCalc extends AbstractIterCalc
    {
        CrossJoinIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (TupleIterable)
                    nativeEvaluator.execute(ResultStyle.ITERABLE);
            }

            Calc[] calcs = getCalcs();
            TupleIterable[] o= new TupleIterable[calcs.length];
            TupleList[] l= new TupleList[calcs.length];
            
            for (int i = 0; i < calcs.length; i++) {
                o[i] = ((IterCalc)calcs[i]).evaluateIterable(evaluator);
                if (o[i] instanceof TupleList) {
                    l[i] = (TupleList) o[i];
                    l[i] = nonEmptyOptimizeList(evaluator, l[i], call, ctag);
                    if (l[i].isEmpty()) {
                        return TupleCollections.emptyList(getType().getArity());
                    }
                    o[i] = l[i];
                }
            }

            return calcs.length==2?makeIterable(o[0],o[1]):makeIterable(o);
        }

        protected TupleIterable makeIterable(
            final TupleIterable it1,
            final TupleIterable it2)
        {
            // There is no knowledge about how large either it1 ore it2
            // are or how many null members they might have, so all
            // one can do is iterate across them:
            // iterate across it1 and for each member iterate across it2

            return new AbstractTupleIterable(it1.getArity() + it2.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(getArity()) {
                        final TupleCursor i1 = it1.tupleCursor();
                        final int arity1 = i1.getArity();
                        TupleCursor i2 =
                            TupleCollections.emptyList(1).tupleCursor();
                        final Member[] members = new Member[arity];

                        public boolean forward() {
                            if (i2.forward()) {
                                return true;
                            }
                            while (i1.forward()) {
                                i2 = it2.tupleCursor();
                                if (i2.forward()) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            i1.currentToArray(members, 0);
                            i2.currentToArray(members, arity1);
                            return Util.flatList(members);
                        }

                        @Override
                        public Member member(int column) {
                            if (column < arity1) {
                                return i1.member(column);
                            } else {
                                return i2.member(column - arity1);
                            }
                        }

                        @Override
                        public void setContext(Evaluator evaluator) {
                            i1.setContext(evaluator);
                            i2.setContext(evaluator);
                        }

                        @Override
                        public void currentToArray(
                            Member[] members,
                            int offset)
                        {
                            i1.currentToArray(members, offset);
                            i2.currentToArray(members, offset + arity1);
                        }
                    };
                }
            };
        }
    

        protected TupleIterable makeIterable(
            final TupleIterable[] it)
        {
            // There is no knowledge about how large either it1 ore it2
            // are or how many null members they might have, so all
            // one can do is iterate across them:
            // iterate across it1 and for each member iterate across it2
            int totalArity=0;
            for (int j = 0; j < it.length; j++) {
                totalArity += it[j].getArity();
            }

            return new AbstractTupleIterable(totalArity) {
                private TupleCursor[] tcs;
                public TupleCursor tupleCursor() {
                    tcs = new TupleCursor[it.length];
                    for (int i = 0; i < it.length-1; i++) {
                        tcs[i] = it[i].tupleCursor();
                        tcs[i].forward();
                    }
                    tcs[it.length-1] = it[it.length-1].tupleCursor();
                    return new AbstractTupleCursor(getArity()) {
                        final Member[] members = new Member[arity];
                        final int lasttc = tcs.length-1;
                        
                        public boolean forward() {
                            int c=lasttc;
                            while(true) {
                                if (tcs[c].forward())
                                    return true;
                                if (c==0) 
                                    return false;
                                tcs[c]=it[c].tupleCursor();
                                tcs[c].forward();
                                c--;
                            }
                        }

                        public List<Member> current() {
                            int cumulArity=0;
                            for (int i = 0; i < tcs.length; i++) {
                                tcs[i].currentToArray(members,cumulArity);
                                cumulArity += it[i].getArity();
                            }
                            return Util.flatList(members);
                        }

                        @Override
                        public Member member(int column) {
                            int cumulArity=0;
                            int i = 0;
                            while ((cumulArity + it[i].getArity())<=column)  {
                                cumulArity += it[i].getArity();
                                i++;
                            }
                            return tcs[i].member(column-cumulArity);

                        }

                        @Override
                        public void setContext(Evaluator evaluator) {
                            for (int i = 0; i < tcs.length; i++) {
                                tcs[i].setContext(evaluator);
                            }

                        }

                        @Override
                        public void currentToArray(
                            Member[] members,
                            int offset)
                        {
                            int cumulArity=0;
                            for (int i = 0; i < tcs.length; i++) {
                                tcs[i].currentToArray(members, offset + cumulArity);
                                cumulArity += it[i].getArity();
                            }
                        }
                    };
                }
            };
        }
    }

    
    ///////////////////////////////////////////////////////////////////////////
    // Immutable List
    ///////////////////////////////////////////////////////////////////////////

    protected ListCalc compileCallImmutableList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));
        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an immutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        return new ImmutableListCalc(call, calcs);
    }

    /**
     * Compiles an expression to list (or mutable list) format. Never returns
     * null.
     *
     * @param compiler Compiler
     * @param exp Expression
     * @return Compiled expression that yields a list or mutable list
     */
    private ListCalc toList(ExpCompiler compiler, final Exp exp) {
        // Want immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            final Calc calc = compiler.compileAs(
                exp, null, ResultStyle.LIST_MUTABLELIST);
            if (calc == null) {
                return compiler.compileList(exp, false);
            }
            return (ListCalc) calc;
        } else {
            return new SetFunDef.SetListCalc(
                new DummyExp(new SetType(type)),
                new Exp[] {exp},
                compiler,
                ResultStyle.LIST_MUTABLELIST);
        }
    }

    abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(
            ResolvedFunCall call,
            Calc[] calcs,
            boolean mutable)
        {
            super(call, calcs, mutable);
        }

        public TupleList evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (TupleList) nativeEvaluator.execute(ResultStyle.LIST);
            }

            Calc[] calcs = getCalcs();
            ListCalc listCalc1 = (ListCalc) calcs[0];
            ListCalc listCalc2 = (ListCalc) calcs[1];

            TupleList l1 = listCalc1.evaluateList(evaluator);
            TupleList l2 = listCalc2.evaluateList(evaluator);

            l1 = nonEmptyOptimizeList(evaluator, l1, call, ctag);
            if (l1.isEmpty()) {
                return TupleCollections.emptyList(
                    l1.getArity() + l2.getArity());
            }
            l2 = nonEmptyOptimizeList(evaluator, l2, call, ctag);
            if (l2.isEmpty()) {
                return TupleCollections.emptyList(
                    l1.getArity() + l2.getArity());
            }

            return makeList(l1, l2);
        }

        protected abstract TupleList makeList(TupleList l1, TupleList l2);
    }

    class ImmutableListCalc
        extends BaseListCalc
    {
        ImmutableListCalc(
            ResolvedFunCall call, Calc[] calcs)
        {
            super(call, calcs, false);
        }

        protected TupleList makeList(final TupleList l1, final TupleList l2) {
            final int arity = l1.getArity() + l2.getArity();
            return new DelegatingTupleList(
                arity,
                new AbstractList<List<Member>>() {
                    final List<List<List<Member>>> lists =
                        Arrays.<List<List<Member>>>asList(
                            l1, l2);
                    final Member[] members = new Member[arity];

                    final CartesianProductList cartesianProductList =
                        new CartesianProductList<List<Member>>(
                            lists);

                    @Override
                    public List<Member> get(int index) {
                        cartesianProductList.getIntoArray(index, members);
                        return Util.flatList(members);
                    }

                    @Override
                    public int size() {
                        return cartesianProductList.size();
                    }
                });
        }
    }

    protected ListCalc compileCallMutableList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));

        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an mutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same,
        // regardless of type, one must materialize the result list; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        return new MutableListCalc(call, calcs);
    }

    class MutableListCalc extends BaseListCalc
    {
        MutableListCalc(ResolvedFunCall call, Calc[] calcs)
        {
            super(call, calcs, true);
        }

        @SuppressWarnings({"unchecked"})
        protected TupleList makeList(final TupleList l1, final TupleList l2) {
            final int arity = l1.getArity() + l2.getArity();
            final List<Member> members =
                new ArrayList<Member>(arity * l1.size() * l2.size());
            for (List<Member> ma1 : l1) {
                for (List<Member> ma2 : l2) {
                    members.addAll(ma1);
                    members.addAll(ma2);
                }
            }
            return new ListTupleList(arity, members);
        }
    }

    
    public static TupleList mutableCrossJoin(
        TupleList list1,
        TupleList list2)
    {
        return mutableCrossJoin(Arrays.asList(list1, list2));
    }

    public static TupleList mutableCrossJoinXL(
            TupleList list1,
            TupleList list2)
    {
        return mutableCrossJoinXL(Arrays.asList(list1, list2));
    }
    
    
    public static TupleList mutableCrossJoin(
        List<TupleList> lists)
    {
        long size = 1;
        int arity = 0;
        for (TupleList list : lists) {
            size *= (long) list.size();
            arity += list.getArity();
        }
        if (size == 0L) {
            return TupleCollections.emptyList(arity);
        }

        // Optimize nonempty(crossjoin(a,b)) ==
        //  nonempty(crossjoin(nonempty(a),nonempty(b))

        // FIXME: If we're going to apply a NON EMPTY constraint later, it's
        // possible that the ultimate result will be much smaller.

        Util.checkCJResultLimit(size);

        // Now we can safely cast size to an integer. It still might be very
        // large - which means we're allocating a huge array which we might
        // pare down later by applying NON EMPTY constraints - which is a
        // concern.
        List<Member> result = new ArrayList<Member>((int) size * arity);

        final Member[] partialArray = new Member[arity];
        final List<Member> partial = Arrays.asList(partialArray);
        cartesianProductRecurse(0, lists, partial, partialArray, 0, result);
        return new ListTupleList(arity, result);
    }

    public static TupleList mutableCrossJoinXL(
            List<TupleList> lists)
        {
            long size = 1;
            int arity = 0;
            for (TupleList list : lists) {
                size *= (long) list.size();
                arity += list.getArity();
            }
            if (size == 0L) {
                return TupleCollections.emptyList(arity);
            }

            // Optimize nonempty(crossjoin(a,b)) ==
            //  nonempty(crossjoin(nonempty(a),nonempty(b))

            // FIXME: If we're going to apply a NON EMPTY constraint later, it's
            // possible that the ultimate result will be much smaller.

            Util.checkCJResultLimit(size);

            // Now we can safely cast size to an integer. It still might be very
            // large - which means we're allocating a huge array which we might
            // pare down later by applying NON EMPTY constraints - which is a
            // concern.
            List<Member> result = new ArrayList<Member>((int) size * arity);

            final Member[] partialArray = new Member[arity];
            final List<Member> partial = Arrays.asList(partialArray);
            cartesianProductRecurse(0, lists, partial, partialArray, 0, result);
            
            List<Member> resultFiltered = new ArrayList<Member>((int) size * arity);
            for (int i = 0; i < result.size(); i+=arity) {
                boolean gotAll1 = false;
                for (int j = i; j < i+lists.get(0).getArity(); j++) {
                    if (result.get(j).isAll()) {
                        gotAll1 = true;
                        break;
                    }
                }
                if (gotAll1) {
                    boolean gotNonAll2 = false;
                    for (int j = i+lists.get(0).getArity(); j < i+arity; j++) {
                        if (!result.get(j).isAll()) {
                            gotNonAll2 = true;
                            break;
                        }
                    }
                    if (gotNonAll2) continue;
                    
                }
                for (int j = i; j < i+arity; j++) {
                    resultFiltered.add(result.get(j));
                }
            }
            
            return new ListTupleList(arity, resultFiltered);
        }
    
    private static void cartesianProductRecurse(
        int i,
        List<TupleList> lists,
        List<Member> partial,
        Member[] partialArray,
        int partialSize,
        List<Member> result)
    {
        final TupleList tupleList = lists.get(i);
        final int partialSizeNext = partialSize + tupleList.getArity();
        final int iNext = i + 1;
        final TupleCursor cursor = tupleList.tupleCursor();
        while (cursor.forward()) {
            cursor.currentToArray(partialArray, partialSize);
            if (i == lists.size() - 1) {
                result.addAll(partial);
            } else {
                cartesianProductRecurse(
                    iNext, lists, partial, partialArray, partialSizeNext,
                    result);
            }
        }
    }

 
  

    private static class StarCrossJoinResolver extends MultiResolver {
        public StarCrossJoinResolver() {
            super(
                "*",
                "<Set1> * <Set2>",
                "Returns the cross product of two sets.",
                new String[]{"ixxx", "ixmx", "ixxm", "ixmm"});
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // This function only applies in contexts which require a set.
            // Elsewhere, "*" is the multiplication operator.
            // This means that [Measures].[Unit Sales] * [Gender].[M] is
            // well-defined.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversions);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new CrossJoinFunDef(dummyFunDef);
        }
    }
    
    private static class ParanthesisResolver extends MultiResolver {
        public ParanthesisResolver() {
            super(
                "()",
                "(<Set1> , <Set2> , ...)",
                "Returns the cross product of two or more sets.",
                new String[]{"rxxx","rxxxx","rxxxxx","rxxxxxx","rxxxxxxx","rxxxxxxxx","rxxxxxxxxx","rxxxxxxxxxx","rxxxxxxxxxxx"});
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // This function only applies in contexts which require a set.
            // Elsewhere, "*" is the multiplication operator.
            // This means that [Measures].[Unit Sales] * [Gender].[M] is
            // well-defined.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversions);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new CrossJoinFunDef(dummyFunDef);
        }
    }
}

// End CrossJoinFunDef.java
