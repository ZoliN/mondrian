/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.calc.impl.ListTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>Except</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class ExceptFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Except",
            "Except(<Set1>, <Set2>[, ALL])",
            "Finds the difference between two sets, optionally retaining duplicates.",
            new String[]{"fxxx", "fxxxy"},
            ExceptFunDef.class);

    static final MinusExceptResolver MinusResolver =
            new MinusExceptResolver();
    static final MinusExceptResolver2 MinusResolver2 =
            new MinusExceptResolver2();
    
    public ExceptFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // todo: implement ALL
        final ListCalc listCalc0 = call.getArgCount()>1?compiler.compileList(call.getArg(0)):null;
        final ListCalc listCalc1 = call.getArgCount()>1?compiler.compileList(call.getArg(1)):compiler.compileList(call.getArg(0));
        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList list1 = listCalc1.evaluateList(evaluator);
                TupleList list0;
            	if (listCalc0!=null) {
	            	list0 = listCalc0.evaluateList(evaluator);
	                if (list0.isEmpty()) {
	                    return list0;
	                }
            	} else {
            		Level level=list1.get(0).get(0).getLevel();
            		List<Member> allmembers=evaluator.getSchemaReader().getLevelMembers(level, null);
            		list0=new ListTupleList(1,allmembers);
            	}

                if (list1.isEmpty()) {
                    return list0;
                }
                final Set<List<Member>> set1 = new HashSet<List<Member>>(list1);
                final TupleList result =
                    new ArrayTupleList(list0.getArity(), list0.size());
                for (List<Member> tuple1 : list0) {
                    if (!set1.contains(tuple1)) {
                        result.add(tuple1);
                    }
                }
                return result;
            }
        };
    }
    
    private static class MinusExceptResolver extends MultiResolver {
        public MinusExceptResolver() {
            super(
                "-",
                "All members - <Set1>",
                "Complementer of Set1.",
                new String[]{"Pxx"});
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // This function only applies in contexts which require a set.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversions);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new ExceptFunDef(dummyFunDef);
        }
    }
    
    private static class MinusExceptResolver2 extends MultiResolver {
        public MinusExceptResolver2() {
            super(
                "-",
                "<Set1> - <Set2>",
                "Set1 minus set2.",
                new String[]{"ixxx"});
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // This function only applies in contexts which require a set.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversions);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new ExceptFunDef(dummyFunDef);
        }
    }

}

// End ExceptFunDef.java
