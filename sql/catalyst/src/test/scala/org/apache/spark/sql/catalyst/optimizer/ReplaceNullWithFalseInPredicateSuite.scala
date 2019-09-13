/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, ArrayExists, ArrayFilter, ArrayTransform, CaseWhen, Expression, GreaterThan, If, LambdaFunction, Literal, MapFilter, NamedExpression, Or, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class ReplaceNullWithFalseInPredicateSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace null literals", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyConditionals,
        ReplaceNullWithFalseInPredicate) :: Nil
  }

  private val testRelation =
    LocalRelation('i.int, 'b.boolean, 'a.array(IntegerType), 'm.map(IntegerType, IntegerType))
  private val anotherTestRelation = LocalRelation('d.int)

  test("replace null inside filter and join conditions") {
    testFilter(originalCond = Literal(null, BooleanType), expectedCond = FalseLiteral)
    testJoin(originalCond = Literal(null, BooleanType), expectedCond = FalseLiteral)
  }

  test("Not expected type - replaceNullWithFalse") {
    val e = intercept[IllegalArgumentException] {
      testFilter(originalCond = Literal(null, IntegerType), expectedCond = FalseLiteral)
    }.getMessage
    assert(e.contains("but got the type `int` in `CAST(NULL AS INT)"))
  }

  test("replace null in branches of If") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      FalseLiteral,
      Literal(null, BooleanType))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace nulls in nested expressions in branches of If") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      TrueLiteral && Literal(null, BooleanType),
      UnresolvedAttribute("b") && Literal(null, BooleanType))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in elseValue of CaseWhen") {
    val branches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) -> TrueLiteral,
      (UnresolvedAttribute("i") > Literal(40)) -> FalseLiteral)
    val originalCond = CaseWhen(branches, Literal(null, BooleanType))
    val expectedCond = CaseWhen(branches, FalseLiteral)
    testFilter(originalCond, expectedCond)
    testJoin(originalCond, expectedCond)
  }

  test("replace null in branch values of CaseWhen") {
    val branches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) -> Literal(null, BooleanType),
      (UnresolvedAttribute("i") > Literal(40)) -> FalseLiteral)
    val originalCond = CaseWhen(branches, Literal(null))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in branches of If inside CaseWhen") {
    val originalBranches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) ->
        If(UnresolvedAttribute("i") < Literal(20), Literal(null, BooleanType), FalseLiteral),
      (UnresolvedAttribute("i") > Literal(40)) -> TrueLiteral)
    val originalCond = CaseWhen(originalBranches)

    val expectedBranches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) -> FalseLiteral,
      (UnresolvedAttribute("i") > Literal(40)) -> TrueLiteral)
    val expectedCond = CaseWhen(expectedBranches)

    testFilter(originalCond, expectedCond)
    testJoin(originalCond, expectedCond)
  }

  test("replace null in complex CaseWhen expressions") {
    val originalBranches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) -> TrueLiteral,
      (Literal(6) <= Literal(1)) -> FalseLiteral,
      (Literal(4) === Literal(5)) -> FalseLiteral,
      (UnresolvedAttribute("i") > Literal(10)) -> Literal(null, BooleanType),
      (Literal(4) === Literal(4)) -> TrueLiteral)
    val originalCond = CaseWhen(originalBranches)

    val expectedBranches = Seq(
      (UnresolvedAttribute("i") < Literal(10)) -> TrueLiteral,
      (UnresolvedAttribute("i") > Literal(10)) -> FalseLiteral,
      TrueLiteral -> TrueLiteral)
    val expectedCond = CaseWhen(expectedBranches)

    testFilter(originalCond, expectedCond)
    testJoin(originalCond, expectedCond)
  }

  test("replace null in Or") {
    val originalCond = Or(UnresolvedAttribute("b"), Literal(null))
    val expectedCond = UnresolvedAttribute("b")
    testFilter(originalCond, expectedCond)
    testJoin(originalCond, expectedCond)
  }

  test("replace null in And") {
    val originalCond = And(UnresolvedAttribute("b"), Literal(null))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace nulls in nested And/Or expressions") {
    val originalCond = And(
      And(UnresolvedAttribute("b"), Literal(null)),
      Or(Literal(null), And(Literal(null), And(UnresolvedAttribute("b"), Literal(null)))))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in And inside branches of If") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      FalseLiteral,
      And(UnresolvedAttribute("b"), Literal(null, BooleanType)))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in branches of If inside And") {
    val originalCond = And(
      UnresolvedAttribute("b"),
      If(
        UnresolvedAttribute("i") > Literal(10),
        Literal(null),
        And(FalseLiteral, UnresolvedAttribute("b"))))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in branches of If inside another If") {
    val originalCond = If(
      If(UnresolvedAttribute("b"), Literal(null), FalseLiteral),
      TrueLiteral,
      Literal(null))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in CaseWhen inside another CaseWhen") {
    val nestedCaseWhen = CaseWhen(Seq(UnresolvedAttribute("b") -> FalseLiteral), Literal(null))
    val originalCond = CaseWhen(Seq(nestedCaseWhen -> TrueLiteral), Literal(null))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("inability to replace null in non-boolean branches of If") {
    val condition = If(
      UnresolvedAttribute("i") > Literal(10),
      Literal(5) > If(
        UnresolvedAttribute("i") === Literal(15),
        Literal(null, IntegerType),
        Literal(3)),
      FalseLiteral)
    testFilter(originalCond = condition, expectedCond = condition)
    testJoin(originalCond = condition, expectedCond = condition)
  }

  test("inability to replace null in non-boolean values of CaseWhen") {
    val nestedCaseWhen = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(20)) -> Literal(2)),
      Literal(null, IntegerType))
    val branchValue = If(
      Literal(2) === nestedCaseWhen,
      TrueLiteral,
      FalseLiteral)
    val branches = Seq((UnresolvedAttribute("i") > Literal(10)) -> branchValue)
    val condition = CaseWhen(branches)
    testFilter(originalCond = condition, expectedCond = condition)
    testJoin(originalCond = condition, expectedCond = condition)
  }

  test("inability to replace null in non-boolean branches of If inside another If") {
    val condition = If(
      Literal(5) > If(
        UnresolvedAttribute("i") === Literal(15),
        Literal(null, IntegerType),
        Literal(3)),
      TrueLiteral,
      FalseLiteral)
    testFilter(originalCond = condition, expectedCond = condition)
    testJoin(originalCond = condition, expectedCond = condition)
  }

  test("replace null in If used as a join condition") {
    // this test is only for joins as the condition involves columns from different relations
    val originalCond = If(
      UnresolvedAttribute("d") > UnresolvedAttribute("i"),
      Literal(null),
      FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
  }

  test("replace null in CaseWhen used as a join condition") {
    // this test is only for joins as the condition involves columns from different relations
    val originalBranches = Seq(
      (UnresolvedAttribute("d") > UnresolvedAttribute("i")) -> Literal(null),
      (UnresolvedAttribute("d") === UnresolvedAttribute("i")) -> TrueLiteral)

    val expectedBranches = Seq(
      (UnresolvedAttribute("d") > UnresolvedAttribute("i")) -> FalseLiteral,
      (UnresolvedAttribute("d") === UnresolvedAttribute("i")) -> TrueLiteral)

    testJoin(
      originalCond = CaseWhen(originalBranches, FalseLiteral),
      expectedCond = CaseWhen(expectedBranches, FalseLiteral))
  }

  test("inability to replace null in CaseWhen inside EqualTo used as a join condition") {
    // this test is only for joins as the condition involves columns from different relations
    val branches = Seq(
      (UnresolvedAttribute("d") > UnresolvedAttribute("i")) -> Literal(null, BooleanType),
      (UnresolvedAttribute("d") === UnresolvedAttribute("i")) -> TrueLiteral)
    val condition = UnresolvedAttribute("b") === CaseWhen(branches, FalseLiteral)
    testJoin(originalCond = condition, expectedCond = condition)
  }

  test("replace null in predicates of If") {
    val predicate = And(GreaterThan(UnresolvedAttribute("i"), Literal(0.5)), Literal(null))
    testProjection(
      originalExpr = If(predicate, Literal(5), Literal(1)).as("out"),
      expectedExpr = Literal(1).as("out"))
  }

  test("replace null in predicates of If inside another If") {
    val predicate = If(
      And(GreaterThan(UnresolvedAttribute("i"), Literal(0.5)), Literal(null)),
      TrueLiteral,
      FalseLiteral)
    testProjection(
      originalExpr = If(predicate, Literal(5), Literal(1)).as("out"),
      expectedExpr = Literal(1).as("out"))
  }

  test("inability to replace null in non-boolean expressions inside If predicates") {
    val predicate = GreaterThan(
      UnresolvedAttribute("i"),
      If(UnresolvedAttribute("b"), Literal(null, IntegerType), Literal(4)))
    val column = If(predicate, Literal(5), Literal(1)).as("out")
    testProjection(originalExpr = column, expectedExpr = column)
  }

  test("replace null in conditions of CaseWhen") {
    val branches = Seq(
      And(GreaterThan(UnresolvedAttribute("i"), Literal(0.5)), Literal(null)) -> Literal(5))
    testProjection(
      originalExpr = CaseWhen(branches, Literal(2)).as("out"),
      expectedExpr = Literal(2).as("out"))
  }

  test("replace null in conditions of CaseWhen inside another CaseWhen") {
    val nestedCaseWhen = CaseWhen(
      Seq(And(UnresolvedAttribute("b"), Literal(null)) -> Literal(5)),
      Literal(2))
    val branches = Seq(GreaterThan(Literal(3), nestedCaseWhen) -> Literal(1))
    testProjection(
      originalExpr = CaseWhen(branches).as("out"),
      expectedExpr = Literal(1).as("out"))
  }

  test("inability to replace null in non-boolean exprs inside CaseWhen conditions") {
    val condition = GreaterThan(
      UnresolvedAttribute("i"),
      If(UnresolvedAttribute("b"), Literal(null, IntegerType), Literal(4)))
    val column = CaseWhen(Seq(condition -> Literal(5)), Literal(2)).as("out")
    testProjection(originalExpr = column, expectedExpr = column)
  }

  private def lv(s: Symbol) = UnresolvedNamedLambdaVariable(Seq(s.name))

  test("replace nulls in lambda function of ArrayFilter") {
    testHigherOrderFunc('a, ArrayFilter, Seq(lv('e)))
  }

  test("replace nulls in lambda function of ArrayExists") {
    testHigherOrderFunc('a, ArrayExists, Seq(lv('e)))
  }

  test("replace nulls in lambda function of MapFilter") {
    testHigherOrderFunc('m, MapFilter, Seq(lv('k), lv('v)))
  }

  test("inability to replace nulls in arbitrary higher-order function") {
    val lambdaFunc = LambdaFunction(
      function = If(lv('e) > 0, Literal(null, BooleanType), TrueLiteral),
      arguments = Seq[NamedExpression](lv('e)))
    val column = ArrayTransform('a, lambdaFunc)
    testProjection(originalExpr = column, expectedExpr = column)
  }

  private def testFilter(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, exp) => rel.where(exp), originalCond, expectedCond)
  }

  private def testJoin(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, exp) => rel.join(anotherTestRelation, Inner, Some(exp)), originalCond, expectedCond)
  }

  private def testProjection(originalExpr: Expression, expectedExpr: Expression): Unit = {
    test((rel, exp) => rel.select(exp), originalExpr, expectedExpr)
  }

  private def testHigherOrderFunc(
      argument: Expression,
      createExpr: (Expression, Expression) => Expression,
      lambdaArgs: Seq[NamedExpression]): Unit = {
    val condArg = lambdaArgs.last
    // the lambda body is: if(arg > 0, null, true)
    val cond = GreaterThan(condArg, Literal(0))
    val lambda1 = LambdaFunction(
      function = If(cond, Literal(null, BooleanType), TrueLiteral),
      arguments = lambdaArgs)
    // the optimized lambda body is: if(arg > 0, false, true)
    val lambda2 = LambdaFunction(
      function = If(cond, FalseLiteral, TrueLiteral),
      arguments = lambdaArgs)
    testProjection(
      originalExpr = createExpr(argument, lambda1) as 'x,
      expectedExpr = createExpr(argument, lambda2) as 'x)
  }

  private def test(
      func: (LogicalPlan, Expression) => LogicalPlan,
      originalExpr: Expression,
      expectedExpr: Expression): Unit = {

    val originalPlan = func(testRelation, originalExpr).analyze
    val optimizedPlan = Optimize.execute(originalPlan)
    val expectedPlan = func(testRelation, expectedExpr).analyze
    comparePlans(optimizedPlan, expectedPlan)
  }
}
