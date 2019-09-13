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

package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.{InternalRow, JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, GetColumnByOrdinal, SimpleAnalyzer, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, InitializeJavaBean, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.optimizer.SimplifyCasts
import org.apache.spark.sql.catalyst.plans.logical.{CatalystSerde, DeserializeToObject, LocalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ObjectType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * A factory for constructing encoders that convert objects and primitives to and from the
 * internal row format using catalyst expressions and code generation.  By default, the
 * expressions used to retrieve values from an input row when producing an object will be created as
 * follows:
 *  - Classes will have their sub fields extracted by name using [[UnresolvedAttribute]] expressions
 *    and [[UnresolvedExtractValue]] expressions.
 *  - Tuples will have their subfields extracted by position using [[BoundReference]] expressions.
 *  - Primitives will have their values extracted from the first ordinal with a schema that defaults
 *    to the name `value`.
 */
object ExpressionEncoder {

  def apply[T : TypeTag](): ExpressionEncoder[T] = {
    val mirror = ScalaReflection.mirror
    val tpe = typeTag[T].in(mirror).tpe

    val cls = mirror.runtimeClass(tpe)
    val serializer = ScalaReflection.serializerForType(tpe)
    val deserializer = ScalaReflection.deserializerForType(tpe)

    new ExpressionEncoder[T](
      serializer,
      deserializer,
      ClassTag[T](cls))
  }

  // TODO: improve error message for java bean encoder.
  def javaBean[T](beanClass: Class[T]): ExpressionEncoder[T] = {
    val schema = JavaTypeInference.inferDataType(beanClass)._1
    assert(schema.isInstanceOf[StructType])

    val objSerializer = JavaTypeInference.serializerFor(beanClass)
    val objDeserializer = JavaTypeInference.deserializerFor(beanClass)

    new ExpressionEncoder[T](
      objSerializer,
      objDeserializer,
      ClassTag[T](beanClass))
  }

  /**
   * Given a set of N encoders, constructs a new encoder that produce objects as items in an
   * N-tuple.  Note that these encoders should be unresolved so that information about
   * name/positional binding is preserved.
   */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    // TODO: check if encoders length is more than 22 and throw exception for it.
    encoders.foreach(_.assertUnresolved())

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val newSerializerInput = BoundReference(0, ObjectType(cls), nullable = true)
    val serializers = encoders.zipWithIndex.map { case (enc, index) =>
      val boundRefs = enc.objSerializer.collect { case b: BoundReference => b }.distinct
      assert(boundRefs.size == 1, "object serializer should have only one bound reference but " +
        s"there are ${boundRefs.size}")

      val originalInputObject = boundRefs.head
      val newInputObject = Invoke(
        newSerializerInput,
        s"_${index + 1}",
        originalInputObject.dataType,
        returnNullable = originalInputObject.nullable)

      val newSerializer = enc.objSerializer.transformUp {
        case BoundReference(0, _, _) => newInputObject
      }

      Alias(newSerializer, s"_${index + 1}")()
    }
    val newSerializer = CreateStruct(serializers)

    val newDeserializerInput = GetColumnByOrdinal(0, newSerializer.dataType)
    val deserializers = encoders.zipWithIndex.map { case (enc, index) =>
      val getColExprs = enc.objDeserializer.collect { case c: GetColumnByOrdinal => c }.distinct
      assert(getColExprs.size == 1, "object deserializer should have only one " +
        s"`GetColumnByOrdinal`, but there are ${getColExprs.size}")

      val input = GetStructField(newDeserializerInput, index)
      enc.objDeserializer.transformUp {
        case GetColumnByOrdinal(0, _) => input
      }
    }
    val newDeserializer = NewInstance(cls, deserializers, ObjectType(cls), propagateNull = false)

    def nullSafe(input: Expression, result: Expression): Expression = {
      If(IsNull(input), Literal.create(null, result.dataType), result)
    }

    new ExpressionEncoder[Any](
      nullSafe(newSerializerInput, newSerializer),
      nullSafe(newDeserializerInput, newDeserializer),
      ClassTag(cls))
  }

  // Tuple1
  def tuple[T](e: ExpressionEncoder[T]): ExpressionEncoder[Tuple1[T]] =
    tuple(Seq(e)).asInstanceOf[ExpressionEncoder[Tuple1[T]]]

  def tuple[T1, T2](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2]): ExpressionEncoder[(T1, T2)] =
    tuple(Seq(e1, e2)).asInstanceOf[ExpressionEncoder[(T1, T2)]]

  def tuple[T1, T2, T3](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3]): ExpressionEncoder[(T1, T2, T3)] =
    tuple(Seq(e1, e2, e3)).asInstanceOf[ExpressionEncoder[(T1, T2, T3)]]

  def tuple[T1, T2, T3, T4](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4]): ExpressionEncoder[(T1, T2, T3, T4)] =
    tuple(Seq(e1, e2, e3, e4)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4)]]

  def tuple[T1, T2, T3, T4, T5](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4],
      e5: ExpressionEncoder[T5]): ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    tuple(Seq(e1, e2, e3, e4, e5)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4, T5)]]
}

/**
 * A generic encoder for JVM objects that uses Catalyst Expressions for a `serializer`
 * and a `deserializer`.
 *
 * @param objSerializer An expression that can be used to encode a raw object to corresponding
 *                   Spark SQL representation that can be a primitive column, array, map or a
 *                   struct. This represents how Spark SQL generally serializes an object of
 *                   type `T`.
 * @param objDeserializer An expression that will construct an object given a Spark SQL
 *                        representation. This represents how Spark SQL generally deserializes
 *                        a serialized value in Spark SQL representation back to an object of
 *                        type `T`.
 * @param clsTag A classtag for `T`.
 */
case class ExpressionEncoder[T](
    objSerializer: Expression,
    objDeserializer: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  /**
   * A sequence of expressions, one for each top-level field that can be used to
   * extract the values from a raw object into an [[InternalRow]]:
   * 1. If `serializer` encodes a raw object to a struct, strip the outer If-IsNull and get
   *    the `CreateNamedStruct`.
   * 2. For other cases, wrap the single serializer with `CreateNamedStruct`.
   */
  val serializer: Seq[NamedExpression] = {
    val clsName = Utils.getSimpleName(clsTag.runtimeClass)

    if (isSerializedAsStructForTopLevel) {
      val nullSafeSerializer = objSerializer.transformUp {
        case r: BoundReference =>
          // For input object of Product type, we can't encode it to row if it's null, as Spark SQL
          // doesn't allow top-level row to be null, only its columns can be null.
          AssertNotNull(r, Seq("top level Product or row object"))
      }
      nullSafeSerializer match {
        case If(_: IsNull, _, s: CreateNamedStruct) => s
        case _ =>
          throw new RuntimeException(s"class $clsName has unexpected serializer: $objSerializer")
      }
    } else {
      // For other input objects like primitive, array, map, etc., we construct a struct to wrap
      // the serializer which is a column of an row.
      //
      // Note: Because Spark SQL doesn't allow top-level row to be null, to encode
      // top-level Option[Product] type, we make it as a top-level struct column.
      CreateNamedStruct(Literal("value") :: objSerializer :: Nil)
    }
  }.flatten

  /**
   * Returns an expression that can be used to deserialize an input row to an object of type `T`
   * with a compatible schema. Fields of the row will be extracted using `UnresolvedAttribute`.
   * of the same name as the constructor arguments.
   *
   * For complex objects that are encoded to structs, Fields of the struct will be extracted using
   * `GetColumnByOrdinal` with corresponding ordinal.
   */
  val deserializer: Expression = {
    if (isSerializedAsStructForTopLevel) {
      // We serialized this kind of objects to root-level row. The input of general deserializer
      // is a `GetColumnByOrdinal(0)` expression to extract first column of a row. We need to
      // transform attributes accessors.
      objDeserializer.transform {
        case UnresolvedExtractValue(GetColumnByOrdinal(0, _),
            Literal(part: UTF8String, StringType)) =>
          UnresolvedAttribute.quoted(part.toString)
        case GetStructField(GetColumnByOrdinal(0, dt), ordinal, _) =>
          GetColumnByOrdinal(ordinal, dt)
        case If(IsNull(GetColumnByOrdinal(0, _)), _, n: NewInstance) => n
        case If(IsNull(GetColumnByOrdinal(0, _)), _, i: InitializeJavaBean) => i
      }
    } else {
      // For other input objects like primitive, array, map, etc., we deserialize the first column
      // of a row to the object.
      objDeserializer
    }
  }

  // The schema after converting `T` to a Spark SQL row. This schema is dependent on the given
  // serialier.
  val schema: StructType = StructType(serializer.map { s =>
    StructField(s.name, s.dataType, s.nullable)
  })

  /**
   * Returns true if the type `T` is serialized as a struct by `objSerializer`.
   */
  def isSerializedAsStruct: Boolean = objSerializer.dataType.isInstanceOf[StructType]

  /**
   * If the type `T` is serialized as a struct, when it is encoded to a Spark SQL row, fields in
   * the struct are naturally mapped to top-level columns in a row. In other words, the serialized
   * struct is flattened to row. But in case of the `T` is also an `Option` type, it can't be
   * flattened to top-level row, because in Spark SQL top-level row can't be null. This method
   * returns true if `T` is serialized as struct and is not `Option` type.
   */
  def isSerializedAsStructForTopLevel: Boolean = {
    isSerializedAsStruct && !classOf[Option[_]].isAssignableFrom(clsTag.runtimeClass)
  }

  // serializer expressions are used to encode an object to a row, while the object is usually an
  // intermediate value produced inside an operator, not from the output of the child operator. This
  // is quite different from normal expressions, and `AttributeReference` doesn't work here
  // (intermediate value is not an attribute). We assume that all serializer expressions use the
  // same `BoundReference` to refer to the object, and throw exception if they don't.
  assert(serializer.forall(_.references.isEmpty), "serializer cannot reference any attributes.")
  assert(serializer.flatMap { ser =>
    val boundRefs = ser.collect { case b: BoundReference => b }
    assert(boundRefs.nonEmpty,
      "each serializer expression should contain at least one `BoundReference`")
    boundRefs
  }.distinct.length <= 1, "all serializer expressions must use the same BoundReference.")

  /**
   * Returns a new copy of this encoder, where the `deserializer` is resolved and bound to the
   * given schema.
   *
   * Note that, ideally encoder is used as a container of serde expressions, the resolution and
   * binding stuff should happen inside query framework.  However, in some cases we need to
   * use encoder as a function to do serialization directly(e.g. Dataset.collect), then we can use
   * this method to do resolution and binding outside of query framework.
   */
  def resolveAndBind(
      attrs: Seq[Attribute] = schema.toAttributes,
      analyzer: Analyzer = SimpleAnalyzer): ExpressionEncoder[T] = {
    val dummyPlan = CatalystSerde.deserialize(LocalRelation(attrs))(this)
    val analyzedPlan = analyzer.execute(dummyPlan)
    analyzer.checkAnalysis(analyzedPlan)
    val resolved = SimplifyCasts(analyzedPlan).asInstanceOf[DeserializeToObject].deserializer
    val bound = BindReferences.bindReference(resolved, attrs)
    copy(objDeserializer = bound)
  }

  @transient
  private lazy val extractProjection = GenerateUnsafeProjection.generate(serializer)

  @transient
  private lazy val inputRow = new GenericInternalRow(1)

  @transient
  private lazy val constructProjection = SafeProjection.create(deserializer :: Nil)

  /**
   * Returns a new set (with unique ids) of [[NamedExpression]] that represent the serialized form
   * of this object.
   */
  def namedExpressions: Seq[NamedExpression] = schema.map(_.name).zip(serializer).map {
    case (_, ne: NamedExpression) => ne.newInstance()
    case (name, e) => Alias(e, name)()
  }

  /**
   * Returns an encoded version of `t` as a Spark SQL row.  Note that multiple calls to
   * toRow are allowed to return the same actual [[InternalRow]] object.  Thus, the caller should
   * copy the result before making another call if required.
   */
  def toRow(t: T): InternalRow = try {
    inputRow(0) = t
    extractProjection(inputRow)
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Error while encoding: $e\n" +
          s"${serializer.map(_.simpleString(SQLConf.get.maxToStringFields)).mkString("\n")}", e)
  }

  /**
   * Returns an object of type `T`, extracting the required values from the provided row.  Note that
   * you must `resolveAndBind` an encoder to a specific schema before you can call this
   * function.
   */
  def fromRow(row: InternalRow): T = try {
    constructProjection(row).get(0, ObjectType(clsTag.runtimeClass)).asInstanceOf[T]
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Error while decoding: $e\n" +
        s"${deserializer.simpleString(SQLConf.get.maxToStringFields)}", e)
  }

  /**
   * The process of resolution to a given schema throws away information about where a given field
   * is being bound by ordinal instead of by name.  This method checks to make sure this process
   * has not been done already in places where we plan to do later composition of encoders.
   */
  def assertUnresolved(): Unit = {
    (deserializer +:  serializer).foreach(_.foreach {
      case a: AttributeReference if a.name != "loopVar" =>
        sys.error(s"Unresolved encoder expected, but $a was found.")
      case _ =>
    })
  }

  protected val attrs = serializer.flatMap(_.collect {
    case _: UnresolvedAttribute => ""
    case a: Attribute => s"#${a.exprId}"
    case b: BoundReference => s"[${b.ordinal}]"
  })

  protected val schemaString =
    schema
      .zip(attrs)
      .map { case(f, a) => s"${f.name}$a: ${f.dataType.simpleString}"}.mkString(", ")

  override def toString: String = s"class[$schemaString]"
}
