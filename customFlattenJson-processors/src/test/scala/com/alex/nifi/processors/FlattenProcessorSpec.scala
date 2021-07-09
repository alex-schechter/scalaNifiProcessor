/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alex.nifi.processors

import com.alex.nifi.processors.FlattenProcessor

import java.io._
import scala.jdk.CollectionConverters.CollectionHasAsScala

// ScalaTest
import org.scalatest._

// NiFi
import org.apache.nifi.util.{ TestRunner, TestRunners }

class FlattenProcessorSpec extends FunSpec {

  import FlattenProcessorRelationships.{ RelSuccess, RelFailure }

  describe("FlattenProcessor") {
    it("should successfully transfer a FlowFile") {
      val SomeContent = """{"test": "test"}"""
      val processor = new FlattenProcessor
      val runner = TestRunners.newTestRunner(processor)

      val content = new ByteArrayInputStream(SomeContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 1)
      runner.assertTransferCount(RelFailure, 0)

      for (flowFile <- runner.getFlowFilesForRelationship(RelSuccess).asScala) {
        flowFile.assertContentEquals("""[{"test":"test"}]""")
      }
    }
    it("should flatten the content") {
      val SomeContent = """{"test": {"asdfasfd": "test"}}"""
      val processor = new FlattenProcessor
      val runner = TestRunners.newTestRunner(processor)

      val content = new ByteArrayInputStream(SomeContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 1)
      runner.assertTransferCount(RelFailure, 0)

      for (flowFile <- runner.getFlowFilesForRelationship(RelSuccess).asScala) {
        flowFile.assertContentEquals("""[{"test_asdfasfd":"test"}]""")
      }
    }
    it("should flatten the content with complex example") {
      val SomeContent = """{"test": {"asdfasfd": [{"test1":"test1"}, {"test2":"test2"}, {"test3":{"test32":"test2"}}]}}"""

      val processor = new FlattenProcessor
      val runner = TestRunners.newTestRunner(processor)

      val content = new ByteArrayInputStream(SomeContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 1)
      runner.assertTransferCount(RelFailure, 0)

      for (flowFile <- runner.getFlowFilesForRelationship(RelSuccess).asScala) {
        flowFile.assertContentEquals("""[{"test_asdfasfd_test1":"test1"},{"test_asdfasfd_test2":"test2"},{"test_asdfasfd_test3_test32":"test2"}]""")
      }
    }
    it("Array test") {
      val SomeContent = """[{"test":"test"}]"""

      val processor = new FlattenProcessor
      val runner = TestRunners.newTestRunner(processor)

      val content = new ByteArrayInputStream(SomeContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 1)
      runner.assertTransferCount(RelFailure, 0)

      for (flowFile <- runner.getFlowFilesForRelationship(RelSuccess).asScala) {
        flowFile.assertContentEquals("""[{"test":"test"}]""")
      }
    }
    it("Invalid json test") {
      val SomeContent = """[{"test":"test]"""

      val processor = new FlattenProcessor
      val runner = TestRunners.newTestRunner(processor)

      val content = new ByteArrayInputStream(SomeContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 0)
      runner.assertTransferCount(RelFailure, 1)
    }
  }
}
