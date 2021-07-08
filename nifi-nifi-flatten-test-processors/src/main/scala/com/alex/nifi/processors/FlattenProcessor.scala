package com.alex.nifi.processors

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

// Commons IO
import org.apache.commons.io.IOUtils

// NiFi
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessorInitializationContext, ProcessContext, ProcessSession }
import org.apache.nifi.processor.io.{ InputStreamCallback, OutputStreamCallback }
import org.apache.nifi.annotation.behavior.{ ReadsAttribute, ReadsAttributes }
import org.apache.nifi.annotation.behavior.{ WritesAttribute, WritesAttributes }
import org.apache.nifi.annotation.documentation.{ CapabilityDescription, SeeAlso, Tags }
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.stream.io.StreamUtils
import MyJsonProtocol._
import spray.json._

// Typesafe Config
import com.typesafe.config.ConfigFactory

@Tags(Array("flatten", "processor", "alex", "schechter"))
@CapabilityDescription("Flatten processor")
class FlattenProcessor extends AbstractProcessor with FlattenProcessorProperties with FlattenProcessorRelationships {

  import scala.collection.JavaConverters._

  private[this] val className = this.getClass.getName

  private[this] lazy val config = ConfigFactory.load().getConfig(className)

  protected[this] override def init(context: ProcessorInitializationContext): Unit = {
  }

  override def getSupportedPropertyDescriptors(): java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  override def getRelationships(): java.util.Set[Relationship] = {
    relationships.asJava
  }

  @OnScheduled
  def onScheduled(context: ProcessContext): Unit = {
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFile = session.get
    val outputFlowFile = session.clone(flowFile)

    Option(flowFile) match {
      case Some(f) => {

        val content = new AtomicReference[String]
        val flowFileContentsBuffer = new Array[Byte](flowFile.getSize.toInt)
        session.read(flowFile, new InputStreamCallback {
          override def process(inputStream: InputStream): Unit = {
            StreamUtils.fillBuffer(inputStream, flowFileContentsBuffer, true)
          }
        })
        val jsonContent = new String(flowFileContentsBuffer, StandardCharsets.UTF_8).parseJson.asJsObject

        val flattenedJson = flattenMessage(jsonContent.fields, Map.empty)
        session.write(outputFlowFile, new OutputStreamCallback {
          override def process(outputStream: OutputStream): Unit =
            outputStream.write(flattenedJson.toJson.toString.getBytes)
        })
        //        session.transfer(outputFlowFile, RelSuccess)

      }
      case _ =>
        getLogger().warn("FlowFile was null")
    }

    session.transfer(outputFlowFile, RelSuccess)
    session.remove(flowFile)
  }

  private def flattenMessage(fieldsToGoOver: Map[String, JsValue], baseObject: Map[String, JsValue], currentPath: String = ""): Seq[Map[String, JsValue]] = {
    if (fieldsToGoOver.isEmpty)
      Seq(baseObject)
    else {
      fieldsToGoOver.head match {
        case simpleField @ ((_, _: JsBoolean) | (_, _: JsString) | (_, _: JsNumber)) =>
          val newBaseObject = baseObject + (concat(currentPath, simpleField._1) -> simpleField._2)
          flattenMessage(fieldsToGoOver.tail, newBaseObject, currentPath)

        case (key: String, values: JsArray) =>
          val newKey = concat(currentPath, key)
          try {
            val nestedObject = values.elements.map(_.asJsObject)
            val b = nestedObject.flatMap((obj: JsObject) => {
              flattenMessage(obj.fields, baseObject, newKey)
            })
            if (fieldsToGoOver.tail.isEmpty)
              b
            else {
              val c = flattenMessage(fieldsToGoOver.tail, baseObject, currentPath)
              for {
                cSpec <- c
                bSpec <- b
              } yield cSpec ++ bSpec
            }

          } catch {
            case _: Throwable => flattenMessage(fieldsToGoOver.tail, baseObject + (concat(currentPath, key) -> values), currentPath)
          }

        case (k, value: JsObject) =>
          if (fieldsToGoOver.tail.isEmpty) flattenMessage(value.fields, baseObject, concat(currentPath, k))
          else {
            val a = flattenMessage(value.fields, baseObject, concat(currentPath, k))
            val b = flattenMessage(fieldsToGoOver.tail, JsObject.empty.fields, currentPath)
            for {
              aSpec <- a
              bSpec <- b
            } yield aSpec ++ bSpec
          }

        case (key, _) => Seq(Map(concat(currentPath, key) -> JsNull))
      }
    }
  }

  def concat(prefix: String, key: String): String = if (prefix.nonEmpty) s"${prefix}_$key" else key

}
