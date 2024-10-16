package org.sunbird.content.actors

import org.apache.commons.lang.StringUtils
import org.sunbird.common.Platform
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.content.util.ContentConstants
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.{Node, Relation}
import org.sunbird.graph.nodes.DataNode
import org.sunbird.telemetry.logger.TelemetryManager
import org.sunbird.telemetry.util.LogTelemetryEventUtil

import java.util
import javax.inject.Inject
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.Future

import scala.collection.JavaConversions._
import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class EventActor @Inject()(implicit oec: OntologyEngineContext, ss: StorageService) extends ContentActor {

  override def onReceive(request: Request): Future[Response] = {
    request.getOperation match {
      case "createContent" => create(request)
      case "readContent" => read(request)
      case "updateContent" => update(request)
      case "retireContent" => retire(request)
      case "discardContent" => discard(request)
      case "publishContent" => publish(request)
      case _ => ERROR(request.getOperation)
    }
  }

  override def update(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.update, request)
  }

  def publish(request: Request): Future[Response] = {
    TelemetryManager.log("EventActor::publish Identifier: " + request.getRequest.getOrDefault("identifier", ""))
    verifyStandaloneEventAndApply(super.update, request, true)
  }

  override def discard(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.discard, request)
  }

  override def retire(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.retire, request)
  }

  private def verifyStandaloneEventAndApply(f: Request => Future[Response], request: Request, isPublish: Boolean = false, dataUpdater: Option[Node => Unit] = None): Future[Response] = {
    DataNode.read(request).flatMap(node => {
      val inRelations = if (node.getInRelations == null) new util.ArrayList[Relation]() else node.getInRelations;
      val hasEventSetParent = inRelations.asScala.exists(rel => "EventSet".equalsIgnoreCase(rel.getStartNodeObjectType))
      if (hasEventSetParent)
        Future(ResponseHandler.ERROR(ResponseCode.CLIENT_ERROR, ResponseCode.CLIENT_ERROR.name(), "ERROR: Can't modify an Event which is part of an Event Set!"))
      else {
        if (dataUpdater.isDefined) {
          dataUpdater.get.apply(node)
        }
        f.apply(request).flatMap(response => {
          // Check if the response is OK
          if (response.getResponseCode == ResponseCode.OK) {
            if (isPublish) {
              TelemetryManager.log("EventActor::verifyStandaloneEventAndApply publish request for Identifier: " + request.getRequest.getOrDefault("identifier", ""))
              pushInstructionEvent(node.getIdentifier, node)
            } else {
              TelemetryManager.log("EventActor::verifyStandaloneEventAndApply Identifier: " + request.getRequest.getOrDefault("identifier", ""))
            }
            Future.successful(response)
          } else {
            // Return the response if it's not OK as it is
            Future.successful(response)
          }
        })
      }
    })
  }

  override def dataModifier(node: Node): Node = {
    TelemetryManager.log("EventActor::dataModifier Identifier: " + node.getIdentifier)
    if (node.getMetadata.containsKey("trackable") &&
      node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].containsKey("enabled") &&
      "Yes".equalsIgnoreCase(node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("enabled", "").asInstanceOf[String])) {
      node.getMetadata.put("contentType", "Event")
      node.getMetadata.put("objectType", "Event")
    }
    node
  }

  @throws[Exception]
	def pushInstructionEvent(identifier: String, node: Node)(implicit oec: OntologyEngineContext): Unit = {
		val (actor, context, objData, eData) = generateInstructionEventMetadata(identifier.replace(".img", ""), node)
		val beJobRequestEvent: String = LogTelemetryEventUtil.logInstructionEvent(actor.asJava, context.asJava, objData.asJava, eData)
		val topic: String = Platform.getString("kafka.topics.event.publish", "dev.publish.job.request")
		if (StringUtils.isBlank(beJobRequestEvent)) throw new ClientException("BE_JOB_REQUEST_EXCEPTION", "Event is not generated properly.")
		oec.kafkaClient.send(beJobRequestEvent, topic)
	}

	def generateInstructionEventMetadata(identifier: String, node: Node): (Map[String, AnyRef], Map[String, AnyRef], Map[String, AnyRef], util.Map[String, AnyRef]) = {
		val metadata: util.Map[String, AnyRef] = node.getMetadata
		val publishType = if (StringUtils.equalsIgnoreCase(metadata.getOrDefault("status", "").asInstanceOf[String], "Unlisted")) "unlisted" else "public"
		val eventMetadata = Map("identifier" -> identifier, "mimeType" -> metadata.getOrDefault("mimeType", ""), "objectType" -> node.getObjectType.replace("Image", ""), "pkgVersion" -> metadata.getOrDefault("pkgVersion", 0.asInstanceOf[AnyRef]), "lastPublishedBy" -> metadata.getOrDefault("lastPublishedBy", ""))
		val actor = Map("id" -> s"${node.getObjectType.toLowerCase().replace("image", "")}-publish", "type" -> "System".asInstanceOf[AnyRef])
		val context = Map("channel" -> metadata.getOrDefault("channel", ""), "pdata" -> Map("id" -> "org.sunbird.platform", "ver" -> "1.0").asJava, "env" -> Platform.getString("cloud_storage.env", "dev"))
		val objData = Map("id" -> identifier, "ver" -> metadata.getOrDefault("versionKey", ""))
		val eData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef] {{
				put("action", "publish")
				put("publish_type", publishType)
				put("metadata", eventMetadata.asJava)
			}}
		(actor, context, objData, eData)
	}

}