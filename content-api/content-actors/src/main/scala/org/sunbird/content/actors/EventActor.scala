package org.sunbird.content.actors


import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.cache.impl.RedisCache
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
import org.sunbird.util.RequestUtil

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util
import javax.inject.Inject
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class EventActor @Inject()(implicit oec: OntologyEngineContext, ss: StorageService) extends ContentActor {
  private val logger: Logger = LoggerFactory.getLogger("EventActor")
  override def onReceive(request: Request): Future[Response] = {
    request.getOperation match {
      case "createContent" => create(request)
      case "readContent" => read(request)
      case "updateContent" => update(request)
      case "retireContent" => retire(request)
      case "discardContent" => discard(request)
      case "publishContent" => publish(request)
      case "rejectEvent" => rejectEvent(request)
      case "systemUpdate" => systemUpdate(request)
      case _ => ERROR(request.getOperation)
    }
  }

  override def update(request: Request): Future[Response] = {
    val startDateTimeStr = request.getRequest.getOrDefault("startDateTime", "").asInstanceOf[String]
    val endDateTimeStr = request.getRequest.getOrDefault("endDateTime", "").asInstanceOf[String]
    if (StringUtils.isNotBlank(startDateTimeStr) && StringUtils.isNotBlank(endDateTimeStr)) {
      try {
        val inputUtcFormatter = DateTimeFormatter.ofPattern(Platform.config.getString("date.input.formatter"))
        val outputIstFormatter = DateTimeFormatter.ofPattern(Platform.config.getString("date.input.formatter"))
        val istZoneId = ZoneId.of("Asia/Kolkata")

        val startDateTimeUtc = ZonedDateTime.parse(startDateTimeStr, inputUtcFormatter)
        val startDateTimeIst = startDateTimeUtc.withZoneSameInstant(istZoneId)
        val formattedStartDateTimeIst = startDateTimeIst.format(outputIstFormatter)
        val startDateTimeEpochMillis = startDateTimeIst.toInstant.toEpochMilli

        request.getRequest.put("startDateTime", formattedStartDateTimeIst)
        request.getRequest.put("startDateTimeInEpoch", startDateTimeEpochMillis.asInstanceOf[java.lang.Long])

        val endDateTimeUtc = ZonedDateTime.parse(endDateTimeStr, inputUtcFormatter)
        val endDateTimeIst = endDateTimeUtc.withZoneSameInstant(istZoneId)
        val formattedEndDateTimeIst = endDateTimeIst.format(outputIstFormatter)
        val endDateTimeEpochMillis = endDateTimeIst.toInstant.toEpochMilli

        request.getRequest.put("endDateTime", formattedEndDateTimeIst)
        request.getRequest.put("endDateTimeInEpoch", endDateTimeEpochMillis.asInstanceOf[java.lang.Long])
      } catch {
        case ex: Exception =>
          return Future.successful(ResponseHandler.
            ERROR(ResponseCode.CLIENT_ERROR,
              "ERR_INVALID_DATE_FORMAT",
              "startDateTime or endDateTime is not in the expected format yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
      }
    }
    populateDefaultersForUpdation(request)
    val versionKey = request.getRequest.getOrDefault("versionKey", "").asInstanceOf[String]
    if (StringUtils.isBlank(versionKey)) {
      throw new ClientException("ERR_INVALID_REQUEST", "Please Provide Version Key!")
    }
    RequestUtil.restrictProperties(request)
    val reviewStatus: String = request.getRequest.getOrDefault("reviewStatus", "").asInstanceOf[String]
    if (reviewStatus == null || reviewStatus.isEmpty) {
      request.getRequest.put("cqfVersion", System.currentTimeMillis().toString)
    }
    DataNode.update(request, dataModifier).map(node => {
      val identifier: String = node.getIdentifier.replace(".img", "")
      ResponseHandler.OK.put("node_id", identifier)
        .put("identifier", identifier)
        .put("versionKey", node.getMetadata.get("versionKey"))
    })
  }

  def publish(request: Request): Future[Response] = {
    TelemetryManager.log("EventActor::publish Identifier: " + request.getRequest.getOrDefault("identifier", ""))
    val identifier = request.get("identifier").asInstanceOf[String]
    val updatedIdentifier = if (!identifier.endsWith(".img")) s"$identifier.img" else identifier
    request.put("identifier", updatedIdentifier)
    // Check if the node exists
    DataNode.read(request).flatMap { node =>
      // If the node exists, proceed with update and delete
      DataNode.updatev2(request, flag = true).flatMap { _ =>
        DataNode.delete(request)
        try {
          RedisCache.delete(identifier)
        } catch {
          case e: Exception =>
            logger.error(s"Error deleting Redis cache entry for identifier: $identifier", e)
        }
        request.put("identifier", updatedIdentifier.replace(".img", ""))
        verifyStandaloneEventAndApply(super.update, request, true)
      }
    }.recoverWith {
      case ex: Exception =>
        // If the node does not exist, directly call verifyStandaloneEventAndApply
        request.put("identifier", updatedIdentifier.replace(".img", ""))
        verifyStandaloneEventAndApply(super.update, request, true)
    }
  }

  override def discard(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.discard, request)
  }

  override def retire(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.retire, request).flatMap(response => {
      deleteRedisKeyOnRetire(request, response)
    })
  }

  private def deleteRedisKeyOnRetire(request: Request, response: Response): Future[Response] = {
    if (response.getResponseCode == ResponseCode.OK) {
      val identifier = request.get("identifier").asInstanceOf[String]
      if (StringUtils.isNotBlank(identifier)) {
        val redisKey = s"$identifier:user-event-enrolments"
        RedisCache.delete(redisKey)
      }
    }
    Future.successful(response)
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

  def rejectEvent(request: Request): Future[Response] = {
    RequestUtil.validateRequest(request)
    DataNode.read(request).map(node => {
      val status = node.getMetadata.get("status").asInstanceOf[String]
      if (StringUtils.isBlank(status)) {
        throw new ClientException("ERR_METADATA_ISSUE", "Event metadata error, status is blank for identifier:" + node.getIdentifier)
      }
      if (StringUtils.equals("sentToPublish", status) || StringUtils.equalsIgnoreCase("sentToPublish",status)) {
        request.getRequest.put("status", "Rejected")
        request.getRequest.put("prevStatus", "sentToPublish")
      }

      else new ClientException("ERR_INVALID_REQUEST", "Content not in Review status.")
      request.getRequest.put("versionKey", node.getMetadata.get("versionKey"))
      request.putIn("publishChecklist", null).putIn("publishComment", null)
      RequestUtil.restrictProperties(request)
      DataNode.update(request).map(node => {
        val identifier: String = node.getIdentifier.replace(".img", "")
        ResponseHandler.OK.put("node_id", identifier).put("identifier", identifier)
      })
    }).flatMap(f => f)
  }

  override def systemUpdate(request: Request): Future[Response] = {
    RedisCache.delete(request.get("identifier").asInstanceOf[String])
    val identifier = request.get("identifier").asInstanceOf[String]
    val updatedIdentifier = if (!identifier.endsWith(".img")) s"$identifier.img" else identifier
    DataNode.read(request).flatMap { node =>
      // Extract attributes to be updated from the request body
      val attributesToUpdate = request.getRequest.asInstanceOf[java.util.Map[String, AnyRef]]

      // Append attributes from the request to the node's metadata
      attributesToUpdate.forEach(new java.util.function.BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit = {
          node.getMetadata.put(key, value)
        }
      })
      // Save the updated node
      DataNode.updatev2(request, _ => node, flag = false).recover {
        case ex: Exception =>
          TelemetryManager.error("Error occurred during updatev2 operation", ex)
          ResponseHandler.ERROR(ResponseCode.SERVER_ERROR, "ERR_UPDATE_FAILED", ex.getMessage)
      }.map(response => {
        if (response.getResponseCode == ResponseCode.OK) {
          ResponseHandler.OK
        } else {
          response
        }
      })
    }
  }
}