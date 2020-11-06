package org.sunbird.actors

import java.util

import javax.inject.Inject
import org.apache.commons.lang3.StringUtils
import org.sunbird.actor.core.BaseActor
import org.sunbird.common.{DateUtils, Platform}
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.ClientException
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.nodes.DataNode
import org.sunbird.graph.utils.NodeUtil
import org.sunbird.kafka.client.KafkaClient
import org.sunbird.telemetry.util.LogTelemetryEventUtil

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class QuestionActor @Inject() (implicit oec: OntologyEngineContext) extends BaseActor {

	implicit val ec: ExecutionContext = getContext().dispatcher
	private val kfClient = new KafkaClient


	override def onReceive(request: Request): Future[Response] = request.getOperation match {
		case "createQuestion" => create(request)
		case "readQuestion" => read(request)
		case "updateQuestion" => update(request)
		case "reviewQuestion" => review(request)
		case "publishQuestion" => publish(request)
		case "retireQuestion" => retire(request)
		case _ => ERROR(request.getOperation)
	}

	def create(request: Request): Future[Response] = {
		val visibility: String = request.getRequest.getOrDefault("visibility", "").asInstanceOf[String]
		if (StringUtils.isBlank(visibility))
			throw new ClientException("ERR_QUESTION_CREATE_FAILED", "Visibility is a mandatory parameter")
		visibility match {
			case "Parent" => if (!request.getRequest.containsKey("parent"))
				throw new ClientException("ERR_QUESTION_CREATE_FAILED", "For visibility Parent, parent id is mandatory") else
				request.getRequest.put("questionSet", List[java.util.Map[String, AnyRef]](Map("identifier" -> request.get("parent")).asJava).asJava)
			case "Public" => if (request.getRequest.containsKey("parent")) throw new ClientException("ERR_QUESTION_CREATE_FAILED", "For visibility Public, question can't have parent id")
		}
		DataNode.create(request).map(node => {
			val response = ResponseHandler.OK
			response.put("identifier", node.getIdentifier)
			response.put("versionKey", node.getMetadata.get("versionKey"))
			response
		})
	}

	def read(request: Request): Future[Response] = {
		val fields: util.List[String] = JavaConverters.seqAsJavaListConverter(request.get("fields").asInstanceOf[String].split(",").filter(field => StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null"))).asJava
		request.getRequest.put("fields", fields)
		DataNode.read(request).map(node => {
			val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), request.getContext.get("version").asInstanceOf[String])
			metadata.put("identifier", node.getIdentifier.replace(".img", ""))
			val response: Response = ResponseHandler.OK
			response.put("question", metadata)
			response
		})
	}

	def update(request: Request): Future[Response] = {
		DataNode.read(request).flatMap(node => {
			request.getRequest.getOrDefault("visibility", "") match {
				case "Public" => request.put("parent", "")
				case "Private" => if (!node.getMetadata.containsKey("parent") && !request.getRequest.containsKey("parent"))
					throw new ClientException("ERR_QUESTION_CREATE_FAILED", "For visibility Parent, parent id is mandatory")
				else request.getRequest.put("questionSet", List[java.util.Map[String, AnyRef]](Map("identifier" -> request.get("parent")).asJava).asJava)
				case _ => request
			}
			DataNode.update(request).map(node => {
				val response: Response = ResponseHandler.OK
				val identifier: String = node.getIdentifier.replace(".img", "")
				response.put("identifier", identifier)
				response.put("versionKey", node.getMetadata.get("versionKey"))
				response
			})
		})
	}

	def review(request: Request): Future[Response] = {
		getValidatedNodeToReview(request).flatMap(node => {
			val updateRequest = new Request(request)
			updateRequest.getContext.put("identifier", request.get("identifier"))
			updateRequest.put("versionKey", node.getMetadata.get("versionKey"))
			updateRequest.put("prevStatus", "Draft")
			updateRequest.put("status", "Review")
			updateRequest.put("lastStatusChangedOn", DateUtils.formatCurrentDate)
			updateRequest.put("lastUpdatedOn", DateUtils.formatCurrentDate)
			DataNode.update(updateRequest).map(node => {
				val response: Response = ResponseHandler.OK
				val identifier: String = node.getIdentifier.replace(".img", "")
				response.put("identifier", identifier)
				response.put("versionKey", node.getMetadata.get("versionKey"))
				response
			})
		})
	}

	def publish(request: Request): Future[Response] = {
		getValidatedNodeToPublish(request).map(node => {
			val identifier: String = node.getIdentifier.replace(".img", "")
			pushInstructionEvent(node.getIdentifier, node)
			val response = new Response()
			response.put("identifier", identifier)
			response.put("message", "Question is successfully sent for Publish")
		})
	}

	def retire(request: Request): Future[Response] = {
		getValidatedNodeToRetire(request).flatMap(node => {
			val updateRequest = new Request(request)
			updateRequest.put("identifiers", java.util.Arrays.asList(request.get("identifier").asInstanceOf[String], request.get("identifier").asInstanceOf[String] + ".img"))
			updateRequest.put("prevStatus", node.getMetadata.get("status"))
			updateRequest.put("status", "Review")
			updateRequest.put("lastStatusChangedOn", DateUtils.formatCurrentDate)
			updateRequest.put("lastUpdatedOn", DateUtils.formatCurrentDate)
			DataNode.update(updateRequest).map(node => {
				val response: Response = ResponseHandler.OK
				val identifier: String = node.getIdentifier.replace(".img", "")
				response.put("identifier", identifier)
				response.put("message", "Question Successfully retired.")
				response
			})
		})
	}


	private def getValidatedNodeToReview(request: Request): Future[Node] = {
		request.put("mode", "edit")
		DataNode.read(request).map(node => {
			if(StringUtils.equalsIgnoreCase(node.getMetadata.getOrDefault("visibility", "").asInstanceOf[String], "Parent"))
				throw new ClientException("ERR_QUESTION_REVIEW", "Questions with visibility Parent, can't be sent for review individually.")
			if(!StringUtils.equalsAnyIgnoreCase(node.getMetadata.getOrDefault("status", "").asInstanceOf[String], "Draft"))
				throw new ClientException("ERR_QUESTION_REVIEW", "Question with status other than Draft can't be sent for review.")
			node
		})
	}

	private def getValidatedNodeToPublish(request: Request): Future[Node] = {
		request.put("mode", "edit")
		DataNode.read(request).map(node => {
			if(StringUtils.equalsIgnoreCase(node.getMetadata.getOrDefault("visibility", "").asInstanceOf[String], "Parent"))
				throw new ClientException("ERR_QUESTION_PUBLISH", "Questions with visibility Parent, can't be sent for review individually.")
			node
		})
	}

	private def getValidatedNodeToRetire(request: Request): Future[Node] = {
		DataNode.read(request).map(node => {
			if (StringUtils.equalsIgnoreCase("Retired", node.getMetadata.get("status").asInstanceOf[String]))
				throw new ClientException("ERR_QUESTION_RETIRE", "Question with Identifier " + node.getIdentifier + " is already Retired.")
			node
		})
	}

	@throws[Exception]
	private def pushInstructionEvent(identifier: String, node: Node): Unit = {
		val actor: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val context: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val objectData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val edata: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		generateInstructionEventMetadata(actor, context, objectData, edata, node, identifier)
		val beJobRequestEvent: String = LogTelemetryEventUtil.logInstructionEvent(actor, context, objectData, edata)
		val topic: String = Platform.getString("kafka.topics.instruction","sunbirddev.learning.job.request")
		if (StringUtils.isBlank(beJobRequestEvent)) throw new ClientException("BE_JOB_REQUEST_EXCEPTION", "Event is not generated properly.")
		kfClient.send(beJobRequestEvent, topic)
	}

	private def generateInstructionEventMetadata(actor: util.Map[String, AnyRef], context: util.Map[String, AnyRef], objectData: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], node: Node, identifier: String): Unit = {
		val actorId: String = "Publish Samza Job"
		val actorType: String = "System"
		val pdataId: String = "org.ekstep.platform"
		val pdataVersion: String = "1.0"
		val action: String = "publish"

		val metadata: util.Map[String, AnyRef] = node.getMetadata
		val publishType = if (StringUtils.equalsIgnoreCase(metadata.getOrDefault("status", "").asInstanceOf[String], "Unlisted")) "unlisted" else "public"

		actor.put("id", actorId)
		actor.put("type", actorType)

		context.put("channel", metadata.get("channel"))
		val pdata = new util.HashMap[String, AnyRef]
		pdata.put("id", pdataId)
		pdata.put("ver", pdataVersion)
		context.put("pdata", pdata)
		if (Platform.config.hasPath("cloud_storage.env")) {
			val env = Platform.config.getString("cloud_storage.env")
			context.put("env", env)
		}
		if (Platform.config.hasPath("cloud_storage.env")) {
			val env: String = Platform.getString("cloud_storage.env", "dev")
			context.put("env", env)
		}
		objectData.put("id", identifier)
		objectData.put("ver", metadata.get("versionKey"))
		val instructionEventMetadata = new util.HashMap[String, AnyRef]
		instructionEventMetadata.put("pkgVersion", metadata.get("pkgVersion"))
		instructionEventMetadata.put("mimeType", metadata.get("mimeType"))
		instructionEventMetadata.put("lastPublishedBy", metadata.get("lastPublishedBy"))
		edata.put("action", action)
		edata.put("metadata", instructionEventMetadata)
		edata.put("publish_type", publishType)
		edata.put("contentType", metadata.get("contentType"))
	}
}
