package org.sunbird.util

import org.slf4j.LoggerFactory
import org.sunbird.common.dto.Request
import org.sunbird.common.exception.{ClientException, ErrorCodes}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.schema.DefinitionNode

import scala.concurrent.ExecutionContext

object RequestUtil {

	private val SYSTEM_UPDATE_ALLOWED_CONTENT_STATUS = List("Live", "Unlisted")
	private val SYSTEM_UPDATE_RESTRICTED_PROPERTIES = List("screenshots")

	def restrictProperties(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Unit = {
		val logger = LoggerFactory.getLogger(this.getClass)
		logger.info("Starting restrictProperties method")
		val graphId = request.getContext.getOrDefault("graph_id", "").asInstanceOf[String]
		logger.info(s"Extracted graphId: $graphId")
		val version = request.getContext.getOrDefault("version", "").asInstanceOf[String]
		logger.info(s"Extracted version: $version")
		val objectType = request.getContext.getOrDefault("objectType", "").asInstanceOf[String]
		logger.info(s"Extracted objectType: $objectType")
		val schemaName = request.getContext.getOrDefault("schemaName", "").asInstanceOf[String]
		logger.info(s"Extracted schemaName: $schemaName")
		val operation = request.getOperation.toLowerCase.replace(objectType.toLowerCase, "")
		logger.info(s"Determined operation: $operation")
		val restrictedProps = DefinitionNode.getRestrictedProperties(graphId, version, operation, schemaName)
		logger.info(s"Retrieved restricted properties: ${restrictedProps.mkString(", ")}")
		if (restrictedProps.exists(prop => request.getRequest.containsKey(prop))) {
			logger.info(s"Request contains restricted properties: ${restrictedProps.mkString(", ")}")
			throw new ClientException("ERROR_RESTRICTED_PROP", "Properties in list " + restrictedProps.mkString("[", ", ", "]") + " are not allowed in request")
		}
		logger.info("Completed restrictProperties method")
	}

	def validateRequest(request: Request): Unit = {
		if (request.getRequest.isEmpty)
			throw new ClientException(ErrorCodes.ERR_BAD_REQUEST.name(), s"Request Body cannot be Empty.")

		if (request.get("status") != null && SYSTEM_UPDATE_ALLOWED_CONTENT_STATUS.contains(request.get("status").asInstanceOf[String]))
			throw new ClientException(ErrorCodes.ERR_BAD_REQUEST.name(), s"Cannot update content status to : ${SYSTEM_UPDATE_ALLOWED_CONTENT_STATUS.mkString("[", ", ", "]")}.")

		SYSTEM_UPDATE_RESTRICTED_PROPERTIES.foreach(prop => {
			if (request.get(prop) != null) throw new ClientException(ErrorCodes.ERR_BAD_REQUEST.name(), s"Properties in list ${SYSTEM_UPDATE_RESTRICTED_PROPERTIES.mkString("[", ", ", "]")} are not allowed in request.")
		})
	}
}
