package org.sunbird.content.review.mgr

import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.nodes.DataNode
import org.sunbird.mimetype.factory.MimeTypeManagerFactory

import scala.collection.Map
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object ReviewManager {

	import org.slf4j.LoggerFactory

	def review(request: Request, node: Node)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
		val logger = LoggerFactory.getLogger(this.getClass)
		logger.info("Starting review method")

		val identifier: String = node.getIdentifier
		logger.info(s"Node identifier: $identifier")

		val mimeType = node.getMetadata().getOrDefault("mimeType", "").asInstanceOf[String]
		logger.info(s"Node mimeType: $mimeType")

		val mgr = MimeTypeManagerFactory.getManager(node.getObjectType, mimeType)
		logger.info(s"Manager obtained for objectType: ${node.getObjectType} and mimeType: $mimeType")

		val reviewFuture: Future[Map[String, AnyRef]] = mgr.review(identifier, node)
		reviewFuture.map(result => {
			logger.info(s"Review result obtained for identifier: $identifier")

			val updateReq = new Request()
			updateReq.setContext(request.getContext)
			updateReq.putAll(result.asJava)
			logger.info(s"Update request created with context and result for identifier: $identifier")

			DataNode.update(updateReq).map(node => {
				logger.info(s"DataNode updated for identifier: ${node.getIdentifier}")
				ResponseHandler.OK.putAll(Map("identifier" -> node.getIdentifier.replace(".img", ""), "versionKey" -> node.getMetadata.get("versionKey")).asJava)
			})
		}).flatMap(f => f)
	}
}


