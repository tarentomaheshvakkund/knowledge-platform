package org.sunbird.graph.schema.validator

import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.CompletionException
import org.sunbird.cache.impl.RedisCache
import org.sunbird.common.{DateUtils, JsonUtils, Platform}
import org.sunbird.common.dto.{Request, ResponseHandler}
import org.sunbird.common.exception.ResourceNotFoundException
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.common.enums.AuditProperties
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.exception.GraphErrorCodes
import org.sunbird.graph.external.ExternalPropsManager
import org.sunbird.graph.schema.{DefinitionFactory, IDefinition}
import org.sunbird.graph.service.operation.{NodeAsyncOperations, SearchAsyncOperations}
import org.sunbird.graph.utils.{NodeUtil, ScalaJsonUtils}
import org.sunbird.telemetry.logger.TelemetryManager

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

trait VersioningNode extends IDefinition {

    val statusList = List("Live", "Unlisted", "Flagged")
    val IMAGE_SUFFIX = ".img"
    val IMAGE_OBJECT_SUFFIX = "Image"
    val COLLECTION_MIME_TYPE = "application/vnd.ekstep.content-collection"

    private val logger = LoggerFactory.getLogger(this.getClass)

    abstract override def getNode(identifier: String, operation: String, mode: String = "read", versioning: Option[String] = None)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Node] = {
        operation match {
            case "update" => getNodeToUpdate(identifier, versioning);
            case "read" => getNodeToRead(identifier, mode)
            case _ => getNodeToRead(identifier, mode)
        }
    }


    private def getNodeToUpdate(identifier: String, versioning: Option[String] = None)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Node] = {
        logger.info(s"Starting getNodeToUpdate for identifier: $identifier with versioning: $versioning")
        val nodeFuture: Future[Node] = super.getNode(identifier, "update", null)
        nodeFuture.map(node => {
            logger.info(s"Retrieved node for identifier: $identifier")
            val versioningEnable = versioning.getOrElse({
                if (schemaValidator.getConfig.hasPath("version")) schemaValidator.getConfig.getString("version") else "disable"
            })
            logger.info(s"Versioning enabled: $versioningEnable")
            if (null == node) {
                logger.error(s"Node not found for identifier: $identifier")
                throw new ResourceNotFoundException(GraphErrorCodes.ERR_INVALID_NODE.toString, s"Node Not Found With Identifier: $identifier")
            } else if ("enable".equalsIgnoreCase(versioningEnable)) {
                logger.info(s"Versioning is enabled for identifier: $identifier, getting editable node")
                getEditableNode(identifier, node)
            } else {
                logger.info(s"Versioning is disabled for identifier: $identifier, returning node as is")
                Future { node }
            }
        }).flatMap(f => f)
    }

    private def getNodeToRead(identifier: String, mode: String)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Node] = {
        if ("edit".equalsIgnoreCase(mode)) {
            val imageNode = super.getNode(identifier + IMAGE_SUFFIX, "read", mode)
            imageNode recoverWith {
                case e: CompletionException => {
                    if (e.getCause.isInstanceOf[ResourceNotFoundException])
                        super.getNode(identifier, "read", mode)
                    else
                        throw e.getCause
                }
            }
        } else {
            val cacheKey = getSchemaName().toLowerCase() + ".cache.enable"
            if (Platform.getBoolean(cacheKey, false)) {
                val ttl: Integer = if (Platform.config.hasPath(getSchemaName().toLowerCase() + ".cache.ttl")) Platform.config.getInt(getSchemaName().toLowerCase() + ".cache.ttl") else 86400
                getCachedNode(identifier, ttl)
            } else
                super.getNode(identifier, "read", mode)
        }
    }

    private def getEditableNode(identifier: String, node: Node)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Node] = {
        logger.info(s"Starting getEditableNode for identifier: $identifier")
        val status = node.getMetadata.get("status").asInstanceOf[String]
        logger.info(s"Node status: $status")
        if (statusList.contains(status)) {
            val imageId = node.getIdentifier + IMAGE_SUFFIX
            logger.info(s"Image ID: $imageId")
            try {
                val imageNode = oec.graphService.getNodeByUniqueId(node.getGraphId, imageId, false, new Request())
                imageNode recoverWith {
                    case e: CompletionException =>
                        TelemetryManager.error("Exception occurred while fetching image node, may not be found", e.getCause)
                        logger.error("Exception occurred while fetching image node", e.getCause)

                        if (e.getCause.isInstanceOf[ResourceNotFoundException]) {
                            logger.info(s"Image node not found, creating new image node for identifier: $identifier")

                            node.setIdentifier(imageId)
                            node.setObjectType(node.getObjectType + IMAGE_OBJECT_SUFFIX)
                            node.getMetadata.put("status", "Draft")
                            node.getMetadata.put("prevStatus", status)
                            node.getMetadata.put(AuditProperties.lastStatusChangedOn.name, DateUtils.formatCurrentDate())

                            oec.graphService.addNode(node.getGraphId, node).map { imgNode =>
                                logger.info(s"Image node created with identifier: ${imgNode.getIdentifier}")
                                imgNode.getMetadata.put("isImageNodeCreated", "yes")
                                val category = node.getMetadata.get("category").asInstanceOf[String]
                                if (!"event".equalsIgnoreCase(category)) {
                                    copyExternalProps(identifier, node.getGraphId, imgNode.getObjectType.toLowerCase().replace("image", "")).map { response =>
                                        if (!ResponseHandler.checkError(response)) {
                                            if (null != response.getResult && !response.getResult.isEmpty)
                                                imgNode.setExternalData(response.getResult)
                                        }
                                        imgNode
                                    }
                                } else {
                                    Future.successful(imgNode)
                                }
                            }.flatMap(f => f)
                        } else {
                            throw e.getCause
                        }
                }
            } catch {
                case e: Exception =>
                    logger.error("Unexpected exception occurred", e)
                    throw e
            }
        } else {
            logger.info(s"Node status is not in the status list, returning node as is for identifier: $identifier")
            Future { node }
        }
    }

    private def copyExternalProps(identifier: String, graphId: String, schemaName: String)(implicit ec: ExecutionContext, oec: OntologyEngineContext) = {
        val request = new Request()
        request.setContext(new util.HashMap[String, AnyRef](){{
            put("schemaName", schemaName)
            put("version", getSchemaVersion())
            put("graph_id", graphId)
        }})
        request.put("identifier", identifier)
        oec.graphService.readExternalProps(request, getExternalPropsList(graphId, schemaName, getSchemaVersion()))
    }

    private def getExternalPropsList(graphId: String, schemaName: String, version: String)(implicit ec: ExecutionContext, oec: OntologyEngineContext): List[String] ={
        val definition = DefinitionFactory.getDefinition(graphId, schemaName, version)
        if(definition.schemaValidator.getConfig.hasPath("external.properties")){
            new util.ArrayList[String](definition.schemaValidator.getConfig.getObject("external.properties").keySet()).toList
        }else{
            List[String]()
        }
    }

    def getCachedNode(identifier: String, ttl: Integer)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Node] = {
        val nodeStringFuture: Future[String] = RedisCache.getAsync(identifier, nodeCacheAsyncHandler, ttl)
        nodeStringFuture.map(nodeString => {
            if (null != nodeString && !nodeString.asInstanceOf[String].isEmpty) {
                val nodeMap: util.Map[String, AnyRef] = JsonUtils.deserialize(nodeString.asInstanceOf[String], classOf[java.util.Map[String, AnyRef]])
                val node: Node = NodeUtil.deserialize(nodeMap, getSchemaName(), schemaValidator.getConfig
                  .getAnyRef("relations").asInstanceOf[java.util.Map[String, AnyRef]])
                Future {node}
            } else {
                super.getNode(identifier, "read", null)
            }
        }).flatMap(f => f)
    }

    private def nodeCacheAsyncHandler(objKey: String)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[String] = {
        super.getNode(objKey, "read", null).map(node => {
            if (List("Live", "Unlisted").contains(node.getMetadata.get("status").asInstanceOf[String])) {

                val nodeMap = NodeUtil.serialize(node, null, node.getObjectType.toLowerCase().replace("image", ""), getSchemaVersion())
                Future(ScalaJsonUtils.serialize(nodeMap))
            } else Future("")
        }).flatMap(f => f)
    }
    
    private def getSchemaNameFromMimeType(node: Node) : String = {
       node.getObjectType.replaceAll("Image", "").toLowerCase()
    }
}
