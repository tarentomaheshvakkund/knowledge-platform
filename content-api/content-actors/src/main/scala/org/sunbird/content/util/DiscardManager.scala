package org.sunbird.content.util

import java.util
import java.util.concurrent.CompletionException

import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.common.{JsonUtils, Platform}
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResourceNotFoundException, ServerException}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.nodes.DataNode
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.external.ExternalPropsManager
import org.sunbird.managers.UpdateHierarchyManager.{fetchHierarchy, shouldImageBeDeleted}
import org.sunbird.telemetry.logger.TelemetryManager
import org.sunbird.utils.{HierarchyConstants, HierarchyErrorCodes}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

object DiscardManager {
    private val CONTENT_DISCARD_STATUS = Platform.getStringList("content.discard.status", util.Arrays.asList("Draft", "FlagDraft"))

    @throws[Exception]
    def discard(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Response] = {
        validateRequest(request)
        getNodeToDiscard(request).flatMap(node => {
            request.put(ContentConstants.IDENTIFIER, node.getIdentifier)
            if (!CONTENT_DISCARD_STATUS.contains(node.getMetadata.get(ContentConstants.STATUS)))
                throw new ClientException(ContentConstants.ERR_CONTENT_NOT_DRAFT, "No changes to discard for content with content id: " + node.getIdentifier + " since content status isn't draft", node.getIdentifier)
            cleanLanguageReferences(node.getIdentifier).flatMap { _ =>
                val discardOp =
                    if (StringUtils.equalsIgnoreCase(
                        node.getMetadata.getOrDefault(ContentConstants.MIME_TYPE, "").asInstanceOf[String],
                        ContentConstants.COLLECTION_MIME_TYPE))
                    discardForCollection(node, request)
                    else
                    DataNode.deleteNode(request)

                discardOp.map { _ =>
                    ResponseHandler.OK()
                    .put("node_id", node.getIdentifier)
                    .put("identifier", node.getIdentifier)
                    .put("message", s"Draft version of the content with id : ${node.getIdentifier} is discarded")
                }
            }
        })recoverWith { case e: CompletionException => throw e.getCause }
    }

    private def getNodeToDiscard(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Node] = {
        val imageRequest = new Request(request)
        imageRequest.put(ContentConstants.MODE, ContentConstants.EDIT_MODE)
        imageRequest.put(ContentConstants.IDENTIFIER, request.get(ContentConstants.IDENTIFIER))
        DataNode.read(imageRequest)
    }

    def validateRequest(request: Request): Unit = {
        if (StringUtils.isBlank(request.getRequest.getOrDefault(ContentConstants.IDENTIFIER, "").asInstanceOf[String]))
            throw new ClientException(ContentConstants.ERR_INVALID_CONTENT_ID, "Please provide valid content identifier")
    }


    private def discardForCollection(node: Node, request: Request)(implicit executionContext: ExecutionContext, oec: OntologyEngineContext): Future[java.lang.Boolean] = {
        request.put(ContentConstants.IDENTIFIERS, if (node.getMetadata.containsKey(ContentConstants.PACKAGE_VERSION)) List(node.getIdentifier) else List(node.getIdentifier, node.getIdentifier + ContentConstants.IMAGE_SUFFIX))
        request.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.COLLECTION_SCHEMA_NAME)
        oec.graphService.deleteExternalProps(request).map(resp => DataNode.deleteNode(request)).flatMap(f => f)
    }

    def cleanLanguageReferences(discardedId: String)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Unit] = {
        val readReq = new Request()
        readReq.put("graph_id", "domain")
        readReq.put("version", "1.0")
        readReq.put("objectType", "Content")
        readReq.put("schemaName", "content")
        readReq.put("identifier", discardedId)
        readReq.put("mode", "edit")

        DataNode.read(readReq).flatMap { discardedNode =>
            val languageMap = discardedNode.getMetadata.getOrDefault("languageMapV1", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
            val targets = languageMap.values().toList.collect {
            case v: util.Map[_, _] if v.asInstanceOf[util.Map[String, AnyRef]].get("id") != discardedId =>
                v.asInstanceOf[util.Map[String, AnyRef]].get("id").asInstanceOf[String]
            }

            val discardLangOpt = languageMap.entrySet().find(e => {
            val value = e.getValue
            value match {
                case map: util.Map[_, _] =>
                map.asInstanceOf[util.Map[String, AnyRef]].get("id") == discardedId
                case _ => false
            }
            }).map(_.getKey)

            discardLangOpt match {
                case Some(discardLang) =>
                    val updateFutures = targets.map { id =>
                    val readNodeReq = new Request()
                    readNodeReq.put("graph_id", "domain")
                    readNodeReq.put("version", "1.0")
                    readNodeReq.put("objectType", "Content")
                    readNodeReq.put("schemaName", "content")
                    readNodeReq.put("identifier", id)
                    readNodeReq.put("mode", "edit")

                    DataNode.read(readNodeReq).flatMap { node =>
                        val versionKey = node.getMetadata.getOrDefault("versionKey", "").asInstanceOf[String]
                        val langMap = node.getMetadata.get("languageMapV1").asInstanceOf[util.Map[String, AnyRef]]
                        langMap.remove(discardLang)

                        val updateReq = new Request()
                        updateReq.setOperation("systemUpdate")
                        updateReq.setRequest(new util.HashMap[String, AnyRef]() {{
                        put("languageMapV1", langMap)
                        put("versionKey", versionKey)
                        }})
                        updateReq.setContext(new util.HashMap[String, AnyRef]() {{
                        put("graph_id", "domain")
                        put("version", "1.0")
                        put("objectType", "Content")
                        put("schemaName", "content")
                        put("identifier", id)
                        }})
                        DataNode.update(updateReq).map(_ => ())
                    }
                    }
                    Future.sequence(updateFutures).map(_ => ())
                case None => Future.successful(())
            }
        }
    }
}
