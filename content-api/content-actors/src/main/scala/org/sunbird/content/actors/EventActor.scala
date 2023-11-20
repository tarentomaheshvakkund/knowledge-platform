package org.sunbird.content.actors

import org.apache.commons.lang.StringUtils
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.content.util.ContentConstants
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.{Node, Relation}
import org.sunbird.graph.nodes.DataNode

import java.util
import javax.inject.Inject
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.Future

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
    verifyStandaloneEventAndApply(super.update, request)
  }

  override def discard(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.discard, request)
  }

  override def retire(request: Request): Future[Response] = {
    verifyStandaloneEventAndApply(super.retire, request)
  }

  private def verifyStandaloneEventAndApply(f: Request => Future[Response], request: Request, dataUpdater: Option[Node => Unit] = None): Future[Response] = {
    DataNode.read(request).flatMap(node => {
      val inRelations = if (node.getInRelations == null) new util.ArrayList[Relation]() else node.getInRelations;
      val hasEventSetParent = inRelations.asScala.exists(rel => "EventSet".equalsIgnoreCase(rel.getStartNodeObjectType))
      if (hasEventSetParent)
        Future(ResponseHandler.ERROR(ResponseCode.CLIENT_ERROR, ResponseCode.CLIENT_ERROR.name(), "ERROR: Can't modify an Event which is part of an Event Set!"))
      else {
        if (dataUpdater.isDefined) {
          dataUpdater.get.apply(node)
        }
        f.apply(request)
      }
    })
  }

  override def dataModifier(node: Node): Node = {
    if (node.getMetadata.containsKey("trackable") &&
      node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].containsKey("enabled") &&
      "Yes".equalsIgnoreCase(node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("enabled", "").asInstanceOf[String])) {
      node.getMetadata.put("contentType", "Event")
    }
    node
  }

}