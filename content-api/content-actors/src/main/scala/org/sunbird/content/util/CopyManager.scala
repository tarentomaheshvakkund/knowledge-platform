package org.sunbird.content.util


import com.datastax.driver.core.utils.UUIDs
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.collections4.MapUtils
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang.StringUtils
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.{JsonUtils, Platform}
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ServerException}
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.common.Identifier
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.nodes.DataNode
import org.sunbird.graph.schema.DefinitionNode
import org.sunbird.graph.utils.{NodeUtil, ScalaJsonUtils}
import org.sunbird.managers.{HierarchyManager, UpdateHierarchyManager}
import org.sunbird.mimetype.factory.MimeTypeManagerFactory
import org.sunbird.mimetype.mgr.impl.H5PMimeTypeMgrImpl
import org.sunbird.models.UploadParams
import org.sunbird.telemetry.logger.TelemetryManager
import org.sunbird.util.HttpUtil

import java.io.{File, IOException}
import java.net.URL
import java.util
import java.util.concurrent.{CompletionException, TimeUnit}
import java.util.{Collections, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


object CopyManager {
    private val TEMP_FILE_LOCATION = Platform.getString("content.upload.temp_location", "/tmp/content")
    private val metadataNotTobeCopied = Platform.config.getStringList("content.copy.props_to_remove")
    private val invalidStatusList: util.List[String] = Platform.getStringList("content.copy.invalid_statusList", new util.ArrayList[String]())
    private val invalidContentTypes: util.List[String] = Platform.getStringList("content.copy.invalid_contentTypes", new util.ArrayList[String]())
    private val originMetadataKeys: util.List[String] = Platform.getStringList("content.copy.origin_data", new util.ArrayList[String]())
    private val internalHierarchyProps = List("identifier", "parent", "index", "depth")
    private val restrictedMimeTypesForUpload = List("application/vnd.ekstep.ecml-archive","application/vnd.ekstep.content-collection")
    private val copyArtifactUrl = Platform.config.getBoolean("content.copy.is_copy_artifacturl")
    private var copySchemeMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private val allowedFieldsFromConfig: util.List[String] = Platform.getStringList("content.copy.mandatory.fields", new util.ArrayList[String]())
    private val copyHierarchyCreatedDelay: Long = Platform.getLong("content.copy.hierarchy.delay", 300)
    private val allowedFieldsFromConfigForAssessment: util.List[String] = Platform.getStringList("content.copy.assessment.mandatory.fields", new util.ArrayList[String]())
    private val questionSetHierarchyUpdateAPI: String = Platform.getString("questionSet_hierarchy_update_api", "")
    implicit val httpUtil: HttpUtil = new HttpUtil
    private val questionSetHierarchyReadAPI: String = Platform.getString("questionSet_hierarchy_read_api", "")

    def copy(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext, ss: StorageService): Future[Response] = {
        request.getContext.put(ContentConstants.COPY_SCHEME, request.getRequest.getOrDefault(ContentConstants.COPY_SCHEME, ""))
        validateRequest(request)
        DataNode.read(request).map(node => {
            validateExistingNode(node)
            copySchemeMap = DefinitionNode.getCopySchemeContentType(request)
            val copiedNodeFuture: Future[Node] = node.getMetadata.get(ContentConstants.MIME_TYPE) match {
                case ContentConstants.COLLECTION_MIME_TYPE =>
                    node.setInRelations(null)
                    node.setOutRelations(null)
                    validateShallowCopyReq(node, request)
                    copyCollection(node, request)
                case _ =>
                    node.setInRelations(null)
                    copyContent(node, request)
            }
            copiedNodeFuture.map(copiedNode => {
                val response = ResponseHandler.OK()
                response.put("node_id", new util.HashMap[String, AnyRef](){{
                    put(node.getIdentifier, copiedNode.getIdentifier)
                }})
                response.put(ContentConstants.VERSION_KEY, copiedNode.getMetadata.get(ContentConstants.VERSION_KEY))
                response
            })
        }).flatMap(f => f) recoverWith { case e: CompletionException => throw e.getCause }
    }

    def copyContent(node: Node, request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext, ss: StorageService): Future[Node] = {
        val targetNodeId = Option(request.getRequest)
          .flatMap {
              case map: java.util.Map[_, _] =>
                  Option(map.get("targetNodeId"))
              case _ => None
          }.map(_.toString)

        if (targetNodeId.isDefined) {
            val readReq = new Request()
            readReq.setContext(request.getContext)
            readReq.put("identifier", targetNodeId.get)
            readReq.put("fields", util.Arrays.asList("body"))

            DataNode.read(readReq).map(copiedNode => {
                Future(copiedNode)
            }).flatMap(f => f)
        } else {
            val copyCreateReq: Future[Request] = getCopyRequest(node, request)
            copyCreateReq.map(req => {
                DataNode.create(req).map(copiedNode => {
                    if (copyArtifactUrl) {
                        artifactUpload(node, copiedNode, request)
                    } else {
                        Future(copiedNode)
                    }
                }).flatMap(f => f)
            }).flatMap(f => f)
        }
    }

    def copyCollection(originNode: Node, request: Request)(implicit ec:ExecutionContext, oec: OntologyEngineContext, ss: StorageService):Future[Node] = {
        val copyType = request.getRequest.get(ContentConstants.COPY_TYPE).asInstanceOf[String]
        copyContent(originNode, request).map(node => {
            val req = new Request(request)
            req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.COLLECTION_SCHEMA_NAME)
            req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
            req.put(ContentConstants.ROOT_ID, request.get(ContentConstants.IDENTIFIER))
            req.put(ContentConstants.MODE, request.get(ContentConstants.MODE))
            HierarchyManager.getHierarchy(req).map(response => {
                val originHierarchy = response.getResult.getOrDefault(ContentConstants.CONTENT, new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
                copyType match {
                    case ContentConstants.COPY_TYPE_SHALLOW => updateShallowHierarchy(request, node, originNode, originHierarchy)
                    case _ => updateHierarchyV2(request,node, originNode, originHierarchy, copyType)
                }
            }).flatMap(f=>f)
        }).flatMap(f => f) recoverWith {case e: CompletionException => throw e.getCause}
    }

    def updateHierarchy(request: Request, node: Node, originNode: Node, originHierarchy: util.Map[String, AnyRef], copyType:String)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Node] = {
        val updateHierarchyRequest = prepareHierarchyRequest(originHierarchy, originNode, node, copyType, request)
        val hierarchyRequest = new Request(request)
        hierarchyRequest.putAll(updateHierarchyRequest)
        hierarchyRequest.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.COLLECTION_SCHEMA_NAME)
        hierarchyRequest.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
        UpdateHierarchyManager.updateHierarchy(hierarchyRequest).map(response=>node)
    }

    def updateShallowHierarchy(request: Request, node: Node, originNode: Node, originHierarchy: util.Map[String, AnyRef])(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Node] = {
        val childrenHierarchy = originHierarchy.get("children").asInstanceOf[util.List[util.Map[String, AnyRef]]]
        val updatedChildrenHierarchy = childrenHierarchy.asScala.toList.map(child => {
            child.put("parent",node.getIdentifier)
            child
        })
        val req = new Request(request)
        req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.COLLECTION_SCHEMA_NAME)
        req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
        req.getContext.put(ContentConstants.IDENTIFIER, node.getIdentifier)
        req.put(ContentConstants.HIERARCHY, ScalaJsonUtils.serialize(new java.util.HashMap[String, AnyRef](){{
            put(ContentConstants.IDENTIFIER, node.getIdentifier)
            put(ContentConstants.CHILDREN, updatedChildrenHierarchy.asJava)
        }}))
        DataNode.update(req).map(node=>node)
    }

    def validateExistingNode(node: Node): Unit = {
        if (!CollectionUtils.isEmpty(invalidContentTypes) && invalidContentTypes.contains(node.getMetadata.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String]))
            throw new ClientException(ContentConstants.CONTENT_TYPE_ASSET_CAN_NOT_COPY, "ContentType " + node.getMetadata.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String] + " can not be copied.")
        if (invalidStatusList.contains(node.getMetadata.get(ContentConstants.STATUS).asInstanceOf[String]))
            throw new ClientException(ContentConstants.ERR_INVALID_REQUEST, "Cannot Copy content which is in " + node.getMetadata.get(ContentConstants.STATUS).asInstanceOf[String].toLowerCase + " status")
    }

    def validateRequest(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Unit = {
        val keysNotPresent = ContentConstants.REQUIRED_KEYS.filter(key => emptyCheckFilter(request.getRequest.getOrDefault(key, "")))
        if (keysNotPresent.nonEmpty)
            throw new ClientException(ContentConstants.ERR_INVALID_REQUEST, "Please provide valid value for " + keysNotPresent)
        if (StringUtils.equalsIgnoreCase(request.getRequest.getOrDefault(ContentConstants.COPY_TYPE, ContentConstants.COPY_TYPE_DEEP).asInstanceOf[String], ContentConstants.COPY_TYPE_SHALLOW) &&
            StringUtils.isNotBlank(request.getContext.get(ContentConstants.COPY_SCHEME).asInstanceOf[String]))
            throw new ClientException(ContentConstants.ERR_INVALID_REQUEST, "Content can not be shallow copied with copy scheme.")
        if(StringUtils.isNotBlank(request.getContext.get(ContentConstants.COPY_SCHEME).asInstanceOf[String]) && !DefinitionNode.getAllCopyScheme(request).contains(request.getRequest.getOrDefault(ContentConstants.COPY_SCHEME, "").asInstanceOf[String]))
            throw new ClientException(ContentConstants.ERR_INVALID_REQUEST, "Invalid copy scheme, Please provide valid copy scheme")
    }

    def emptyCheckFilter(key: AnyRef): Boolean = key match {
        case k: String => k.asInstanceOf[String].isEmpty
        case k: util.Map[String, AnyRef] => MapUtils.isEmpty(k.asInstanceOf[util.Map[String, AnyRef]])
        case k: util.List[String] => CollectionUtils.isEmpty(k.asInstanceOf[util.List[String]])
        case _ => true
    }

    def getCopyRequest(node: Node, request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Request] = {
        val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, new util.ArrayList(), node.getObjectType.toLowerCase.replace("image", ""), ContentConstants.SCHEMA_VERSION)
        val requestMap = request.getRequest
        requestMap.remove(ContentConstants.MODE)
        requestMap.remove(ContentConstants.COPY_SCHEME).asInstanceOf[String]
        val copyType = requestMap.remove(ContentConstants.COPY_TYPE).asInstanceOf[String]
        val originData: java.util.Map[String, AnyRef] = getOriginData(metadata, copyType)
        cleanUpCopiedData(metadata, copyType)
        metadata.putAll(requestMap)
        metadata.put(ContentConstants.STATUS, "Draft")
        metadata.put(ContentConstants.ORIGIN, node.getIdentifier)
        metadata.put(ContentConstants.IDENTIFIER, Identifier.getIdentifier(request.getContext.get("graph_id").asInstanceOf[String], Identifier.getUniqueIdFromTimestamp))
        if (MapUtils.isNotEmpty(originData))
            metadata.put(ContentConstants.ORIGIN_DATA, originData)
        request.getContext().put(ContentConstants.SCHEMA_NAME, node.getObjectType.toLowerCase.replace("image", ""))
        updateToCopySchemeContentType(request, metadata.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String], metadata)
        val req = new Request(request)
        req.setRequest(metadata)
        if (StringUtils.equalsIgnoreCase("application/vnd.ekstep.ecml-archive", node.getMetadata.get("mimeType").asInstanceOf[String])) {
            val readReq = new Request()
            readReq.setContext(request.getContext)
            readReq.put("identifier", node.getIdentifier)
            readReq.put("fields", util.Arrays.asList("body"))
            DataNode.read(readReq).map(node => {
                if (null != node.getMetadata.get("body"))
                    req.put("body", node.getMetadata.get("body").asInstanceOf[String])
                req
            })
        } else Future {req}
    }

    def getOriginData(metadata: util.Map[String, AnyRef], copyType:String): java.util.Map[String, AnyRef] = {
        new java.util.HashMap[String, AnyRef](){{
            putAll(originMetadataKeys.asScala.filter(key => metadata.containsKey(key)).map(key => key -> metadata.get(key)).toMap.asJava)
            put(ContentConstants.COPY_TYPE, copyType)
        }}
    }

    def cleanUpCopiedData(metadata: util.Map[String, AnyRef], copyType:String): util.Map[String, AnyRef] = {
        if(StringUtils.equalsIgnoreCase(ContentConstants.COPY_TYPE_SHALLOW, copyType)) {
            metadata.keySet().removeAll(metadataNotTobeCopied.asScala.toList.filter(str => !str.contains("dial")).asJava)
        } else metadata.keySet().removeAll(metadataNotTobeCopied)
        metadata
    }

    def copyURLToFile(objectId: String, fileUrl: String): File = try {
        val file = new File(getBasePath(objectId) + File.separator + getFileNameFromURL(fileUrl))
        FileUtils.copyURLToFile(new URL(fileUrl), file)
        file
    } catch {
        case e: IOException => throw new ClientException("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
    }

    def getBasePath(objectId: String): String = {
        if (!StringUtils.isBlank(objectId)) TEMP_FILE_LOCATION + File.separator + System.currentTimeMillis + "_temp" + File.separator + objectId else ""
    }

    //    def cleanUpNodeRelations(node: Node): Unit = {
    //        val relationsToDelete: util.List[Relation] = node.getOutRelations.asScala.filter(relation => ContentConstants.END_NODE_OBJECT_TYPES.contains(relation.getEndNodeObjectType)).toList.asJava
    //        node.getOutRelations.removeAll(relationsToDelete)
    //    }

    def getUpdateRequest(request: Request, copiedNode: Node, artifactUrl: String): Request = {
        val req = new Request()
        val context = request.getContext
        context.put(ContentConstants.IDENTIFIER, copiedNode.getIdentifier)
        req.setContext(context)
        req.put(ContentConstants.VERSION_KEY, copiedNode.getMetadata.get(ContentConstants.VERSION_KEY))
        req.put(ContentConstants.ARTIFACT_URL, artifactUrl)
        req
    }

    protected def getFileNameFromURL(fileUrl: String): String = if (!FilenameUtils.getExtension(fileUrl).isEmpty)
        FilenameUtils.getBaseName(fileUrl) + "_" + System.currentTimeMillis + "." + FilenameUtils.getExtension(fileUrl) else FilenameUtils.getBaseName(fileUrl) + "_" + System.currentTimeMillis

    protected def isInternalUrl(url: String)(implicit ss: StorageService): Boolean = url.contains(ss.getContainerName())

    def prepareHierarchyRequest(originHierarchy: util.Map[String, AnyRef], originNode: Node, node: Node, copyType: String, request: Request):util.HashMap[String, AnyRef] = {
        val children:util.List[util.Map[String, AnyRef]] = originHierarchy.get("children").asInstanceOf[util.List[util.Map[String, AnyRef]]]
        if(null != children && !children.isEmpty) {
            val nodesModified = new util.HashMap[String, AnyRef]()
            val hierarchy = new util.HashMap[String, AnyRef]()
            hierarchy.put(node.getIdentifier, new util.HashMap[String, AnyRef](){{
                put(ContentConstants.CHILDREN, new util.ArrayList[String]())
                put(ContentConstants.ROOT, true.asInstanceOf[AnyRef])
                put(ContentConstants.CONTENT_TYPE, node.getMetadata.get(ContentConstants.CONTENT_TYPE))
            }})
            populateHierarchyRequest(children, nodesModified, hierarchy, node.getIdentifier, copyType, request)
            new util.HashMap[String, AnyRef](){{
                put(ContentConstants.NODES_MODIFIED, nodesModified)
                put(ContentConstants.HIERARCHY, hierarchy)
            }}
        } else new util.HashMap[String, AnyRef]()
    }

    def populateHierarchyRequest(children: util.List[util.Map[String, AnyRef]], nodesModified: util.HashMap[String, AnyRef], hierarchy: util.HashMap[String, AnyRef], parentId: String, copyType: String, request: Request): Unit = {
        if (null != children && !children.isEmpty) {
            children.asScala.toList.foreach(child => {
                val id = if ("Parent".equalsIgnoreCase(child.get(ContentConstants.VISIBILITY).asInstanceOf[String])) {
                        val identifier = UUID.randomUUID().toString
                        updateToCopySchemeContentType(request, child.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String],  child)
                        nodesModified.put(identifier, new util.HashMap[String, AnyRef]() {{
                                put(ContentConstants.METADATA,  cleanUpCopiedData(new util.HashMap[String, AnyRef]() {{
                                    putAll(child)
                                    put(ContentConstants.CHILDREN, new util.ArrayList())
                                    internalHierarchyProps.map(key => remove(key))
                                }}, copyType))
                                put(ContentConstants.ROOT, false.asInstanceOf[AnyRef])
                                put("isNew", true.asInstanceOf[AnyRef])
                                put("setDefaultValue", false.asInstanceOf[AnyRef])
                            }})
                        identifier
                    } else
                        child.get(ContentConstants.IDENTIFIER).asInstanceOf[String]
                hierarchy.put(id, new util.HashMap[String, AnyRef]() {{
                        put(ContentConstants.CHILDREN, new util.ArrayList[String]())
                        put(ContentConstants.ROOT, false.asInstanceOf[AnyRef])
                        put(ContentConstants.CONTENT_TYPE, child.get(ContentConstants.CONTENT_TYPE))
                    }})
                hierarchy.get(parentId).asInstanceOf[util.Map[String, AnyRef]].get(ContentConstants.CHILDREN).asInstanceOf[util.List[String]].add(id)
                populateHierarchyRequest(child.get(ContentConstants.CHILDREN).asInstanceOf[util.List[util.Map[String, AnyRef]]], nodesModified, hierarchy, id, copyType, request)
            })
        }
    }

    def artifactUpload(node: Node, copiedNode: Node, request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext, ss: StorageService): Future[Node] = {
        val artifactUrl = node.getMetadata.getOrDefault(ContentConstants.ARTIFACT_URL, "").asInstanceOf[String]
        val mimeType = node.getMetadata.get(ContentConstants.MIME_TYPE).asInstanceOf[String]
        val contentType = node.getMetadata.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String]
        if (StringUtils.isNotBlank(artifactUrl) && !restrictedMimeTypesForUpload.contains(mimeType)) {
            val mimeTypeManager = MimeTypeManagerFactory.getManager(contentType, mimeType)
            val uploadFuture = if (isInternalUrl(artifactUrl)) {
                val file = copyURLToFile(copiedNode.getIdentifier, artifactUrl)
                if (mimeTypeManager.isInstanceOf[H5PMimeTypeMgrImpl])
                    mimeTypeManager.asInstanceOf[H5PMimeTypeMgrImpl].copyH5P(file, copiedNode)
                else
                    mimeTypeManager.upload(copiedNode.getIdentifier, copiedNode, file, None, UploadParams())
            } else mimeTypeManager.upload(copiedNode.getIdentifier, copiedNode, node.getMetadata.getOrDefault(ContentConstants.ARTIFACT_URL, "").asInstanceOf[String], None, UploadParams())
            uploadFuture.map(uploadData => {
                DataNode.update(getUpdateRequest(request, copiedNode, uploadData.getOrElse(ContentConstants.ARTIFACT_URL, "").asInstanceOf[String]))
            }).flatMap(f => f)
        } else Future(copiedNode)
    }

    def validateShallowCopyReq(node: Node, request: Request) = {
        val copyType: String = request.getRequest.get("copyType").asInstanceOf[String]
        if(StringUtils.equalsIgnoreCase("shallow", copyType) && !StringUtils.equalsIgnoreCase("Live", node.getMetadata.get("status").asInstanceOf[String]))
            throw new ClientException(ContentConstants.ERR_INVALID_REQUEST, "Content with status " + node.getMetadata.get(ContentConstants.STATUS).asInstanceOf[String].toLowerCase + " cannot be partially (shallow) copied.")
        //TODO: check if need to throw client exception for combination of copyType=shallow and mode=edit
    }

    def updateToCopySchemeContentType(request: Request, contentType: String, metadata: util.Map[String, AnyRef]): Unit = {
        if (StringUtils.isNotBlank(request.getContext.getOrDefault(ContentConstants.COPY_SCHEME, "").asInstanceOf[String]))
            metadata.put(ContentConstants.CONTENT_TYPE, copySchemeMap.getOrDefault(contentType, contentType))
    }

    def updateHierarchyV2(request: Request, node: Node, originNode: Node, originHierarchy: util.Map[String, AnyRef], copyType: String)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Node] = {
        val updateHierarchyRequest = prepareHierarchyRequestV2(originHierarchy, originNode, node, copyType, request)
        val keysToExtract = Seq(ContentConstants.NODES_MODIFIED, ContentConstants.HIERARCHY)
        val questionSetHierarchy: util.Map[String, AnyRef] = new util.HashMap()
        val hierarchy = updateHierarchyRequest.get(ContentConstants.HIERARCHY).asInstanceOf[util.HashMap[String, Object]]
        val questionSetList: util.ArrayList[String] = new util.ArrayList[String]()
        if (MapUtils.isNotEmpty(hierarchy)) {
            val rootObject = hierarchy.get(node.getIdentifier).asInstanceOf[util.HashMap[String, Object]]
            if (MapUtils.isNotEmpty(rootObject)){
                val childrens = rootObject.get(ContentConstants.CHILDREN).asInstanceOf[util.ArrayList[String]]
                // Do the segregation for the QuestionSet Object from the Parent hierarchy
                keysToExtract.foreach { key =>
                    val sectionObj = updateHierarchyRequest.get(key)
                    if (sectionObj != null && sectionObj.isInstanceOf[util.Map[_, _]]) {
                        val section = sectionObj.asInstanceOf[util.Map[String, AnyRef]]
                        val extracted = new util.HashMap[String, AnyRef]()
                        val iterator = section.entrySet().iterator()
                        while (iterator.hasNext) {
                            val entry = iterator.next()
                            val original = entry.getValue.asInstanceOf[util.Map[String, AnyRef]]
                            val valueMap = new util.HashMap[String, AnyRef](original)
                            val objectType = Option(valueMap.get("objectType")).map(_.toString).getOrElse("")
                            if (objectType == "QuestionSet") {
                                if (childrens.contains(entry.getKey)) {
                                    questionSetList.add(entry.getKey)
                                    valueMap.put(ContentConstants.ROOT, true.asInstanceOf[AnyRef])
                                    original.put(ContentConstants.CHILDREN, new util.ArrayList[String]())
                                } else {
                                    iterator.remove()
                                }
                                extracted.put(entry.getKey, valueMap)
                            }
                        }
                        if (!extracted.isEmpty) {
                            questionSetHierarchy.put(key, extracted)
                        }
                    }
                }
            }
        }

        //Generating hierarchy update metadata for the QuestionSet object
        if (MapUtils.isNotEmpty(questionSetHierarchy)) {
            extractFullHierarchies(questionSetHierarchy).foreach {
                case (_, fullTreeMap) =>
                    val hierarchyDataNode = questionSetHierarchy.get(ContentConstants.HIERARCHY).asInstanceOf[util.Map[String, util.Map[String, Object]]]
                    val hierarchyRequest = new util.HashMap[String, Object]()
                    val hierarchy = new util.HashMap[String, Object]()
                    val nodesModified = new util.HashMap[String, Object]()
                    val requestDataMap = new util.HashMap[String, Object]()

                    fullTreeMap.foreach { case (key, _) =>
                        val mapValue = hierarchyDataNode.get(key)
                        if (mapValue.containsKey(ContentConstants.METADATA)) {
                            if (mapValue != null) {
                                nodesModified.put(key ,mapValue)
                            }
                        } else {
                            if (mapValue != null) {
                                hierarchy.put(key, mapValue)
                            }
                        }
                    }
                    hierarchyRequest.put(ContentConstants.HIERARCHY, hierarchy)
                    hierarchyRequest.put(ContentConstants.NODES_MODIFIED, nodesModified)
                    requestDataMap.put(ContentConstants.DATA, hierarchyRequest)
                    updateQuestionSetHierarchy(requestDataMap) // Call the Update QuestionSet Hierarchy API

            }
        }


        val hierarchyRequest = new Request(request)
        hierarchyRequest.putAll(updateHierarchyRequest)
        hierarchyRequest.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.COLLECTION_SCHEMA_NAME)
        hierarchyRequest.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
        UpdateHierarchyManager.updateHierarchy(hierarchyRequest).map(_ => node)
    }

    def prepareHierarchyRequestV2(originHierarchy: util.Map[String, AnyRef], originNode: Node, node: Node, copyType: String, request: Request)(implicit ec:ExecutionContext, oec: OntologyEngineContext):util.HashMap[String, AnyRef] = {
        val children:util.List[util.Map[String, AnyRef]] = originHierarchy.getOrDefault(ContentConstants.CHILDREN, new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
        if(null != children && !children.isEmpty) {
            val nodesModified = new util.HashMap[String, AnyRef]()
            val hierarchy = new util.HashMap[String, AnyRef]()
            hierarchy.put(node.getIdentifier, new util.HashMap[String, AnyRef](){{
                put(ContentConstants.CHILDREN, new util.ArrayList[String]())
                put(ContentConstants.ROOT, true.asInstanceOf[AnyRef])
                put(ContentConstants.CONTENT_TYPE, node.getMetadata.get(ContentConstants.CONTENT_TYPE))
            }})
            val result = new util.HashMap[String, AnyRef]()
            try {
                // Blocking call to ensure Future completes before proceeding
                Await.result(
                    populateHierarchyRequestV2(children, nodesModified, hierarchy, node.getIdentifier, copyType, request),
                    Duration.create(copyHierarchyCreatedDelay, TimeUnit.SECONDS)
                )
                result.put(ContentConstants.NODES_MODIFIED, nodesModified)
                result.put(ContentConstants.HIERARCHY, hierarchy)
            } catch {
                case ex: Exception =>
                    TelemetryManager.error("Error while creating the copy: " + ex.getMessage)
                    ex.printStackTrace()
            }
            result
        } else new util.HashMap[String, AnyRef]()
    }

    def populateHierarchyRequestV2(children: java.util.List[java.util.Map[String, AnyRef]], nodesModified: java.util.HashMap[String, AnyRef], hierarchy: java.util.HashMap[String, AnyRef], parentId: String, copyType: String, request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Unit] = {
        if (children == null || children.isEmpty) {
            Future.successful(())
        } else {
            val allowedFieldsSet = Option(request.get(ContentConstants.FIELD_TO_COPY)).map(_.asInstanceOf[java.util.List[String]].asScala.toSet).getOrElse(allowedFieldsFromConfig.asScala.toSet)
            val allowedFieldSetAssessment = allowedFieldsFromConfigForAssessment.asScala.toSet
            val requestMetadata = Option(request.get(ContentConstants.METADATA)).map(_.asInstanceOf[java.util.Map[String, AnyRef]]).getOrElse(new java.util.HashMap[String, AnyRef]())
            children.asScala.foldLeft(Future.successful(())) { (acc, child) =>
                acc.flatMap { _ =>
                    updateToCopySchemeContentType(request, child.get(ContentConstants.CONTENT_TYPE).asInstanceOf[String], child)
                    val objectType = child.get(ContentConstants.OBJECT_TYPE)
                    val cleanedMetadata = new java.util.HashMap[String, AnyRef]()
                    if (objectType.asInstanceOf[String].equalsIgnoreCase(ContentConstants.QUESTION_SET)) {
                        if (child.get(ContentConstants.SCORE_CUT_OFF_TYPE).asInstanceOf[String].equalsIgnoreCase(ContentConstants.SECTIONAL_LEVEL) ||
                                child.get(ContentConstants.SCORE_CUT_OFF_TYPE).asInstanceOf[String].equalsIgnoreCase(ContentConstants.ASSESSMENT_LEVEL)) {
                            val req = new Request(request)
                            req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.QUESTION_SET)
                            req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
                            req.put(ContentConstants.ROOT_ID, child.get(ContentConstants.IDENTIFIER))
                            req.put(ContentConstants.MODE, child.get(ContentConstants.MODE))

                            val response = getQuestionSetHierarchy(child.get(ContentConstants.IDENTIFIER).asInstanceOf[String])
                            val contentObj = response.getResult.get(ContentConstants.QUESTION_SET_CAMEL_CASE)
                            if (contentObj != null) {
                                val originMap = contentObj.asInstanceOf[util.HashMap[String, Object]]
                                val newChildrenList = new util.ArrayList[String]()
                                val newChildNodes = new util.HashMap[String, util.Map[String, AnyRef]]()
                                val childrenObj = originMap.get(ContentConstants.CHILDREN)
                                if (childrenObj != null && childrenObj.isInstanceOf[util.ArrayList[_]]) {
                                    val childrenList = childrenObj.asInstanceOf[util.ArrayList[_]]
                                    for (childrenObject <- childrenList.asScala) {
                                        val childObject = childrenObject.asInstanceOf[util.Map[String, Object]]
                                        val newUUID = UUIDs.timeBased().toString
                                        val newChild = new util.HashMap[String, AnyRef]()
                                        allowedFieldSetAssessment.foreach { key =>
                                            if (requestMetadata.containsKey(key)) newChild.put(key, requestMetadata.get(key))
                                            else if (childObject.containsKey(key)) newChild.put(key, childObject.get(key))
                                        }
                                        newChildrenList.add(newUUID)
                                        val newChildMap = new util.HashMap[String, AnyRef]()
                                        newChildMap.put(ContentConstants.METADATA, newChild)
                                        newChildMap.put(ContentConstants.ROOT, java.lang.Boolean.FALSE)
                                        newChildMap.put("isNew", java.lang.Boolean.TRUE)
                                        newChildMap.put("setDefaultValue", java.lang.Boolean.FALSE)
                                        newChildMap.put(ContentConstants.OBJECT_TYPE, newChild.get(ContentConstants.OBJECT_TYPE))

                                        newChildNodes.put(newUUID, newChildMap)
                                    }
                                    child.put(ContentConstants.NEW_CHILD_METADATA, newChildNodes)
                                    child.put(ContentConstants.NEW_CHILDREN_ID, newChildrenList)
                                }
                            }
                        } else {
                            child.remove(ContentConstants.CHILDREN)
                        }
                        allowedFieldSetAssessment.foreach { key =>
                            if (requestMetadata.containsKey(key)) cleanedMetadata.put(key, requestMetadata.get(key))
                            else if (child.containsKey(key)) cleanedMetadata.put(key, child.get(key))
                        }
                    } else {
                        allowedFieldsSet.foreach { key =>
                            if (requestMetadata.containsKey(key)) cleanedMetadata.put(key, requestMetadata.get(key))
                            else if (child.containsKey(key)) cleanedMetadata.put(key, child.get(key))
                        }
                    }
                    TelemetryManager.info("the size for allowed data cleanupdata is: " + allowedFieldsSet.size + " : the child MetdataRequest" + child.size())
                    cleanedMetadata.put(ContentConstants.CHILDREN, new java.util.ArrayList[AnyRef]())
                    internalHierarchyProps.foreach(key => cleanedMetadata.remove(key))
                    val req = new Request(request)
                    val createdBy = request.getRequest.getOrDefault(ContentConstants.CREATED_BY, "").asInstanceOf[String]
                    val creatorIDs = requestMetadata.getOrDefault(ContentConstants.CREATOR_IDS, Collections.emptyList[String]()).asInstanceOf[java.util.List[String]]
                    if (StringUtils.isNotBlank(createdBy)) {
                        cleanedMetadata.put(ContentConstants.CREATED_BY, createdBy)
                    }
                    if (CollectionUtils.isNotEmpty(creatorIDs)) {
                        cleanedMetadata.put(ContentConstants.CREATOR_IDS, creatorIDs)
                    }
                    cleanedMetadata.put("code", scala.util.Random.nextInt(900000000) + 1000000000 toString)
                    if (objectType != null && objectType.isInstanceOf[String] && objectType.asInstanceOf[String].equalsIgnoreCase(ContentConstants.QUESTION_SET)) {
                        req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.QUESTION_SET)
                        cleanedMetadata.remove(ContentConstants.CREATOR)
                        cleanedMetadata.remove(ContentConstants.CREATOR_IDS)
                        req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
                    } else if (objectType != null && objectType.isInstanceOf[String] && objectType.asInstanceOf[String].equalsIgnoreCase(ContentConstants.QUESTION)) {
                        req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.QUESTION)
                        cleanedMetadata.remove(ContentConstants.CREATOR)
                        cleanedMetadata.remove(ContentConstants.CREATOR_IDS)
                        req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
                    } else {
                        req.getContext.put(ContentConstants.SCHEMA_NAME, ContentConstants.CONTENT_SCHEMA_NAME)
                        req.getContext.put(ContentConstants.VERSION, ContentConstants.SCHEMA_VERSION)
                    }
                    req.setRequest(cleanedMetadata)
                    DataNode.create(req).flatMap { node =>
                        val identifier = node.getIdentifier
                        hierarchy.get(parentId).asInstanceOf[util.Map[String, AnyRef]].get(ContentConstants.CHILDREN).asInstanceOf[util.List[String]].add(identifier)
                        hierarchy.put(identifier, new util.LinkedHashMap[String, AnyRef]() {{
                            put(ContentConstants.CHILDREN, new util.ArrayList[String]())
                            put(ContentConstants.ROOT, false.asInstanceOf[AnyRef])
                            put(ContentConstants.CONTENT_TYPE, child.get(ContentConstants.CONTENT_TYPE))
                            put(ContentConstants.OBJECT_TYPE, node.getMetadata.get(ContentConstants.OBJECT_TYPE))
                        }})
                        if (child.get(ContentConstants.NEW_CHILD_METADATA) != null && child.get(ContentConstants.NEW_CHILDREN_ID) != null) {
                            hierarchy.get(identifier).asInstanceOf[util.Map[String, AnyRef]].get(ContentConstants.CHILDREN).asInstanceOf[util.List[String]].
                              addAll(child.get(ContentConstants.NEW_CHILDREN_ID).asInstanceOf[util.List[String]])
                            nodesModified.putAll(child.get(ContentConstants.NEW_CHILD_METADATA).asInstanceOf[java.util.Map[String, AnyRef]])
                            child.remove(ContentConstants.NEW_CHILDREN_ID)
                            child.remove(ContentConstants.NEW_CHILD_METADATA)
                            child.remove(ContentConstants.CHILDREN)
                        }
                        val childChildren = child.get(ContentConstants.CHILDREN).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                        populateHierarchyRequestV2(childChildren, nodesModified, hierarchy, identifier, copyType, request).map(_ => ())
                    }.recoverWith {
                        case ex: Exception =>
                            TelemetryManager.error(s"Failed to create node for child identifier ${child.get("identifier")}: ${ex.getMessage}", ex)
                            val failedId = Option(child.get("identifier")).getOrElse("unknown-child").toString
                            nodesModified.put(failedId + "_error", new java.util.HashMap[String, AnyRef]() {{
                                put("error", ex.getMessage)
                                put("stackTrace", ex.getStackTrace.mkString("\n"))
                            }})
                            Future.successful(())
                    }
                }
            }
        }
    }

    def updateQuestionSetHierarchy(questionSetMetData: util.Map[String, Object])(implicit httpUtil: HttpUtil): String = {
        val requestData = new util.HashMap[String, AnyRef]()
        requestData.put("request", questionSetMetData)
        val httpResponse = httpUtil.patch(questionSetHierarchyUpdateAPI, new ObjectMapper().writeValueAsString(requestData))
        if (200 != httpResponse.status) throw new ServerException("ERR_FETCHING_OBJECT_CATEGORY", "Error while fetching object categories for additional category list.")
        "ok"

    }

    def extractFullHierarchies(data: util.Map[String, AnyRef]): Map[String, Map[String, AnyRef]] = {
        val hierarchyDataNode = data.getOrDefault(ContentConstants.HIERARCHY, new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
        val dataNode = data.getOrDefault(ContentConstants.NODES_MODIFIED, new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
        if (MapUtils.isNotEmpty(dataNode)) {
            hierarchyDataNode.putAll(dataNode);
        }
        if (MapUtils.isNotEmpty(hierarchyDataNode)) {
            def collectAllDescendants(nodeId: String, acc: Map[String, Map[String, AnyRef]]): Map[String, Map[String, AnyRef]] = {
                if (acc.contains(nodeId)) acc
                else {
                    val rawNode = hierarchyDataNode.get(nodeId).asInstanceOf[util.Map[String, AnyRef]]
                    val nodeMap = rawNode.asScala.toMap
                    val updatedAcc = acc + (nodeId -> nodeMap)

                    val children = rawNode.get("children") match {
                        case list: util.List[_] => list.asScala.collect { case id: String => id }
                        case _ => Seq.empty
                    }

                    children.foldLeft(updatedAcc) { case (mapAcc, childId) =>
                        collectAllDescendants(childId, mapAcc)
                    }
                }
            }
            hierarchyDataNode.asScala.collect {
                case (id, rawNode: util.Map[_, _])
                    if rawNode.get("root") == java.lang.Boolean.TRUE =>
                    val fullHierarchy = collectAllDescendants(id, Map.empty)
                    id -> fullHierarchy
            }.toMap
        } else {
            Map.empty[String, Map[String, AnyRef]]
        }
    }

    def getQuestionSetHierarchy(identifier: String)(implicit httpUtil: HttpUtil):  Response= {
        val httpResponse = httpUtil.get(questionSetHierarchyReadAPI + identifier)
        if (200 != httpResponse.status) throw new ServerException("ERR_FETCHING_OBJECT_CATEGORY", "Error while fetching object categories for additional category list.")
        val response: Response = JsonUtils.deserialize(httpResponse.body, classOf[Response])
        response
    }
}
