package org.sunbird.content.actors

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.`object`.importer.{ImportConfig, ImportManager}
import org.sunbird.actor.core.BaseActor
import org.sunbird.cache.impl.RedisCache
import org.sunbird.cassandra.CassandraConnector
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.common.{ContentParams, JsonUtils, Platform, Slug}
import org.sunbird.content.dial.DIALManager
import org.sunbird.content.review.mgr.ReviewManager
import org.sunbird.content.upload.mgr.UploadManager
import org.sunbird.content.util._
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.external.store.ExternalStore
import org.sunbird.graph.nodes.DataNode
import org.sunbird.graph.utils.NodeUtil
import org.sunbird.managers.HierarchyManager
import org.sunbird.managers.HierarchyManager.hierarchyPrefix
import org.sunbird.util.RequestUtil

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import java.util.concurrent.CompletionException
import javax.inject.Inject
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, Map}
import scala.concurrent.{ExecutionContext, Future, Promise}

class ContentActor @Inject() (implicit oec: OntologyEngineContext, ss: StorageService) extends BaseActor {

	implicit val ec: ExecutionContext = getContext().dispatcher
	private lazy val importConfig = getImportConfig()
	private lazy val importMgr = new ImportManager(importConfig)
	private val logger: Logger = LoggerFactory.getLogger("ContentActor")
	val excludedCategories: Set[String] = Set(ContentConstants.LEARNING_RESOURCE)
  private val retirementRequestKeyspace: String =
    Platform.getString(ContentConstants.SUNBIRD_COURSE_KEYSPACE, "sunbird_courses")

  private val retirementRequestTable: String =
    Platform.getString(ContentConstants.CONTENT_RETIREMENT_RQST_TABLE, "content_retirement_requests")

  private val retirementRequestStore =
    new ExternalStore(
      retirementRequestKeyspace,
      retirementRequestTable,
      util.Arrays.asList(ContentConstants.RETITEMENT_PRIMARY_KEY)
    )

	private val extendedContentReadCacheTTL: Int = Platform.getInteger(ContentConstants.EXTENDED_CONTENT_READ_CACHE_TTL, 86400)
	private val contentEnrichmentFields: util.List[String] = Platform.config.getStringList(ContentConstants.EXTENDED_CONTENT_ENRICHMENT_FIELDS).asScala.toList.asJava
	private val contentHierarchyFields: Set[String] = Platform.config.getStringList(ContentConstants.EXTENDED_CONTENT_HIERARCHY_CHILDREN_FIELDS).asScala.toSet

	override def onReceive(request: Request): Future[Response] = {
		request.getOperation match {
			case "createContent" => create(request)
			case "readContent" => read(request)
			case "readPrivateContent" => privateRead(request)
			case "updateContent" => update(request)
			case "uploadContent" => upload(request)
			case "retireContent" => retire(request)
			case "copy" => copy(request)
			case "uploadPreSignedUrl" => uploadPreSignedUrl(request)
			case "discardContent" => discard(request)
			case "flagContent" => flag(request)
			case "acceptFlag" => acceptFlag(request)
			case "linkDIALCode" => linkDIALCode(request)
			case "importContent" => importContent(request)
			case "systemUpdate" => systemUpdate(request)
			case "reviewContent" => reviewContent(request)
			case "rejectContent" => rejectContent(request)
			case "adminReadContent" => adminRead(request)
			case "createMLContent" => createMLContent(request)
			case "reviewMLContent" => reviewMLContent(request)
			case "updateReviewStatusMLContent" => updateReviewStatusMLContent(request)
			case "extendedReadContent" => extendedRead(request)
			case _ => ERROR(request.getOperation)
				}
		}

	def create(request: Request): Future[Response] = {
		populateDefaultersForCreation(request)
		RequestUtil.restrictProperties(request)
		val startDateTimeStr = request.getRequest.getOrDefault("startDateTime", "").asInstanceOf[String]
		val endDateTimeStr = request.getRequest.getOrDefault("endDateTime", "").asInstanceOf[String]
		if (StringUtils.isNotBlank(startDateTimeStr) && StringUtils.isNotBlank(endDateTimeStr)) {
			try {
				val inputUtcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX")
				val outputIstFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
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
		request.getRequest.put("cqfVersion", System.currentTimeMillis().toString)
		DataNode.create(request, dataModifier).map(node => {
			ResponseHandler.OK.put("identifier", node.getIdentifier).put("node_id", node.getIdentifier)
				.put("versionKey", node.getMetadata.get("versionKey"))
		})
	}

	def read(request: Request): Future[Response] = {
		val responseSchemaName: String = request.getContext.getOrDefault(ContentConstants.RESPONSE_SCHEMA_NAME, "").asInstanceOf[String]
		val fields: util.List[String] = JavaConverters.seqAsJavaListConverter(request.get("fields").asInstanceOf[String].split(",").filter(field => StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null"))).asJava
		request.getRequest.put("fields", fields)
		DataNode.read(request).map(node => {
			val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), request.getContext.get("version").asInstanceOf[String])
			metadata.put("identifier", node.getIdentifier.replace(".img", ""))
			if (StringUtils.equalsIgnoreCase(metadata.get("visibility").asInstanceOf[String],"Private")) {
				throw new ClientException("ERR_ACCESS_DENIED", "content visibility is private, hence access denied")
			}
			var sa = metadata.get("secureSettings")
			var securityAttribute : util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
			if(sa.isInstanceOf[String]) {
				securityAttribute = JsonUtils.deserialize(sa.asInstanceOf[String], classOf[java.util.Map[String, AnyRef]])
				metadata.put("secureSettings", securityAttribute)
			} else if (sa.isInstanceOf[util.Map[String, AnyRef]]) {
				securityAttribute = metadata.getOrDefault("secureSettings", new util.HashMap[String, AnyRef]).asInstanceOf[util.Map[String, AnyRef]]
			}
			//var securityAttribute : util.Map[String, AnyRef] = metadata.getOrDefault("secureSettings", new util.HashMap[String, AnyRef]).asInstanceOf[util.Map[String, AnyRef]]
			if (MapUtils.isNotEmpty(securityAttribute)) {
				var orgList : util.ArrayList[String] = securityAttribute.getOrDefault("organisation", new util.ArrayList[String]).asInstanceOf[util.ArrayList[String]]
				if (!CollectionUtils.isEmpty(orgList)) {
					//Content should be read by unique org users only.
					var userChannelId : String = request.getRequest.getOrDefault("x-user-channel-id", "").asInstanceOf[String]
					if (!orgList.contains(userChannelId)) {
						throw new ClientException("ERR_ACCESS_DENIED", "User is not allowed to read this content.")
					}
				}
			}
			val response: Response = ResponseHandler.OK
			if (responseSchemaName.isEmpty) {
				response.put("content", metadata)
			} else {
				response.put(responseSchemaName, metadata)
			}
			response
		})
	}

	def privateRead(request: Request): Future[Response] = {
		val responseSchemaName: String = request.getContext.getOrDefault(ContentConstants.RESPONSE_SCHEMA_NAME, "").asInstanceOf[String]
		val fields: util.List[String] = JavaConverters.seqAsJavaListConverter(request.get("fields").asInstanceOf[String].split(",").filter(field => StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null"))).asJava
		request.getRequest.put("fields", fields)
		if (StringUtils.isBlank(request.getRequest.getOrDefault("channel", "").asInstanceOf[String])) throw new ClientException("ERR_INVALID_CHANNEL", "Please Provide Channel!")
		DataNode.read(request).map(node => {
			val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), request.getContext.get("version").asInstanceOf[String])
			metadata.put("identifier", node.getIdentifier.replace(".img", ""))
			val response: Response = ResponseHandler.OK
				if (StringUtils.equalsIgnoreCase(metadata.getOrDefault("channel", "").asInstanceOf[String],request.getRequest.getOrDefault("channel", "").asInstanceOf[String])) {
					if (responseSchemaName.isEmpty) {
						response.put("content", metadata)
					}
					else {
						response.put(responseSchemaName, metadata)
					}
					response
				}
				else {
					throw new ClientException("ERR_ACCESS_DENIED", "Channel id is not matched")
				}
		})
	}

	def update(request: Request): Future[Response] = {
		val startDateTimeStr = request.getRequest.getOrDefault("startDateTime", "").asInstanceOf[String]
		val endDateTimeStr = request.getRequest.getOrDefault("endDateTime", "").asInstanceOf[String]
		if (StringUtils.isNotBlank(startDateTimeStr) && StringUtils.isNotBlank(endDateTimeStr)) {
			try {
				val inputUtcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX")
				val outputIstFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
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
		if (StringUtils.isBlank(request.getRequest.getOrDefault("versionKey", "").asInstanceOf[String])) throw new ClientException("ERR_INVALID_REQUEST", "Please Provide Version Key!")
		RequestUtil.restrictProperties(request)
		val reviewStatus: String = request.getRequest.getOrDefault("reviewStatus", "").asInstanceOf[String]
		if(reviewStatus == null ||  reviewStatus.isEmpty ) {
			request.getRequest.put("cqfVersion", System.currentTimeMillis().toString)
		}
		DataNode.update(request, dataModifier).map(node => {
			val identifier: String = node.getIdentifier.replace(".img", "")
			val courseCategory = node.getMetadata.get(ContentConstants.COURSE_CATEGORY).asInstanceOf[String]
			logger.info("The courseCategory is: " + courseCategory)
			if (StringUtils.isNotBlank(courseCategory) && courseCategory.equalsIgnoreCase(ContentConstants.MULTILINGUAL_COURSE)) {
				val status: String = request.getRequest.getOrDefault("status", "").asInstanceOf[String]
				val reviewStatus: String = request.getRequest.getOrDefault("reviewStatus", "").asInstanceOf[String]
				if (StringUtils.isNotBlank(status)) {
					syncLanguageMapStatus(identifier, status, reviewStatus)
				} else {
					logger.info("The status is not present into the requestMap: " + identifier)
				}
			}
			val resourceCategoryOpt = Option(node.getMetadata.get("resourceCategory")).map(_.asInstanceOf[String])
			val primaryCategoryOpt = Option(node.getMetadata.get("primaryCategory")).map(_.asInstanceOf[String])
			val categoryToCheck = resourceCategoryOpt.filter(_.nonEmpty).orElse(primaryCategoryOpt).getOrElse("")
      val status: String = request.getRequest.getOrDefault(ContentConstants.STATUS, "").asInstanceOf[String]
      val restrictNotification = status.equalsIgnoreCase(ContentConstants.REVIEW) && reviewStatus.equalsIgnoreCase(ContentConstants.REVIEWED)
			//TODO: THIS BLOCK NEED TO BE OPTIMIZE TO HANDLE UPDATE REVIEW STATUS USE CASES.
			if (request.getContext.getOrDefault("sendNotification", Boolean.box(false)).asInstanceOf[Boolean] && !excludedCategories.contains(categoryToCheck) && !restrictNotification) {
				try {
					NotificationManager.sendNotification(
						"CONTENT_EDITED",
						"UPDATE",
						List(node.getMetadata.get("createdBy").asInstanceOf[String]),
						node.getMetadata.get("name").asInstanceOf[String],
						Map[String, Any]("id" -> identifier)
					)
					logger.info(s"Notification sent | identifier=$identifier | resourceCategory=$categoryToCheck")
				} catch {
					case e: Exception => logger.info("Error while sending notification ", e)
				}
			}
			ResponseHandler.OK.put("node_id", identifier).put("identifier", identifier)
				.put("versionKey", node.getMetadata.get("versionKey"))
		})
	}

	def upload(request: Request): Future[Response] = {
		val identifier: String = request.getContext.getOrDefault("identifier", "").asInstanceOf[String]
		val readReq = new Request(request)
		readReq.put("identifier", identifier)
		readReq.put("fields", new util.ArrayList[String])
		DataNode.read(readReq).map(node => {
			if (null != node & StringUtils.isNotBlank(node.getObjectType))
				request.getContext.put("schemaName", node.getObjectType.toLowerCase())
			UploadManager.upload(request, node)
		}).flatMap(f => f)
	}

	def copy(request: Request): Future[Response] = {
		RequestUtil.restrictProperties(request)
		CopyManager.copy(request)
	}

	def uploadPreSignedUrl(request: Request): Future[Response] = {
		val `type`: String = request.get("type").asInstanceOf[String].toLowerCase()
		val fileName: String = request.get("fileName").asInstanceOf[String]
		val filePath: String = request.getRequest.getOrDefault("filePath","").asInstanceOf[String]
			.replaceAll("^/+|/+$", "")
		val identifier: String = request.get("identifier").asInstanceOf[String]
		validatePreSignedUrlRequest(`type`, fileName, filePath)
		DataNode.read(request).map(node => {
			val objectKey = if (StringUtils.isEmpty(filePath)) "content" + File.separator + `type` + File.separator + identifier + File.separator + Slug.makeSlug(fileName, true)
				else filePath + File.separator + "content" + File.separator + `type` + File.separator + identifier + File.separator + Slug.makeSlug(fileName, true)
			val expiry = Platform.config.getString("cloud_storage.upload.url.ttl")
			val preSignedURL = ss.getSignedURL(objectKey, Option.apply(expiry.toInt), Option.apply("w"))
			ResponseHandler.OK().put("identifier", identifier).put("pre_signed_url", preSignedURL)
				.put("url_expiry", expiry)
		}) recoverWith { case e: CompletionException => throw e.getCause }
	}

	def retire(request: Request): Future[Response] = {
		RetireManager.retire(request)
	}

	def discard(request: Request): Future[Response] = {
		RequestUtil.restrictProperties(request)
		DiscardManager.discard(request)
	}

	def flag(request: Request): Future[Response] = {
		FlagManager.flag(request)
	}

	def acceptFlag(request: Request): Future[Response] = {
		AcceptFlagManager.acceptFlag(request)
	}

	def linkDIALCode(request: Request): Future[Response] = DIALManager.link(request)

	def importContent(request: Request): Future[Response] = importMgr.importObject(request)

	def reviewContent(request: Request): Future[Response] = {
		val identifier: String = request.getContext.getOrDefault("identifier", "").asInstanceOf[String]
		val readReq = new Request(request)
		readReq.put("identifier", identifier)
		readReq.put("mode", "edit")
		DataNode.read(readReq).map(node => {
			if (null != node & StringUtils.isNotBlank(node.getObjectType))
				request.getContext.put("schemaName", node.getObjectType.toLowerCase())
			if (StringUtils.equalsAnyIgnoreCase("Processing", node.getMetadata.getOrDefault("status", "").asInstanceOf[String]))
				throw new ClientException("ERR_NODE_ACCESS_DENIED", "Review Operation Can't Be Applied On Node Under Processing State")
			else {
				val response = ReviewManager.review(request, node)
				try {
					val reviewers = node.getMetadata.get("reviewerIDs") match {
						case arr: Array[String] => arr.toList
						case list: java.util.List[_] => list.asScala.toList.map(_.toString)
					}
					if (reviewers.nonEmpty) {
						NotificationManager.sendNotification(
							"CONTENT_REVIEW_REQUEST",
							"ALERT",
							reviewers,
							node.getMetadata.get("name").asInstanceOf[String],
							Map[String, Any]("id" -> identifier)
						)
					} else {
						logger.warn("No reviewers found for content with identifier: " + identifier)
					}
				} catch {
					case e: Exception => logger.info("Error while sending notification ", e)
				}
				val courseCategory = node.getMetadata.get(ContentConstants.COURSE_CATEGORY).asInstanceOf[String]
				logger.info("The courseCategory inside review method is: " + courseCategory)
				if (StringUtils.isNotBlank(courseCategory) && courseCategory.equalsIgnoreCase(ContentConstants.MULTILINGUAL_COURSE)) {
					syncLanguageMapStatus(identifier, "Review", "InReview")
				}
			}
			Future.successful(ResponseHandler.OK())
		}).flatMap(f => f)
	}


	def populateDefaultersForCreation(request: Request) = {
		setDefaultsBasedOnMimeType(request, ContentParams.create.name)
		setDefaultLicense(request)
	}

	private def setDefaultLicense(request: Request): Unit = {
		if (StringUtils.isEmpty(request.getRequest.getOrDefault("license", "").asInstanceOf[String])) {
			val cacheKey = "channel_" + request.getRequest.getOrDefault("channel", "").asInstanceOf[String] + "_license"
			val defaultLicense = RedisCache.get(cacheKey, null, 0)
			if (StringUtils.isNotEmpty(defaultLicense)) request.getRequest.put("license", defaultLicense)
			else println("Default License is not available for channel: " + request.getRequest.getOrDefault("channel", "").asInstanceOf[String])
		}
	}

	def populateDefaultersForUpdation(request: Request) = {
		if (request.getRequest.containsKey(ContentParams.body.name)) request.put(ContentParams.artifactUrl.name, null)
	}

	private def setDefaultsBasedOnMimeType(request: Request, operation: String): Unit = {
		val mimeType = request.get(ContentParams.mimeType.name).asInstanceOf[String]
		if (StringUtils.isNotBlank(mimeType) && operation.equalsIgnoreCase(ContentParams.create.name)) {
			if (StringUtils.equalsIgnoreCase("application/vnd.ekstep.plugin-archive", mimeType)) {
				val code = request.get(ContentParams.code.name).asInstanceOf[String]
				if (null == code || StringUtils.isBlank(code)) throw new ClientException("ERR_PLUGIN_CODE_REQUIRED", "Unique code is mandatory for plugins")
				request.put(ContentParams.identifier.name, request.get(ContentParams.code.name))
			}
			else request.put(ContentParams.osId.name, "org.ekstep.quiz.app")
			if (mimeType.endsWith("archive") || mimeType.endsWith("vnd.ekstep.content-collection") || mimeType.endsWith("epub")) request.put(ContentParams.contentEncoding.name, ContentParams.gzip.name)
			else request.put(ContentParams.contentEncoding.name, ContentParams.identity.name)
			if (mimeType.endsWith("youtube") || mimeType.endsWith("x-url")) request.put(ContentParams.contentDisposition.name, ContentParams.online.name)
			else request.put(ContentParams.contentDisposition.name, ContentParams.inline.name)
		}
	}

	private def validatePreSignedUrlRequest(`type`: String, fileName: String, filePath: String): Unit = {
		if (StringUtils.isEmpty(fileName))
			throw new ClientException("ERR_CONTENT_BLANK_FILE_NAME", "File name is blank")
		if (StringUtils.isBlank(FilenameUtils.getBaseName(fileName)) || StringUtils.length(Slug.makeSlug(fileName, true)) > 256)
			throw new ClientException("ERR_CONTENT_INVALID_FILE_NAME", "Please Provide Valid File Name.")
		if (!preSignedObjTypes.contains(`type`))
			throw new ClientException("ERR_INVALID_PRESIGNED_URL_TYPE", "Invalid pre-signed url type. It should be one of " + StringUtils.join(preSignedObjTypes, ","))
		if(StringUtils.isNotBlank(filePath) && filePath.size > 100)
			throw new ClientException("ERR_CONTENT_INVALID_FILE_PATH", "Please provide valid filepath of character length 100 or Less ")
	}

	def dataModifier(node: Node): Node = {
		if(node.getMetadata.containsKey("trackable") &&
				node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].containsKey("enabled") &&
		"Yes".equalsIgnoreCase(node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("enabled", "").asInstanceOf[String])) {
			node.getMetadata.put("contentType", "Course")
		}
		node
	}

	def getImportConfig(): ImportConfig = {
		val requiredProps = Platform.getStringList("import.required_props", java.util.Arrays.asList("name", "code", "mimeType", "contentType", "artifactUrl", "framework")).asScala.toList
		val validStages = Platform.getStringList("import.valid_stages", java.util.Arrays.asList("create", "upload", "review", "publish")).asScala.toList
		val propsToRemove = Platform.getStringList("import.remove_props", java.util.Arrays.asList("downloadUrl", "variants", "previewUrl", "streamingUrl", "itemSets")).asScala.toList
		val topicName = Platform.config.getString("import.output_topic_name")
		val reqLimit = Platform.getInteger("import.request_size_limit", 200)
		val validSourceStatus = Platform.getStringList("import.valid_source_status", java.util.Arrays.asList()).asScala.toList
		ImportConfig(topicName, reqLimit, requiredProps, validStages, propsToRemove, validSourceStatus)
	}

	def systemUpdate(request: Request): Future[Response] = {
		val identifier = request.getContext.get("identifier").asInstanceOf[String]
		RequestUtil.validateRequest(request)
		RedisCache.delete(hierarchyPrefix + request.get("rootId"))
		RedisCache.delete(identifier)

		val readReq = new Request(request)
		val identifiers = new util.ArrayList[String](){{
			add(identifier)
			if (!identifier.endsWith(".img"))
				add(identifier.concat(".img"))
		}}
		readReq.put("identifiers", identifiers)
		DataNode.list(readReq).flatMap(response => {
			val objectType = request.getContext.get("objectType").asInstanceOf[String]
			if (objectType.toLowerCase.equals("collection"))
				DataNode.systemUpdate(request, response, "content", Option(HierarchyManager.getHierarchy))
			else
				DataNode.systemUpdate(request, response,"", None)
		}).map(node => {
      try {
        val reviewStatus = Option(request.get(ContentConstants.REVIEW_STATUS)).map(_.toString).getOrElse("")
        if (ContentConstants.REVIEWED.equalsIgnoreCase(reviewStatus) || ContentConstants.SEND_TO_PUBLISH.equalsIgnoreCase(reviewStatus)) {
          NotificationManager.sendNotification(
            ContentConstants.CONTENT_EDITED,
            ContentConstants.UPDATE,
            List(node.getMetadata.get(ContentConstants.CREATED_BY).asInstanceOf[String]),
            node.getMetadata.get(ContentConstants.NAME).asInstanceOf[String],
            Map[String, Any](ContentConstants.ID -> identifier)
          )
        }
      } catch {
        case e: Exception => logger.info("Error while sending notification ", e)
      }
			ResponseHandler.OK.put("identifier", identifier).put("status", "success")
		})
	}

	def rejectContent(request: Request): Future[Response] = {
		RequestUtil.validateRequest(request)
		val id: String = request.getContext.getOrDefault("identifier", "").asInstanceOf[String]
		DataNode.read(request).map(node => {
			val status = node.getMetadata.get("status").asInstanceOf[String]
			if (StringUtils.isBlank(status))
				throw new ClientException("ERR_METADATA_ISSUE", "Content metadata error, status is blank for identifier:" + node.getIdentifier)
      if (StringUtils.equals("Review", status)) {
        request.getRequest.put("status", "Draft")
				request.getRequest.put("prevStatus", "Review")
      } else if (StringUtils.equals("FlagReview", status)) {
        request.getRequest.put("status", "FlagDraft")
				request.getRequest.put("prevStatus", "FlagReview")
			}
      else new ClientException("ERR_INVALID_REQUEST", "Content not in Review status.")

			request.getRequest.put("versionKey", node.getMetadata.get("versionKey"))
			request.putIn("publishChecklist", null).putIn("publishComment", null)
      //updating node after changing the status
			RequestUtil.restrictProperties(request)
			DataNode.update(request).map(node => {
				val identifier: String = node.getIdentifier.replace(".img", "")
				try {
					if(node.getMetadata.containsKey("reviewerIDs")) {
						NotificationManager.sendNotification(
							"CONTENT_REJECTED",
							"UPDATE",
							List(node.getMetadata.get("createdBy").asInstanceOf[String]),
							node.getMetadata.get("name").asInstanceOf[String],
							Map[String, Any]("id" -> node.getMetadata.get("identifier").asInstanceOf[String])
						)
					}
				} catch {
					case e: Exception => logger.info("Error while sending notification ", e)
				}
				val courseCategory = node.getMetadata.get(ContentConstants.COURSE_CATEGORY).asInstanceOf[String]
				logger.info("The courseCategory inside reject method is: " + courseCategory)
				if (StringUtils.isNotBlank(courseCategory) && courseCategory.equalsIgnoreCase(ContentConstants.MULTILINGUAL_COURSE)) {
					  val reviewStatus: String = request.getRequest.getOrDefault("reviewStatus", "").asInstanceOf[String]
						syncLanguageMapStatus(identifier, "Draft", reviewStatus)
				}
				ResponseHandler.OK.put("node_id", identifier).put("identifier", identifier)
			})
		}).flatMap(f => f)
	}

	def adminRead(request: Request): Future[Response] = {
		val responseSchemaName: String = request.getContext.getOrDefault(ContentConstants.RESPONSE_SCHEMA_NAME, "").asInstanceOf[String]
		val fields: util.List[String] = JavaConverters.seqAsJavaListConverter(request.get("fields").asInstanceOf[String].split(",").filter(field => StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null"))).asJava
		request.getRequest.put("fields", fields)
		DataNode.read(request).map(node => {
			val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), request.getContext.get("version").asInstanceOf[String])
			metadata.put("identifier", node.getIdentifier.replace(".img", ""))
			val response: Response = ResponseHandler.OK
			if (responseSchemaName.isEmpty) {
				response.put("content", metadata)
			} else {
				response.put(responseSchemaName, metadata)
			}
			response
		})
	}

	def createMLContent(request: Request)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
		val sourceCollectionId = request.getRequest.get("sourceCollectionId").asInstanceOf[String]
		val languages = request.getRequest.get("language").asInstanceOf[java.util.List[String]]
		val createdBy = request.getRequest.get("createdBy").asInstanceOf[String]
		val creator = request.getRequest.get("creator").asInstanceOf[String]
		val createdFor = request.getRequest.get("createdFor").asInstanceOf[java.util.List[String]]
		val organisation = request.getRequest.get("organisation").asInstanceOf[java.util.List[String]]
		val creatorContacts = request.getRequest.get("creatorContacts").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
		val channel = request.getRequest.get("channel").asInstanceOf[String]

		val readRequest = new Request()
		readRequest.setContext(new java.util.HashMap[String, AnyRef]() {{
			put("graph_id", "domain")
			put("version", "1.0")
			put("objectType", "Content")
			put("schemaName", "content")
		}})
		readRequest.setObjectType("Content")
		readRequest.put("identifier", sourceCollectionId)
		readRequest.put("mode", "read")
		readRequest.put("fields", new util.ArrayList[String]())

		DataNode.read(readRequest).flatMap { node =>
			val metadata = node.getMetadata
			val status = metadata.getOrDefault("status", "").asInstanceOf[String]
			val name = metadata.getOrDefault("name", "").asInstanceOf[String]

			if (!StringUtils.equalsIgnoreCase(status, "Live"))
				throw new ClientException("ERR_INVALID_CONTENT_STATUS", s"Content $sourceCollectionId must be in Live status")

			val existingLanguageMap = metadata.getOrDefault("languageMapV1", new util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
			val sourceLangList = metadata.getOrDefault("language", new util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
			val baseLang = if (CollectionUtils.isNotEmpty(sourceLangList)) sourceLangList.get(0).toLowerCase else throw new ClientException("ERR_MISSING_LANGUAGE", "Source content must have one language")
			val versionKey = metadata.getOrDefault("versionKey", "").asInstanceOf[String]
			val contentType = metadata.getOrDefault("contentType", "").asInstanceOf[String]
			val mimeType = metadata.getOrDefault("mimeType", "").asInstanceOf[String]
			val posterImage = metadata.getOrDefault("posterImage", "").asInstanceOf[String]
			val appIcon = metadata.getOrDefault("appIcon", "").asInstanceOf[String]
			val creatorLogo = metadata.getOrDefault("creatorLogo", "").asInstanceOf[String]

			val creationFutures = languages.asScala.map { lang =>
				val contentMap = new java.util.HashMap[String, AnyRef]()
				contentMap.put("contentType", contentType)
				contentMap.put("mimeType", mimeType)
				contentMap.put("courseCategory", "Multilingual Course")
				contentMap.put("primaryCategory", "Course")
				contentMap.put("language", util.Arrays.asList(lang.capitalize))
				contentMap.put("code", scala.util.Random.nextInt(900000000) + 1000000000 toString)
				contentMap.put("channel", channel)
				contentMap.put("createdBy", createdBy)
				contentMap.put("creator", creator)
				contentMap.put("createdFor", createdFor)
				contentMap.put("organisation", organisation)
				contentMap.put("creatorContacts", creatorContacts)
				contentMap.put("name", name + " - " + lang.capitalize)
				contentMap.put("appIcon", appIcon)
				contentMap.put("posterImage", posterImage)
				if (StringUtils.isNotBlank(creatorLogo)) {
					contentMap.put("creatorLogo", creatorLogo)
				}

				val createRequest = new Request()
				createRequest.setOperation("createContent")
				createRequest.setRequest(contentMap)
				createRequest.setContext(new java.util.HashMap[String, AnyRef]() {{
					put("graph_id", "domain")
					put("version", "1.0")
					put("objectType", "Collection")
					put("schemaName", "collection")
				}})

				create(createRequest).map(resp => lang.toLowerCase -> resp.get("identifier").asInstanceOf[String])
			}

			Future.sequence(creationFutures).flatMap { createdEntries =>
				// Combine all entries (existing + new + baseLang) into one map with lowercase keys
				val finalLangMap = new java.util.HashMap[String, AnyRef]()
				val baseLangMap = new java.util.HashMap[String, AnyRef]()
				// Add existing entries
				existingLanguageMap.forEach(new java.util.function.BiConsumer[String, AnyRef] {
					override def accept(k: String, v: AnyRef): Unit = finalLangMap.put(k.toLowerCase, v)
				})
				// Add new entries
				createdEntries.foreach { case (lang, id) =>
					finalLangMap.put(lang.toLowerCase, new java.util.HashMap[String, AnyRef]() {{
						put("id", id)
						put("status", "draft")
						put("createdBy", createdBy)
						put("isBaseLang", Boolean.box(false))
					}})
				}
				// Ensure baseLang is present
				finalLangMap.put(baseLang.toLowerCase, new java.util.HashMap[String, AnyRef]() {{
					put("id", sourceCollectionId)
					put("status", status)
					put("createdBy", createdBy)
					put("isBaseLang", Boolean.box(true))
				}})

				baseLangMap.put(baseLang.toLowerCase, new java.util.HashMap[String, AnyRef]() {{
					put("id", sourceCollectionId)
					put("status", status)
					put("createdBy", createdBy)
					put("isBaseLang", Boolean.box(true))
				}})

				// Update all nodes (newly created + existing languageMapV1) with latest languageMapV1
				val allLangNodes = finalLangMap.asScala.toSeq.map { case (lang, map) =>
					lang -> map.asInstanceOf[java.util.Map[String, AnyRef]].get("id").asInstanceOf[String]
				}

				val updateFutures = allLangNodes.map { case (lang, id) =>
					val readReq = new Request()
					readReq.setContext(new java.util.HashMap[String, AnyRef]() {{
						put("graph_id", "domain")
						put("version", "1.0")
						put("objectType", "Content")
						put("schemaName", "content")
					}})
					readReq.setObjectType("Content")
					readReq.put("identifier", id)
					readReq.put("mode", "read")

					DataNode.read(readReq).flatMap { node =>
						val nodeVersionKey = node.getMetadata.getOrDefault("versionKey", "").asInstanceOf[String]
						val status = node.getMetadata.get(ContentConstants.STATUS).asInstanceOf[String]
						val updateReq = new Request()
						updateReq.setOperation("systemUpdate")
						updateReq.setRequest(new java.util.HashMap[String, AnyRef]() {{
							if (status.equalsIgnoreCase("Live")) {
								put("languageMapV1", finalLangMap)
							} else {
								put("languageMapV1", baseLangMap)
							}
							put("versionKey", nodeVersionKey)
						}})
						updateReq.setContext(new java.util.HashMap[String, AnyRef]() {{
							put("graph_id", "domain")
							put("version", "1.0")
							put("objectType", "Content")
							put("schemaName", "content")
							put("identifier", id)
						}})

						systemUpdate(updateReq)
					}
				}

				Future.sequence(updateFutures).map { _ =>
					val result = new java.util.HashMap[String, String]()
					createdEntries.foreach { case (lang, id) => result.put(lang.capitalize, id) }
					val response = ResponseHandler.OK()
					response.setId("api.content.ml.create")
					response.put("content", result)
					response
				}
			}
  		}
	}

	private def syncLanguageMapStatus(identifier: String, status: String, reviewStatus: String): Future[Response] = {
		logger.info("ContentActor: syncLanguageMapStatus called for identifier: " + identifier + " with status: " + status + ", reviewStatus" + reviewStatus)
		val confirmReadReq = new Request()
		confirmReadReq.setContext(new java.util.HashMap[String, AnyRef]() {{
			put("graph_id", "domain")
			put("version", "1.0")
			put("objectType", "Content")
			put("schemaName", "content")
		}})
		confirmReadReq.setObjectType("Content")
		confirmReadReq.put("identifier", identifier)
		confirmReadReq.put("mode", "edit")

		DataNode.read(confirmReadReq).flatMap { confirmedNode =>
			val languageMapRaw = confirmedNode.getMetadata.getOrDefault("languageMapV1", new util.HashMap[String, AnyRef]())
			val publisherIDs =	confirmedNode.getMetadata.getOrDefault("publisherIDs", new util.ArrayList[String]())
			val reviewerIDs = confirmedNode.getMetadata.getOrDefault("reviewerIDs", new util.ArrayList[String]())
			val languageMap = languageMapRaw match {
				case s: String => JsonUtils.deserialize(s, classOf[java.util.Map[String, AnyRef]])
				case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]]
				case _ => new util.HashMap[String, AnyRef]()
			}
			logger.info("ContentActor: syncLanguageMapStatus - latestStatus: " + status + ", languageMap: " + languageMap)
			if (MapUtils.isNotEmpty(languageMap)) {
				val updatedLanguageMap = new util.HashMap[String, AnyRef]()
				val updatedBaseLanguageMap = new util.HashMap[String, AnyRef]()
				languageMap.forEach(new java.util.function.BiConsumer[String, AnyRef] {
					override def accept(lang: String, entry: AnyRef): Unit = {
						val entryMap = new util.HashMap[String, AnyRef]()
						entryMap.putAll(entry.asInstanceOf[java.util.Map[String, AnyRef]])
						// Check if "isBaseLang" == true
						val isBaseLang = entryMap.get("isBaseLang") match {
							case b: java.lang.Boolean => b.booleanValue()
							case _ => false
						}
						if (isBaseLang) {
							updatedBaseLanguageMap.put(lang.toLowerCase, entry.asInstanceOf[java.util.Map[String, AnyRef]])
						}
					}
				})
				val updateFutures = updatedBaseLanguageMap.asScala.toSeq.map { case (_, v) =>
					val id = v.asInstanceOf[java.util.Map[String, AnyRef]].get("id").asInstanceOf[String]
					val readNodeReq = new Request()
					readNodeReq.setContext(new util.HashMap[String, AnyRef]() {
						{
							put("graph_id", "domain")
							put("version", "1.0")
							put("objectType", "Content")
							put("schemaName", "content")
						}
					})
					readNodeReq.setObjectType("Content")
					readNodeReq.put("identifier", id)
					readNodeReq.put("mode", "read")

					DataNode.read(readNodeReq).flatMap { n =>
						val versionKey = n.getMetadata.getOrDefault("versionKey", "").asInstanceOf[String]
						val baseLanguageMapRaw = n.getMetadata.getOrDefault("languageMapV1", new util.HashMap[String, AnyRef]())
						val baseLanguageMap = baseLanguageMapRaw match {
							case s: String => JsonUtils.deserialize(s, classOf[java.util.Map[String, AnyRef]])
							case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]]
							case _ => new util.HashMap[String, AnyRef]()
						}
						baseLanguageMap.forEach(new java.util.function.BiConsumer[String, AnyRef] {
							override def accept(lang: String, entry: AnyRef): Unit = {
								val entryMap = new util.HashMap[String, AnyRef]()
								entryMap.putAll(entry.asInstanceOf[java.util.Map[String, AnyRef]])
								if (identifier == entryMap.get("id")) {
									entryMap.put("status", status)
									entryMap.put("reviewStatus", reviewStatus)
									entryMap.put("reviewerIDs", reviewerIDs)
									entryMap.put("publisherIDs", publisherIDs)
								}
								updatedLanguageMap.put(lang.toLowerCase, entryMap)
							}
						})
						logger.info("ContentActor: syncLanguageMapStatus - after language update latestStatus: " + status + ", updatedLanguageMap: " + updatedLanguageMap + " , updatedLanguageBasemAO" + updatedBaseLanguageMap)
						logger.info("ContentActor: syncLanguageMapStatus called for baseLangId: " + id)
						val updateReq = new Request()
						updateReq.setOperation("systemUpdate")
						updateReq.setRequest(new util.HashMap[String, AnyRef]() {
							{
								put("languageMapV1", updatedLanguageMap)
								put("versionKey", versionKey)
							}
						})
						updateReq.setContext(new util.HashMap[String, AnyRef]() {
							{
								put("graph_id", "domain")
								put("version", "1.0")
								put("objectType", "Content")
								put("schemaName", "content")
								put("identifier", id)
							}
						})
						systemUpdate(updateReq)
					}
				}

				Future.sequence(updateFutures).map(_ => ResponseHandler.OK())
			} else {
				Future.successful(ResponseHandler.OK())
			}
		}
	}

	def reviewMLContent(request: Request)(implicit ec: ExecutionContext): Future[Response] = {

		val identifiers: List[String] = request.getRequest.getOrDefault("identifier", List.empty[String])
		match {
			case s: String if StringUtils.isNotBlank(s) => List(s)
			case arr: Array[String]                     => arr.toList
			case list: java.util.List[_]                => list.asScala.toList.map(_.toString)
			case _                                      => List.empty[String]
		}

		identifiers.foldLeft(Future.successful(())) { (acc, identifier) =>
				acc.flatMap { _ =>
					val readReq = new Request(request)
					readReq.put("identifier", identifier)
					readReq.put("mode", "edit")

					DataNode.read(readReq).flatMap { node =>
						if (node != null && StringUtils.isNotBlank(node.getObjectType)) {
							request.getContext.put("schemaName", node.getObjectType.toLowerCase())
						}
						request.getContext.put(ContentConstants.IDENTIFIER, identifier)
						if (StringUtils.equalsAnyIgnoreCase("Processing", node.getMetadata.getOrDefault("status", "").asInstanceOf[String])) {
							Future.failed(new ClientException("ERR_NODE_ACCESS_DENIED", s"Review Operation Can't Be Applied On Node $identifier Under Processing State"))
						} else {
							ReviewManager.review(request, node).flatMap { _ =>
								try {
									val reviewers = node.getMetadata.get("reviewerIDs") match {
										case arr: Array[String] => arr.toList
										case list: java.util.List[_] => list.asScala.toList.map(_.toString)
									}

									if (reviewers.nonEmpty) {
										NotificationManager.sendNotification(
											"CONTENT_REVIEW_REQUEST",
											"ALERT",
											reviewers,
											node.getMetadata.get("name").asInstanceOf[String],
											Map[String, Any]("id" -> identifier)
										)
									} else {
										logger.warn(s"No reviewers found for content with identifier: $identifier")
									}
								} catch {
									case e: Exception => logger.info("Error while sending notification ", e)
								}

								val courseCategory = node.getMetadata.get(ContentConstants.COURSE_CATEGORY).asInstanceOf[String]

								logger.info(s"The courseCategory inside review method is: $courseCategory")

								if (StringUtils.isNotBlank(courseCategory) && courseCategory.equalsIgnoreCase(ContentConstants.MULTILINGUAL_COURSE)) {
									val reviewStatus: String = request.getRequest.getOrDefault("reviewStatus", "").asInstanceOf[String]
									syncLanguageMapStatus(identifier, "Review", reviewStatus).map(_ => ())
								} else {
									Future.successful(())
								}
							}
						}
					}
				}
			}
			.map(_ => ResponseHandler.OK())
	}

	def updateReviewStatusMLContent(request: Request)(implicit ec: ExecutionContext): Future[Response] = {

		val identifiers: List[String] = request.getRequest.getOrDefault("identifier", List.empty[String]) match {
			case s: String if StringUtils.isNotBlank(s) => List(s)
			case arr: Array[String]                     => arr.toList
			case list: java.util.List[_]                => list.asScala.toList.map(_.toString)
			case _                                      => List.empty[String]
		}

		identifiers.foldLeft(Future.successful(Map.empty[String, AnyRef])) { (accFut, identifier) =>
			for {
				acc <- accFut
				node <- {
					val readReq = new Request(request)
					readReq.put("identifier", identifier)
					readReq.put("mode", "edit")
					DataNode.read(readReq)
				}
				updateResponse <- {
					val updateReq = new Request(request)
					updateReq.put("identifier", identifier)
					updateReq.put("reviewStatus", request.getRequest.getOrDefault("reviewStatus", "Reviewed"))
					updateReq.put("status", request.getRequest.getOrDefault("status", "Review"))
					updateReq.getRequest.put(ContentConstants.IDENTIFIER, identifier)
					updateReq.getContext.put(ContentConstants.IDENTIFIER, identifier)
					updateReq.getRequest.put(ContentConstants.VERSION_KEY, node.getMetadata.get(ContentConstants.VERSION_KEY))
					updateReq.getContext.put("sendNotification", Boolean.box(true))
					updateReq.setOperation("updateReviewStatusMLContent")
					update(updateReq) // Future[Response]
				}
			} yield {
				val nodeMap = new java.util.HashMap[String, AnyRef]()
				val responseMap = updateResponse.getResult
				nodeMap.put("response", if (MapUtils.isNotEmpty(responseMap)) responseMap else new util.HashMap())
				acc + (identifier -> nodeMap)
			}
		}.map { resultMap =>
			ResponseHandler.OK().putAll(resultMap.asJava)
		}
	}

	/**
	 * Extended read operation for Learning Pathway that enriches milestones with course hierarchy and assessment details.
	 * Implements Redis caching with key pattern: extended_read_learningpathway_{{identifier}}
	 *
	 * Flow:
	 * 1. Check Redis cache for existing enriched data
	 * 2. If not cached, perform standard content read
	 * 3. Check if content is a Learning Pathway with milestones_v1
	 * 4. Extract course IDs and assessment IDs from all milestones
	 * 5. Fetch course data (with hierarchy) and assessment data in parallel
	 * 6. Merge fetched data back into milestone structure
	 * 7. Cache the enriched response and return
	 *
	 * @param request Request with identifier
	 * @return Future[Response] with enriched milestone data
	 */
	def extendedRead(request: Request): Future[Response] = {
		//Extract identifier and build cache key
		val identifier = request.getRequest.getOrDefault(ContentConstants.IDENTIFIER, "").asInstanceOf[String]
		val cacheKey = s"${ContentConstants.EXTENDED_READ_LEARNINGPATHWAY_CACHE_KEY_PREFIX}$identifier"
		//Check Redis cache for pre-computed enriched data
		val cachedData = RedisCache.get(cacheKey)
		if (cachedData != null && cachedData.nonEmpty) {
			try {
				val cachedResponse = JsonUtils.deserialize(cachedData, classOf[Response])
				return Future.successful(cachedResponse)
			} catch {
				case e: Exception =>
					logger.warn(s"[extendedRead] Cache deserialization failed for $identifier", e)
			}
		}
		//Cache miss - perform standard content read
		read(request).flatMap { response =>
			//Extract content metadata from response
			val responseSchemaName: String = request.getContext.getOrDefault(ContentConstants.RESPONSE_SCHEMA_NAME, "").asInstanceOf[String]
			val contentKey = if (responseSchemaName.isEmpty) ContentConstants.CONTENT else responseSchemaName
			val contentMetadata = response.getResult.get(contentKey).asInstanceOf[util.Map[String, AnyRef]]
			//Check if this is a Learning Pathway with milestones
			val courseCategory = contentMetadata.getOrDefault(ContentConstants.COURSE_CATEGORY, "").asInstanceOf[String]
			if (StringUtils.equalsIgnoreCase(courseCategory, ContentConstants.LEARNING_PATHWAY) &&
				contentMetadata.containsKey(ContentConstants.MILESTONES_V1) &&
				contentMetadata.get(ContentConstants.MILESTONES_V1) != null) {
				//Extract and parse milestones_v1 (handle both String and List types)
				val milestonesRaw = contentMetadata.get(ContentConstants.MILESTONES_V1)
				val milestones: util.List[util.Map[String, AnyRef]] = milestonesRaw match {
					case s: String =>
						JsonUtils.deserialize(s, classOf[java.util.List[java.util.Map[String, AnyRef]]])
					case list: util.List[_] =>
						list.asInstanceOf[util.List[util.Map[String, AnyRef]]]
					case _ =>
						logger.warn(s"[extendedRead] Unexpected milestones_v1 type: $identifier")
						new util.ArrayList[util.Map[String, AnyRef]]()
				}
				//Enrich milestones by fetching course hierarchy and assessment data
				enrichMilestonesWithHierarchy(milestones, request).map { enrichedMilestones =>
					contentMetadata.put(ContentConstants.MILESTONES_V1, enrichedMilestones)
					response.getResult.put(contentKey, contentMetadata)
					// Cache the enriched response for future requests
					try {
						val serializedResponse = JsonUtils.serialize(response)
						RedisCache.set(cacheKey, serializedResponse, extendedContentReadCacheTTL)
					} catch {
						case e: Exception =>
							logger.error(s"[extendedRead] Cache set failed for $identifier", e)
					}
					response
				}
			} else {
				// Not a Learning Pathway or no milestones - return standard response
				Future.successful(response)
			}
		}.recover {
			case e: ClientException =>
				logger.error(s"[extendedRead] ClientException for $identifier: ${e.getMessage}", e)
				throw e
			case e: Exception =>
				logger.error(s"[extendedRead] Exception for $identifier: ${e.getMessage}", e)
				throw e
		}
	}

	/**
	 * Enriches milestones by fetching course read + hierarchy data and assessment read data.
	 *
	 * Process:
	 * 1. Extract all unique course IDs from all milestones
	 * 2. Extract all unique assessment IDs from all milestones
	 * 3. Fetch course data (with hierarchy children) in parallel for all course IDs
	 * 4. Fetch assessment data in parallel for all assessment IDs
	 * 5. Merge fetched data back into respective milestone objects
	 *
	 * @param milestones List of milestone objects (each may contain courses and assessmentDetail)
	 * @param request    Original request context (used for authentication, version, etc.)
	 * @return Future[List] of enriched milestone objects with full course and assessment data
	 */
	private def enrichMilestonesWithHierarchy(milestones: util.List[util.Map[String, AnyRef]], request: Request): Future[util.List[util.Map[String, AnyRef]]] = {
		//Convert Java List to Scala List for easier processing
		val milestonesList = JavaConverters.asScalaIteratorConverter(milestones.iterator()).asScala.toList
		//Extract all unique course IDs from all milestones
		// Each milestone may have a "courses" array with multiple course objects
		val allCourseIds = milestonesList.flatMap { milestone =>
			if (milestone.containsKey(ContentConstants.COURSES) && milestone.get(ContentConstants.COURSES) != null) {
				val courses = milestone.get(ContentConstants.COURSES).asInstanceOf[util.List[util.Map[String, AnyRef]]]
				JavaConverters.asScalaIteratorConverter(courses.iterator()).asScala
					.map(_.getOrDefault(ContentConstants.COURSE_ID, "").asInstanceOf[String])
					.filter(_.nonEmpty)
					.toList
			} else {
				List.empty[String]
			}
		}.distinct
		// Extract all unique assessment IDs from all milestones
		// Each milestone may have an "assessmentDetail" object with an identifier
		val allAssessmentIds = milestonesList.flatMap { milestone =>
			if (milestone.containsKey(ContentConstants.ASSESSMENT_DETAIL) && milestone.get(ContentConstants.ASSESSMENT_DETAIL) != null) {
				val assessmentDetail = milestone.get(ContentConstants.ASSESSMENT_DETAIL).asInstanceOf[util.Map[String, AnyRef]]
				val identifier = assessmentDetail.getOrDefault(ContentConstants.IDENTIFIER, "").asInstanceOf[String]
				if (identifier.nonEmpty) List(identifier) else List.empty[String]
			} else {
				List.empty[String]
			}
		}.distinct
		//Fetch all course data in parallel (includes hierarchy children)
		val courseDataFuture = Future.sequence(
			allCourseIds.map { courseId =>
				fetchCourseWithHierarchy(courseId, request).map(data => courseId -> data)
			}
		).map(_.toMap)
		//  Fetch all assessment data in parallel
		val assessmentDataFuture = Future.sequence(
			allAssessmentIds.map { assessmentId =>
				fetchContentRead(assessmentId, request).map(data => assessmentId -> data)
			}
		).map(_.toMap)
		//Wait for both course and assessment data, then merge into milestones
		for {
			courseMap <- courseDataFuture
			assessmentMap <- assessmentDataFuture
		} yield {
			//Iterate through each milestone and enrich with fetched data
			val enrichedList = milestonesList.map { milestone =>
				val enrichedMilestone = new util.HashMap[String, AnyRef](milestone)
				//Enrich courses in this milestone
				if (milestone.containsKey(ContentConstants.COURSES) && milestone.get(ContentConstants.COURSES) != null) {
					val courses = milestone.get(ContentConstants.COURSES).asInstanceOf[util.List[util.Map[String, AnyRef]]]
					val enrichedCourses = JavaConverters.asScalaIteratorConverter(courses.iterator()).asScala.map { course =>
						val courseId = course.getOrDefault(ContentConstants.COURSE_ID, "").asInstanceOf[String]
						val enrichedCourse = new util.HashMap[String, AnyRef](course)
						// Merge fetched course data (includes hierarchy children)
						if (courseId.nonEmpty && courseMap.contains(courseId)) {
							enrichedCourse.putAll(courseMap(courseId))
						}
						enrichedCourse.asInstanceOf[util.Map[String, AnyRef]]
					}.toList
					enrichedMilestone.put(ContentConstants.COURSES, JavaConverters.seqAsJavaListConverter(enrichedCourses).asJava)
				}
				//Enrich assessment in this milestone
				if (milestone.containsKey(ContentConstants.ASSESSMENT_DETAIL) && milestone.get(ContentConstants.ASSESSMENT_DETAIL) != null) {
					val assessmentDetail = milestone.get(ContentConstants.ASSESSMENT_DETAIL).asInstanceOf[util.Map[String, AnyRef]]
					val assessmentId = assessmentDetail.getOrDefault(ContentConstants.IDENTIFIER, "").asInstanceOf[String]
					val enrichedAssessment = new util.HashMap[String, AnyRef](assessmentDetail)
					// Merge fetched assessment data
					if (assessmentId.nonEmpty && assessmentMap.contains(assessmentId)) {
						enrichedAssessment.putAll(assessmentMap(assessmentId))
					}
					enrichedMilestone.put(ContentConstants.ASSESSMENT_DETAIL, enrichedAssessment)
				}
				enrichedMilestone.asInstanceOf[util.Map[String, AnyRef]]
			}
			//Convert enriched Scala list back to Java List and return
			JavaConverters.seqAsJavaListConverter(enrichedList).asJava
		}
	}

	/**
	 * Filters children to only include configured fields (for response optimization).
	 * Uses childrenFields configuration to limit which fields are returned in hierarchy children.
	 *
	 * Purpose: Reduces response payload size by filtering out unnecessary fields from children nodes.
	 *
	 * @param children List of child content objects (may contain many fields)
	 * @return Filtered list with only configured fields from application.conf
	 */
	private def filterChildrenFields(children: AnyRef): AnyRef = {
		children match {
			case list: util.List[_] =>
				val filteredList = new util.ArrayList[util.Map[String, AnyRef]]()
				list.asScala.foreach {
					case child: util.Map[_, _] =>
						val childMap = child.asInstanceOf[util.Map[String, AnyRef]]
						val filteredChild = new util.HashMap[String, AnyRef]()
						// Only include fields that are configured in contentHierarchyFields
						contentHierarchyFields.foreach { field =>
							if (childMap.containsKey(field)) {
								filteredChild.put(field, childMap.get(field))
							}
						}
						filteredList.add(filteredChild)
					case _ =>
				}
				filteredList
			case _ =>
				children
		}
	}

	/**
	 * Fetches course read data + hierarchy children.
	 * Implements Redis caching with key pattern: extended_read_content_{{courseId}}
	 *
	 * Process:
	 * 1. Check Redis cache for pre-computed course+hierarchy data
	 * 2. If not cached, fetch course metadata from Neo4j (DataNode.read)
	 * 3. Fetch hierarchy children from Cassandra (HierarchyManager.getHierarchy)
	 * 4. Filter children to include only configured fields
	 * 5. Merge course metadata + children
	 * 6. Cache the result and return
	 *
	 * Note: Course data comes from Neo4j, hierarchy structure comes from Cassandra
	 *
	 * @param courseId        Course identifier
	 * @param originalRequest Request context (authentication, version, etc.)
	 * @return Future[Map] with course read data and filtered children from hierarchy
	 */
	private def fetchCourseWithHierarchy(courseId: String, originalRequest: Request): Future[util.Map[String, AnyRef]] = {
		//Build cache key and check Redis cache
		val cacheKey = s"${ContentConstants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX}$courseId"
		val cachedData = RedisCache.get(cacheKey)
		if (cachedData != null && cachedData.nonEmpty) {
			try {
				val cachedMap = JsonUtils.deserialize(cachedData, classOf[java.util.Map[String, AnyRef]])
				return Future.successful(cachedMap)
			} catch {
				case e: Exception =>
					logger.warn(s"[fetchCourse] Cache deserialization failed for $courseId", e)
			}
		}
		val readRequest = new Request(originalRequest)
		readRequest.put(ContentConstants.IDENTIFIER, courseId)
		readRequest.put(ContentConstants.FIELDS, contentEnrichmentFields)
		val hierarchyRequest = new Request(originalRequest)
		hierarchyRequest.getRequest.put(ContentConstants.IDENTIFIER, courseId)
		hierarchyRequest.getRequest.put(ContentConstants.ROOT_ID, courseId)
		//Fetch course metadata from Neo4j
		val readFuture = DataNode.read(readRequest).map { node =>
			val fields = contentEnrichmentFields
			val version = originalRequest.getContext.getOrDefault(ContentConstants.VERSION, "").asInstanceOf[String]
			val metadata = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), version)
			metadata.put(ContentConstants.IDENTIFIER, node.getIdentifier.replace(".img", ""))
			metadata
		}
		//Fetch hierarchy structure from Cassandra (hierarchy_store.content_hierarchy)
		val hierarchyFuture = HierarchyManager.getHierarchy(hierarchyRequest).map { hierarchyResponse =>
			if (hierarchyResponse.getResponseCode == ResponseCode.OK) {
				val responseSchemaName: String = originalRequest.getContext.getOrDefault(ContentConstants.RESPONSE_SCHEMA_NAME, "").asInstanceOf[String]
				val contentKey = if (responseSchemaName.isEmpty) ContentConstants.CONTENT else responseSchemaName
				val hierarchyData = hierarchyResponse.getResult.get(contentKey).asInstanceOf[util.Map[String, AnyRef]]
				// Extract children array from hierarchy response
				if (hierarchyData != null && hierarchyData.containsKey(ContentConstants.CHILDREN)) {
					Some(hierarchyData.get(ContentConstants.CHILDREN))
				} else {
					None
				}
			} else {
				logger.warn(s"[fetchCourse] Hierarchy fetch failed for $courseId: ${hierarchyResponse.getResponseCode}")
				None
			}
		}
		//Wait for both futures and merge results
		for {
			courseData <- readFuture
			children <- hierarchyFuture
		} yield {
			children.foreach { c =>
				val filteredChildren = filterChildrenFields(c)
				courseData.put(ContentConstants.CHILDREN, filteredChildren)
			}
			try {
				val serializedData = JsonUtils.serialize(courseData)
				RedisCache.set(cacheKey, serializedData, extendedContentReadCacheTTL)
			} catch {
				case e: Exception =>
					logger.error(s"[fetchCourse] Cache set failed for $courseId", e)
			}
			courseData
		}
	}.recover {
		case e: Exception =>
			logger.error(s"[fetchCourse] Failed to fetch $courseId", e)
			val errorMap = new util.HashMap[String, AnyRef]()
			errorMap.put(ContentConstants.IDENTIFIER, courseId)
			errorMap.put(ContentConstants.ERROR, s"Failed to fetch: ${e.getMessage}")
			errorMap
	}

	/**
	 * Fetches content read data for an assessment.
	 * Implements Redis caching with key pattern: extended_read_assessment_{{assessmentId}}
	 *
	 * Process:
	 * 1. Check Redis cache for pre-computed assessment data
	 * 2. If not cached, fetch assessment metadata from Neo4j (DataNode.read)
	 * 3. Serialize node to include only configured enrichment fields
	 * 4. Cache the result and return
	 *
	 * Note: Unlike courses, assessments don't have hierarchy children, so we only fetch metadata from Neo4j.
	 * Assessments are leaf nodes in the content hierarchy and don't require Cassandra hierarchy lookup.
	 *
	 * @param assessmentId    Assessment identifier
	 * @param originalRequest Request context (authentication, version, etc.)
	 * @return Future[Map] with assessment metadata (no children)
	 */
	private def fetchContentRead(assessmentId: String, originalRequest: Request): Future[util.Map[String, AnyRef]] = {
		// Build cache key and check Redis cache
		val cacheKey = s"${ContentConstants.EXTENDED_READ_ASSESSMENT_CACHE_KEY_PREFIX}$assessmentId"
		val cachedData = RedisCache.get(cacheKey)
		if (cachedData != null && cachedData.nonEmpty) {
			try {
				val cachedMap = JsonUtils.deserialize(cachedData, classOf[java.util.Map[String, AnyRef]])
				return Future.successful(cachedMap)
			} catch {
				case e: Exception =>
					logger.warn(s"[fetchAssessment] Cache deserialization failed for $assessmentId", e)
			}
		}
		val readRequest = new Request(originalRequest)
		readRequest.put(ContentConstants.IDENTIFIER, assessmentId)
		readRequest.put(ContentConstants.FIELDS, contentEnrichmentFields)
		// Fetch assessment metadata from Neo4j graph database
		DataNode.read(readRequest).map { node =>
			val fields = contentEnrichmentFields
			val version = originalRequest.getContext.getOrDefault(ContentConstants.VERSION, "").asInstanceOf[String]
			val metadata = NodeUtil.serialize(node, fields, node.getObjectType.toLowerCase.replace("image", ""), version)
			metadata.put(ContentConstants.IDENTIFIER, node.getIdentifier.replace(".img", ""))
			try {
				val serializedData = JsonUtils.serialize(metadata)
				RedisCache.set(cacheKey, serializedData, extendedContentReadCacheTTL)
			} catch {
				case e: Exception =>
					logger.error(s"[fetchAssessment] Cache set failed for $assessmentId", e)
			}
			metadata
		}.recover {
			case e: Exception =>
				logger.error(s"[fetchAssessment] Failed to fetch $assessmentId", e)
				val errorMap = new util.HashMap[String, AnyRef]()
				errorMap.put(ContentConstants.IDENTIFIER, assessmentId)
				errorMap.put(ContentConstants.ERROR, s"Failed to fetch: ${e.getMessage}")
				errorMap
		}
	}
}
