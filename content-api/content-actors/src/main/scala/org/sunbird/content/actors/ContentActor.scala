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
			case "createVersionContent" => createNewVersionOfContent(request)
      case "scheduleRetirement" => scheduleRetirement(request)
      case "isRetirementScheduled" => isRetirementScheduled(request)
      case "decideRetirementRequest" => decideRetirementRequest(request)
			case "getRetirementStatus" => getRetirementStatus(request)
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
  def scheduleRetirement(request: Request): Future[Response] = {
    RetireManager.scheduleRetirement(request)
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

	def createNewVersionOfContent(request: Request)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
		val sourceCollectionId = request.getRequest.get(ContentConstants.SOURCE_COLLECTION_ID).asInstanceOf[String]
		val createdBy = request.getRequest.get(ContentConstants.CREATED_BY).asInstanceOf[String]
		val creator = request.getRequest.get(ContentConstants.CREATOR).asInstanceOf[String]
		val createdFor = request.getRequest.get(ContentConstants.CREATED_FOR).asInstanceOf[java.util.List[String]]
		val organisation = request.getRequest.get(ContentConstants.ORGANISATION).asInstanceOf[java.util.List[String]]
		val creatorContacts = request.getRequest.get(ContentConstants.CREATOR_CONTACTS).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
		val channel = request.getRequest.get(ContentConstants.CHANNEL).asInstanceOf[String]
		if (StringUtils.isBlank(sourceCollectionId))
			throw new ClientException("ERR_INVALID_REQUEST", "sourceCollectionId is required")

		val readReq = new Request()
		readReq.setContext(new java.util.HashMap[String, AnyRef]() {
			{
				put("graph_id", "domain")
				put("version", ContentConstants.SCHEMA_VERSION)
				put("objectType", ContentConstants.CONTENT_OBJECT_TYPE)
				put("schemaName", ContentConstants.CONTENT_SCHEMA_NAME)
			}
		})
		readReq.put(ContentConstants.IDENTIFIER, sourceCollectionId)
		readReq.put(ContentConstants.MODE, "read")
		DataNode.read(readReq).flatMap { oldNode =>
			val oldMeta = oldNode.getMetadata
			val status = oldMeta.getOrDefault(ContentConstants.STATUS, "").asInstanceOf[String]
			val name = oldMeta.getOrDefault(ContentConstants.NAME, "").asInstanceOf[String]
			val newName: String =
				Option(request.getRequest.get(ContentConstants.NAME))
					.map(_.toString.trim)
					.filter(_.nonEmpty)
					.getOrElse(name)
			val sourceLangList = oldMeta.getOrDefault(ContentConstants.LANGUAGE, new util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
			val baseLang = if (CollectionUtils.isNotEmpty(sourceLangList)) sourceLangList.get(0).toLowerCase else throw new ClientException("ERR_MISSING_LANGUAGE", "Source content must have one language")
			val contentType = oldMeta.getOrDefault(ContentConstants.CONTENT_TYPE, "").asInstanceOf[String]
			val primaryCategory = oldMeta.getOrDefault(ContentConstants.PRIMARY_CATEGORY, "").asInstanceOf[String]
			val mimeType = oldMeta.getOrDefault(ContentConstants.MIME_TYPE, "").asInstanceOf[String]
			val posterImage = oldMeta.getOrDefault(ContentConstants.POSTER_IMAGE, "").asInstanceOf[String]
			val appIcon = oldMeta.getOrDefault(ContentConstants.APP_ICON, "").asInstanceOf[String]
			val creatorLogo = oldMeta.getOrDefault(ContentConstants.CREATOR_LOGO, "").asInstanceOf[String]
			val framework = oldMeta.getOrDefault(ContentConstants.FRAMEWORK, "").asInstanceOf[String]

			if (!StringUtils.equalsIgnoreCase(status, "Live"))
				throw new ClientException("ERR_INVALID_CONTENT_STATUS", s"Content $sourceCollectionId must be in Live status")

			// DETERMINE NEXT VERSION
			val oldVersion = Option(oldMeta.get(ContentConstants.CONTENT_VERSION)).map(_.toString).getOrElse("v1")
			var nextVersionNum = extractVersionNumber(oldVersion) + 1

			// check contentVersionInfo list
			val versionInfoObj = oldMeta.getOrDefault(ContentConstants.CONTENT_VERSION_INFO, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
			val versionList = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
			versionInfoObj match {
				case list: java.util.List[_] => list.asScala.foreach(i => versionList.add(i.asInstanceOf[java.util.Map[String, AnyRef]]))
				case map: java.util.Map[_, _] => versionList.add(map.asInstanceOf[java.util.Map[String, AnyRef]])
				case s: String =>
					try {
						val parsed = JsonUtils.deserialize(s, classOf[java.util.List[java.util.Map[String, AnyRef]]])
						if (parsed != null) parsed.asScala.foreach(i => versionList.add(i))
					} catch {
						case _: Throwable =>
					}
				case _ =>
			}
			val highestExisting = versionList.asScala.toList.map { entry =>
				val v = Option(entry.get("version")).map(_.toString).getOrElse("v1")
				extractVersionNumber(v)
			}.foldLeft(0)((acc, n) => Math.max(acc, n))

			if (highestExisting >= nextVersionNum)
				nextVersionNum = highestExisting + 1
			val nextVersion = s"v$nextVersionNum"
			val contentMap = new java.util.HashMap[String, AnyRef]()
			contentMap.put(ContentConstants.NAME, newName)
			contentMap.put(ContentConstants.CREATED_BY, createdBy)
			contentMap.put(ContentConstants.CREATED_FOR, createdFor)
			contentMap.put(ContentConstants.CREATOR, creator)
			contentMap.put(ContentConstants.ORGANISATION, organisation)
			contentMap.put(ContentConstants.CHANNEL, channel)
			contentMap.put(ContentConstants.CREATOR_CONTACTS, creatorContacts)
			contentMap.put(ContentConstants.CONTENT_TYPE, contentType)
			contentMap.put(ContentConstants.PRIMARY_CATEGORY, primaryCategory)
			contentMap.put(ContentConstants.MIME_TYPE, mimeType)
			contentMap.put(ContentConstants.APP_ICON, appIcon)
			contentMap.put(ContentConstants.POSTER_IMAGE, posterImage)
			contentMap.put(ContentConstants.COURSE_CATEGORY, oldMeta.get(ContentConstants.COURSE_CATEGORY))
			contentMap.put(ContentConstants.CODE, scala.util.Random.nextInt(900000000) + 1000000000 toString)
			contentMap.put(ContentConstants.LANGUAGE, util.Arrays.asList(baseLang.capitalize))
			contentMap.put(ContentConstants.FRAMEWORK, framework)

			if (StringUtils.isNotBlank(creatorLogo)) {
				contentMap.put(ContentConstants.CREATOR_LOGO, creatorLogo)
			}
			contentMap.put(ContentConstants.PREVIOUS_VERSION_COURSE_ID, sourceCollectionId)
			contentMap.put(ContentConstants.CONTENT_VERSION, nextVersion)

      val createReq = new Request()
      createReq.setOperation("createContent")
      createReq.setRequest(contentMap)
      createReq.setContext(new java.util.HashMap[String, AnyRef]() {{
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "Collection")
        put("schemaName", "collection")
      }})
			create(createReq).flatMap { createResp =>
				val newCourseId = createResp.get(ContentConstants.IDENTIFIER).asInstanceOf[String]
				copyAccessSettingsForNewCourse(sourceCollectionId, newCourseId)
					.map { _ =>
						val response = ResponseHandler.OK()
						response.put("newVersionId", newCourseId)
						response.put("previousVersionId", sourceCollectionId)
						response.put(ContentConstants.CONTENT_VERSION, nextVersion)
						response
					}
			}
		}
	}

	private def extractVersionNumber(v: String): Int = {
		if (v == null) return 1
		val s = v.toLowerCase.trim
		val VersionRegex = ".*?v?\\s*(\\d+)(?:\\.\\d+)?$".r
		s match {
			case VersionRegex(n) => try {
				n.toInt
			} catch {
				case _: Throwable => 1
			}
			case _ => 1
		}
	}

	private def createRetirementAudit(request: Request): Future[Response] = {
		val req = request.getRequest.asInstanceOf[java.util.Map[String, Object]]

		def getOrError(key: String): String = {
			val v = req.get(key)
			if (v == null) throw new ClientException("ERR_INVALID_REQUEST", s"$key is required")
			v.toString
		}

		val contentId = getOrError(ContentConstants.CONTENT_ID)
		val userId = getOrError(ContentConstants.USER_ID_RAISED)
		val reason = req.get(ContentConstants.REASON_FOR_RETIREMENT)

		// Handle LAST/RETIREMENT DATE as LocalDate (Cassandra date)
		val lastEnrollment = parseToCassandraDate(req.get(ContentConstants.LAST_ENROLLMENT_DATE).toString)
		val retirementDate = parseToCassandraDate(req.get(ContentConstants.RETIREMENT_DATE).toString)
		val allowedStatuses = ContentConstants.VALID_RETIREMENT_STATUSES

		val status = Option(req.get(ContentConstants.STATUS))
			.map(_.toString.trim)
			.filter(_.nonEmpty)
			.map(_.toUpperCase)
			.getOrElse(throw new ClientException("ERR_INVALID_REQUEST", "status is required"))

		if (!allowedStatuses.contains(status)) {
			throw new ClientException("ERR_INVALID_STATUS",
				s"Invalid status: $status. Allowed: PENDING, APPROVED, REJECTED, RETIRED")
		}
		val reviewedBy: AnyRef = req.get(ContentConstants.REVIEWED_BY)
		val reviewedAt: AnyRef = parseToCassandraTimestamp(req.get(ContentConstants.REVIEWED_AT))
		val reviewedComment: AnyRef = req.get(ContentConstants.REVIEWED_COMMENT)

		import com.datastax.driver.core.utils.UUIDs
		val requestId = Option(req.get(ContentConstants.REQUEST_ID)).map(_.toString).getOrElse(UUIDs.timeBased().toString)
		val id = UUIDs.timeBased().toString
		val nowTs = new java.sql.Timestamp(System.currentTimeMillis())

		val row = new java.util.HashMap[String, Object]()
		row.put("identifier", contentId)
		row.put("id", id)
		row.put("request_id", requestId)
		row.put("user_id_raised", userId)
		row.put("reason_for_retirement", reason)
		row.put("last_enrollment_date", lastEnrollment)
		row.put("retirement_date", retirementDate)
		row.put("reviewed_by", reviewedBy)
		row.put("reviewed_at", reviewedAt)
		row.put("reviewed_comment", reviewedComment)
		row.put("created_at", nowTs)
		row.put("updated_at", nowTs)
		row.put("status", status)

		val primaryKeys = java.util.Arrays.asList("content_id", "id")

		val auditStore = new ExternalStore(
			Platform.config.getString("cassandra.keyspace.course.content"),
			Platform.config.getString("content.retirement.requests.audit"),
			primaryKeys
		)
		auditStore.insert(row, Map.empty[String, String]).map { _ =>
			val resp = ResponseHandler.OK()
			resp.put("contentId", contentId)
			resp.put("id", id)
			resp.put("requestId", requestId)
			resp.put("status", status)
			resp
		}
	}

	private def parseToCassandraDate(date: String): com.datastax.driver.core.LocalDate = {
		val parts = date.split("-")
		com.datastax.driver.core.LocalDate.fromYearMonthDay(
			parts(0).toInt, parts(1).toInt, parts(2).toInt
		)
	}

	private def parseToCassandraTimestamp(v: Any): java.util.Date = {
		if (v == null) return null
		val s = v.toString.trim

		try {
			// If full ISO timestamp → parse directly
			return java.util.Date.from(java.time.Instant.parse(s))
		} catch {
			case _: Throwable =>
				try {
					val localDate = java.time.LocalDate.parse(s)
					val instant = localDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant
					return java.util.Date.from(instant)
				} catch {
					case _: Throwable =>
						throw new ClientException("ERR_INVALID_DATE_FORMAT",
							s"Invalid date format: $s. Expected ISO timestamp or yyyy-MM-dd")
				}
		}
	}


  def isRetirementScheduled(request: Request): Future[Response] = {
    RetireManager.isRetirementScheduled(request)
  }

	def decideRetirementRequest(request: Request)(implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Response] = {
		logger.info("Inside decideRetirementRequest method of RetireManager")
		validateDecideRetirementRequest(request)
		val outerMap = request.getRequest
		val reqMap = Option(outerMap.get(ContentConstants.RQST))
			.map(_.asInstanceOf[java.util.Map[String, AnyRef]])
			.getOrElse(
				throw new ClientException(
					ContentConstants.ERR_INVALID_REQUEST,
					"Request body is missing."
				)
			)
		val contentId = Option(reqMap.get(ContentConstants.CONTENT_ID))
			.map(_.toString.trim)
			.filter(org.apache.commons.lang3.StringUtils.isNotBlank)
			.getOrElse(
				throw new ClientException(
					ContentConstants.ERR_INVALID_CONTENT_ID,
					ContentConstants.ERR_CONTENT_ID_MISSING
				)
			)
		val action = Option(reqMap.get(ContentConstants.ACTION))
			.map(_.toString.trim.toUpperCase)
			.getOrElse(
				throw new ClientException(
					ContentConstants.ERR_INVALID_REQUEST,
					"Action is missing"
				)
			)
		val externalProperties: List[String] =
			Platform.config.getStringList(ContentConstants.RETIREMENT_READ_COLUMNS)
				.asScala
				.toList
		val propertyTypeMapping =
			scala.collection.immutable.Map.empty[String, String]
		for {
			_ <- RetireManager.isRetirementScheduled(request).map(_ => ())
			readResp <- retirementRequestStore.read(contentId, externalProperties, propertyTypeMapping)
			result <- {
				if (readResp.getResponseCode != ResponseCode.OK) {
					Future.successful(ResponseHandler.ERROR(ResponseCode.CLIENT_ERROR, "NOT_FOUND", "Retirement request not found"))
				} else {
					val dbRow = readResp.getResult.asInstanceOf[java.util.Map[String, AnyRef]]
					val requestId = dbRow.get(ContentConstants.RQST_ID).toString
					val approvedBy = extractUserId(request)

					//Build Audit Row from DB result
					val auditRow = buildAuditRowFromDecisionResult(
						contentId = contentId,
						result = dbRow,
						action = action,
						approvedBy = approvedBy
					)
					updateRetirementRequestByCompositeKey(
						contentId = contentId,
						requestId = requestId,
						approvedBy = approvedBy,
						keySpace = Platform.getString(ContentConstants.SUNBIRD_COURSE_KEYSPACE, "sunbird_courses"),
						table = Platform.getString(ContentConstants.CONTENT_RETIREMENT_RQST_TABLE, "content_retirement_requests"),
						action = action
					).flatMap { _ =>

						// Then insert audit log
						RetireManager.createRetirementAuditLog(auditRow).flatMap { _ =>

							markContentPendingRetirement(
								request = request,
								retirementResult = dbRow,
								action = action
							).map { resp =>
								logger.info(s"[RETIRE-DECIDE][CONTENT-UPDATE][SUCCESS] contentId=$contentId, action=$action")
								resp
							}
						}
					}
				}
			}
		} yield {
			logger.info(
				s"[RETIRE-DECIDE][SUCCESS] contentId=$contentId, action=$action"
			)
			ResponseHandler.OK
				.put("node_id", contentId)
				.put("identifier", contentId)
		}
	}

  private def extractUserId(req: Request): String = {
    val fromContext = Option(req.getContext.get("X-Authenticated-Userid"))
      .map(_.toString)
      .filter(StringUtils.isNotBlank)
    if (fromContext.isDefined) return fromContext.get
    val params = req.getParams
    if (params != null && params.getUid != null)
      return params.getUid
    ""
  }

  private def validateDecideRetirementRequest(request: Request): Unit = {
    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get(ContentConstants.RQST))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Request body is missing."
      ))
    Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_CONTENT_ID,
        ContentConstants.ERR_CONTENT_ID_MISSING
      ))
    val action = Option(reqMap.get(ContentConstants.ACTION))
      .map(_.toString.trim.toUpperCase)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Action is mandatory."
      ))
    if (!Set("APPROVE", "REJECT").contains(action)) {
      throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        s"Invalid action: $action. Allowed values are APPROVE or REJECT."
      )
    }
    Option(reqMap.get(ContentConstants.COMMENT))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(throw new ClientException(
        ContentConstants.ERR_INVALID_REQUEST,
        "Comment is mandatory."
      ))
  }

  private def updateRetirementRequestByCompositeKey(
                                                     keySpace: String,
                                                     table: String,
                                                     contentId: String,
                                                     requestId: String,
                                                     approvedBy: String,
                                                     action : String
                                                   )(implicit ec: ExecutionContext): Future[Response] = {

    val update = QueryBuilder.update(keySpace, table)
    update.where
      .and(QueryBuilder.eq(ContentConstants.RETITEMENT_PRIMARY_KEY, contentId))
      .and(QueryBuilder.eq(ContentConstants.RQST_ID, requestId))
		val (approvedFlag, statusValue) =
			action match {
				case ContentConstants.APPROVE =>
					(java.lang.Boolean.TRUE, ContentConstants.APPROVED_KEY)
				case ContentConstants.REJECT =>
					(java.lang.Boolean.FALSE, ContentConstants.REJECTED)
				case _ =>
					throw new ClientException(
						ContentConstants.ERR_INVALID_REQUEST,
						s"Invalid action for retirement decision: $action"
					)
			}
    update
      .`with`(QueryBuilder.set(ContentConstants.APPROVED, java.lang.Boolean.TRUE))
      .and(QueryBuilder.set(ContentConstants.APPROVED_BY_RQST, approvedBy))
      .and(QueryBuilder.set(ContentConstants.APPROVED_AT, new java.util.Date()))
      .and(QueryBuilder.set(ContentConstants.STATUS, statusValue))
      .and(QueryBuilder.set(ContentConstants.APPROVED_COMMENT, action))

    CassandraConnector.getSession
      .executeAsync(update)
      .asScala
      .map { _ =>
        logger.info(
          s"[RETIRE-DECIDE][UPDATE-SUCCESS] contentId=$contentId, requestId=$requestId, approvedBy=$approvedBy"
        )
        ResponseHandler.OK()
      }

  }

  implicit class RichListenableFuture[T](lf: ListenableFuture[T]) {
    def asScala : Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p failure t
        def onSuccess(result: T): Unit    = p success result
      }, MoreExecutors.directExecutor())
      p.future
    }
  }

  def markContentPendingRetirement(
                                    request: Request,
                                    retirementResult: java.util.Map[String, AnyRef],
                                    action: String
                                  )(
                                    implicit ec: ExecutionContext,
                                    oec: OntologyEngineContext
                                  ): Future[Response] = {

    val outerMap = request.getRequest
    val reqMap = Option(outerMap.get("request"))
      .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_REQUEST,
          "Request body is missing."
        )
      )

    val id = Option(reqMap.get(ContentConstants.CONTENT_ID))
      .map(_.toString.trim)
      .filter(StringUtils.isNotBlank)
      .getOrElse(
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          ContentConstants.ERR_CONTENT_ID_MISSING
        )
      )

    val lastEnrollmentDate =
      retirementResult.get(ContentConstants.LAST_ENROLLMENT_DATE_RQST)

    val retirementDate =
      retirementResult.get(ContentConstants.RETIREMENT_DATE_RQST)

    val readReq = new Request()
    readReq.setContext(new util.HashMap[String, AnyRef]() {{
      put("graph_id", "domain")
      put("version", "1.0")
      put("objectType", "Content")
      put("schemaName", "content")
    }})
    readReq.put("identifier", id)
    readReq.setObjectType("Content")
    readReq.put(ContentConstants.MODE, "read")
    DataNode.read(readReq).flatMap { node =>
      if (node == null)
        throw new ClientException(
          ContentConstants.ERR_INVALID_CONTENT_ID,
          s"Content is not found for identifier: $id"
        )
      val metadata = node.getMetadata
      val status = Option(metadata.get("status")).map(_.toString).getOrElse("")
      if (StringUtils.isBlank(status))
        throw new ClientException(
          "ERR_METADATA_ISSUE",
          s"Content metadata error, status is blank for identifier: ${node.getIdentifier}"
        )
      action match {
        case ContentConstants.APPROVE =>
          request.getRequest.put(
            ContentConstants.CONTENT_RETIREMENT_STS,
            ContentConstants.PENDING_RETIREMENT
          )
          if (lastEnrollmentDate != null) {
            request.getRequest.put(
              ContentConstants.LAST_ENROLLMENT_DATE,
              toOffsetTimestamp(lastEnrollmentDate)
            )
          }
					if (lastEnrollmentDate != null && retirementDate != null) {
						val lastEnrollmentdateFetched = toLocalDate(lastEnrollmentDate)
						val retirementDateFetched  = toLocalDate(retirementDate)
						val retirementGapInDays =
							ChronoUnit.DAYS.between(lastEnrollmentdateFetched, retirementDateFetched)
						val newRetirementDate =
							LocalDate.now().plusDays(retirementGapInDays)
						request.getRequest.put(
							ContentConstants.RETIREMENT_DATE,
							toOffsetTimestamp(newRetirementDate)
						)
						logger.info(
							s"[RETIRE-DECIDE][RETIREMENT-DATE-RECALC] " + s"lastEnrollment=$lastEnrollmentdateFetched, " +
								s"oldRetirement=$retirementDateFetched, " + s"diffDays=$retirementGapInDays, " + s"newRetirement=$newRetirementDate"
						)
					}
				case ContentConstants.REJECT =>
          request.getRequest.put(
            ContentConstants.CONTENT_RETIREMENT_STS,
            ContentConstants.REJECTED
          )
      }
			request.setContext(new java.util.HashMap[String, AnyRef]() {{
				put("graph_id", "domain")
				put("version", "1.0")
				put("objectType", "Collection")
				put("schemaName", "collection")
			}})
      request.getRequest.put("versionKey", metadata.get("versionKey"))
      RequestUtil.restrictProperties(request)
      request.getContext.put(ContentConstants.IDENTIFIER, id)
      systemUpdate(request).map { updatedResp =>
        logger.info(
          s"[RETIRE-DECIDE][CONTENT-UPDATE] action=$action, contentId=$id"
        )
        ResponseHandler.OK
          .put("identifier", id)
          .put("status", action)
      }
    }
  }

  private def toOffsetTimestamp(value: AnyRef): String = {
    val formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    value match {
      case date: java.util.Date =>
        ZonedDateTime
          .ofInstant(date.toInstant, ZoneId.systemDefault())
          .format(formatter)
      case date: java.time.LocalDate =>
        date.atStartOfDay(ZoneId.systemDefault())
          .format(formatter)
      case str: String =>
        str
      case _ =>
        ZonedDateTime
          .now(ZoneId.systemDefault())
          .format(formatter)
    }
  }

	def getRetirementStatus(request: Request)(implicit ec: ExecutionContext): Future[Response] = {
		logger.info("[RETIREMENT-STATUS] Inside getRetirementStatus")
		import scala.collection.JavaConverters._
		val reqMap: java.util.Map[String, AnyRef] =
			Option(request.getRequest)
				.getOrElse(
					throw new ClientException(
						ContentConstants.ERR_INVALID_REQUEST,
						"Request body is missing"
					)
				)
		val contentIds: List[String] =
			Option(reqMap.get("contentId"))
				.map(_.asInstanceOf[java.util.List[String]].asScala.toList)
				.getOrElse(
					throw new ClientException(
						ContentConstants.ERR_INVALID_REQUEST,
						"contentId is missing"
					)
				)
		logger.info(
			s"[RETIREMENT-STATUS][REQUEST] contentIds=${contentIds.mkString(",")}"
		)
		val externalProperties: List[String] =
			Platform.config
				.getStringList(ContentConstants.RETIREMENT_READ_COLUMNS)
				.asScala
				.toList
		val propertiesMapping: scala.collection.immutable.Map[String, String] =
			scala.collection.immutable.Map.empty
		retirementRequestStore
			.read(contentIds, externalProperties, propertiesMapping)
			.map { resp: Response =>
				logger.info(
					s"[RETIREMENT-STATUS][SUCCESS] responseCode=${resp.getResponseCode}"
				)
				val fetchedData =
					resp.getResult.asInstanceOf[java.util.Map[String, AnyRef]]
				val contentList = buildRetirementStatusResponse(fetchedData)
				ResponseHandler.OK
					.put("content", contentList)
			}
	}

	private def buildRetirementStatusResponse(fetchedData: java.util.Map[String, AnyRef]): java.util.List[java.util.Map[String, AnyRef]] = {
		import scala.collection.JavaConverters._
		fetchedData.asScala.map {
			case (contentId: String, rowAny: AnyRef) =>
				val retirementData =
					rowAny.asInstanceOf[java.util.Map[String, AnyRef]]
				val updatedData: java.util.Map[String, AnyRef] =
					new java.util.HashMap[String, AnyRef]()
				// always present
				updatedData.put(ContentConstants.CONTENT_ID, contentId)
				Option(retirementData.get(ContentConstants.LAST_ENROLLMENT_DATE_RQST))
					.foreach { v =>
						updatedData.put(
							ContentConstants.LAST_ENROLLMENT_DATE,
							toIsoDate(v)
						)
					}
				Option(retirementData.get(ContentConstants.RETIREMENT_DATE_RQST))
					.foreach { v =>
						updatedData.put(
							ContentConstants.RETIREMENT_DATE,
							toIsoDate(v)
						)
					}
				Option(retirementData.get(ContentConstants.STATUS))
					.foreach { result =>
						updatedData.put(
							ContentConstants.STATUS,
							result.toString
						)
					}
				Option(retirementData.get(ContentConstants.RSN_FOR_RETIREMENT))
					.foreach { result =>
						updatedData.put(
							ContentConstants.REASON,
							result
						)
					}
				Option(retirementData.get(ContentConstants.USER_ID_RAISED_FIELD))
					.foreach { result =>
						updatedData.put(
							ContentConstants.USER_ID_RAISED,
							result
						)
					}
				updatedData
		}.toList.asJava
	}

	import java.time.format.DateTimeFormatter
	import java.time.{ZoneOffset, LocalDate => JLocalDate}

	private val ISO_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

	private def toIsoDate(value: AnyRef): String = {
		value match {
			case date: com.datastax.driver.core.LocalDate =>
				JLocalDate
					.of(date.getYear, date.getMonth, date.getDay)
					.atStartOfDay()
					.atZone(ZoneOffset.UTC)
					.format(ISO_FORMATTER)
			case date: java.util.Date =>
				date.toInstant
					.atZone(ZoneOffset.UTC)
					.format(ISO_FORMATTER)
			case _ =>
				null
		}
	}

	import java.time._

	private val OFFSET_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

	private def toLocalDate(value: AnyRef): LocalDate = {
		value match {
			case date: com.datastax.driver.core.LocalDate =>
				LocalDate.of(date.getYear, date.getMonth, date.getDay)
			case dateString: String =>
				LocalDate.parse(dateString)
			case _ =>
				throw new IllegalArgumentException(s"Unsupported date type: $value")
		}
	}

	private def toOffsetTimestamp(date: LocalDate): String = {
		date
			.atStartOfDay()
			.atZone(ZoneId.systemDefault())
			.format(OFFSET_FORMATTER)
	}

	private def copyAccessSettingsForNewCourse(oldCourseId: String, newCourseId: String)(implicit ec: ExecutionContext): Future[Unit] = {
		val keySpace = Platform.getString(ContentConstants.SUNBIRD_COURSE_KEYSPACE, "sunbird_courses")
		val table    = Platform.getString(ContentConstants.ACCESS_SETTING_RULES_V2_TABLE, "access_setting_rules_v2")
		val accessRuleStore = new ExternalStore(
			keySpace = keySpace,
			table = table,
			primaryKey = java.util.Arrays.asList(ContentConstants.CONTEXT_ID, ContentConstants.CONTEXT_ID_TYPE)
		)
		val readColumns = List(
			ContentConstants.CONTEXT_ID_TYPE,
			ContentConstants.CONTEXT_DATA,
			ContentConstants.IS_ARCHIVED
		)
		val propsMapping: scala.collection.immutable.Map[String, String] =
			scala.collection.immutable.Map(ContentConstants.CONTEXT_DATA -> "string")
		val sourceId: String = oldCourseId

		accessRuleStore
			.read(
				identifier = sourceId,
				extProps = readColumns,
				propsMapping = propsMapping
			)
			.flatMap { readResp =>
				if (readResp.getResponseCode == ResponseCode.OK) {

					val insertMap = new java.util.HashMap[String, AnyRef]()

					// Primary key for new row
					insertMap.put(ContentConstants.IDENTIFIER, newCourseId)

					// Copy remaining fields as-is
					val contextIdType: String =
						Option(readResp.get(ContentConstants.CONTEXT_ID_TYPE))
							.map(_.toString)
							.filter(_.nonEmpty)
							.getOrElse("Course")
					insertMap.put(ContentConstants.CONTEXT_ID_TYPE, contextIdType)

					val contextDataRaw = readResp.get(ContentConstants.CONTEXT_DATA)

					val updatedContextDataString: String = try {
						val parsed: java.util.Map[String, AnyRef] =
							contextDataRaw match {
								case s: String =>
									JsonUtils.deserialize(s, classOf[java.util.Map[String, AnyRef]])
								case m: java.util.Map[_, _] =>
									m.asInstanceOf[java.util.Map[String, AnyRef]]
								case _ => null
							}

						if (parsed != null) {
							parsed.put(ContentConstants.CONTENT_ID, newCourseId)
							JsonUtils.serialize(parsed)
						} else {
							contextDataRaw.toString
						}
					} catch {
						case e: Exception =>
							logger.warn("[ACCESS-SETTINGS] Failed to parse contextdata, using raw string", e)
							contextDataRaw.toString
					}
					insertMap.put(ContentConstants.CONTEXT_DATA, updatedContextDataString)
					insertMap.put(ContentConstants.IS_ARCHIVED, readResp.get(ContentConstants.IS_ARCHIVED))

					accessRuleStore.insert(insertMap, propsMapping).map(_ => ())
				} else {
					// No record exists → nothing to copy
					Future.successful(())
				}
			}
			.recover {
				case e: Exception =>
					logger.error(
						s"[ACCESS-SETTINGS][COPY-FAILED] oldCourseId=$oldCourseId, newCourseId=$newCourseId",
						e
					)
					()
			}
	}

	private def buildAuditRowFromDecisionResult(contentId: String, result: java.util.Map[String, AnyRef], action: String, approvedBy: String): java.util.Map[String, AnyRef] = {
		val nowTs = new java.sql.Timestamp(System.currentTimeMillis())
		val auditRow = new java.util.HashMap[String, AnyRef]()

		auditRow.put(ContentConstants.IDENTIFIER, contentId)
		auditRow.put(ContentConstants.RQST_ID, result.get(ContentConstants.RQST_ID))
		auditRow.put(ContentConstants.USER_ID_RAISED_FIELD, result.get(ContentConstants.USER_ID_RAISED_FIELD))
		auditRow.put(ContentConstants.RSN_FOR_RETIREMENT, result.get(ContentConstants.RSN_FOR_RETIREMENT))
		auditRow.put(ContentConstants.LST_ENR_DATE, result.get(ContentConstants.LST_ENR_DATE))
		auditRow.put(ContentConstants.RET_DATE, result.get(ContentConstants.RET_DATE))

		// fields
		auditRow.put(ContentConstants.STATUS, action)
		auditRow.put(ContentConstants.REVIEWED_BY, approvedBy)
		auditRow.put(ContentConstants.REVIEWED_AT, nowTs)
		auditRow.put(ContentConstants.REVIEWED_COMMENT, action)
		auditRow
	}

}
