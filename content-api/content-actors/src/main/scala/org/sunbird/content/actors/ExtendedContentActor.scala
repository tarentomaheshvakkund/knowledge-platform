package org.sunbird.content.actors

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.mashape.unirest.http.Unirest
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.actor.core.BaseActor
import org.sunbird.cache.impl.RedisCache
import org.sunbird.cassandra.CassandraConnector
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.common.{ContentParams, JsonUtils, Platform}
import org.sunbird.content.util.ExtendedRetireManager.RichListenableFuture
import org.sunbird.content.util._
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.external.store.ExternalStore
import org.sunbird.graph.nodes.DataNode
import org.sunbird.managers.HierarchyManager
import org.sunbird.managers.HierarchyManager.hierarchyPrefix
import org.sunbird.util.RequestUtil

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate => JLocalDate}
import java.time.{LocalDate, ZoneId, ZoneOffset, ZonedDateTime}
import java.util
import javax.inject.Inject
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ExtendedContentActor @Inject() (implicit oec: OntologyEngineContext, ss: StorageService) extends BaseActor {

  implicit val ec: ExecutionContext = getContext().dispatcher
  private val logger: Logger = LoggerFactory.getLogger("ExtendedContentActor")
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
  private val copyFields: Set[String] =
    Platform.config.getStringList(ContentConstants.CONTENT_COPY_FIELDS).asScala.toSet


  override def onReceive(request: Request): Future[Response] = {
    request.getOperation match {
      case "createVersionContent" => createNewVersionOfContent(request)
      case "scheduleRetirement" => scheduleRetirement(request)
      case "isRetirementScheduled" => isRetirementScheduled(request)
      case "decideRetirementRequest" => decideRetirementRequest(request)
      case "getRetirementStatus" => getRetirementStatus(request)
      case _ => ERROR(request.getOperation)
    }
  }

  def scheduleRetirement(request: Request): Future[Response] = {
    ExtendedRetireManager.scheduleRetirement(request)
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
      copyConfigurableFields(oldMeta, contentMap, copyFields)

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
      val id = oldMeta.get(ContentConstants.IDENTIFIER)
      create(createReq).flatMap { createResp =>
        try {
          if (StringUtils.isNotBlank(createdBy)) {
            NotificationManager.sendNotification(
              "PUBLISHED_NEW_VERSION",
              "ALERT",
              List(createdBy),
              oldMeta.get("name").toString,
              Map[String, Any]("id" -> id)
            )
            logger.info(s"[RETIRE-NOTIFY][SUCCESS] contentId=$id, userId=$createdBy")
          } else {
            logger.warn(s"[RETIRE-NOTIFY][SKIPPED] No userIdRaised found for contentId=$id")
          }
        } catch {
          case e: Exception =>
            logger.error(s"[RETIRE-NOTIFY][FAILED] contentId=$id", e)
        }
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

  def dataModifier(node: Node): Node = {
    if(node.getMetadata.containsKey("trackable") &&
      node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].containsKey("enabled") &&
      "Yes".equalsIgnoreCase(node.getMetadata.getOrDefault("trackable", new java.util.HashMap[String, AnyRef]).asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("enabled", "").asInstanceOf[String])) {
      node.getMetadata.put("contentType", "Course")
    }
    node
  }

  def populateDefaultersForCreation(request: Request) = {
    setDefaultsBasedOnMimeType(request, ContentParams.create.name)
    setDefaultLicense(request)
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

  private def setDefaultLicense(request: Request): Unit = {
    if (StringUtils.isEmpty(request.getRequest.getOrDefault("license", "").asInstanceOf[String])) {
      val cacheKey = "channel_" + request.getRequest.getOrDefault("channel", "").asInstanceOf[String] + "_license"
      val defaultLicense = RedisCache.get(cacheKey, null, 0)
      if (StringUtils.isNotEmpty(defaultLicense)) request.getRequest.put("license", defaultLicense)
      else println("Default License is not available for channel: " + request.getRequest.getOrDefault("channel", "").asInstanceOf[String])
    }
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

  def isRetirementScheduled(request: Request): Future[Response] = {
    ExtendedRetireManager.isRetirementScheduled(request)
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
      _ <- ExtendedRetireManager.isRetirementScheduled(request).map(_ => ())
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
            ExtendedRetireManager.createRetirementAuditLog(auditRow).flatMap { _ =>

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

  def markContentPendingRetirement(request: Request, retirementResult: java.util.Map[String, AnyRef], action: String)
                                  (implicit ec: ExecutionContext, oec: OntologyEngineContext): Future[Response] = {
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
      var notificationType = "RETIRED"
      action match {
        case ContentConstants.APPROVE =>
          request.getRequest.put(
            ContentConstants.CONTENT_RETIREMENT_STS,
            ContentConstants.PENDING_RETIREMENT
          )
          if (lastEnrollmentDate != null) {
            request.getRequest.put(
              ContentConstants.LAST_ENROLLMENT_DATE,
              toOffsetTimestamp(toLocalDate(lastEnrollmentDate))
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
          notificationType = "RETIRED_REJECTED"
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
      try {
        val requestedBy =
          Option(retirementResult.get(ContentConstants.USER_ID_RAISED_FIELD))
            .map(_.toString)
            .getOrElse("")

        if (StringUtils.isNotBlank(requestedBy)) {
          NotificationManager.sendNotification(
            notificationType,
            "ALERT",
            List(requestedBy),
            node.getMetadata.get("name").toString,
            Map[String, Any]("id" -> id)
          )
          logger.info(s"[RETIRE-NOTIFY][SUCCESS] contentId=$id, userId=$requestedBy")
        } else {
          logger.warn(s"[RETIRE-NOTIFY][SKIPPED] No userIdRaised found for contentId=$id")
        }
      } catch {
        case e: Exception =>
          logger.error(s"[RETIRE-NOTIFY][FAILED] contentId=$id", e)
      }

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


  private val OUTPUT_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private def toOffsetTimestamp(value: AnyRef): String = {
    val zone = ZoneId.systemDefault()
    value match {
      case d: java.util.Date =>
        ZonedDateTime
          .ofInstant(d.toInstant, zone)
          .format(OUTPUT_FORMATTER)
      case d: LocalDate =>
        d.atStartOfDay(zone)
          .format(OUTPUT_FORMATTER)
      case s: String if s.nonEmpty =>
        Try {
          LocalDate.parse(s)
            .atStartOfDay(zone)
            .format(OUTPUT_FORMATTER)
        }
          .orElse {
            Try {
              ZonedDateTime
                .parse(s)
                .withZoneSameInstant(zone)
                .format(OUTPUT_FORMATTER)
            }
          }
          .getOrElse {
            throw new IllegalArgumentException(
              s"Unsupported date format: $s"
            )
          }
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported date value: $value"
        )
    }
  }

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

  def copyConfigurableFields(oldMeta: java.util.Map[String, AnyRef], targetMap: java.util.Map[String, AnyRef], fieldsToCopy: Set[String]): Unit = {
    fieldsToCopy.foreach { field =>
      Option(oldMeta.get(field)) match {
        case Some(b: java.lang.Boolean) =>
          targetMap.put(field, b)
        case Some(s: String) if StringUtils.isNotBlank(s) =>
          targetMap.put(field, s)
        case Some(l: java.util.List[_]) =>
          targetMap.put(field, l)
        case Some(m: java.util.Map[_, _]) =>
          targetMap.put(field, m.asInstanceOf[AnyRef])

        // Ignore everything else safely
        case _ =>
      }
    }
  }


}
