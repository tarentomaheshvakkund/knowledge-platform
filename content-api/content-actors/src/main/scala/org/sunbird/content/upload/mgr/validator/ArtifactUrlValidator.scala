package org.sunbird.content.upload.mgr.validator

import java.net.URI
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.common.Platform
import org.sunbird.content.util.ContentConstants

object ArtifactUrlValidator {

  private val logger = LoggerFactory.getLogger(getClass)
  private val allowedDomains: Set[String] = Platform
    .getStringList(ContentConstants.CONTENT_ARTIFACT_URL_ALLOWED_DOMAINS, java.util.Arrays.asList("karmayogibharat.net"))
    .toArray
    .map(_.toString.toLowerCase.trim)
    .toSet

  def isValid(url: String): Boolean = {

    logger.info(s"Validating artifactUrl: [$url]")

    if (StringUtils.isBlank(url)) {
      logger.warn("artifactUrl is blank")
      return false
    }

    try {
      val uri = new URI(url.trim)

      val scheme = Option(uri.getScheme)
        .map(_.toLowerCase)
        .getOrElse("")

      val host = Option(uri.getHost)
        .map(_.toLowerCase)
        .getOrElse("")

      logger.info(s"Parsed URL -> scheme: [$scheme], host: [$host]")

      if (scheme != "https") {
        logger.warn(s"Rejected URL due to invalid scheme: $scheme")
        return false
      }

      val isAllowed = isAllowedHost(host)

      logger.info(
        s"Host validation result: host=[$host], allowed=[$isAllowed]"
      )

      isAllowed

    } catch {
      case ex: Exception =>
        logger.error(s"Error while validating artifactUrl: [$url]", ex)
        false
    }
  }

  private def isAllowedHost(host: String): Boolean = {

    val matchedDomain = allowedDomains.find { domain =>
      host == domain || host.endsWith("." + domain)
    }
    logger.warn(s"Allowed domains configured: ${allowedDomains.mkString(", ")}")

    logger.warn(
      s"Host [$host] matched against domain: ${matchedDomain.getOrElse("NONE")}"
    )

    matchedDomain.isDefined
  }
}