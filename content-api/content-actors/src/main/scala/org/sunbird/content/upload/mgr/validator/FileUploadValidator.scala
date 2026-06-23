package org.sunbird.content.upload.mgr.validator

import java.io.File
import org.apache.tika.Tika
import org.slf4j.LoggerFactory
import scala.xml.XML.loadFile

object FileUploadValidator {

  private val tika = new Tika()
  private val logger = LoggerFactory.getLogger(getClass)

  private val blockedExtensions = Set(
    "php", "php3", "php4", "php5", "phtml",
    "jsp", "jspx",
    "asp", "aspx",
    "cgi", "pl",
    "sh", "bat", "cmd",
    "exe"
  )

  private val mimeAliases = Map(
    "image/jpg" -> Set("image/jpeg"),
    "image/jpeg" -> Set("image/jpg"),

    "application/epub" -> Set("application/epub+zip"),
    "application/epub+zip" -> Set("application/epub")
  )

  def validate(file: File, metadataMimeType: String): Unit = {

    // Extension validation
    val extension =
      Option(file.getName)
        .filter(_.contains("."))
        .map(_.substring(file.getName.lastIndexOf('.') + 1).toLowerCase.trim)
        .getOrElse("")

    if (blockedExtensions.contains(extension)) {
      throw new IllegalArgumentException(
        s"File type not allowed: $extension"
      )
    }

    // Magic-byte/content validation
    val detectedMimeType =
      Option(tika.detect(file))
        .getOrElse("")
        .toLowerCase
        .trim

    val expectedMimeType =
      Option(metadataMimeType)
        .getOrElse("")
        .toLowerCase
        .trim

    logger.info(
      s"File validation started. File=${file.getName}, MetadataMime=$expectedMimeType, DetectedMime=$detectedMimeType"
    )

    // SVG Security Validation MUST happen BEFORE return
    if (detectedMimeType == "image/svg+xml") {
      logger.warn(s"Running SVG security validation for ${file.getName}")
      validateSvg(file)
    }

    // Exact match
    if (expectedMimeType == detectedMimeType) {
      return
    }

    // Alias match
    val aliases =
      mimeAliases.getOrElse(
        expectedMimeType,
        Set.empty[String]
      )

    if (!aliases.contains(detectedMimeType)) {
      throw new IllegalArgumentException(
        s"Mime mismatch. Metadata=$expectedMimeType Detected=$detectedMimeType"
      )
    }
  }
  private def validateSvg(file: File): Unit = {

    val svg = loadFile(file)

    val dangerousTags = Set(
      "script",
      "foreignobject",
      "iframe",
      "object",
      "embed",
      "animate",
      "animatemotion",
      "animatetransform",
      "set"
    )

    val elements =
      svg.descendant_or_self.collect {
        case elem: scala.xml.Elem => elem
      }

    elements.foreach { elem =>

      val tagName = elem.label.toLowerCase

      logger.debug(s"SVG Tag Found: $tagName")

      if (dangerousTags.contains(tagName)) {

        logger.warn(
          s"Unsafe SVG tag detected. File=${file.getName}, Tag=$tagName"
        )

        throw new IllegalArgumentException(
          s"Unsafe SVG element detected: $tagName"
        )
      }

      elem.attributes.asAttrMap.foreach {
        case (name, value) =>

          val attrName =
            Option(name).getOrElse("").toLowerCase

          val attrValue =
            Option(value).getOrElse("").toLowerCase

          logger.debug(
            s"SVG Attribute Found: $attrName=$attrValue"
          )

          if (attrName.startsWith("on")) {

            logger.warn(
              s"Unsafe SVG event handler detected. File=${file.getName}, Attribute=$attrName"
            )

            throw new IllegalArgumentException(
              s"Unsafe SVG attribute detected: $attrName"
            )
          }

          if (
            attrValue.contains("javascript:") ||
              attrValue.contains("vbscript:") ||
              attrValue.contains("data:text/html")
          ) {

            logger.warn(
              s"Unsafe SVG URI detected. File=${file.getName}"
            )

            throw new IllegalArgumentException(
              "Unsafe SVG URI detected"
            )
          }
      }
    }

    logger.warn(
      s"SVG security validation successful for ${file.getName}"
    )
  }
}