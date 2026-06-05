package org.sunbird.content.upload.mgr.validator

import java.io.File
import org.apache.tika.Tika

object FileUploadValidator {

  private val tika = new Tika()

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
}