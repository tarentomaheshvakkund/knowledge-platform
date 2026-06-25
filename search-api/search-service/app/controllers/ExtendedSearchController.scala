package controllers

import akka.actor.{ActorRef, ActorSystem}
import com.google.inject.Inject
import com.google.inject.name.Named
import handlers.LoggingAction
import managers.SearchManager
import org.sunbird.search.util.SearchConstants
import play.api.mvc.ControllerComponents
import utils.{ActorNames, ApiId}
import java.util
import java.util.Base64
import org.sunbird.common.JsonUtils
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class ExtendedSearchController @Inject()(@Named(ActorNames.SEARCH_ACTOR) searchActor: ActorRef, loggingAction: LoggingAction, cc: ControllerComponents, actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends SearchBaseController(cc) {

    val apiVersion = "3.0"

    val mgr: SearchManager = new SearchManager()

    def searchV5() = loggingAction.async { implicit request =>
        val internalReq = getRequest(ApiId.APPLICATION_SEARCH)
        val requestMap: java.util.Map[String, Any] = internalReq.getRequest.asInstanceOf[util.Map[String, Any]]
        requestMap.put(SearchConstants.isSecureSettingsDisabled, true)
        setHeaderContext(internalReq)

        // Parse JWT token from request headers to extract user_roles and org
        val tokenOpt = request.headers.get("x-authenticated-user-token")
          .orElse(request.headers.get("Authorization").map(h => if (h.startsWith("Bearer ")) h.substring(7) else h))

        var userRoles: java.util.List[String] = new java.util.ArrayList[String]()
        var org: String = ""

        tokenOpt.foreach { token =>
          val claims = getClaimsFromToken(token)
          if (claims != null) {
            val rolesObj = claims.get(SearchConstants.USER_ROLES)
            if (rolesObj != null && rolesObj.isInstanceOf[java.util.List[_]]) {
              userRoles = rolesObj.asInstanceOf[java.util.List[String]]
            }
            val orgObj = claims.get(SearchConstants.ORG)
            if (orgObj != null) {
              org = orgObj.toString
            }
          }
        }

        internalReq.getContext.put(SearchConstants.USER_ROLES, userRoles)
        internalReq.getContext.put(SearchConstants.ORG, org)
        internalReq.getContext.put(SearchConstants.API_VERSION, SearchConstants.VERSION_V5)

        val filters = internalReq.getRequest.getOrDefault(SearchConstants.filters, new java.util.HashMap()).asInstanceOf[java.util.Map[String, Object]]
        val visibilityObject = filters.getOrDefault("visibility","")
        var visibility:util.List[String] = null
        if (visibilityObject != null) {
            if (visibilityObject.isInstanceOf[util.ArrayList[_]]) visibility = visibilityObject.asInstanceOf[util.ArrayList[String]]
            else if (visibilityObject.isInstanceOf[String]) visibility = util.Arrays.asList(visibilityObject.asInstanceOf[String])
        }
        if (visibility != null && visibility.contains("Private")) {
            getErrorResponse(ApiId.APPLICATION_SEARCH, apiVersion, SearchConstants.ERR_ACCESS_DENIED, "Cannot access private content through public search api")
        }
        else {
            internalReq.getContext.put(SearchConstants.setDefaultVisibility, "true")
            getResult(mgr.search(internalReq, searchActor), ApiId.APPLICATION_SEARCH)
        }
    }

    private def getClaimsFromToken(token: String): java.util.Map[String, Object] = {
        try {
            val parts = token.split("\\.")
            if (parts.length >= 2) {
                val payloadBytes = Base64.getUrlDecoder.decode(parts(1))
                val payloadString = new String(payloadBytes, "UTF-8")
                JsonUtils.deserialize(payloadString, classOf[java.util.Map[String, Object]])
            } else {
                new java.util.HashMap[String, Object]()
            }
        } catch {
            case NonFatal(e) =>
                new java.util.HashMap[String, Object]()
        }
    }
}
