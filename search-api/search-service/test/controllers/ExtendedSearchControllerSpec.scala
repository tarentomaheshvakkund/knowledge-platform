package controllers

import org.junit.runner._
import org.specs2.runner._
import play.api.libs.json.{JsValue, Json}
import play.api.test._
import play.api.test.Helpers._

@RunWith(classOf[JUnitRunner])
class ExtendedSearchControllerSpec extends BaseSpec {

    val volunteerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX3JvbGVzIjpbIlZPTFVOTEVFUiJdLCJvcmciOiJvcmdfdm9sdW50ZWVyIn0.dummy-signature"
    val standardToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX3JvbGVzIjpbIlBVQkxJQyJdLCJvcmciOiJvcmcxIn0.dummy-signature"

    "ExtendedSearchController" should {
        "return success response for searchV5 API with standard token" in {
            val controller = app.injector.instanceOf[controllers.ExtendedSearchController]
            val json: JsValue = Json.parse("""{"request": {"filters": {"objectType": ["Framework"]}}}""")
            val fakeRequest = FakeRequest("POST", "/v5/search")
              .withJsonBody(json)
              .withHeaders("x-authenticated-user-token" -> standardToken)
            val response = controller.searchV5()(fakeRequest)
            isOK(response)
            status(response) must equalTo(OK)
        }

        "reproduce the XContentBuilder failure on searchV5" in {
            val controller = app.injector.instanceOf[controllers.ExtendedSearchController]
            val jsonStr = """{
                "request": {
                    "filters": {
                        "courseCategory": ["course"],
                        "status": "Live"
                    },
                    "fields": [
                        "identifier",
                        "compatibilityLevel",
                        "courseCategory",
                        "organisation",
                        "source",
                        "name"
                    ],
                    "query": "",
                    "sort_by": {
                        "lastUpdatedOn": "desc"
                    },
                    "limit": 20,
                    "offset": 0,
                    "facets": [
                        "courseCategory"
                    ]
                }
            }"""
            val json: JsValue = Json.parse(jsonStr)
            val fakeRequest = FakeRequest("POST", "/v5/search")
              .withJsonBody(json)
            val response = controller.searchV5()(fakeRequest)
            status(response) must equalTo(OK)
        }

        "test SearchProcessor query serialization" in {
            val searchDTO = new org.sunbird.search.dto.SearchDTO()
            searchDTO.setSecureSettingsDisabled(true)
            
            val userRoles = new java.util.ArrayList[String]()
            userRoles.add("VOLUNTEER")
            searchDTO.addAdditionalProperty("user_roles", userRoles)
            searchDTO.addAdditionalProperty("org", "some-volunteer-org")
            searchDTO.addAdditionalProperty("apiVersion", "v5")
            
            val properties = new java.util.ArrayList[java.util.Map[_, _]]()
            
            val prop1 = new java.util.HashMap[String, Object]()
            prop1.put("propertyName", "courseCategory")
            prop1.put("operation", "EQ")
            prop1.put("values", java.util.Arrays.asList("course"))
            properties.add(prop1.asInstanceOf[java.util.Map[_, _]])
            
            val prop2 = new java.util.HashMap[String, Object]()
            prop2.put("propertyName", "status")
            prop2.put("operation", "EQ")
            prop2.put("values", java.util.Arrays.asList("Live"))
            properties.add(prop2.asInstanceOf[java.util.Map[_, _]])
            
            searchDTO.setProperties(properties.asInstanceOf[java.util.List[java.util.Map[_, _]]])
            searchDTO.setLimit(20)
            searchDTO.setOffset(0)
            searchDTO.setOperation("AND")
            
            val processor = new org.sunbird.search.processor.SearchProcessor()
            val processMethod = classOf[org.sunbird.search.processor.SearchProcessor].getDeclaredMethod("processSearchQuery", 
                classOf[org.sunbird.search.dto.SearchDTO], classOf[java.util.List[java.util.Map[String, Object]]], java.lang.Boolean.TYPE)
            processMethod.setAccessible(true)
            
            val groupByList = new java.util.ArrayList[java.util.Map[String, Object]]()
            val queryBuilder = processMethod.invoke(processor, searchDTO, groupByList, java.lang.Boolean.TRUE).asInstanceOf[org.elasticsearch.search.builder.SearchSourceBuilder]
            
            val queryJson = queryBuilder.toString()
            queryJson must contain("org_eligibility_alias")
            queryJson must contain("courseIds")
            queryJson must contain("some-volunteer-org")
        }

        "test SearchProcessor query serialization for non-v5 API" in {
            val searchDTO = new org.sunbird.search.dto.SearchDTO()
            searchDTO.setSecureSettingsDisabled(true)
            
            val userRoles = new java.util.ArrayList[String]()
            userRoles.add("VOLUNTEER")
            searchDTO.addAdditionalProperty("user_roles", userRoles)
            searchDTO.addAdditionalProperty("org", "some-volunteer-org")
            searchDTO.addAdditionalProperty("apiVersion", "v4")
            
            val properties = new java.util.ArrayList[java.util.Map[_, _]]()
            
            val prop1 = new java.util.HashMap[String, Object]()
            prop1.put("propertyName", "courseCategory")
            prop1.put("operation", "EQ")
            prop1.put("values", java.util.Arrays.asList("course"))
            properties.add(prop1.asInstanceOf[java.util.Map[_, _]])
            
            searchDTO.setProperties(properties.asInstanceOf[java.util.List[java.util.Map[_, _]]])
            searchDTO.setLimit(20)
            searchDTO.setOffset(0)
            searchDTO.setOperation("AND")
            
            val processor = new org.sunbird.search.processor.SearchProcessor()
            val processMethod = classOf[org.sunbird.search.processor.SearchProcessor].getDeclaredMethod("processSearchQuery", 
                classOf[org.sunbird.search.dto.SearchDTO], classOf[java.util.List[java.util.Map[String, Object]]], java.lang.Boolean.TYPE)
            processMethod.setAccessible(true)
            
            val groupByList = new java.util.ArrayList[java.util.Map[String, Object]]()
            val queryBuilder = processMethod.invoke(processor, searchDTO, groupByList, java.lang.Boolean.TRUE).asInstanceOf[org.elasticsearch.search.builder.SearchSourceBuilder]
            
            val queryJson = queryBuilder.toString()
            queryJson must not(contain("org_eligibility_alias"))
            queryJson must not(contain("courseIds"))
            queryJson must not(contain("some-volunteer-org"))
        }

        "return success response for searchV5 API with volunteer token" in {
            val controller = app.injector.instanceOf[controllers.ExtendedSearchController]
            val json: JsValue = Json.parse("""{"request": {"filters": {"objectType": ["Framework"]}}}""")
            val fakeRequest = FakeRequest("POST", "/v5/search")
              .withJsonBody(json)
              .withHeaders("x-authenticated-user-token" -> volunteerToken)
            val response = controller.searchV5()(fakeRequest)
            isOK(response)
            status(response) must equalTo(OK)
        }

        "return client error response for searchV5 API if visibility is 'Private'" in {
            val controller = app.injector.instanceOf[controllers.ExtendedSearchController]
            val json: JsValue = Json.parse("""{"request": {"filters": {"visibility": ["Private"]}}}""")
            val fakeRequest = FakeRequest("POST", "/v5/search")
              .withJsonBody(json)
              .withHeaders("x-authenticated-user-token" -> standardToken)
            val result = controller.searchV5()(fakeRequest)
            status(result) must equalTo(BAD_REQUEST)
        }
        "test SearchProcessor fetch source fields for V5 search" in {
            val searchDTO = new org.sunbird.search.dto.SearchDTO()
            searchDTO.setSecureSettingsDisabled(true)
            searchDTO.addAdditionalProperty("apiVersion", "v5")
            
            val fields = new java.util.ArrayList[String]()
            fields.add("name")
            fields.add("description")
            searchDTO.setFields(fields)
            
            val facets = new java.util.ArrayList[String]()
            facets.add("contentType")
            searchDTO.setFacets(facets)
            
            val properties = new java.util.ArrayList[java.util.Map[_, _]]()
            searchDTO.setProperties(properties.asInstanceOf[java.util.List[java.util.Map[_, _]]])
            searchDTO.setLimit(20)
            searchDTO.setOffset(0)
            searchDTO.setOperation("AND")
            
            val processor = new org.sunbird.search.processor.SearchProcessor()
            val processMethod = classOf[org.sunbird.search.processor.SearchProcessor].getDeclaredMethod("processSearchQuery", 
                classOf[org.sunbird.search.dto.SearchDTO], classOf[java.util.List[java.util.Map[String, Object]]], java.lang.Boolean.TYPE)
            processMethod.setAccessible(true)
            
            val groupByList = new java.util.ArrayList[java.util.Map[String, Object]]()
            val queryBuilder = processMethod.invoke(processor, searchDTO, groupByList, java.lang.Boolean.TRUE).asInstanceOf[org.elasticsearch.search.builder.SearchSourceBuilder]
            
            val fetchSource = queryBuilder.fetchSource()
            fetchSource must not be null
            val includes = fetchSource.includes()
            includes must not be null
            
            val includesList = includes.toList
            includesList must contain("identifier")
            includesList must contain("objectType")
            includesList must contain("contentType")
            includesList must not(contain("name"))
            includesList must not(contain("description"))
        }

        "test SearchActor filterV5Results to only return identifier and facets" in {
            val finalResult = new java.util.HashMap[String, Object]()
            val doc1 = new java.util.HashMap[String, Object]()
            doc1.put("identifier", "do_12345")
            doc1.put("name", "Test content")
            doc1.put("contentType", "Resource")
            doc1.put("objectType", "Content")

            val resultsList = new java.util.ArrayList[java.util.Map[String, Object]]()
            resultsList.add(doc1)
            finalResult.put("results", resultsList)

            val request = new org.sunbird.common.dto.Request()
            request.setContext(new java.util.HashMap[String, Object]())
            request.getContext.put("apiVersion", "v5")
            request.setRequest(new java.util.HashMap[String, Object]())
            val facets = new java.util.ArrayList[String]()
            facets.add("contentType")
            request.getRequest.put("facets", facets)

            val filteredResult = org.sunbird.actors.SearchActor.filterV5Results(finalResult, request)
            filteredResult must not be null
            val filteredList = filteredResult.get("results").asInstanceOf[java.util.List[java.util.Map[String, Object]]]
            filteredList.size() must equalTo(1)
            val filteredDoc = filteredList.get(0)
            filteredDoc.containsKey("identifier") must beTrue
            filteredDoc.containsKey("contentType") must beTrue
            filteredDoc.containsKey("name") must beFalse
            filteredDoc.containsKey("objectType") must beFalse
            filteredDoc.get("identifier") must equalTo("do_12345")
            filteredDoc.get("contentType") must equalTo("Resource")
        }
    }
}
