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
    }
}
