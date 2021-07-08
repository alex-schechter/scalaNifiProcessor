package com.alex.nifi.processors

import spray.json.{ DefaultJsonProtocol, JsBoolean, JsNumber, JsObject, JsString, JsValue, JsonFormat, JsTrue, JsFalse }

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit object MapJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
    }
    def read(value: JsValue) = ???
  }
}