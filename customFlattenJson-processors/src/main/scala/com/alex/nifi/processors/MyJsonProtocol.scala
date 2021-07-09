package com.alex.nifi.processors

import spray.json.{ DefaultJsonProtocol, JsFalse, JsNumber, JsString, JsTrue, JsValue, JsonFormat }

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit object MapJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue with Serializable = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
    }

    def read(value: JsValue): Any = ???
  }
}
