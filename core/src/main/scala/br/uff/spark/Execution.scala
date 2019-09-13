package br.uff.spark

import java.time.LocalDateTime
import java.util.UUID

class Execution (val name:String){

  var ID: UUID = UUID.randomUUID()
  var startTime = LocalDateTime.now()
  var endTime: LocalDateTime = null

}
