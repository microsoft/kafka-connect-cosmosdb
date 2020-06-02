package com.microsoft.azure.cosmosdb.kafka.connect.model


class Address(var city: String, var state: String) {

  def setCity (city:String)  {
    this.city = city
  }

  def setAge (state:String) {
    this.state = state
  }

  def getCity () : String = {
    city
  }

  def getState () : String = {
    state
  }
}