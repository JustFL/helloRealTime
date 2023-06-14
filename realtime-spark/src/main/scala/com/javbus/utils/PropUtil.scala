package com.javbus.utils

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

object PropUtil {

  private val props: Properties = new Properties()
  private val in: InputStream = PropUtil.getClass.getResourceAsStream("/config.properties")
  props.load(in)

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def getPropWithStream(propName: String): String = {
    props.getProperty(propName)
  }

  def getPropWithBundle(propName: String): String = {
    bundle.getString(propName)
  }
}
