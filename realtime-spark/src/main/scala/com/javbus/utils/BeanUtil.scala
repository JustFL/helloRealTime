package com.javbus.utils

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

object BeanUtil {


  def copyProperties(src: AnyRef, des: AnyRef) = {
    // 判断原始对象和目标对象都不为空
    if (src != null && des != null) {

      // 获取原始对象的所有属性
      val fields: Array[Field] = src.getClass.getDeclaredFields

      // 遍历所有属性
      for (field <- fields) {
        Breaks.breakable {
          // 获取原始对象的get方法
          val getMethod: Method = src.getClass.getDeclaredMethod(field.getName)
          // 如果目标对象有set方法 则获取 如果没有则跳出此次循环
          val setMethod: Method =
            try {
              des.getClass.getDeclaredMethod(field.getName + "_$eq", field.getType)
            } catch {
              case ex: Exception => Breaks.break()
            }

          // 如果目标对象的属性是val 则跳过此属性 继续下一个属性copy
          val destField: Field = des.getClass.getDeclaredField(field.getName)
          if (destField.getModifiers.equals(Modifier.FINAL)) {
            Breaks.break()
          }

          // 将原始对象属性复制给目标对象
          setMethod.invoke(des, getMethod.invoke(src))
        }
      }
    }
  }
}