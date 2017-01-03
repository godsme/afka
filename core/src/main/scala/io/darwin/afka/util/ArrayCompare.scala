package io.darwin.afka.util

/**
  * Created by darwin on 3/1/2017.
  */

object ArrayCompare {

  def apply[A](lhs: Array[A], rhs: Array[A]): Boolean = {
    if(lhs.length == rhs.length) {
      for(i ← 0 until lhs.length) {
        if(lhs(i) != rhs(i)) return false
      }
      return true
    }

    false
  }

}