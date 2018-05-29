package com.wavesplatform.lang.v1.evaluator

import monix.eval.Coeval
import monix.execution.atomic.{Atomic, _}

sealed trait CoevalRef[A] {
  def read: Coeval[A]
  def write(a: A): Coeval[Unit]
}

object CoevalRef {
  def of[A, R <: Atomic[A]](a: A)(implicit ab: AtomicBuilder[A, R]): CoevalRef[A] = {
    new CoevalRef[A] {
      private val atom: Atomic[A] = Atomic(a)
      override def read: Coeval[A] = Coeval.delay(atom.get)
      override def write(a: A): Coeval[Unit] = Coeval.delay(atom.set(a))
    }
  }
}