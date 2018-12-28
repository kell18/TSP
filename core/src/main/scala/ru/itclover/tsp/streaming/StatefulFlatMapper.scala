package ru.itclover.tsp.streaming

trait StatefulFlatMapper[In, State, Out] extends ((In, State) => (Seq[Out], State)) { self =>
  def initialState: State
}