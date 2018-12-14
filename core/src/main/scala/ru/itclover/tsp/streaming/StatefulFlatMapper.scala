package ru.itclover.tsp.streaming

trait StatefulFlatMapper[In, State, Out] extends ((In, State) => (Seq[Out], State)) { self =>
  def initialState: State

  def andThen[OtherS, NewOut](other: StatefulFlatMapper[Out, OtherS, NewOut]) =
    new StatefulFlatMapper[In, (State, OtherS), NewOut] {
      override def initialState = (self.initialState, other.initialState)

      override def apply(in: In, states: (State, OtherS)) = {
        val (s1, s2) = states
        val (r1, newS) = self.apply(in, s1)
        // val (r2, newOtherS) = other.apply(r1, s2) // todo after vectorization
        // r2 -> (newS, newOtherS)
        ???
      }
    }
}