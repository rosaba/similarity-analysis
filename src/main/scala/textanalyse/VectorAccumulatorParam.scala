package textanalyse

import org.apache.spark.AccumulatorParam

object VectorAccumulatorParam extends AccumulatorParam[Vector[Int]]{
  
   def zero(initialValue: Vector[Int]): Vector[Int] = {
     Vector.fill(initialValue.size){0}
   }
   
   def addInPlace(v1: Vector[Int], v2: Vector[Int]): Vector[Int] = {
     (for (i <- v1.indices) yield (v1(i)+v2(i))).toVector
   }
}