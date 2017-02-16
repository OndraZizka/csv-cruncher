package cz.dynawest.util;

public class InfiniteLoopPrevention {
   private long maxLoops;

   public InfiniteLoopPrevention(long maxLoops) {
      this.maxLoops = maxLoops;
   }

   public void doStep() throws InfiniteLoopPrevention.LimitReached {
      if(this.maxLoops <= 0L) {
         throw new InfiniteLoopPrevention.LimitReached("Reached the limit of maximum loops: " + this.maxLoops + "." + " This means you probably programmed an infinite loop or it had inexpectedly big number of iterations.");
      } else {
         --this.maxLoops;
      }
   }

   public class LimitReached extends RuntimeException {
      public LimitReached(String message) {
         super(message);
      }
   }
}
