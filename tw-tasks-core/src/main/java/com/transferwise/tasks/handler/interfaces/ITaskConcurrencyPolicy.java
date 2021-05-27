package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;
import java.time.Instant;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

public interface ITaskConcurrencyPolicy {

  /**
   * Engine will ask from it if we can start processing of this task.
   */
  @NonNull
  BookSpaceResponse bookSpace(IBaseTask task);

  @Data
  @Accessors(chain = true)
  @NoArgsConstructor
  class BookSpaceResponse {

    public BookSpaceResponse(boolean hasRoom) {
      setHasRoom(hasRoom);
    }

    /**
     * Set to true, if you want this task to start getting processed.
     */
    private boolean hasRoom;

    /**
     * If there is no room, when should we check again?
     *
     * <p>It is only necessary for some kind of rate limiting based policy, where we are returning "no-room", even when there may be no tasks
     * processing at the same moment.
     * `tryAgainTime` is not needed for a simple counter based concurrency like `SimpleTaskConcurrencyPolicy` implementation.
     *
     * <p>If you have the case, where you are returning no-room while there is no task getting processed at the same time, and you will leave this
     * attribute empty, there is a chance for small (by default 5 seconds) processing pause. So be careful.
     *
     * <p>The trade-off between a small and a large duration is CPU burn vs some latency. Probably best to think how large processing latency you can
     * tolerate in worst case.
     *
     * <p>It is specifying the worst case, usually as other tasks are constantly getting processed, triggered, finished, the engine will do new
     * concurrency checks after those anyway.
     */
    private Instant tryAgainTime;
  }

  /**
   * Engine will call this, after a task processing finished.
   */
  void freeSpace(IBaseTask task);
}
