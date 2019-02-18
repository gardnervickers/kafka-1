/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import org.apache.kafka.common.internals.FatalExitError

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)
        extends Thread(name) with Logging {
  this.setDaemon(false)
  this.logIdent = "[" + name + "]: "


  private object ThreadStates extends Enumeration {
    type ThreadState = Value
    val Created: ThreadStates.Value = Value(0)
    val Running: ThreadStates.Value = Value(1)
    val ShuttingDown: ThreadStates.Value = Value(2)
    val Shutdown: ThreadStates.Value = Value(3)
  }

  private var threadState: ThreadStates.Value = ThreadStates.Created
  private val threadStateLock: Lock = new ReentrantLock()
  private val shuttingDownCondition: Condition = threadStateLock.newCondition()
  private val shutdownCondition: Condition = threadStateLock.newCondition()
  private def withLock[T](lock: Lock)(f: => T): T = {
    lock.lock()
    try f
    finally lock.unlock()
  }

   /**
    * Set the current thread state, ensuring that we follow the linear progression from
    * Created -> Running -> ShuttingDown -> Shutdown. If a valid state transition occurs that involves a state being
    * waited on, the relevant Condition's are signaled.
    *
    * Returns a tuple of (oldState, newState), which can be used to check if any progress was made.
    * In the event that an invalid state transition is attempted, this method returns a tuple of (oldState, oldState)
    * to signify nothing was changed.
    *
    */
  private def setThreadState(newState: ThreadStates.Value): (ThreadStates.Value, ThreadStates.Value) = withLock(threadStateLock) {
    if (newState >= threadState) {
      val oldState = threadState
      threadState = newState
      newState match {
        case ThreadStates.ShuttingDown =>
          shuttingDownCondition.signalAll()
        case ThreadStates.Shutdown =>
          shuttingDownCondition.signalAll()
          shutdownCondition.signalAll()
        case _ =>
      }
      (oldState, newState)
    } else {
      (threadState, threadState)
    }
  }


  def shutdown(): Unit = {
    initiateShutdown()
    awaitShutdown()
  }

  def isShutdownComplete: Boolean = withLock(threadStateLock) {
    threadState == ThreadStates.Shutdown
  }

  /**
   * Initiate thread shutdown. If the thread has been started, any threads waiting on
   * ShutdownableThread.pause will be signaled. The thread will be interrupted if isInterruptible
   * is true. If the thread has not been started, returns false. If the thread was previously running, returns true.
   */
  def initiateShutdown(): Boolean = {
    val (oldState, newState) = setThreadState(ThreadStates.ShuttingDown)
    if (isInterruptible)
      interrupt()
    oldState == ThreadStates.Running
  }

  /**
   *  After calling initiateShutdown(), use this API to wait until the shutdown is complete. If the thread was not
   *  started or has already finished shutting down, this method returns immediately.
   */
  def awaitShutdown(): Unit = withLock(threadStateLock) {
    if (ThreadStates.Running <= threadState && threadState < ThreadStates.Shutdown)
      shutdownCondition.await()
    info("Shutdown completed")
  }

  /**
   *  Causes the current thread to wait until shutdown is initiated or the specified waiting time elapses.
   *  Blocks the caller thread even if this thread has not been started.
   */
  def pause(timeout: Long, unit: TimeUnit): Unit = withLock(threadStateLock) {
    if (threadState < ThreadStates.ShuttingDown) {
      shuttingDownCondition.await(timeout, unit)
      trace("Shutdown initiated")
    }
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  def doWork(): Unit

  def isRunning: Boolean = withLock(threadStateLock) {
    threadState == ThreadStates.Running
  }


  override def run(): Unit = {
    setThreadState(ThreadStates.Running)
    info("Starting")
    try {
      while (isRunning)
        doWork()
    } catch {
      case e: FatalExitError =>
        setThreadState(ThreadStates.Shutdown)
        Exit.exit(e.statusCode())
      case e: Throwable =>
        if (isRunning)
          error("Error due to", e)
    } finally {
      setThreadState(ThreadStates.Shutdown)
    }
    info("ShutDown")
  }
}
