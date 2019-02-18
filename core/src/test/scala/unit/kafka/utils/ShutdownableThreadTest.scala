/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kafka.common.internals.FatalExitError
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Test}

class ShutdownableThreadTest {

  @After
  def tearDown(): Unit = Exit.resetExitProcedure()

  @Test
  def testShutdownWhenCalledAfterThreadStart(): Unit = {
    val shutdownLatch = new CountDownLatch(1)
    try {
      @volatile var statusCodeOption: Option[Int] = None
      Exit.setExitProcedure { (statusCode, _) =>
        statusCodeOption = Some(statusCode)
        // Sleep until interrupted to emulate the fact that `System.exit()` never returns
        shutdownLatch.await()
        throw new AssertionError
      }
      val latch = new CountDownLatch(1)
      val thread = new ShutdownableThread("shutdownable-thread-test") {
        override def doWork(): Unit = {
          latch.countDown()
          throw new FatalExitError
        }
      }
      thread.start()
      assertTrue("doWork was not invoked", latch.await(10, TimeUnit.SECONDS))

      thread.shutdown()
      TestUtils.waitUntilTrue(() => statusCodeOption.isDefined, "Status code was not set by exit procedure")
      assertEquals(1, statusCodeOption.get)
    } finally {
      // allow thread to shutdown at the end of the test
      shutdownLatch.countDown()
    }
  }

  @Test
  def testInterrupt(): Unit =  {
    val startupLatch = new CountDownLatch(1)
    val shutdownLatch = new CountDownLatch(1)
    try {
      @volatile var caughtInterrupt = false
      val thread = new ShutdownableThread("shutdownable-thread-test") {
        override def doWork(): Unit = {
          startupLatch.countDown()
          try {
            shutdownLatch.await()
          } catch {
            case e: InterruptedException =>
              caughtInterrupt = true
              throw e
          }
        }
      }
      thread.start()
      startupLatch.await(10, TimeUnit.SECONDS)
      assertEquals("Interrupt should not have been triggered yet", caughtInterrupt, false)

      thread.shutdown()
      thread.awaitShutdown()
      assertEquals("Expected interrupt to be caught", caughtInterrupt, true)

    } finally {
      // release the thread if it hasn't already been released
      shutdownLatch.countDown()
    }
  }

  @Test
  def testAwaitingOnShutdown(): Unit = {

    val threadStartedLatch = new CountDownLatch(1)
    val thread = new ShutdownableThread("shutdownable-thread-test") {
      override def doWork(): Unit = {
        threadStartedLatch.countDown()
        while (true) {
          Thread.sleep(100)
        }
      }
    }
    thread.start()
    threadStartedLatch.await(10, TimeUnit.SECONDS)

    val shutdownInitiated = new CountDownLatch(1)
    val shutdownInitiatedThread = new Thread() {
      override def run(): Unit = {
        thread.pause(10, TimeUnit.SECONDS)
        shutdownInitiated.countDown()
      }
    }
    shutdownInitiatedThread.start()

    val shutdownCompleted = new CountDownLatch(1)
    val awaitShutdownThread = new Thread() {
      override def run(): Unit = {
        thread.awaitShutdown()
        shutdownCompleted.countDown()
      }
    }
    awaitShutdownThread.start()

    assertEquals(shutdownInitiated.getCount, 1)
    assertEquals(shutdownCompleted.getCount, 1)


    assertTrue(thread.initiateShutdown())
    shutdownInitiated.await(10, TimeUnit.SECONDS)
    assertEquals(shutdownInitiated.getCount, 0)


    thread.awaitShutdown()
    shutdownCompleted.await(10, TimeUnit.SECONDS)
    assertEquals(shutdownCompleted.getCount, 0)

  }

  @Test
  def testNonStartedThread(): Unit = {
    val thread = new ShutdownableThread("shutdownable-thread-test") {
      override def doWork(): Unit = {
      }
    }

    val awaitShutdownLatch = new CountDownLatch(1)
    val awaitShutdownThread = new Thread() {
      override def run(): Unit = {
        thread.awaitShutdown()
        awaitShutdownLatch.countDown()
      }
    }
    awaitShutdownThread.start()
    awaitShutdownLatch.await(10, TimeUnit.SECONDS)
    assertEquals("Expected awaitShutdown() to return without blocking", awaitShutdownLatch.getCount, 0)
  }
}
