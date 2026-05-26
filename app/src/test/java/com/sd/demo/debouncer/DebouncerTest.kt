package com.sd.demo.debouncer

import app.cash.turbine.test
import com.sd.lib.debouncer.Debouncer
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class DebouncerTest {
  @Test
  fun `test success`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    val job = launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)
    job.cancel()
  }

  @Test
  fun `test start multi times`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    launch { debouncer.start(1000) { events.add("onStart") } }
    val lastJob = launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onStart", "onStart", "onBlock"), events)
    lastJob.cancel()
  }

  @Test
  fun `test send multi times`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    val job = launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    debouncer.send()
    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)

    debouncer.send()
    advanceTimeBy(1001)
    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock", "onBlock", "onBlock"), events)
    job.cancel()
  }

  @Test
  fun `test send in onStart block`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    val job = launch {
      debouncer.start(1000) {
        events.add("onStart")
        debouncer.send()
      }
    }

    advanceUntilIdle()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)
    job.cancel()
  }

  @Test
  fun `test launch in onStart block`() = runTest {
    val debouncer = Debouncer { }

    var job: Job? = null

    val startJob = launch {
      debouncer.start(1000) {
        job = launch { delay(Long.MAX_VALUE) }
      }
    }

    advanceUntilIdle()
    assertEquals(true, job?.isActive)

    startJob.cancel()
    advanceUntilIdle()
    assertEquals(true, job?.isCancelled)
  }

  @Test
  fun `test isStarted`() = runTest {
    val debouncer = Debouncer { }
    debouncer.isStartedFlow.test {
      assertEquals(false, awaitItem())

      val job = launch { debouncer.start(1000) }
      advanceUntilIdle()
      assertEquals(true, awaitItem())

      job.cancel()
      advanceUntilIdle()
      assertEquals(false, awaitItem())
    }
  }

  @Test
  fun `test isDebouncePending`() = runTest {
    val debouncer = Debouncer { }
    debouncer.isDebouncePendingFlow.test {
      assertEquals(false, awaitItem())

      val job = launch { debouncer.start(1000) }
      advanceUntilIdle()

      debouncer.send()
      advanceUntilIdle()
      assertEquals(true, awaitItem())
      advanceTimeBy(1001)
      assertEquals(false, awaitItem())

      debouncer.send()
      debouncer.send()
      debouncer.send()
      advanceUntilIdle()
      assertEquals(true, awaitItem())
      advanceTimeBy(1001)
      assertEquals(false, awaitItem())
      job.cancel()
    }
  }
}