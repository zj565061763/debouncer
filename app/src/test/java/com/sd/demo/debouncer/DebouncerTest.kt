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

    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)

    debouncer.cancel()
  }

  @Test
  fun `test start multi times`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    launch { debouncer.start(1000) { events.add("onStart") } }
    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onStart", "onStart", "onBlock"), events)

    debouncer.cancel()
  }

  @Test
  fun `test send multi times`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
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

    debouncer.cancel()
  }

  @Test
  fun `test send in onStart block`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch {
      debouncer.start(1000) {
        events.add("onStart")
        debouncer.send()
      }
    }

    advanceUntilIdle()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)

    debouncer.cancel()
  }

  @Test
  fun `test cancel`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)

    debouncer.cancel()
    debouncer.send()
    advanceTimeBy(1001)
    assertEquals(listOf("onStart", "onBlock"), events)
  }

  @Test
  fun `test cancelAndJoin`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()
    assertEquals(true, debouncer.isStartedFlow.value)

    debouncer.send()
    advanceTimeBy(500)
    assertEquals(true, debouncer.isDebouncePendingFlow.value)

    debouncer.cancelAndJoin()

    assertEquals(false, debouncer.isStartedFlow.value)
    assertEquals(false, debouncer.isDebouncePendingFlow.value)

    advanceTimeBy(1000)
    assertEquals(listOf("onStart"), events)
  }

  @Test
  fun `test launch in onStart block`() = runTest {
    val debouncer = Debouncer { }

    var job: Job? = null

    launch {
      debouncer.start(1000) {
        job = launch { delay(Long.MAX_VALUE) }
      }
    }

    advanceUntilIdle()
    assertEquals(true, job?.isActive)

    debouncer.cancel()
    assertEquals(true, job?.isCancelled)
  }

  @Test
  fun `test isStarted`() = runTest {
    val debouncer = Debouncer { }
    debouncer.isStartedFlow.test {
      assertEquals(false, awaitItem())

      launch { debouncer.start(1000) }
      advanceUntilIdle()
      assertEquals(true, awaitItem())

      debouncer.cancel()
      advanceUntilIdle()
      assertEquals(false, awaitItem())
    }
  }

  @Test
  fun `test isDebouncePending`() = runTest {
    val debouncer = Debouncer { }
    debouncer.isDebouncePendingFlow.test {
      assertEquals(false, awaitItem())

      launch { debouncer.start(1000) }
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

      debouncer.cancel()
    }
  }
}