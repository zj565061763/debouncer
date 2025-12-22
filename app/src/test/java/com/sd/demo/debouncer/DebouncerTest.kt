package com.sd.demo.debouncer

import com.sd.lib.debouncer.Debouncer
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
    debouncer.cancel()

    assertEquals(listOf("onStart", "onBlock"), events)
  }

  @Test
  fun `test success multi`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    launch { debouncer.start(1000) { events.add("onStart") } }
    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    debouncer.send()
    debouncer.send()
    advanceTimeBy(1001)
    debouncer.cancel()

    assertEquals(listOf("onStart", "onBlock"), events)
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
    debouncer.cancel()

    assertEquals(listOf("onStart", "onBlock"), events)
  }

  @Test
  fun `test cancel`() = runTest {
    val events = mutableListOf<String>()
    val debouncer = Debouncer { events.add("onBlock") }

    launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    advanceTimeBy(1001)

    debouncer.cancel()
    debouncer.send()

    advanceUntilIdle()
    assertEquals(listOf("onStart", "onBlock"), events)
  }
}