package com.sd.demo.debouncer

import com.sd.lib.debouncer.Debouncer
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class DebouncerTest {
  @Test
  fun `test success`() = runTest {
    val events = mutableListOf<String>()

    var debounceJob: Job? = null

    val debouncer = Debouncer {
      events.add("onBlock")
      debounceJob?.cancel()
    }

    debounceJob = launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()

    advanceUntilIdle()
    assertEquals(listOf("onStart", "onBlock"), events)
  }

  @Test
  fun `test success multi`() = runTest {
    val events = mutableListOf<String>()

    var debounceJob1: Job? = null
    var debounceJob2: Job? = null

    val debouncer = Debouncer {
      events.add("onBlock")
      debounceJob1?.cancel()
      debounceJob2?.cancel()
    }

    debounceJob1 = launch { debouncer.start(1000) { events.add("onStart") } }
    debounceJob2 = launch { debouncer.start(1000) { events.add("onStart") } }
    advanceUntilIdle()

    debouncer.send()
    debouncer.send()

    advanceUntilIdle()
    assertEquals(listOf("onStart", "onBlock"), events)
  }

  @Test
  fun `test send in onStart block`() = runTest {
    val events = mutableListOf<String>()

    var debounceJob: Job? = null

    val debouncer = Debouncer {
      events.add("onBlock")
      debounceJob?.cancel()
    }

    debounceJob = launch {
      debouncer.start(1000) {
        events.add("onStart")
        debouncer.send()
      }
    }
    advanceUntilIdle()

    assertEquals(listOf("onStart", "onBlock"), events)
  }
}