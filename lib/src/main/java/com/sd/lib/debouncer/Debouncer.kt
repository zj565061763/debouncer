package com.sd.lib.debouncer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface Debouncer {
  /** 是否已经开始收集 */
  val isStartedFlow: StateFlow<Boolean>

  /** 是否有事件触发Debounce等待中 */
  val isDebouncePendingFlow: StateFlow<Boolean>

  /** 开始挂起收集事件，多次调用会取消上一次的调用。 */
  suspend fun start(
    /** debounce时长必须大于0，否则抛异常[IllegalArgumentException] */
    timeoutMillis: Long,
    /** 开始回调 */
    onStart: CoroutineScope.() -> Unit = {},
  )

  /** 发送事件 */
  fun send()
}

fun Debouncer(onBlock: () -> Unit): Debouncer {
  return DebouncerImpl(onBlock)
}

/** 等待Debounce结束，如果当前有事件触发Debounce等待中，则挂起 */
suspend fun Debouncer.awaitNotPending() {
  isDebouncePendingFlow.first { !it }
}

private class DebouncerImpl(
  private val onBlock: () -> Unit,
) : Debouncer {
  private val _jobMutex = Mutex()
  private var _job: Job? = null

  private val _debounceFlow = MutableSharedFlow<Unit>(
    extraBufferCapacity = 1,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )

  private val _isStartedFlow = MutableStateFlow(false)
  private val _isDebouncePendingFlow = MutableStateFlow(false)

  override val isStartedFlow: StateFlow<Boolean> = _isStartedFlow.asStateFlow()
  override val isDebouncePendingFlow: StateFlow<Boolean> = _isDebouncePendingFlow.asStateFlow()

  @OptIn(ExperimentalCoroutinesApi::class)
  override suspend fun start(
    timeoutMillis: Long,
    onStart: CoroutineScope.() -> Unit,
  ) {
    require(timeoutMillis > 0) { "timeoutMillis should > 0" }
    coroutineScope {
      val debounceJob = coroutineContext.job

      _jobMutex.withLock {
        _job?.cancelAndJoin()
        _job = debounceJob
        debounceJob.invokeOnCompletion {
          _jobMutex.extTryLock { if (_job === debounceJob) _job = null }
        }
      }

      try {
        _debounceFlow
          .onSubscription {
            _isStartedFlow.value = true
            onStart()
          }
          .mapLatest {
            _isDebouncePendingFlow.value = true
            delay(timeoutMillis)
            _isDebouncePendingFlow.value = false
          }
          .collect {
            onBlock()
          }
      } finally {
        _isDebouncePendingFlow.value = false
        _isStartedFlow.value = false
      }
    }
  }

  override fun send() {
    _debounceFlow.tryEmit(Unit)
  }
}

private inline fun Mutex.extTryLock(block: () -> Unit) {
  if (tryLock()) {
    try {
      block()
    } finally {
      unlock()
    }
  }
}