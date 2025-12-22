package com.sd.lib.debouncer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.job
import kotlin.coroutines.cancellation.CancellationException

interface Debouncer {
  /** 是否已经开始收集 */
  val isStarted: StateFlow<Boolean>

  /** 是否有事件触发Debounce等待中 */
  val isDebouncePending: StateFlow<Boolean>

  /**
   * 开始收集事件，如果本次调用触发开始，则在调用处挂起，否则方法直接返回，
   * 如果挂起后，调用了[cancel]，则调用处会抛异常[CancellationException]
   */
  suspend fun start(
    /** debounce时长必须大于0，否则抛异常[IllegalArgumentException] */
    timeoutMillis: Long,
    /** 开始回调 */
    onStart: CoroutineScope.() -> Unit = {},
  )

  /** 发送事件 */
  fun send()

  /** 取消收集事件 */
  fun cancel()
}

fun Debouncer(onBlock: () -> Unit): Debouncer {
  return DebouncerImpl(onBlock)
}

private class DebouncerImpl(
  private val onBlock: () -> Unit,
) : Debouncer {
  private val _debounceFlow = MutableSharedFlow<Unit>(
    replay = 1,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )

  @Volatile
  private var _debounceJob: Job? = null

  private val _isStartedFlow = MutableStateFlow(false)
  private val _isDebouncePendingFlow = MutableStateFlow(false)

  override val isStarted: StateFlow<Boolean> = _isStartedFlow.asStateFlow()
  override val isDebouncePending: StateFlow<Boolean> = _isDebouncePendingFlow.asStateFlow()

  @OptIn(FlowPreview::class)
  override suspend fun start(
    timeoutMillis: Long,
    onStart: CoroutineScope.() -> Unit,
  ) {
    require(timeoutMillis > 0) { "timeoutMillis should > 0" }
    if (_debounceJob == null && _isStartedFlow.compareAndSet(expect = false, update = true)) {
      try {
        coroutineScope {
          _debounceJob = coroutineContext.job
          onStart()
          _debounceFlow
            .onEach { _isDebouncePendingFlow.value = true }
            .debounce(timeoutMillis)
            .collect {
              _isDebouncePendingFlow.value = false
              onBlock()
            }
        }
      } finally {
        cleanup()
      }
    }
  }

  override fun send() {
    if (_isStartedFlow.value) {
      _debounceFlow.tryEmit(Unit)
    }
  }

  override fun cancel() {
    cleanup()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private fun cleanup() {
    if (_isStartedFlow.compareAndSet(expect = true, update = false)) {
      _debounceJob?.cancel()
      _debounceFlow.resetReplayCache()
      _isDebouncePendingFlow.value = false
      _debounceJob = null
    }
  }
}