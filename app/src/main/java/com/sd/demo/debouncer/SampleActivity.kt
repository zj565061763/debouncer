package com.sd.demo.debouncer

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.sd.demo.debouncer.theme.AppTheme
import com.sd.lib.debouncer.Debouncer
import kotlinx.coroutines.launch

class SampleActivity : ComponentActivity() {
  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    setContent {
      AppTheme {
        Content()
      }
    }
  }
}

@Composable
private fun Content(
  modifier: Modifier = Modifier,
) {
  val debouncer = remember { Debouncer { logMsg { "onBlock" } } }
  val isStarted by debouncer.isStartedFlow.collectAsStateWithLifecycle()
  val isPendingDebounce by debouncer.isDebouncePendingFlow.collectAsStateWithLifecycle()
  val coroutineScope = rememberCoroutineScope()

  Column(
    modifier = modifier.fillMaxSize(),
    horizontalAlignment = Alignment.CenterHorizontally,
  ) {
    if (isStarted) {
      Button(onClick = { debouncer.send() }) {
        Text(text = "send")
      }
      Button(onClick = { debouncer.cancel() }) {
        Text(text = "cancel")
      }
    } else {
      Button(onClick = {
        coroutineScope.launch {
          debouncer.start(2000) {
            logMsg { "onStart" }
            debouncer.send()
          }
        }
      }) {
        Text(text = "start")
      }
    }

    if (isPendingDebounce) {
      CircularProgressIndicator()
    }
  }
}