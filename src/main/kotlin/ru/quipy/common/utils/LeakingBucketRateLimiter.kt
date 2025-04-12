package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.payments.logic.now
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    fun tickBlocking(deadlineMillis: Long) : Boolean {
        if (deadlineMillis <= 0) return false
        val start = now()
        while (!tick()) {
            Thread.sleep(10)
            if (now() > start + deadlineMillis) return false
        }
        return true
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(window.toMillis())
            for (i in 0..rate) {
                queue.poll()
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}