package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)

    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();

    private val maxRetries = 3
    private val requestTimeout = requestAverageProcessingTime.toMillis() * 2.5
    private val requiredRequestMillis = requestAverageProcessingTime.toMillis() * 1.5
    private val delay = 0L


    private val resultSaver = ThreadPoolExecutor(
        16,
        16,
        0,
        TimeUnit.MINUTES,
        LinkedBlockingQueue<Runnable>(5000),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.DiscardPolicy()
    )

    val scheduler = Executors.newSingleThreadScheduledExecutor()

    private val paymentExecutor = ThreadPoolExecutor(
        256,
        256,
        1,
        TimeUnit.MINUTES,
        LinkedBlockingQueue<Runnable>(5000),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.DiscardPolicy()
    )

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        paymentExecutor.submit {
            pay(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private fun pay(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        tryNumber: Int = 1
    ) {
        if (tryNumber > maxRetries) return

        if (!semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)) {
            throw TimeoutException()
        }

        val remainedTime = (deadline - now() - requiredRequestMillis)

        if (remainedTime <= 0 || !rateLimiter.tickBlocking(remainedTime.toLong())) {
            semaphore.release()
            throw TimeoutException()
        }

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        resultSaver.submit {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        val request = HttpRequest.newBuilder()
            .uri(URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .version(HttpClient.Version.HTTP_2)
            .timeout(Duration.ofMillis(requestTimeout.toLong()))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAcceptAsync { response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                logger.warn("[$accountName][Попытка $tryNumber] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                resultSaver.submit {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }

                if (response.statusCode() in setOf(429, 500, 502, 503, 504) || !body.result) {
                    scheduler.schedule({
                        paymentExecutor.submit {
                            pay(paymentId, amount, paymentStartedAt, deadline, tryNumber + 1)
                        }
                    }, delay, TimeUnit.MILLISECONDS)
                }
            }
            .exceptionally { e ->
                val cause = if (e is CompletionException && e.cause != null) e.cause!! else e
                when (cause) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        resultSaver.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                    }

                    is HttpTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )

                        resultSaver.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                        scheduler.schedule({
                            paymentExecutor.submit {
                                pay(paymentId, amount, paymentStartedAt, deadline, tryNumber + 1)
                            }
                        }, delay, TimeUnit.MILLISECONDS)
                    }

                    else -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        resultSaver.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
                null
            }.whenComplete { _, _ -> semaphore.release() }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()