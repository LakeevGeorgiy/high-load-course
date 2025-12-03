package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.kotlin.bulkhead.executeSuspendFunction
import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.prometheus.metrics.core.metrics.Summary
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import okhttp3.ConnectionPool
import okhttp3.ConnectionSpec
import okhttp3.Dispatcher
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.TlsVersion
import okio.IOException
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.lang.Thread.sleep
import java.net.SocketTimeoutException
import java.sql.Time
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    metricRegistry: MeterRegistry
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
    private val rate_limiter = SlidingWindowRateLimiter(rateLimitPerSec * 1L, Duration.ofMillis(1000))
    private val bulkhead = Bulkhead.of("http-client", BulkheadConfig.custom()
        .maxConcurrentCalls(parallelRequests)
        .maxWaitDuration(Duration.ofMillis(1_000_000))
        .build())

    private val sent_to_bank: Counter = Counter
        .builder("sent_request_to_bank")
        .tags("account_name", accountName)
        .register(metricRegistry)

    private val repeat_request: Counter = Counter
        .builder("repeat_request")
        .register(metricRegistry)

    var requestLatency: DistributionSummary = DistributionSummary
        .builder("request_latency")
        .publishPercentiles( 0.9, 0.99, 0.999, 0.9999)
        .register(metricRegistry)

    private val dispatcher = Dispatcher().apply {
        maxRequests = parallelRequests
        maxRequestsPerHost = parallelRequests
    }

    private val connectionPool = ConnectionPool(
        maxIdleConnections = 50,
        keepAliveDuration = 13,
        timeUnit = TimeUnit.MINUTES,
    )

//    private val connectionSpecs = listOf(
//        ConnectionSpec.CLEARTEXT,
//        ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
//            .tlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_2)
//            .build()
//    )

    private val client = HttpClient(io.ktor.client.engine.okhttp.OkHttp) {

        install(HttpTimeout) {
            requestTimeoutMillis = 13_000L
            connectTimeoutMillis = 5_000L
            socketTimeoutMillis = 5_000L
        }

        engine {
            config {
                // Reuse your existing OkHttp configuration
                retryOnConnectionFailure(true)

                // ENABLE HTTP/2 PROTOCOLS
                protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))

                // Your existing connection pool
                connectionPool(connectionPool)
            }
        }

    }

    private val databaseThreadPool = ScheduledThreadPoolExecutor(
        20,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val semaphore = Semaphore(permits = parallelRequests)


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        CoroutineScope(Dispatchers.IO + SupervisorJob()).launch {
            bankPayment(transactionId, paymentId, amount, paymentStartedAt)
        }
    }

    private suspend fun bankPayment(transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        try {
            val urlString = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            sendRequestWithRetry(transactionId, paymentId, urlString, paymentStartedAt)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> handleTimeout(transactionId, paymentId, e)
                else -> handleError(transactionId, paymentId, e)
            }
        }
    }

    private suspend fun sendRequestWithRetry(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long,
    ) {
        val maxAttempts = 3;
        val currentDelay = 2_000
        var attempt = 0

        while (attempt < maxAttempts) {
            try {
                if (sendRequest(transactionId, paymentId, url, paymentStartedAt)) {
                    return
                }
            } catch (e: Exception) {

            }
            attempt++
            if (attempt == maxAttempts) {
                return
            }
        }
    }

    suspend fun sendRequest(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long
    ): Boolean {
        semaphore.withPermit {
            sent_to_bank.increment()
            val startTime = now()

            try {
                val response = client.post(url) { setBody("") }
                val success = handleSuccess(
                    response.status.value,
                    response.bodyAsText(),
                    transactionId,
                    paymentId
                )
                requestLatency.record((now() - startTime).toDouble())
                return true
            } catch (e: Exception) {
                requestLatency.record((now() - startTime).toDouble())
                handleError(transactionId, paymentId, e)
                return false
            }
        }
    }

    private fun handleTimeout(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
        }
    }

    private fun handleError(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = e.message)
        }
    }

    fun handleSuccess(responseCode: Int, responseBody: String, transactionId: UUID, paymentId: UUID): Boolean {
        val body = try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${responseCode}, reason: ${responseBody}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }

        return body.result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
