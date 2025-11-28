package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.prometheus.metrics.core.metrics.Summary
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.ConnectionSpec
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import okhttp3.TlsVersion
import okhttp3.internal.wait
import okio.IOException
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.sql.Time
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


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

    private val connectionSpecs = listOf(
        ConnectionSpec.CLEARTEXT,  // Для HTTP (ваш случай)
        ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
            .tlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_2)
            .build()
    )

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(true)
        .connectTimeout(5_000, TimeUnit.MILLISECONDS)
        .readTimeout(5_000, TimeUnit.SECONDS)
        .writeTimeout(5_000, TimeUnit.MILLISECONDS)
        .callTimeout(13_000, TimeUnit.MILLISECONDS)
        .connectionPool(connectionPool)
        .dispatcher(dispatcher)
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectionSpecs(connectionSpecs)
        .pingInterval(20, TimeUnit.SECONDS)
        .build()

    private val databaseThreadPool = ScheduledThreadPoolExecutor(
        20,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        databaseThreadPool.submit {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        bankPayment(transactionId, paymentId, amount, paymentStartedAt)
    }

    fun sendRequestWithRetry(
        transactionId: UUID,
        paymentId: UUID,
        request: Request,
        paymentStartedAt: Long,
        maxAttempts: Int = 3,
        initialDelayMs: Long = 2_000L
    ) {
        fun attempt(attempt: Int, delayMs: Long) {
            if (attempt > maxAttempts) return

            sendRequest(transactionId, paymentId, request, paymentStartedAt) { success ->
                if (!success && attempt < maxAttempts) {
                    databaseThreadPool.schedule({
                        attempt(attempt + 1, delayMs)
                    }, delayMs, TimeUnit.MILLISECONDS)
                }
            }
        }

        attempt(1, initialDelayMs)
    }


    fun bankPayment(transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            sendRequestWithRetry(transactionId, paymentId, request, paymentStartedAt)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    databaseThreadPool.submit {
                        handleTimeout(transactionId, paymentId, e)
                    }
                }
                else -> {
                    databaseThreadPool.submit {
                        handleError(transactionId, paymentId, e)
                    }
                }

            }
        }
    }


    fun sendRequest(
        transactionId: UUID,
        paymentId: UUID,
        request: Request,
        paymentStartedAt: Long,
        onComplete: (Boolean) -> Unit
    ) {
        bulkhead.executeCallable {
            rate_limiter.tickBlocking()

            sent_to_bank.increment()
            val startTime = now()

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    databaseThreadPool.submit {
                        handleError(transactionId, paymentId, e)
                        requestLatency.record((now() - startTime).toDouble())
                        onComplete(false)
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    val responseCode: Int
                    val responseBody: String
                    response.use {
                        responseCode = response.code
                        responseBody = response.body?.use { it.string() } ?: ""
                    }
                    databaseThreadPool.submit {
                        val success = handleSuccess(responseCode, responseBody, transactionId, paymentId)
                        requestLatency.record((now() - startTime).toDouble())
                        onComplete(success)
                    }
                }
            })
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
