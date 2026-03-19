package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import io.github.resilience4j.kotlin.ratelimiter.executeSuspendFunction
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.headers
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.log

class PaymentDto(
    val paymentId: UUID,
    val transactionId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

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
    private val rateLimiter = RateLimiter.of("rate-limiter", RateLimiterConfig.custom()
        .limitForPeriod(rateLimitPerSec)
        .limitRefreshPeriod(Duration.ofMillis(1000))
        .timeoutDuration(Duration.ofSeconds(100))
        .build()
    )

    private val sent_to_bank: Counter = Counter
        .builder("sent_request_to_bank")
        .tags("account_name", accountName)
        .register(metricRegistry)

    private val repeatRequest: Counter = Counter
        .builder("repeat_request")
        .register(metricRegistry)

    private val hedgedRequest: Counter = Counter
        .builder("hedged_request")
        .register(metricRegistry)

    private var requestLatency: DistributionSummary = DistributionSummary
        .builder("request_latency")
        .publishPercentiles( 0.9, 0.99, 0.999, 0.9999)
        .register(metricRegistry)

    private var circuitBreakerConfig = CircuitBreakerConfig.custom()
        .failureRateThreshold(50f)
        .slowCallRateThreshold(50f)
        .slowCallDurationThreshold(Duration.ofMillis(1500))
        .waitDurationInOpenState(Duration.ofMillis(5_000))
        .maxWaitDurationInHalfOpenState(Duration.ofMillis(5_000))
        .permittedNumberOfCallsInHalfOpenState(rateLimitPerSec / 10)
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
        .slidingWindowSize(300)
        .build()

    private val circuitBreaker = CircuitBreaker.of("circuit-breaker", circuitBreakerConfig)
    private var circuitStatus: Counter = Counter
        .builder("circuit-status")
        .register(metricRegistry)

    private val paymentQueue = Collections.synchronizedList(mutableListOf<PaymentDto>())
    private val paymentChannel = Channel<PaymentDto>(Channel.UNLIMITED)
    private val channelSize = AtomicInteger(0)


    private val dispatcherClient = Executors.newFixedThreadPool(60).asCoroutineDispatcher()
    private val dispatcherPayment = Executors.newFixedThreadPool(60).asCoroutineDispatcher()

    private val client = HttpClient(Java) {

        install(HttpTimeout) {
            requestTimeoutMillis = 150000L
        }

        engine {
            pipelining=true
            dispatcher=dispatcherClient
        }
    }

    private val semaphore = Semaphore(permits = parallelRequests)

    init {
        processQueue()
    }

    private fun processQueue() {
        CoroutineScope(dispatcherPayment + SupervisorJob()).launch {
            paymentChannel.consumeAsFlow()
                .onEach { payment ->
                    val success = performPayment(payment)
                    if (!success) {
                        paymentChannel.send(payment)
                    } else {
                        channelSize.decrementAndGet()
                    }

                    val curSize = channelSize.get()
                    logger.error("Channel size: $curSize")
                }
                .launchIn(this)
        }
    }


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val paymentDto = PaymentDto(paymentId, transactionId, amount, paymentStartedAt, deadline)

        CoroutineScope(dispatcherPayment + SupervisorJob()).launch {
            val result = performPayment(paymentDto)
            if (!result) {
                channelSize.incrementAndGet()
                paymentChannel.send(paymentDto)
            }
        }
    }

    private suspend fun performPayment(paymentDto: PaymentDto): Boolean {
        var result = false
        when (circuitBreaker.state) {
            CircuitBreaker.State.OPEN -> {
                paymentChannel.send(paymentDto)
                logger.error("Open - timeout: ${circuitBreaker.metrics.slowCallRate} failures: ${circuitBreaker.metrics.failureRate}")
            }

            CircuitBreaker.State.HALF_OPEN -> {
                if (!circuitBreaker.tryAcquirePermission()) {
                    paymentChannel.send(paymentDto)
                } else {
                    result = bankPayment(paymentDto)
                }
            }

            CircuitBreaker.State.CLOSED -> {
                delay(1000)
                result = bankPayment(paymentDto)
            }

            else -> {
                logger.error("Strange state in circuit breaker: ${circuitBreaker.state}")
            }
        }
        return result
    }

    private suspend fun bankPayment(paymentDto: PaymentDto): Boolean {
        val urlString = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${paymentDto.transactionId}&paymentId=${paymentDto.paymentId}&amount=${paymentDto.amount}"
        return sendRequestWithRetry(paymentDto.transactionId, paymentDto.paymentId, urlString, paymentDto.paymentStartedAt)
    }

    private suspend fun sendRequestWithRetry(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long,
    ): Boolean {

        val delayMs = 3000L
        val maxRetries = 3
        var curRetry = 1

        while (curRetry <= maxRetries) {
            if (curRetry > 1) {
                repeatRequest.increment()
            }
            if (sendHedgedRequest(transactionId, paymentId, url, paymentStartedAt)) {
                return true
            }
            delay(delayMs)
            curRetry += 1
        }
        return false
    }

    suspend fun sendRequestWithLimiting(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long,
        idempotencyKey: String
    ): Boolean {
        var result = true
        semaphore.withPermit {
            rateLimiter.executeSuspendFunction {

                sent_to_bank.increment()
                result = sendRequest(transactionId, paymentId, url, idempotencyKey)

            }
        }
        return result
    }

    suspend fun sendHedgedRequest(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long,
    ): Boolean {

        val idempotencyKey = UUID.randomUUID().toString()
        val requestJob = SupervisorJob()
        var result = AtomicBoolean(false)
        val completed = AtomicBoolean(false)
        val responseReceived = CompletableDeferred<Boolean>()

        coroutineScope {
            val primaryJob = launch(requestJob) {
                val ok = sendRequestWithLimiting(transactionId, paymentId, url, paymentStartedAt, idempotencyKey)
                if (ok) {
                    result.compareAndSet(false, true)
                }
                if (completed.compareAndSet(false, true)) {
                    responseReceived.complete(true)
                }
            }

            val hedgedJob = launch(requestJob) {
                delay(90)
                if (!completed.get()){
                    hedgedRequest.increment()
                    val ok = sendRequestWithLimiting(transactionId, paymentId, url, paymentStartedAt, idempotencyKey)
                    if (ok) {
                        result.compareAndSet(false, true)
                    }
                    if (completed.compareAndSet(false, true)) {
                        responseReceived.complete(true)
                    }
                }
            }

            val allJobs = listOf(primaryJob) + hedgedJob
            if (completed.get()) {
                allJobs.forEach { it.cancel() }
            }
        }
        return result.get()
    }

    private suspend fun sendRequest(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        idempotencyKey: String
    ): Boolean {
        var result = false
        val paymentStartedAt = now()
        try {
            val response = client.post(url) {
                setBody("")
                headers {
                    append("x-idempotency-key", idempotencyKey)
                }
            }
            result = handleSuccess(
                response.status.value,
                response.bodyAsText(),
                transactionId,
                paymentId,
                paymentStartedAt
            )
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> handleTimeout(transactionId, paymentId, e)
                else -> handleError(transactionId, paymentId, e)
            }
            result = false
        }

        val paymentDuration = now() - paymentStartedAt
        requestLatency.record(paymentDuration.toDouble())

        if (result) {
            circuitBreaker.onSuccess(paymentDuration, TimeUnit.MILLISECONDS)
        } else {
            circuitBreaker.onError(paymentDuration, TimeUnit.MILLISECONDS, Exception())
        }
        return result
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

    fun handleSuccess(responseCode: Int, responseBody: String, transactionId: UUID, paymentId: UUID, paymentStartedAt: Long): Boolean {
        val body = try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${responseCode}, reason: ${responseBody}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body?.message}")

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
