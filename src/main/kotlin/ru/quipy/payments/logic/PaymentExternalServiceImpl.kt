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
import io.github.resilience4j.ratelimiter.RequestNotPermitted
import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpRequestTimeoutException
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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.http.HttpRequest
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec * 1L, Duration.ofSeconds(1))

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
        // Окно, которым считаем успехи и неуспехи
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
        .slidingWindowSize(100)
        .minimumNumberOfCalls(50)

        // Если упало 10% запросов или 10% были медленными,
        // то открываем цепочку
        .failureRateThreshold(10f)
        .slowCallRateThreshold(10f)
        .slowCallDurationThreshold(Duration.ofMillis(800))

        // Переход из открытой цепочки в полуоткрытую
        .waitDurationInOpenState(Duration.ofSeconds(1))
        // Переход из полуоткрытой в закрытую цепочку
        .permittedNumberOfCallsInHalfOpenState(10)
        .automaticTransitionFromOpenToHalfOpenEnabled(true)
        .recordExceptions(Exception::class.java, HttpRequestTimeoutException::class.java)
        .build()

    private val circuitBreaker = CircuitBreaker.of("circuit-breaker", circuitBreakerConfig)


    private val dispatcherClient = Executors.newFixedThreadPool(40).asCoroutineDispatcher()
    private val dispatcherPayment = Executors.newFixedThreadPool(40).asCoroutineDispatcher()
    private val clientTimeout = 10000000L

    init {
        circuitBreaker.eventPublisher
            .onStateTransition { event ->
                logger.error("[$accountName] CircuitBreaker transition: ${event.stateTransition}")
            }
    }

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
            bankPayment(paymentDto)
        }
    }

    private suspend fun bankPayment(paymentDto: PaymentDto) {
        val urlString = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${paymentDto.transactionId}&paymentId=${paymentDto.paymentId}&amount=${paymentDto.amount}"
        sendRequestWithRetry(paymentDto, urlString)
    }

    private fun calculateRetryDelay(retryCount: Int): Long {
        // 1 << (retrycCount - 1) * 10 + jitter
        val expBackoff = (1L shl (retryCount - 1).coerceAtMost(8)) * 10L
        val jitter = ThreadLocalRandom.current().nextLong(0, 15)
        return (expBackoff + jitter).coerceAtMost(3000L)
    }

    private suspend fun sendRequestWithRetry(
        paymentDto: PaymentDto,
        url: String,
    ) {

        val maxRetries = 100
        var curRetry = 1

        while (curRetry <= maxRetries) {
            if (sendHedgedRequest(paymentDto, url)) {
                paymentESService.update(paymentDto.paymentId) {
                    it.logProcessing(true, now(), paymentDto.transactionId, reason = "success")
                }
                return
            }
            if (now() - paymentDto.paymentStartedAt >= clientTimeout) {
                logger.error("Timeout - duration: ${now() - paymentDto.paymentStartedAt}")
                paymentESService.update(paymentDto.paymentId) {
                    it.logProcessing(false, now(), paymentDto.transactionId, reason = "timeout")
                }
                return
            }
            delay(calculateRetryDelay(curRetry))
            curRetry += 1
            repeatRequest.increment()
        }

        logger.error("Not successful request")
        paymentESService.update(paymentDto.paymentId) {
            it.logProcessing(false, now(), paymentDto.transactionId, reason = "error")
        }
    }

    suspend fun sendHedgedRequest(
        paymentDto: PaymentDto,
        url: String,
    ): Boolean {

        val idempotencyKey = UUID.randomUUID().toString()
        val requestJob = SupervisorJob()
        var result = AtomicBoolean(false)
        val completed = AtomicBoolean(false)
        val responseReceived = CompletableDeferred<Boolean>()

        coroutineScope {
            val primaryJob = launch(requestJob) {
                val ok = sendRequestWithLimiting(paymentDto, url, idempotencyKey)
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
                    val ok = sendRequestWithLimiting(paymentDto, url, idempotencyKey)
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

    suspend fun sendRequestWithLimiting(
        paymentDto: PaymentDto,
        url: String,
        idempotencyKey: String
    ): Boolean {
            semaphore.withPermit {
                rateLimiter.tickBlocking()
                    sent_to_bank.increment()
                    while (!circuitBreaker.tryAcquirePermission()) {
                        return false
                    }

                    return sendRequest(paymentDto, url, idempotencyKey)
            }
        return true
    }

    private suspend fun sendRequest(
        paymentDto: PaymentDto,
        url: String,
        idempotencyKey: String
    ): Boolean {
        try {
            val response = client.post(url) {
                setBody("")
                headers {
                    append("x-idempotency-key", idempotencyKey)
                }
            }

            return handleSuccess(
                response.status.value,
                response.bodyAsText(),
                paymentDto.transactionId,
                paymentDto.paymentId,
                paymentDto.paymentStartedAt
            )
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> handleTimeout(paymentDto.transactionId, paymentDto.paymentId, e)
                else -> handleError(paymentDto.transactionId, paymentDto.paymentId, e)
            }
        }

        val paymentDuration = now() - paymentDto.paymentStartedAt
        requestLatency.record(paymentDuration.toDouble())
//        throw Exception()
        circuitBreaker.onError(paymentDuration, TimeUnit.MILLISECONDS, Exception())
        return false
    }

    private fun handleTimeout(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
    }

    private fun handleError(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
    }

    fun handleSuccess(responseCode: Int, responseBody: String, transactionId: UUID, paymentId: UUID, paymentStartedAt: Long): Boolean {

        val paymentDuration = now() - paymentStartedAt
        if (responseCode != 200) {
            circuitBreaker.onError(paymentDuration, TimeUnit.MILLISECONDS, Exception())

            return false
        }

        if (responseBody.isBlank()) {
            circuitBreaker.onError(paymentDuration, TimeUnit.MILLISECONDS, Exception())
            return false
        }

        val body = try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${responseCode}, reason: ${responseBody}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        circuitBreaker.onSuccess(paymentDuration, TimeUnit.MILLISECONDS)


        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body?.message}")

        return body.result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
