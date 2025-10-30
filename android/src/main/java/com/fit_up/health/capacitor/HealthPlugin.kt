package com.fit_up.health.capacitor

import android.content.Intent
import android.net.Uri
import android.util.Log
import androidx.activity.result.ActivityResultCallback
import androidx.activity.result.ActivityResultLauncher
import androidx.activity.result.contract.ActivityResultContract
import androidx.health.connect.client.HealthConnectClient
import androidx.health.connect.client.PermissionController
import androidx.health.connect.client.aggregate.AggregateMetric
import androidx.health.connect.client.aggregate.AggregationResult
import androidx.health.connect.client.aggregate.AggregationResultGroupedByPeriod
import androidx.health.connect.client.records.ActiveCaloriesBurnedRecord
import androidx.health.connect.client.records.DistanceRecord
import androidx.health.connect.client.records.ExerciseRouteResult
import androidx.health.connect.client.records.ExerciseSessionRecord
import androidx.health.connect.client.records.HeartRateRecord
import androidx.health.connect.client.records.StepsRecord
import androidx.health.connect.client.records.TotalCaloriesBurnedRecord
import androidx.health.connect.client.request.AggregateGroupByPeriodRequest
import androidx.health.connect.client.request.AggregateRequest
import androidx.health.connect.client.request.ReadRecordsRequest
import androidx.health.connect.client.time.TimeRangeFilter
import com.getcapacitor.JSArray
import com.getcapacitor.JSObject
import com.getcapacitor.Plugin
import com.getcapacitor.PluginCall
import com.getcapacitor.PluginMethod
import com.getcapacitor.annotation.CapacitorPlugin
import com.getcapacitor.annotation.Permission
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.time.Instant
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneId
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import kotlin.jvm.optionals.getOrDefault
import androidx.health.connect.client.records.BloodGlucoseRecord
import androidx.health.connect.client.records.OxygenSaturationRecord
import androidx.health.connect.client.records.RespiratoryRateRecord
import androidx.health.connect.client.records.WeightRecord
import androidx.health.connect.client.records.BodyFatRecord
import androidx.health.connect.client.records.BloodPressureRecord
import androidx.health.connect.client.records.RestingHeartRateRecord
import androidx.health.connect.client.records.Record

// Constants for data types and buckets
private const val BUCKET_DAY = "day"
private const val DATA_TYPE_STEPS = "steps"
private const val DATA_TYPE_ACTIVE_CALORIES = "active-calories"
private const val DATA_TYPE_TOTAL_CALORIES = "total-calories"
private const val DATA_TYPE_DISTANCE = "distance"
private const val MAX_QUERY_LIMIT = 5000  // Android Health Connect API limit

/**
 * Health Connect plugin for Capacitor that provides access to health and fitness data
 * through Android's Health Connect API.
 */
@CapacitorPlugin(
    name = "HealthPlugin",
    permissions = [
        Permission(
            alias = "READ_STEPS",
            strings = ["android.permission.health.READ_STEPS"]
        ),
        Permission(
            alias = "READ_WORKOUTS",
            strings = ["android.permission.health.READ_EXERCISE"]
        ),
        Permission(
            alias = "READ_DISTANCE",
            strings = ["android.permission.health.READ_DISTANCE"]
        ),
        Permission(
            alias = "READ_ACTIVE_CALORIES",
            strings = ["android.permission.health.READ_ACTIVE_CALORIES_BURNED"]
        ),
        Permission(
            alias = "READ_TOTAL_CALORIES",
            strings = ["android.permission.health.READ_TOTAL_CALORIES_BURNED"]
        ),
        Permission(
            alias = "READ_HEART_RATE",
            strings = ["android.permission.health.READ_HEART_RATE"]
        ),
        Permission(
            alias = "READ_ROUTE",
            strings = ["android.permission.health.READ_EXERCISE_ROUTE"]
        ),
        Permission(
            alias = "READ_BLOOD_GLUCOSE",
            strings = ["android.permission.health.READ_BLOOD_GLUCOSE"]
        ),
        Permission(
            alias = "READ_OXYGEN_SATURATION",
            strings = ["android.permission.health.READ_OXYGEN_SATURATION"]
        ),
        Permission(
            alias = "READ_RESPIRATORY_RATE",
            strings = ["android.permission.health.READ_RESPIRATORY_RATE"]
        ),
        Permission(
            alias = "READ_BODY_MASS",
            strings = ["android.permission.health.READ_BODY_MASS"]
        ),
        Permission(
            alias = "READ_BODY_FAT",
            strings = ["android.permission.health.READ_BODY_FAT"]
        ),
        Permission(
            alias = "READ_BLOOD_PRESSURE", 
            strings = ["android.permission.health.READ_BLOOD_PRESSURE"]
        ),
        Permission(
            alias = "READ_RESTING_HEART_RATE",
            strings = ["android.permission.health.READ_RESTING_HEART_RATE"]
        )
    ]
)
class HealthPlugin : Plugin() {

    private val tag = "CapHealth"
    private lateinit var healthConnectClient: HealthConnectClient
    private var available: Boolean = false
    private lateinit var permissionsLauncher: ActivityResultLauncher<Set<String>>

    /**
     * Enum representing all supported Health Connect permissions
     */
    enum class CapHealthPermission {
        READ_STEPS, READ_WORKOUTS, READ_HEART_RATE, READ_ROUTE, 
        READ_ACTIVE_CALORIES, READ_TOTAL_CALORIES, READ_DISTANCE, 
        READ_BLOOD_GLUCOSE, READ_OXYGEN_SATURATION, READ_RESPIRATORY_RATE, 
        READ_BODY_MASS, READ_BODY_FAT, READ_BLOOD_PRESSURE, READ_RESTING_HEART_RATE;

        companion object {
            fun from(s: String): CapHealthPermission? {
                return try {
                    valueOf(s)
                } catch (e: Exception) {
                    null
                }
            }
        }
    }

    /**
     * Mapping between plugin permissions and Android Health Connect permissions
     */
    private val permissionMapping = mapOf(
        Pair(CapHealthPermission.READ_WORKOUTS, "android.permission.health.READ_EXERCISE"),
        Pair(CapHealthPermission.READ_ROUTE, "android.permission.health.READ_EXERCISE_ROUTE"),
        Pair(CapHealthPermission.READ_HEART_RATE, "android.permission.health.READ_HEART_RATE"),
        Pair(CapHealthPermission.READ_ACTIVE_CALORIES, "android.permission.health.READ_ACTIVE_CALORIES_BURNED"),
        Pair(CapHealthPermission.READ_TOTAL_CALORIES, "android.permission.health.READ_TOTAL_CALORIES_BURNED"),
        Pair(CapHealthPermission.READ_DISTANCE, "android.permission.health.READ_DISTANCE"),
        Pair(CapHealthPermission.READ_STEPS, "android.permission.health.READ_STEPS"),
        Pair(CapHealthPermission.READ_BLOOD_GLUCOSE, "android.permission.health.READ_BLOOD_GLUCOSE"),
        Pair(CapHealthPermission.READ_OXYGEN_SATURATION, "android.permission.health.READ_OXYGEN_SATURATION"),
        Pair(CapHealthPermission.READ_RESPIRATORY_RATE, "android.permission.health.READ_RESPIRATORY_RATE"),
        Pair(CapHealthPermission.READ_BODY_MASS, "android.permission.health.READ_BODY_MASS"),
        Pair(CapHealthPermission.READ_BODY_FAT, "android.permission.health.READ_BODY_FAT"),
        Pair(CapHealthPermission.READ_BLOOD_PRESSURE, "android.permission.health.READ_BLOOD_PRESSURE"),
        Pair(CapHealthPermission.READ_RESTING_HEART_RATE, "android.permission.health.READ_RESTING_HEART_RATE")
    )

    /**
     * Exercise type mapping from Health Connect codes to readable names
     */
    private val exerciseTypeMapping = mapOf(
        0 to "OTHER",
        2 to "BADMINTON",
        4 to "BASEBALL",
        5 to "BASKETBALL",
        8 to "BIKING",
        9 to "BIKING_STATIONARY",
        10 to "BOOT_CAMP",
        11 to "BOXING",
        13 to "CALISTHENICS",
        14 to "CRICKET",
        16 to "DANCING",
        25 to "ELLIPTICAL",
        26 to "EXERCISE_CLASS",
        27 to "FENCING",
        28 to "FOOTBALL_AMERICAN",
        29 to "FOOTBALL_AUSTRALIAN",
        31 to "FRISBEE_DISC",
        32 to "GOLF",
        33 to "GUIDED_BREATHING",
        34 to "GYMNASTICS",
        35 to "HANDBALL",
        36 to "HIGH_INTENSITY_INTERVAL_TRAINING",
        37 to "HIKING",
        38 to "ICE_HOCKEY",
        39 to "ICE_SKATING",
        44 to "MARTIAL_ARTS",
        46 to "PADDLING",
        47 to "PARAGLIDING",
        48 to "PILATES",
        50 to "RACQUETBALL",
        51 to "ROCK_CLIMBING",
        52 to "ROLLER_HOCKEY",
        53 to "ROWING",
        54 to "ROWING_MACHINE",
        55 to "RUGBY",
        56 to "RUNNING",
        57 to "RUNNING_TREADMILL",
        58 to "SAILING",
        59 to "SCUBA_DIVING",
        60 to "SKATING",
        61 to "SKIING",
        62 to "SNOWBOARDING",
        63 to "SNOWSHOEING",
        64 to "SOCCER",
        65 to "SOFTBALL",
        66 to "SQUASH",
        68 to "STAIR_CLIMBING",
        69 to "STAIR_CLIMBING_MACHINE",
        70 to "STRENGTH_TRAINING",
        71 to "STRETCHING",
        72 to "SURFING",
        73 to "SWIMMING_OPEN_WATER",
        74 to "SWIMMING_POOL",
        75 to "TABLE_TENNIS",
        76 to "TENNIS",
        78 to "VOLLEYBALL",
        79 to "WALKING",
        80 to "WATER_POLO",
        81 to "WEIGHTLIFTING",
        82 to "WHEELCHAIR",
        83 to "YOGA"
    )

    override fun load() {
        super.load()

        val contract: ActivityResultContract<Set<String>, Set<String>> =
            PermissionController.createRequestPermissionResultContract()

        val callback: ActivityResultCallback<Set<String>> = ActivityResultCallback { grantedPermissions ->
            val context = requestPermissionContext.get()
            if (context != null) {
                val result = grantedPermissionResult(context.requestedPermissions, grantedPermissions)
                context.pluginCall.resolve(result)
            }
        }
        permissionsLauncher = activity.registerForActivityResult(contract, callback)
    }

    /**
     * Check if Health Connect is available on the device
     */
    @PluginMethod
    fun isHealthAvailable(call: PluginCall) {
        if (!available) {
            try {
                healthConnectClient = HealthConnectClient.getOrCreate(context)
                available = true
            } catch (e: Exception) {
                Log.e("CAP-HEALTH", "Error initializing Health Connect client", e)
                available = false
            }
        }

        val result = JSObject()
        result.put("available", available)
        call.resolve(result)
    }

    /**
     * Check if the specified permissions are granted
     */
    @PluginMethod
    fun checkHealthPermissions(call: PluginCall) {
        val permissionsToCheck = call.getArray("permissions")
        if (permissionsToCheck == null) {
            call.reject("Must provide permissions to check")
            return
        }

        val permissions = permissionsToCheck.toList<String>()
            .mapNotNull { CapHealthPermission.from(it) }
            .toSet()

        CoroutineScope(Dispatchers.IO).launch {
            try {
                val grantedPermissions = healthConnectClient.permissionController.getGrantedPermissions()
                val result = grantedPermissionResult(permissions, grantedPermissions)
                call.resolve(result)
            } catch (e: Exception) {
                call.reject("Checking permissions failed: ${e.message}")
            }
        }
    }

    /**
     * Request health permissions from the user
     */
    @PluginMethod
    fun requestHealthPermissions(call: PluginCall) {
        val permissionsToRequest = call.getArray("permissions")
        if (permissionsToRequest == null) {
            call.reject("Must provide permissions to request")
            return
        }

        val permissions = permissionsToRequest.toList<String>()
            .mapNotNull { CapHealthPermission.from(it) }
            .toSet()
        val healthConnectPermissions = permissions.mapNotNull { permissionMapping[it] }.toSet()

        CoroutineScope(Dispatchers.IO).launch {
            try {
                requestPermissionContext.set(RequestPermissionContext(permissions, call))
                permissionsLauncher.launch(healthConnectPermissions)
            } catch (e: Exception) {
                call.reject("Permission request failed: ${e.message}")
                requestPermissionContext.set(null)
            }
        }
    }

    /**
     * Open Health Connect settings
     */
    @PluginMethod
    fun openHealthConnectSettings(call: PluginCall) {
        try {
            val intent = Intent().apply {
                action = HealthConnectClient.ACTION_HEALTH_CONNECT_SETTINGS
            }
            context.startActivity(intent)
            call.resolve()
        } catch (e: Exception) {
            call.reject("Failed to open Health Connect settings: ${e.message}")
        }
    }

    /**
     * Open Health Connect in Play Store for installation
     */
    @PluginMethod
    fun showHealthConnectInPlayStore(call: PluginCall) {
        try {
            val uri = Uri.parse("https://play.google.com/store/apps/details?id=com.google.android.apps.healthdata")
            val intent = Intent(Intent.ACTION_VIEW, uri)
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            context.startActivity(intent)
            call.resolve()
        } catch (e: Exception) {
            call.reject("Failed to open Play Store: ${e.message}")
        }
    }

    /**
     * Query aggregated health data
     */
    @PluginMethod
    fun queryAggregated(call: PluginCall) {
        try {
            val startDate = call.getString("startDate")
            val endDate = call.getString("endDate")
            val dataType = call.getString("dataType")
            val bucket = call.getString("bucket")

            if (startDate == null || endDate == null || dataType == null || bucket == null) {
                call.reject("Missing required parameters: startDate, endDate, dataType, or bucket")
                return
            }

            // Validate time range
            val timeRange = validateTimeRange(startDate, endDate)
            if (timeRange == null) {
                call.reject("Invalid time range: startDate must be before endDate")
                return
            }

            val (startDateTime, endDateTime) = timeRange
            val metricAndMapper = try {
                getMetricAndMapper(dataType)
            } catch (e: IllegalArgumentException) {
                call.reject("${e.message}")
                return
            }

            val period = when (bucket) {
                BUCKET_DAY -> Period.ofDays(1)
                else -> {
                    call.reject("Unsupported bucket: $bucket. Only 'day' is currently supported.")
                    return
                }
            }

            CoroutineScope(Dispatchers.IO).launch {
                try {
                    val results = queryAggregatedMetric(
                        metricAndMapper,
                        TimeRangeFilter.between(startDateTime, endDateTime),
                        period
                    )

                    val aggregatedList = JSArray()
                    results.forEach { aggregatedList.put(it.toJs()) }

                    val finalResult = JSObject()
                    finalResult.put("aggregatedData", aggregatedList)
                    call.resolve(finalResult)
                } catch (e: Exception) {
                    Log.e(tag, "Error querying aggregated data", e)
                    call.reject("Error querying aggregated data: ${e.message}")
                }
            }
        } catch (e: Exception) {
            Log.e(tag, "Invalid parameters", e)
            call.reject("Invalid parameters: ${e.message}")
        }
    }

    /**
     * Query workout data with optional additional metrics
     */
    @PluginMethod
    fun queryWorkouts(call: PluginCall) {
        val startDate = call.getString("startDate")
        val endDate = call.getString("endDate")
        val includeHeartRate: Boolean = call.getBoolean("includeHeartRate", false) == true
        val includeRoute: Boolean = call.getBoolean("includeRoute", false) == true
        val includeSteps: Boolean = call.getBoolean("includeSteps", false) == true
        
        if (startDate == null || endDate == null) {
            call.reject("Missing required parameters: startDate or endDate")
            return
        }

        // Validate time range
        val timeRange = validateTimeRange(startDate, endDate)
        if (timeRange == null) {
            call.reject("Invalid time range: startDate must be before endDate")
            return
        }

        val (startDateTime, endDateTime) = timeRange
        val timeRangeFilter = TimeRangeFilter.between(startDateTime, endDateTime)
        val request = ReadRecordsRequest(
            ExerciseSessionRecord::class, 
            timeRangeFilter, 
            emptySet(), 
            true, 
            1000
        )

        CoroutineScope(Dispatchers.IO).launch {
            try {
                val response = healthConnectClient.readRecords(request)
                val workoutsArray = JSArray()

                for (workout in response.records) {
                    val workoutObject = JSObject()
                    workoutObject.put("id", workout.metadata.id)
                    workoutObject.put(
                        "sourceName",
                        Optional.ofNullable(workout.metadata.device?.manufacturer).getOrDefault("") +
                                Optional.ofNullable(workout.metadata.device?.model).getOrDefault("")
                    )
                    workoutObject.put("sourceBundleId", workout.metadata.dataOrigin.packageName)
                    workoutObject.put("startDate", workout.startTime.toString())
                    workoutObject.put("endDate", workout.endTime.toString())
                    workoutObject.put("workoutType", exerciseTypeMapping.getOrDefault(workout.exerciseType, "OTHER"))
                    workoutObject.put("title", workout.title)
                    
                    val duration = if (workout.segments.isEmpty()) {
                        workout.endTime.epochSecond - workout.startTime.epochSecond
                    } else {
                        workout.segments.map { it.endTime.epochSecond - it.startTime.epochSecond }
                            .stream().mapToLong { it }.sum()
                    }
                    workoutObject.put("duration", duration)

                    // Add optional metrics
                    if (includeSteps) {
                        addWorkoutMetric(workout, workoutObject, getMetricAndMapper(DATA_TYPE_STEPS))
                    }

                    val readTotalCaloriesResult = addWorkoutMetric(workout, workoutObject, getMetricAndMapper(DATA_TYPE_TOTAL_CALORIES))
                    if (!readTotalCaloriesResult) {
                        addWorkoutMetric(workout, workoutObject, getMetricAndMapper(DATA_TYPE_ACTIVE_CALORIES))
                    }

                    addWorkoutMetric(workout, workoutObject, getMetricAndMapper(DATA_TYPE_DISTANCE))

                    if (includeHeartRate && hasPermission(CapHealthPermission.READ_HEART_RATE)) {
                        val heartRates = queryHeartRateForWorkout(workout.startTime, workout.endTime)
                        workoutObject.put("heartRate", heartRates)
                    }

                    if (includeRoute && workout.exerciseRouteResult is ExerciseRouteResult.Data) {
                        val route = queryRouteForWorkout(workout.exerciseRouteResult as ExerciseRouteResult.Data)
                        workoutObject.put("route", route)
                    }

                    workoutsArray.put(workoutObject)
                }

                val result = JSObject()
                result.put("workouts", workoutsArray)
                call.resolve(result)
            } catch (e: Exception) {
                call.reject("Error querying workouts: ${e.message}")
            }
        }
    }

    /**
     * Query individual health data samples
     */
    @PluginMethod
    fun querySamples(call: PluginCall) {
        try {
            val dataType = call.getString("dataType")
            val startDate = call.getString("startDate")
            val endDate = call.getString("endDate")
            val rawLimit = call.getInt("limit")
            // If limit is 0 or null, use MAX_QUERY_LIMIT (get all samples)
            // Otherwise use the provided limit, clamped to valid range
            val limit = when {
                rawLimit == null || rawLimit == 0 -> MAX_QUERY_LIMIT
                else -> rawLimit.coerceIn(1, MAX_QUERY_LIMIT)
            }

            Log.d(tag, "querySamples called: dataType=$dataType, startDate=$startDate, endDate=$endDate, rawLimit=$rawLimit, finalLimit=$limit")

            if (dataType == null || startDate == null || endDate == null) {
                call.reject("Missing required parameters: dataType, startDate, endDate")
                return
            }

            // Validate time range
            val timeRange = validateTimeRange(startDate, endDate)
            if (timeRange == null) {
                call.reject("Invalid time range: startDate must be before endDate")
                return
            }

            val (startInstant, endInstant) = timeRange

            CoroutineScope(Dispatchers.IO).launch {
                try {
                    Log.d(tag, "Querying samples for dataType: $dataType")
                    Log.d(tag, "Before when statement for dataType: $dataType")
                    val samples = when (dataType) {
                        "steps" -> {
                            Log.d(tag, "Reading steps samples...")
                            readStepsSamples(startInstant, endInstant, limit)
                        }
                        "heartRate" -> readHeartRateSamples(startInstant, endInstant, limit)
                        "restingHeartRate" -> readRestingHeartRateSamples(startInstant, endInstant, limit)
                        "activeEnergyBurned" -> readActiveCaloriesSamples(startInstant, endInstant, limit)
                        "distanceWalkingRunning" -> readDistanceSamples(startInstant, endInstant, limit)
                        "bloodGlucose" -> readBloodGlucoseSamples(startInstant, endInstant, limit)
                        "oxygenSaturation" -> readOxygenSaturationSamples(startInstant, endInstant, limit)
                        "respiratoryRate" -> readRespiratoryRateSamples(startInstant, endInstant, limit)
                        "bodyMass" -> readBodyMassSamples(startInstant, endInstant, limit)
                        "bodyFatPercentage" -> readBodyFatSamples(startInstant, endInstant, limit)
                        "bloodPressureSystolic" -> readBloodPressureSystolicSamples(startInstant, endInstant, limit)
                        "bloodPressureDiastolic" -> readBloodPressureDiastolicSamples(startInstant, endInstant, limit)
                        else -> {
                            Log.e(tag, "Unsupported data type: $dataType")
                            call.reject("Unsupported data type: $dataType")
                            return@launch
                        }
                    }

                    Log.d(tag, "After when statement, samples size: ${samples.length()}")
                    Log.d(tag, "querySamples: Retrieved ${samples.length()} samples for dataType=$dataType")
                    val result = JSObject()
                    result.put("samples", samples)
                    Log.d(tag, "Calling call.resolve()")
                    call.resolve(result)
                    Log.d(tag, "call.resolve() completed")
                } catch (e: Exception) {
                    Log.e(tag, "Error querying samples for $dataType: ${e.message}", e)
                    e.printStackTrace()
                    call.reject("Error querying samples: ${e.message}")
                }
            }
        } catch (e: Exception) {
            Log.e(tag, "Invalid parameters in querySamples: ${e.message}", e)
            call.reject("Invalid parameters: ${e.message}")
        }
    }

    // ============ PRIVATE HELPER METHODS AND CLASSES ============

    /**
     * Context for permission requests
     */
    data class RequestPermissionContext(
        val requestedPermissions: Set<CapHealthPermission>, 
        val pluginCall: PluginCall
    )

    /**
     * Metric and mapper for aggregated data queries
     */
    data class MetricAndMapper(
        val name: String,
        val permission: CapHealthPermission,
        val metric: AggregateMetric<Any>,
        val mapper: (Any?) -> Double?
    ) {
        fun getValue(a: AggregationResult): Double? {
            return mapper(a[metric])
        }
    }

    /**
     * Aggregated sample data structure
     */
    data class AggregatedSample(
        val startDate: Instant,
        val endDate: Instant,
        val value: Double?
    ) {
        fun toJs(): JSObject {
            val o = JSObject()
            o.put("startDate", startDate.toString())
            o.put("endDate", endDate.toString())
            o.put("value", value)
            return o
        }
    }

    private val requestPermissionContext = AtomicReference<RequestPermissionContext>()

    /**
     * Get the appropriate metric and mapper for a data type
     */
    private fun getMetricAndMapper(dataType: String): MetricAndMapper {
        return when (dataType) {
            DATA_TYPE_STEPS -> metricAndMapper("steps", CapHealthPermission.READ_STEPS, StepsRecord.COUNT_TOTAL) { it?.toDouble() }
            DATA_TYPE_ACTIVE_CALORIES -> metricAndMapper(
                "calories",
                CapHealthPermission.READ_ACTIVE_CALORIES,
                ActiveCaloriesBurnedRecord.ACTIVE_CALORIES_TOTAL
            ) { it?.inKilocalories }
            DATA_TYPE_TOTAL_CALORIES -> metricAndMapper(
                "calories",
                CapHealthPermission.READ_TOTAL_CALORIES,
                TotalCaloriesBurnedRecord.ENERGY_TOTAL
            ) { it?.inKilocalories }
            DATA_TYPE_DISTANCE -> metricAndMapper("distance", CapHealthPermission.READ_DISTANCE, DistanceRecord.DISTANCE_TOTAL) { it?.inMeters }
            else -> throw IllegalArgumentException("Unsupported dataType: $dataType")
        }
    }

    /**
     * Create a MetricAndMapper with proper type casting
     */
    @Suppress("UNCHECKED_CAST")
    private fun <M : Any> metricAndMapper(
        name: String,
        permission: CapHealthPermission,
        metric: AggregateMetric<M>,
        mapper: (M?) -> Double?
    ): MetricAndMapper {
        return MetricAndMapper(name, permission, metric as AggregateMetric<Any>, mapper as (Any?) -> Double?)
    }

    /**
     * Validate time range parameters
     */
    private fun validateTimeRange(startDate: String, endDate: String): Pair<Instant, Instant>? {
        return try {
            val start = Instant.parse(startDate)
            val end = Instant.parse(endDate)
            if (start.isAfter(end)) {
                Log.w(tag, "Invalid time range: startDate is after endDate")
                null
            } else {
                Pair(start, end)
            }
        } catch (e: Exception) {
            Log.e(tag, "Error parsing time range", e)
            null
        }
    }

    /**
     * Check if a specific permission is granted
     */
    private suspend fun hasPermission(permission: CapHealthPermission): Boolean {
        val grantedPermissions = healthConnectClient.permissionController.getGrantedPermissions()
        return grantedPermissions.contains(permissionMapping[permission])
    }

    /**
     * Process permission results
     */
    private fun grantedPermissionResult(
        requestPermissions: Set<CapHealthPermission>, 
        grantedPermissions: Set<String>
    ): JSObject {
        val readPermissions = JSObject()
        
        for (permission in requestPermissions) {
            val requiredPermission = permissionMapping[permission]
            val isGranted = requiredPermission?.let { grantedPermissions.contains(it) } ?: false
            readPermissions.put(permission.name, isGranted)
        }

        val result = JSObject()
        result.put("permissions", readPermissions)
        return result
    }

    /**
     * Query aggregated metrics with period grouping
     */
    private suspend fun queryAggregatedMetric(
        metricAndMapper: MetricAndMapper, 
        timeRange: TimeRangeFilter, 
        period: Period
    ): List<AggregatedSample> {
        if (!hasPermission(metricAndMapper.permission)) {
            return emptyList()
        }

        val response: List<AggregationResultGroupedByPeriod> = healthConnectClient.aggregateGroupByPeriod(
            AggregateGroupByPeriodRequest(
                metrics = setOf(metricAndMapper.metric),
                timeRangeFilter = timeRange,
                timeRangeSlicer = period
            )
        )

        return response.map {
            val mappedValue = metricAndMapper.getValue(it.result)
            val startInstant = it.startTime.atZone(ZoneId.systemDefault()).toInstant()
            val endInstant = it.endTime.atZone(ZoneId.systemDefault()).toInstant()
            AggregatedSample(startInstant, endInstant, mappedValue)
        }
    }

    /**
     * Add workout metric to the result object
     */
    private suspend fun addWorkoutMetric(
        workout: ExerciseSessionRecord,
        jsWorkout: JSObject,
        metricAndMapper: MetricAndMapper,
    ): Boolean {
        if (!hasPermission(metricAndMapper.permission)) {
            return false
        }

        return try {
            val request = AggregateRequest(
                setOf(metricAndMapper.metric),
                TimeRangeFilter.between(workout.startTime, workout.endTime),
                emptySet()
            )
            val aggregation = healthConnectClient.aggregate(request)
            metricAndMapper.getValue(aggregation)?.let { value ->
                jsWorkout.put(metricAndMapper.name, value)
                true
            } ?: false
        } catch (e: Exception) {
            Log.e(tag, "Error reading workout metric: ${metricAndMapper.name}", e)
            false
        }
    }

    /**
     * Query heart rate data for a specific workout
     */
    private suspend fun queryHeartRateForWorkout(startTime: Instant, endTime: Instant): JSArray {
        val request = ReadRecordsRequest(
            HeartRateRecord::class, 
            TimeRangeFilter.between(startTime, endTime)
        )
        val heartRateRecords = healthConnectClient.readRecords(request)

        val heartRateArray = JSArray()
        val samples = heartRateRecords.records.flatMap { it.samples }
        
        for (sample in samples) {
            val heartRateObject = JSObject()
            heartRateObject.put("timestamp", sample.time.toString())
            heartRateObject.put("bpm", sample.beatsPerMinute)
            heartRateArray.put(heartRateObject)
        }
        return heartRateArray
    }

    /**
     * Query route data for a specific workout
     */
    private fun queryRouteForWorkout(routeResult: ExerciseRouteResult.Data): JSArray {
        val routeArray = JSArray()
        for (record in routeResult.exerciseRoute.route) {
            val routeObject = JSObject()
            routeObject.put("timestamp", record.time.toString())
            routeObject.put("lat", record.latitude)
            routeObject.put("lng", record.longitude)
            routeObject.put("alt", record.altitude?.inMeters) // Added null safety for altitude
            routeArray.put(routeObject)
        }
        return routeArray
    }

    // ============ SAMPLE READING METHODS ============

    /**
     * Generic method to convert records to JSArray
     */
    private fun <T : Record> convertSamplesToJSArray(
        records: List<T>,
        mapper: (T) -> JSObject
    ): JSArray {
        val samplesArray = JSArray()
        records.forEach { record ->
            val sampleObject = mapper(record)
            // Add common metadata to all samples
            // Only add startDate/endDate if not already in mapper
            if (!sampleObject.has("startDate")) {
                sampleObject.put("startDate", record.metadata.lastModifiedTime.toString())
            }
            if (!sampleObject.has("endDate")) {
                sampleObject.put("endDate", record.metadata.lastModifiedTime.toString())
            }
            if (!sampleObject.has("source")) {
                sampleObject.put("source", record.metadata.dataOrigin.packageName)
            }
            samplesArray.put(sampleObject)
        }
        return samplesArray
    }

    private suspend fun readStepsSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_STEPS)) {
            Log.w(tag, "READ_STEPS permission not granted - cannot read step samples")
            return JSArray()
        }

        try {
            val request = ReadRecordsRequest(
                StepsRecord::class,
                TimeRangeFilter.between(startTime, endTime),
                pageSize = limit
            )
            val response = healthConnectClient.readRecords(request)
            Log.d(tag, "readStepsSamples: Retrieved ${response.records.size} step records from $startTime to $endTime")
            return convertSamplesToJSArray(response.records) { record ->
                JSObject().apply {
                    put("value", record.count)
                    put("unit", "count")
                    put("dataType", "steps")
                    put("startDate", record.startTime.toString())
                    put("endDate", record.endTime.toString())
                    put("source", record.metadata.dataOrigin.packageName)
                }
            }
        } catch (e: Exception) {
            Log.e(tag, "Error reading step samples: ${e.message}", e)
            return JSArray()
        }
    }

    private suspend fun readHeartRateSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_HEART_RATE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            HeartRateRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        val samplesArray = JSArray()
        
        response.records.forEach { record ->
            record.samples.forEach { sample ->
                samplesArray.put(JSObject().apply {
                    put("value", sample.beatsPerMinute)
                    put("unit", "bpm")
                    put("startDate", sample.time.toString())
                    put("endDate", sample.time.toString())
                    put("dataType", "heartRate")
                    put("source", record.metadata.dataOrigin.packageName)
                })
            }
        }
        return samplesArray
    }

    private suspend fun readRestingHeartRateSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_RESTING_HEART_RATE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            RestingHeartRateRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.beatsPerMinute)
                put("unit", "bpm")
                put("dataType", "restingHeartRate")
            }
        }
    }

    private suspend fun readActiveCaloriesSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_ACTIVE_CALORIES)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            ActiveCaloriesBurnedRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.energy.inKilocalories)
                put("unit", "kcal")
                put("dataType", "activeEnergyBurned")
            }
        }
    }

    private suspend fun readDistanceSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_DISTANCE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            DistanceRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.distance.inMeters)
                put("unit", "meters")
                put("dataType", "distanceWalkingRunning")
            }
        }
    }

    private suspend fun readBloodGlucoseSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_BLOOD_GLUCOSE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            BloodGlucoseRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.level.inMillimolesPerLiter)
                put("unit", "mmol/L")
                put("dataType", "bloodGlucose")
            }
        }
    }

    private suspend fun readOxygenSaturationSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_OXYGEN_SATURATION)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            OxygenSaturationRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.percentage.value)
                put("unit", "percent")
                put("dataType", "oxygenSaturation")
            }
        }
    }

    private suspend fun readRespiratoryRateSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_RESPIRATORY_RATE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            RespiratoryRateRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.rate)
                put("unit", "breaths/min")
                put("dataType", "respiratoryRate")
            }
        }
    }

    private suspend fun readBodyMassSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_BODY_MASS)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            WeightRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.weight.inKilograms)
                put("unit", "kg")
                put("dataType", "bodyMass")
            }
        }
    }

    private suspend fun readBodyFatSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_BODY_FAT)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            BodyFatRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.percentage.value)
                put("unit", "percent")
                put("dataType", "bodyFatPercentage")
            }
        }
    }

    private suspend fun readBloodPressureSystolicSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_BLOOD_PRESSURE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            BloodPressureRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.systolic.inMillimetersOfMercury)
                put("unit", "mmHg")
                put("dataType", "bloodPressureSystolic")
            }
        }
    }

    private suspend fun readBloodPressureDiastolicSamples(startTime: Instant, endTime: Instant, limit: Int): JSArray {
        if (!hasPermission(CapHealthPermission.READ_BLOOD_PRESSURE)) {
            return JSArray()
        }

        val request = ReadRecordsRequest(
            BloodPressureRecord::class,
            TimeRangeFilter.between(startTime, endTime),
            pageSize = limit
        )
        val response = healthConnectClient.readRecords(request)
        return convertSamplesToJSArray(response.records) { record ->
            JSObject().apply {
                put("value", record.diastolic.inMillimetersOfMercury)
                put("unit", "mmHg")
                put("dataType", "bloodPressureDiastolic")
            }
        }
    }
}