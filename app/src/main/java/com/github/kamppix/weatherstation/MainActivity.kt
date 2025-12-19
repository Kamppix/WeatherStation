package com.github.kamppix.weatherstation

import android.app.DatePickerDialog
import android.app.TimePickerDialog
import android.os.Bundle
import android.util.Log
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import com.github.kamppix.weatherstation.ui.theme.WeatherStationTheme
import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.TrustManagerFactory
import com.patrykandpatrick.vico.compose.cartesian.CartesianChartHost
import com.patrykandpatrick.vico.compose.cartesian.axis.rememberBottom
import com.patrykandpatrick.vico.compose.cartesian.axis.rememberStart
import com.patrykandpatrick.vico.compose.cartesian.layer.rememberLine
import com.patrykandpatrick.vico.compose.cartesian.layer.rememberLineCartesianLayer
import com.patrykandpatrick.vico.compose.cartesian.rememberCartesianChart
import com.patrykandpatrick.vico.compose.common.fill
import com.patrykandpatrick.vico.compose.common.shader.verticalGradient
import com.patrykandpatrick.vico.core.cartesian.axis.HorizontalAxis
import com.patrykandpatrick.vico.core.cartesian.axis.VerticalAxis
import com.patrykandpatrick.vico.core.cartesian.data.CartesianChartModelProducer
import com.patrykandpatrick.vico.core.cartesian.data.CartesianValueFormatter
import com.patrykandpatrick.vico.core.cartesian.layer.LineCartesianLayer
import com.patrykandpatrick.vico.core.cartesian.marker.DefaultCartesianMarker
import com.patrykandpatrick.vico.core.common.shader.ShaderProvider
import okhttp3.OkHttpClient
import okhttp3.Request
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.reflect.TypeToken
import com.patrykandpatrick.vico.compose.cartesian.rememberVicoScrollState
import com.patrykandpatrick.vico.core.cartesian.data.CartesianLayerRangeProvider
import com.patrykandpatrick.vico.core.cartesian.data.lineSeries
import com.patrykandpatrick.vico.core.cartesian.marker.LineCartesianLayerMarkerTarget
import java.lang.reflect.Type
import java.text.DecimalFormat
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlin.math.ceil
import kotlin.math.floor

data class Measurement(
    val temperature: Double,
    val pressure: Double,
    val timestamp: Instant
)

class InstantDeserializer : JsonDeserializer<Instant> {
    override fun deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Instant {
        return Instant.parse(json.asString)
    }
}

class MainActivity : ComponentActivity() {
    private val topics = arrayOf("temp", "pressure")
    private val dateFormatter = CartesianValueFormatter { _, value, _ ->
        val instant = Instant.ofEpochMilli(value.toLong())
        DateTimeFormatter.ofPattern("   MMM d HH:mm   ").withZone(ZoneId.systemDefault()).format(instant)
    }
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault())
    private val tempFormat = DecimalFormat("#.## °C")
    private val tempFormatter = CartesianValueFormatter.decimal(tempFormat)
    private val tempMarkerFormatter = DefaultCartesianMarker.ValueFormatter { _, targets ->
        val lineTarget = targets.firstOrNull() as? LineCartesianLayerMarkerTarget

        if (lineTarget != null) {
            val point = lineTarget.points.firstOrNull()

            if (point != null) {
                val xMillis = point.entry.x.toLong()
                val yValue = point.entry.y

                val dateTime = Instant.ofEpochMilli(xMillis)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime()

                val dateString = timeFormatter.format(dateTime)
                val yString = tempFormat.format(yValue)

                "$yString [$dateString]"
            } else ""
        } else ""
    }
    private val pressureFormat = DecimalFormat("#.## kPa")
    private val pressureFormatter = CartesianValueFormatter.decimal(pressureFormat)
    private val pressureMarkerFormatter = DefaultCartesianMarker.ValueFormatter { _, targets ->
        val lineTarget = targets.firstOrNull() as? LineCartesianLayerMarkerTarget

        if (lineTarget != null) {
            val point = lineTarget.points.firstOrNull()

            if (point != null) {
                val xMillis = point.entry.x.toLong()
                val yValue = point.entry.y

                val dateTime = Instant.ofEpochMilli(xMillis)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime()

                val dateString = timeFormatter.format(dateTime)
                val yString = pressureFormat.format(yValue)

                "$yString [$dateString]"
            } else ""
        } else ""
    }

    private var showBrokerDialog by mutableStateOf(false)
    private var showFetchDialog by mutableStateOf(false)
    private var mqttClient: Mqtt5AsyncClient? = null
    private var brokerIp by mutableStateOf(BROKER_IP)
    private var brokerPort by mutableStateOf(BROKER_PORT)
    private var username by mutableStateOf(USERNAME)
    private var password by mutableStateOf(PASSWORD)
    private var currentValues = mutableStateListOf<Float>()
    private var dataStartDate by mutableStateOf(Instant.now().minus(1, ChronoUnit.DAYS))
    private var dataEndDate by mutableStateOf(Instant.now())
    private var measurements = mutableStateListOf<Measurement>()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        while (currentValues.size < topics.size) {
            currentValues.add(Float.NaN)
        }

        connectToMqtt(brokerIp, brokerPort.toIntOrNull() ?: 1883, username, password)
        fetchData(dataStartDate, dataEndDate)

        enableEdgeToEdge()
        setContent {
            WeatherStationTheme {
                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    Column(
                        modifier = Modifier.padding(innerPadding).padding(16.dp)
                    ) {
                        TemperatureDisplay()
                        Spacer(modifier = Modifier.height(32.dp))
                        PressureDisplay()

                        Spacer(modifier = Modifier.height(32.dp))
                        Button(onClick = { showFetchDialog = true }) {
                            Text("Fetch historical data")
                        }
                        Spacer(modifier = Modifier.height(8.dp))
                        Button(onClick = { showBrokerDialog = true }) {
                            Text("Modify MQTT connection")
                        }

                        if (showFetchDialog) {
                            FetchDialog(
                                onDismiss = { showFetchDialog = false },
                                onFetch = { startDate, endDate ->
                                    dataStartDate = startDate
                                    dataEndDate = endDate
                                    fetchData(startDate, endDate)
                                }
                            )
                        }

                        if (showBrokerDialog) {
                            ConnectDialog(
                                onDismiss = { showBrokerDialog = false },
                                onConnect = { ip, port, user, pwd ->
                                    brokerIp = ip
                                    brokerPort = port.toString()
                                    username = user
                                    password = pwd
                                    connectToMqtt(ip, port, user, pwd)
                                }
                            )
                        }
                    }
                }
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        mqttClient?.disconnect()
    }

    @Composable
    private fun ConnectDialog(
        onDismiss: () -> Unit,
        onConnect: (String, Int, String, String) -> Unit
    ) {
        var ip by remember { mutableStateOf(brokerIp) }
        var port by remember { mutableStateOf(brokerPort) }
        var user by remember { mutableStateOf(username) }
        var pwd by remember { mutableStateOf(password) }

        AlertDialog(
            onDismissRequest = onDismiss,
            title = { Text("Connect to MQTT") },
            text = {
                Column {
                    OutlinedTextField(
                        value = ip,
                        onValueChange = { ip = it },
                        label = { Text("Broker IP") }
                    )
                    OutlinedTextField(
                        value = port,
                        onValueChange = { port = it },
                        label = { Text("Port") },
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number)
                    )
                    OutlinedTextField(
                        value = user,
                        onValueChange = { user = it },
                        label = { Text("Username") }
                    )
                    OutlinedTextField(
                        value = pwd,
                        onValueChange = { pwd = it },
                        label = { Text("Password") }
                    )
                }
            },
            confirmButton = {
                Button(onClick = {
                    val portInt = port.toIntOrNull() ?: 1883
                    onConnect(ip, portInt, user, pwd)
                    onDismiss()
                }) {
                    Text("Connect")
                }
            },
            dismissButton = {
                Button(onClick = onDismiss) { Text("Cancel") }
            }
        )
    }

    private fun connectToMqtt(ip: String, port: Int, username: String, password: String) {
        mqttClient?.let { client ->
            if (client.state.isConnected) {
                try {
                    client.disconnect().whenComplete { _, error ->
                        if (error != null) {
                            error.printStackTrace()
                        } else {
                            Log.i("INFO", "Previous MQTT connection closed.")
                        }
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }

        mqttClient = MqttClient.builder()
            .useMqttVersion5()
            .identifier("android-${System.currentTimeMillis()}")
            .serverHost(ip)
            .serverPort(port)
            .sslConfig()
            .trustManagerFactory(createTrustManagerFactory())
            .hostnameVerifier { _, _ -> true }
            .applySslConfig()
            .buildAsync()

        CoroutineScope(Dispatchers.IO).launch {
            mqttClient?.connectWith()
                ?.simpleAuth()
                ?.username(username)
                ?.password(password.toByteArray())
                ?.applySimpleAuth()
                ?.send()
                ?.whenComplete { _, error ->
                    if (error != null) {
                        error.printStackTrace()
                        runOnUiThread {
                            val failMsg = "Failed to connect to MQTT broker."
                            Log.e("ERROR", failMsg)
                            runOnUiThread {
                                Toast.makeText(
                                    this@MainActivity,
                                    failMsg,
                                    Toast.LENGTH_LONG
                                ).show()
                            }
                        }
                    } else {
                        val connectMsg = "Connected to MQTT broker."
                        Log.i("INFO", connectMsg)
                        runOnUiThread {
                            Toast.makeText(
                                this@MainActivity,
                                connectMsg,
                                Toast.LENGTH_SHORT
                            ).show()
                        }

                        for (i in topics.indices) {
                            val topic = "picow/" + topics[i]
                            mqttClient?.subscribeWith()
                                ?.topicFilter(topic)
                                ?.qos(MqttQos.AT_LEAST_ONCE)
                                ?.callback { msg ->
                                    val payload = String(msg.payloadAsBytes)
                                    Log.i("INFO", "Received data from $topic: $payload")
                                    runOnUiThread { currentValues[i] = payload.toFloat() }
                                }
                                ?.send()
                            Log.i("INFO", "Subscribed to $topic")
                        }
                    }
                }
        }
    }

    private fun createTrustManagerFactory(): TrustManagerFactory {
        val certFactory = CertificateFactory.getInstance("X.509")
        val inputStream = resources.openRawResource(R.raw.ca)
        val cert = certFactory.generateCertificate(inputStream)
        inputStream.close()

        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
        keyStore.load(null, null)
        keyStore.setCertificateEntry("ca", cert)

        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(keyStore)

        return tmf
    }

    @Composable
    private fun FetchDialog(
        onDismiss: () -> Unit,
        onFetch: (Instant, Instant) -> Unit
    ) {
        var startDate by remember { mutableStateOf(dataStartDate) }
        var endDate by remember { mutableStateOf(dataEndDate) }

        AlertDialog(
            onDismissRequest = onDismiss,
            title = { Text("Choose time range") },
            text = {
                Column {
                    Text("Start date")
                    DatePickerBox(
                        value = startDate,
                        onChooseDate = { startDate = it }
                    )
                    Text("End date")
                    DatePickerBox(
                        value = endDate,
                        onChooseDate = { endDate = it }
                    )
                }
            },
            confirmButton = {
                Button(onClick = {
                    onFetch(startDate, endDate)
                    onDismiss()
                }) {
                    Text("Fetch")
                }
            },
            dismissButton = {
                Button(onClick = onDismiss) { Text("Cancel") }
            }
        )
    }

    @Composable
    private fun DatePickerBox(
        value: Instant,
        onChooseDate: (Instant) -> Unit
    ) {
        var date by remember { mutableStateOf(value) }
        var showDatePicker by remember { mutableStateOf(false) }

        Box(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp)
                .clickable { showDatePicker = true }
                .border(1.dp, Color.Gray, RoundedCornerShape(8.dp))
                .padding(16.dp)
        ) {
            Text(
                text = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm").withZone(ZoneId.systemDefault()).format(date),
                style = MaterialTheme.typography.bodyLarge
            )
        }


        if (showDatePicker) {
            val context = LocalContext.current
            val zonedDateTime = date.atZone(ZoneId.systemDefault())

            DatePickerDialog(
                context,
                { _, year, month, day ->
                    TimePickerDialog(
                        context,
                        { _, hour, minute ->
                            date = ZonedDateTime.of(
                                year,
                                month + 1,
                                day,
                                hour,
                                minute,
                                0,
                                0,
                                ZoneId.systemDefault()
                            ).toInstant()
                            onChooseDate(date)
                        },
                        zonedDateTime.hour,
                        zonedDateTime.minute,
                        true
                    ).show()
                },
                zonedDateTime.year,
                zonedDateTime.monthValue - 1,
                zonedDateTime.dayOfMonth
            ).also {
                it.setOnDismissListener { showDatePicker = false }
                it.show()
            }
        }
    }

    private fun fetchData(startDate: Instant, endDate: Instant) {
        val client = OkHttpClient()

        CoroutineScope(Dispatchers.IO).launch {
            val request = Request.Builder()
                .url("https://iot-project-webapp-git-iot-project-holmavil.2.rahtiapp.fi/api/weatherInfos/between?start=$startDate&end=$endDate")
                .addHeader("Cookie", "127dcb4d79b8a9cd80bae57cbab444ab=154b8b26851775d9bcc69144aff826d4")
                .build()
            val response = client.newCall(request).execute()
            if (response.isSuccessful) {
                val json = response.body?.string()

                val gson = GsonBuilder()
                    .registerTypeAdapter(Instant::class.java, InstantDeserializer())
                    .create()
                val measurementType = object : TypeToken<List<Measurement>>() {}.type
                val newMeasurements: List<Measurement> = gson.fromJson(json, measurementType)

                measurements.clear()
                measurements.addAll(newMeasurements)

                val connectMsg = "Fetched data."
                Log.i("INFO", connectMsg)
                runOnUiThread {
                    Toast.makeText(
                        this@MainActivity,
                        connectMsg,
                        Toast.LENGTH_SHORT
                    ).show()
                }
            } else {
                val failMsg = "Failed to fetch data."
                Log.e("ERROR", failMsg)
                runOnUiThread {
                    Toast.makeText(
                        this@MainActivity,
                        failMsg,
                        Toast.LENGTH_LONG
                    ).show()
                }
            }
        }
    }

    @Composable
    private fun TemperatureDisplay() {
        val i = topics.indexOf("temp")
        Column {
            Text(text =
                if (measurements.isEmpty() && currentValues[i].isNaN())
                    "No temperature data"
                else
                    "Current temperature: ${
                    if (!currentValues[i].isNaN())
                        tempFormat.format(currentValues[i])
                    else
                        "N/A"
                    }"
            )

            if (measurements.isNotEmpty()) {
                CreateChart(tempFormatter, tempMarkerFormatter, measurements.map { it.temperature.toFloat() })
            }
        }
    }

    @Composable
    private fun PressureDisplay() {
        val i = topics.indexOf("pressure")
        Column {
            Text(text =
            if (measurements.isEmpty() && currentValues[i].isNaN())
                "No pressure data"
            else
                "Current pressure: ${
                    if (!currentValues[i].isNaN())
                        pressureFormat.format(currentValues[i])
                    else
                        "N/A"
                }"
            )

            if (measurements.isNotEmpty()) {
                CreateChart(pressureFormatter, pressureMarkerFormatter, measurements.map { it.pressure.toFloat() })
            }
        }
    }

    @Composable
    private fun CreateChart(
        yAxisFormatter: CartesianValueFormatter,
        markerFormatter: DefaultCartesianMarker.ValueFormatter,
        dataPoints: List<Float>,
        modifier: Modifier = Modifier,
    ) {
        val modelProducer = remember { CartesianChartModelProducer() }

        LaunchedEffect(measurements.size) {
            modelProducer.runTransaction {
                lineSeries {
                    series(
                        measurements.map { it.timestamp.toEpochMilli() },
                        dataPoints
                    )
                }
            }
        }

        val padding = 0.1f
        val lineColor = MaterialTheme.colorScheme.primaryContainer

        CartesianChartHost(
            rememberCartesianChart(
                rememberLineCartesianLayer(
                    rangeProvider = CartesianLayerRangeProvider.fixed(
                        minY = floor((dataPoints.minOrNull() ?: 0f).toDouble() - padding),
                        maxY = ceil((dataPoints.maxOrNull() ?: 0f).toDouble() + padding)
                    ),
                    lineProvider = LineCartesianLayer.LineProvider.series(
                        LineCartesianLayer.rememberLine(
                            fill = LineCartesianLayer.LineFill.single(fill(lineColor)),
                            areaFill =
                            LineCartesianLayer.AreaFill.single(
                                fill(
                                    ShaderProvider.verticalGradient(
                                        arrayOf(lineColor.copy(alpha = 0.4f), Color.Transparent)
                                    )
                                )
                            )
                        )
                    )
                ),
                startAxis = VerticalAxis.rememberStart(valueFormatter = yAxisFormatter),
                bottomAxis = HorizontalAxis.rememberBottom(valueFormatter = dateFormatter),
                marker = rememberMarker(markerFormatter)
            ),
            modelProducer,
            modifier.height(220.dp),
            rememberVicoScrollState(scrollEnabled = false)
        )
    }
}
