package main

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/goinggo/tracelog"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
)

type IonPumpDataPoint struct {
	LPN       string  `json:"LPN"` // KLA LPN
	PumpID    string  `json:"PID"` // <RS485 ID>-<Pump Channel> 00-01
	DepotID   string  `json:"DID"` // Depot CODE
	Voltage   float64 `json:"V"`
	Current   float64 `json:"I"`
	Pressure  float64 `json:"P"`
	Timestamp int64   `json:"T"`
}

type FlipperDewPoint struct {
	FlipperID   string  `json:"FID"`
	Channel     int     `json:"CH"`
	Temperature float64 `json:"TEMP"`
	Timestamp   int64   `json:"T"`
	DepotID     string  `json:"DID"`
}

type RoughVacuumGaugeDataPoint struct {
	DepotID   string  `json:"DID"`
	GaugeID   int     `json:"GID"`
	Pressure  float64 `json:"P"`
	Timestamp int64   `json:"T"`
}

type MqttPackage struct {
	ID        string      `json:"ID"`
	EventName string      `json:"EVENT"`
	Source    string      `json:"SOURCE"`
	Target    string      `json:"TARGET"`
	Data      interface{} `json:"DATA"`
	Timestamp int64       `json:"TIMESTAMP"`
}

var inFluxClient influxdb2.Client
var writeApi api.WriteApiBlocking

func main() {
	tracelog.Start(tracelog.LevelInfo)

	// tracelog.StartFile(tracelog.LevelWarn, "collectorLog", 5)
	inFluxClient = influxdb2.NewClient("http://localhost:9999", "v8JhGRu6SxrCHNVAluyrhAOv-21YZn3NwouvtNwe-a2VVLokXaK9BdEu2sgbUNdXFYqi7xQLKwi2ICTDGzAF5w==")

	writeApi = inFluxClient.WriteApiBlocking("KLA", "udc")

	defer func() {

		tracelog.Warning("Client Close", "pushCollectedMultiDataToDatabase", "")

		inFluxClient.Close()
	}()

	for {
		newDataPoint := IonPumpDataPoint{DepotID: "UDC", PumpID: "00-01", Timestamp: time.Now().Unix(), LPN: "SU-000006E9DAE4", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "00-02", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D6B9", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "01-01", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D6T8", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "01-02", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D6T7", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "02-01", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D68T", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "02-02", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D6AA", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "TKO", PumpID: "00-01", Timestamp: time.Now().Unix(), LPN: "SU-000007D66838", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}
		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "TKO", PumpID: "01-03", Timestamp: time.Now().Unix(), LPN: "SU-000007D62234", Current: randFloats(10e-9, 10e-6, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}
		sendIonPumpDataPoint(&newDataPoint, &writeApi)

		flipperPoint := FlipperDewPoint{DepotID: "UDC", Channel: 1, Temperature: randFloats(-100, 0, 1)[0], Timestamp: time.Now().Unix(), FlipperID: "0001"}
		sendFlipperDatapoint(&flipperPoint, &writeApi)

		flipperPoint = FlipperDewPoint{DepotID: "UDC", Channel: 2, Temperature: randFloats(-100, 0, 1)[0], Timestamp: time.Now().Unix(), FlipperID: "0002"}
		sendFlipperDatapoint(&flipperPoint, &writeApi)

		flipperPoint = FlipperDewPoint{DepotID: "TKO", Channel: 1, Temperature: randFloats(-100, 0, 1)[0], Timestamp: time.Now().Unix(), FlipperID: "0003"}
		sendFlipperDatapoint(&flipperPoint, &writeApi)
		// time.Sleep(time.Minute)
		time.Sleep(10 * time.Second)
	}
}

func sendIonPumpDataPoint(dataPoint *IonPumpDataPoint, writeAPI *api.WriteApiBlocking) {

	p := influxdb2.NewPointWithMeasurement("IonPump")
	p.AddTag("LPN", dataPoint.LPN)
	p.AddTag("DepotID", dataPoint.DepotID)
	p.AddTag("PumpID", dataPoint.PumpID)
	p.AddField("Voltage", dataPoint.Voltage)
	p.AddField("Current", dataPoint.Current)
	p.AddField("Pressure", dataPoint.Pressure)
	p.SetTime(time.Unix(dataPoint.Timestamp, 0))

	err := (*writeAPI).WritePoint(context.Background(), p)

	if err != nil {
		tracelog.Error(err, "write to DB fail", "IonPump")
	}
}

func sendFlipperDatapoint(flipperPoint *FlipperDewPoint, writeAPI *api.WriteApiBlocking) {

	p := influxdb2.NewPointWithMeasurement("Flipper")
	p.AddTag("DepotID", flipperPoint.DepotID)
	p.AddTag("Channel", strconv.Itoa(flipperPoint.Channel))
	p.AddTag("FlipperID", flipperPoint.FlipperID)
	p.AddField("Temperature", flipperPoint.Temperature)
	p.SetTime(time.Unix(flipperPoint.Timestamp, 0))

	err := (*writeAPI).WritePoint(context.Background(), p)

	if err != nil {
		tracelog.Error(err, "write to DB fail", "IonPump")
	}
}

func randFloats(min, max float64, n int) []float64 {
	res := make([]float64, n)
	for i := range res {
		res[i] = min + rand.Float64()*(max-min)
	}
	return res
}
