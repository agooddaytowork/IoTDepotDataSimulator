package main

import (
	"context"
	"math/rand"
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
	Temperature float32 `json:"TEMP"`
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
	inFluxClient = influxdb2.NewClient("http://dynim.ddns.net:8800", "QVyFUZnEGrI3jOwTPbrSpOIGbBacd_OTk8NdD6b9aEPVJW1Ttxe5QiULGqF5TNzNCJM7NjLUUMOSDK971Mzzaw==")

	writeApi = inFluxClient.WriteApiBlocking("Kindhelm", "tam")

	defer func() {

		tracelog.Warning("Client Close", "pushCollectedMultiDataToDatabase", "")

		inFluxClient.Close()
	}()

	for {
		newDataPoint := IonPumpDataPoint{DepotID: "UDC", PumpID: "00-01", Timestamp: time.Now().Unix(), LPN: "SU-000006E9DAE4", Current: randFloats(10e-9, 0.4, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}

		sendIonPumpDataPoint(&newDataPoint, &writeApi)
		newDataPoint = IonPumpDataPoint{DepotID: "UDC", PumpID: "00-01", Timestamp: time.Now().Unix(), LPN: "SU-000007D6D6B9", Current: randFloats(10e-9, 0.4, 1)[0], Pressure: randFloats(10e-12, 10e-7, 1)[0], Voltage: float64(rand.Intn(7000-0) + 0)}
		sendIonPumpDataPoint(&newDataPoint, &writeApi)
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

func randFloats(min, max float64, n int) []float64 {
	res := make([]float64, n)
	for i := range res {
		res[i] = min + rand.Float64()*(max-min)
	}
	return res
}
