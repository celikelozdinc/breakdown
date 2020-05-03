package main

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"

	statistics "github.com/gonum/stat"
	plot "gonum.org/v1/plot"
	plotter "gonum.org/v1/plot/plotter"
	plotutil "gonum.org/v1/plot/plotutil"
	vg "gonum.org/v1/plot/vg"
)

// Represents the data structure: Started,start_communication,prepareCkpts,applyCktps,Applied
type RestoreDurationBreakdown struct {
	StartJvm                []float64
	Mean_StartJvm           float64
	StartCommunication      []float64
	Mean_StartCommunication float64
	PrepareCkpts            []float64
	Mean_PrepareCkpts       float64
	ApplyCkpts              []float64
	Mean_ApplyCkpts         float64
}

var (
	input_csv  = "input/12Replicas.csv"
	output_png = "output/12_breakdown.png"
	xlabel     = "#replicas=12"
)

func main() {
	readRestoreDurations := func() [][]string {
		fileHandler, err := os.Open(input_csv)
		defer fileHandler.Close()
		if err != nil {
			log.Panic("Can not open file")
		}

		experiments, err := csv.NewReader(fileHandler).ReadAll()

		if err != nil {
			log.Panic("Can not read experiments from csv file")
		}

		return experiments
	}

	// Started,start_communication,prepareCkpts,applyCktps,Applied
	// also, calculates statistics
	parseRestoreDurations := func(restoreDurations [][]string, distributed *RestoreDurationBreakdown, centralized *RestoreDurationBreakdown, conventional *RestoreDurationBreakdown) {

		for row := range restoreDurations {
			if row == 0 {
				// Header row
				continue
			}

			// Distributed --> has 1 extra step : prepare ckpts
			if row >= 1 && row <= 10 {
				startJVM, _ := strconv.ParseFloat(restoreDurations[row][0], 64)
				distributed.StartJvm = append(distributed.StartJvm, startJVM)
				distributed.Mean_StartJvm = statistics.Mean(distributed.StartJvm, nil)

				startComm, _ := strconv.ParseFloat(restoreDurations[row][1], 64)
				distributed.StartCommunication = append(distributed.StartCommunication, startComm)
				distributed.Mean_StartCommunication = statistics.Mean(distributed.StartCommunication, nil)

				prepare, _ := strconv.ParseFloat(restoreDurations[row][2], 64)
				distributed.PrepareCkpts = append(distributed.PrepareCkpts, prepare)
				distributed.Mean_PrepareCkpts = statistics.Mean(distributed.PrepareCkpts, nil)

				apply, _ := strconv.ParseFloat(restoreDurations[row][3], 64)
				distributed.ApplyCkpts = append(distributed.ApplyCkpts, apply)
				distributed.Mean_ApplyCkpts = statistics.Mean(distributed.ApplyCkpts, nil)
			}
			// Centralized
			if row >= 11 && row <= 20 {
				startJVM, _ := strconv.ParseFloat(restoreDurations[row][0], 64)
				centralized.StartJvm = append(centralized.StartJvm, startJVM)
				centralized.Mean_StartJvm = statistics.Mean(centralized.StartJvm, nil)

				start, _ := strconv.ParseFloat(restoreDurations[row][1], 64)
				centralized.StartCommunication = append(centralized.StartCommunication, start)
				centralized.Mean_StartCommunication = statistics.Mean(centralized.StartCommunication, nil)

				centralized.PrepareCkpts = append(centralized.PrepareCkpts, 0)
				centralized.Mean_PrepareCkpts = statistics.Mean(centralized.PrepareCkpts, nil)

				apply, _ := strconv.ParseFloat(restoreDurations[row][3], 64)
				centralized.ApplyCkpts = append(centralized.ApplyCkpts, apply)
				centralized.Mean_ApplyCkpts = statistics.Mean(centralized.ApplyCkpts, nil)
			}
			// Conventional
			if row >= 21 && row <= 30 {
				startJVM, _ := strconv.ParseFloat(restoreDurations[row][0], 64)
				conventional.StartJvm = append(conventional.StartJvm, startJVM)
				conventional.Mean_StartJvm = statistics.Mean(conventional.StartJvm, nil)

				start, _ := strconv.ParseFloat(restoreDurations[row][1], 64)
				conventional.StartCommunication = append(conventional.StartCommunication, start)
				conventional.Mean_StartCommunication = statistics.Mean(conventional.StartCommunication, nil)

				conventional.PrepareCkpts = append(conventional.PrepareCkpts, 0)
				conventional.Mean_PrepareCkpts = statistics.Mean(conventional.PrepareCkpts, nil)

				apply, _ := strconv.ParseFloat(restoreDurations[row][3], 64)
				conventional.ApplyCkpts = append(conventional.ApplyCkpts, apply)
				conventional.Mean_ApplyCkpts = statistics.Mean(conventional.ApplyCkpts, nil)
			}

		}
	}

	// plot bar graph
	makePlot := func(distributed *RestoreDurationBreakdown, centralized *RestoreDurationBreakdown, conventional *RestoreDurationBreakdown) {

		/* StartJvm */
		width := 0.3 * vg.Centimeter
		var group_StartJvm plotter.Values
		group_StartJvm = append(group_StartJvm, distributed.Mean_StartJvm, centralized.Mean_StartJvm, conventional.Mean_StartJvm)
		bars_StartJvm, err := plotter.NewBarChart(group_StartJvm, width)
		if err != nil {
			panic(err)
		}
		bars_StartJvm.LineStyle.Width = vg.Length(0)
		bars_StartJvm.Color = plotutil.Color(0)
		bars_StartJvm.Offset = -width

		/* StartCommunication */
		var group_StartCommunication plotter.Values
		group_StartCommunication = append(group_StartCommunication, distributed.Mean_StartCommunication, centralized.Mean_StartCommunication, conventional.Mean_StartCommunication)
		bars_StartCommunication, err := plotter.NewBarChart(group_StartCommunication, width)
		if err != nil {
			panic(err)
		}
		bars_StartCommunication.LineStyle.Width = vg.Length(0)
		bars_StartCommunication.Color = plotutil.Color(1)

		/* PrepareCkpts */
		var group_prepareCkpts plotter.Values
		group_prepareCkpts = append(group_prepareCkpts, distributed.Mean_PrepareCkpts, centralized.Mean_PrepareCkpts, conventional.Mean_PrepareCkpts)
		bars_prepareCkpts, err := plotter.NewBarChart(group_prepareCkpts, width)
		if err != nil {
			panic(err)
		}
		bars_prepareCkpts.LineStyle.Width = vg.Length(0)
		bars_prepareCkpts.Color = plotutil.Color(2)
		bars_prepareCkpts.Offset = width

		/* ApplyCkpts */
		var group_applyCkpts plotter.Values
		group_applyCkpts = append(group_applyCkpts, distributed.Mean_ApplyCkpts, centralized.Mean_ApplyCkpts, conventional.Mean_ApplyCkpts)
		bars_applyCkpts, err := plotter.NewBarChart(group_applyCkpts, width)
		if err != nil {
			panic(err)
		}
		bars_applyCkpts.LineStyle.Width = vg.Length(0)
		bars_applyCkpts.Color = plotutil.Color(3)
		bars_applyCkpts.Offset = 2 * width

		// Vertical BarChart
		plot, err := plot.New()
		if err != nil {
			log.Panic(err)
		}

		// metadata for plot
		plot.Title.Text = " "
		plot.X.Label.Text = xlabel
		plot.Y.Label.Text = "Breakdown for restore duration(sec)"
		labels := []string{"Distributed", "Centralized", "Conventional"}

		plot.Add(bars_StartJvm, bars_StartCommunication, bars_prepareCkpts, bars_applyCkpts)
		plot.Legend.Add("Start JVM", bars_StartJvm)
		plot.Legend.Add("Start Communication", bars_StartCommunication)
		plot.Legend.Add("Prepare Checkpoints", bars_prepareCkpts)
		plot.Legend.Add("Apply Checkpoints", bars_applyCkpts)
		plot.Legend.Top = true
		plot.NominalX(labels...)

		err = plot.Save(3*vg.Inch, 5*vg.Inch, output_png)
		if err != nil {
			log.Panic(err)
		}

	}

	distributedBreakdown, centralizedBreakdown, conventionalBreakdown := RestoreDurationBreakdown{}, RestoreDurationBreakdown{}, RestoreDurationBreakdown{}
	restoreDurations := readRestoreDurations()
	parseRestoreDurations(restoreDurations, &distributedBreakdown, &centralizedBreakdown, &conventionalBreakdown)
	makePlot(&distributedBreakdown, &centralizedBreakdown, &conventionalBreakdown)
}
