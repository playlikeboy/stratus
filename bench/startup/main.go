// Command startup measures how long an emulator container takes from
// `docker run` until its resources exist, and its memory then.
//
//	go run ./bench/startup -label stratus -ready-url http://localhost:4566/_stratus/ready \
//	  -reps 5 -out startup.csv -- -v "$PWD/m.yaml:/m.yaml:ro" stratus -manifest /m.yaml
//
// Everything after -- is passed to `docker run -d --name <name> -p <port>:4566`.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "startup:", err)
		os.Exit(1)
	}
}

func run() error {
	label := flag.String("label", "", "name of the emulator in the results")
	readyURL := flag.String("ready-url", "", "URL that answers 200 once the resources exist")
	readyMatch := flag.String("ready-match", "", "text the ready answer must contain, if any")
	port := flag.Int("port", 4566, "host port the container's 4566 is published on")
	reps := flag.Int("reps", 5, "number of starts")
	timeout := flag.Duration("timeout", 3*time.Minute, "how long one start may take")
	out := flag.String("out", "", "CSV file to append the results to")
	flag.Parse()
	if *label == "" || *readyURL == "" || *out == "" || flag.NArg() == 0 {
		return errors.New("-label, -ready-url, -out and the docker run arguments after -- are required")
	}
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if info, _ := f.Stat(); info.Size() == 0 {
		_ = w.Write([]string{"emulator", "rep", "ready_s", "mem_mib"})
	}
	name := "stratus-bench-startup"
	for rep := 1; rep <= *reps; rep++ {
		ready, mem, err := start(name, *port, *readyURL, *readyMatch, *timeout, flag.Args())
		_ = exec.Command("docker", "rm", "-f", name).Run()
		if err != nil {
			return fmt.Errorf("%s start %d: %w", *label, rep, err)
		}
		row := []string{*label, strconv.Itoa(rep), fmt.Sprintf("%.3f", ready.Seconds()), fmt.Sprintf("%.1f", mem)}
		_ = w.Write(row)
		w.Flush()
		fmt.Println(strings.Join(row, " "))
	}
	return w.Error()
}

// start runs the container and waits for it to be ready; it returns the time
// that took and the container's memory in MiB once ready.
func start(name string, port int, readyURL, match string, timeout time.Duration, args []string) (time.Duration, float64, error) {
	_ = exec.Command("docker", "rm", "-f", name).Run()
	run := append([]string{"run", "-d", "--name", name, "-p", strconv.Itoa(port) + ":4566"}, args...)
	began := time.Now()
	if out, err := exec.Command("docker", run...).CombinedOutput(); err != nil {
		return 0, 0, fmt.Errorf("docker run: %v: %s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	for !isReady(client, readyURL, match) {
		select {
		case <-ctx.Done():
			return 0, 0, errors.New("not ready in time")
		case <-time.After(5 * time.Millisecond):
		}
	}
	ready := time.Since(began)
	mem, err := memoryMiB(name)
	return ready, mem, err
}

func isReady(client *http.Client, url, match string) bool {
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode == http.StatusOK && strings.Contains(string(body), match)
}

// memoryMiB is the container's memory use as docker stats reports it.
func memoryMiB(name string) (float64, error) {
	out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name).Output()
	if err != nil {
		return 0, err
	}
	return parseMemory(strings.Fields(string(out))[0])
}

// parseMemory reads a docker stats size such as 552.3MiB or 1.2GiB.
func parseMemory(s string) (float64, error) {
	units := []struct {
		suffix string
		mib    float64
	}{{"GiB", 1024}, {"MiB", 1}, {"KiB", 1.0 / 1024}, {"B", 1.0 / (1024 * 1024)}}
	for _, u := range units {
		if v, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.ParseFloat(v, 64)
			return n * u.mib, err
		}
	}
	return 0, fmt.Errorf("unknown size %q", s)
}
