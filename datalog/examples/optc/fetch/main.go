// Command fetch downloads the starter slice of the DARPA OpTC dataset
// (host bucket AIA-201-225 for red-team day 1, 23 Sep 2019) into the
// data/ directory beside this example. The datalog jsonfacts loader
// reads .gz sources directly, so the files stay compressed on disk
// (~2.2 GB total).
//
// Google Drive fronts large files with a "can't scan for viruses"
// interstitial; this program handles that confirm-token form itself, so
// no gdown/python is needed. If Drive changes the interstitial format,
// download the files by browser or rclone as described in README.md and
// re-run this program to verify them.
//
// OpTC data is released by DARPA / Five Directions; see
// https://github.com/FiveDirections/OpTC-data for terms and documentation.
package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
)

// Starter slice: ecar/evaluation/23Sep19-red/AIA-201-225/ — the day-1
// initial-compromise host (SysClient0201) plus its 24 bucket neighbors.
// Google Drive file IDs verified 2026-07-18.
var starterFiles = map[string]string{
	"AIA-201-225.ecar-2019-12-08T11-05-10.046.json.gz": "1pJLxJsDV8sngiedbfVajMetczIgM3PQd",
	"AIA-201-225.ecar-last.json.gz":                    "1HFSyvmgH0jvdnnnTdKfWRjZYOrLWoIkv",
}

func main() {
	dataDir := flag.String("data", "", "directory to download into (default: data/ beside this example)")
	flag.Parse()
	if *dataDir == "" {
		*dataDir = defaultDataDir()
	}
	if err := run(*dataDir); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// defaultDataDir resolves data/ relative to this source file so the
// program works from any working directory under `go run`.
func defaultDataDir() string {
	_, src, _, ok := runtime.Caller(0)
	if !ok {
		return "data"
	}
	return filepath.Join(filepath.Dir(src), "..", "data")
}

func run(dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	client := &http.Client{Jar: jar}

	names := make([]string, 0, len(starterFiles))
	for name := range starterFiles {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		dest := filepath.Join(dataDir, name)
		if _, err := os.Stat(dest); err == nil {
			fmt.Println("Already present:", name)
			continue
		}
		fmt.Println("Downloading", name, "...")
		if err := download(client, starterFiles[name], dest); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}

	fmt.Println("Verifying gzip integrity ...")
	for _, name := range names {
		if err := verifyGzip(filepath.Join(dataDir, name)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Println("  ok:", name)
	}

	fmt.Println("Done. Data in", dataDir+":")
	for _, name := range names {
		if info, err := os.Stat(filepath.Join(dataDir, name)); err == nil {
			fmt.Printf("  %8.1f MB  %s\n", float64(info.Size())/1e6, name)
		}
	}
	return nil
}

// download fetches one Drive file by id into dest, going through the
// virus-scan interstitial if Drive serves one. It writes to dest+".part"
// and renames only on success, so an interrupted run never leaves a
// truncated file that "Already present" would later skip.
func download(client *http.Client, id, dest string) error {
	resp, err := client.Get("https://drive.google.com/uc?export=download&id=" + id)
	if err != nil {
		return err
	}
	if isHTML(resp) {
		confirmURL, err := parseInterstitial(resp)
		resp.Body.Close()
		if err != nil {
			return err
		}
		resp, err = client.Get(confirmURL)
		if err != nil {
			return err
		}
		if isHTML(resp) {
			resp.Body.Close()
			return fmt.Errorf("Drive returned HTML instead of the file; the interstitial format may have changed — download by browser or rclone per README.md")
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	part := dest + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, &progressReader{r: resp.Body, total: resp.ContentLength})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(part)
		return err
	}
	fmt.Println()
	return os.Rename(part, dest)
}

func isHTML(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return len(ct) >= 9 && ct[:9] == "text/html"
}

var (
	formActionRe  = regexp.MustCompile(`<form[^>]*action="([^"]+)"`)
	hiddenInputRe = regexp.MustCompile(`<input type="hidden" name="([^"]+)" value="([^"]*)"`)
)

// parseInterstitial extracts the confirm form from Drive's virus-scan
// page and rebuilds it as a GET URL (the form method is GET).
func parseInterstitial(resp *http.Response) (string, error) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	action := formActionRe.FindSubmatch(body)
	if action == nil {
		return "", fmt.Errorf("no download form on Drive interstitial page; download by browser or rclone per README.md")
	}
	u, err := url.Parse(html.UnescapeString(string(action[1])))
	if err != nil {
		return "", err
	}
	q := u.Query()
	for _, m := range hiddenInputRe.FindAllSubmatch(body, -1) {
		q.Set(html.UnescapeString(string(m[1])), html.UnescapeString(string(m[2])))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// progressReader wraps the download body and prints a running count
// every 100 MB, since these files are gigabytes each.
type progressReader struct {
	r     io.Reader
	total int64
	read  int64
	next  int64
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.read += int64(n)
	if p.read >= p.next {
		if p.total > 0 {
			fmt.Printf("\r  %d / %d MB", p.read/1e6, p.total/1e6)
		} else {
			fmt.Printf("\r  %d MB", p.read/1e6)
		}
		p.next += 100e6
	}
	return n, err
}

func verifyGzip(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()
	_, err = io.Copy(io.Discard, zr)
	return err
}
