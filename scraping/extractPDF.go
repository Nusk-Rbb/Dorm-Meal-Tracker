/*
 * Extract all tables from the specified pages of one or more PDF files.
 *
 * Run as: go run pdf_tables.go input.pdf
 */

package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"golang.org/x/text/unicode/norm"

	"github.com/bmatcuk/doublestar"
	"github.com/joho/godotenv"
	"github.com/unidoc/unipdf/v3/common"
	"github.com/unidoc/unipdf/v3/common/license"
	"github.com/unidoc/unipdf/v3/extractor"
	"github.com/unidoc/unipdf/v3/model"
	"github.com/unidoc/unipdf/v3/pdfutil"
)

func init() {
	// Make sure to load your metered License API key prior to using the library.
	// If you need a key, you can sign up and create a free one at https://cloud.unidoc.io
	err := godotenv.Load()
	if err != nil {
		panic("Error loading .env file")
	}
	apiKey := os.Getenv("UNIDOC_LICENSE_API_KEY")
	err = license.SetMeteredKey(apiKey)
	if err != nil {
		panic(err)
	}
}

type Options struct {
	CSVDir    string
	FirstPage int
	LastPage  int
	Width     int
	Height    int
	Verbose   int
	Debug     bool
	Trace     bool
	DoProfile bool
}

type Option func(*Options)

func csvDir(dir string) Option {
	return func(opts *Options) {
		opts.CSVDir = dir
	}
}

func FirstPage(page int) Option {
	return func(opts *Options) {
		opts.FirstPage = page
	}
}

func LastPage(page int) Option {
	return func(opts *Options) {
		opts.LastPage = page
	}
}

func Width(width int) Option {
	return func(opts *Options) {
		opts.Width = width
	}
}

func Height(height int) Option {
	return func(opts *Options) {
		opts.Height = height
	}
}

func Verbose(verbose int) Option {
	return func(opts *Options) {
		opts.Verbose = verbose
	}
}

func Debug(debug bool) Option {
	return func(opts *Options) {
		opts.Debug = debug
	}
}

func Trace(trace bool) Option {
	return func(opts *Options) {
		opts.Trace = trace
	}
}

func DoProfile(doProfile bool) Option {
	return func(opts *Options) {
		opts.DoProfile = doProfile
	}
}

func extractPDF(PDFFilePath []string, options ...Option) error {
	// Default Options
	opts := Options{
		CSVDir:    "./outcsv",
		FirstPage: -1,
		LastPage:  10000,
		Width:     0,
		Height:    0,
		Verbose:   1,
		Debug:     false,
		Trace:     false,
		DoProfile: false,
	}

	for _, option := range options {
		option(&opts)
	}

	if opts.Trace {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelTrace))
	} else if opts.Debug {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelDebug))
	} else {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelInfo))
	}

	makeDir("CSV directory", opts.CSVDir)

	pathList, err := patternsToPaths(PDFFilePath)
	if err != nil {
		return err
	}
	fmt.Printf("%d PDF files\n", len(pathList))

	if opts.DoProfile {
		f, err := os.Create("cpu.profile")
		if err != nil {
			return fmt.Errorf("could not create CPU profile: err=%w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("could not start CPU profile: err=%w", err)
		}
		defer pprof.StopCPUProfile()
	}

	for i, inPath := range pathList {
		t0 := time.Now()
		result, err := extractTables(inPath, opts.FirstPage, opts.LastPage)
		if err != nil {
			log.Fatalf("Error: %v\n", err)
			continue
		}
		duration := time.Since(t0).Seconds()
		numPages := len(result.pageTables)
		result = result.filter(opts.Width, opts.Height)
		log.Printf("%3d of %d: %4.1f MB %3d pages %4.1f sec %q %s",
			i+1, len(pathList), fileSizeMB(inPath), numPages, duration, inPath, result.describe(opts.Verbose))
		csvYearDirName, err := extractDirectory(inPath, 1)
		csvMonthDirName, err := extractDirectory(inPath, -1)
		if err != nil {
			log.Fatalf("Failed to extract directory: %v\n", err)
		}
		csvSubDir := opts.CSVDir + "/" + csvYearDirName + "/" + csvMonthDirName
		makeDir("CSV Sub directory", csvSubDir)
		csvRoot := changeDirExt(csvSubDir, filepath.Base(inPath), "", "")
		fmt.Println(csvRoot)
		if err := result.saveCSVFiles(csvRoot); err != nil {
			log.Fatalf("Failed to write %q: %v\n", csvRoot, err)
			continue
		}
	}

	return nil
}

// extractTables extracts tables from pages `firstPage` to `lastPage` in PDF file `inPath`.
func extractTables(inPath string, firstPage, lastPage int) (docTables, error) {
	f, err := os.Open(inPath)
	if err != nil {
		return docTables{}, fmt.Errorf("Could not open %q err=%w", inPath, err)
	}
	defer f.Close()

	pdfReader, err := model.NewPdfReaderLazy(f)
	if err != nil {
		return docTables{}, fmt.Errorf("NewPdfReaderLazy failed. %q err=%w", inPath, err)
	}
	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return docTables{}, fmt.Errorf("GetNumPages failed. %q err=%w", inPath, err)
	}

	if firstPage < 1 {
		firstPage = 1
	}
	if lastPage > numPages {
		lastPage = numPages
	}

	result := docTables{pageTables: make(map[int][]stringTable)}
	for pageNum := firstPage; pageNum <= lastPage; pageNum++ {
		tables, err := extractPageTables(pdfReader, pageNum)
		if err != nil {
			return docTables{}, fmt.Errorf("extractPageTables failed. inPath=%q pageNum=%d err=%w",
				inPath, pageNum, err)
		}
		result.pageTables[pageNum] = tables
	}
	return result, nil
}

// extractPageTables extracts the tables from (1-offset) page number `pageNum` in opened
// PdfReader `pdfReader.
func extractPageTables(pdfReader *model.PdfReader, pageNum int) ([]stringTable, error) {
	page, err := pdfReader.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	if err := pdfutil.NormalizePage(page); err != nil {
		return nil, err
	}

	ex, err := extractor.New(page)
	if err != nil {
		return nil, err
	}
	pageText, _, _, err := ex.ExtractPageText()
	if err != nil {
		return nil, err
	}
	tables := pageText.Tables()
	stringTables := make([]stringTable, len(tables))
	for i, table := range tables {
		stringTables[i] = asStringTable(table)
	}
	return stringTables, nil
}

// docTables describes the tables in a document.
type docTables struct {
	pageTables map[int][]stringTable
}

// stringTable is the strings in TextTable.
type stringTable [][]string

func (r docTables) saveCSVFiles(csvRoot string) error {
	for _, pageNum := range r.pageNumbers() {
		for i, table := range r.pageTables[pageNum] {
			csvPath := fmt.Sprintf("%s.page%d.table%d.csv", csvRoot, pageNum, i+1)
			contents := table.csv()
			if err := ioutil.WriteFile(csvPath, []byte(contents), 0666); err != nil {
				return fmt.Errorf("failed to write csvPath=%q err=%w", csvPath, err)
			}
		}
	}
	return nil
}

// wh returns the width and height of table `t`.
func (t stringTable) wh() (int, int) {
	if len(t) == 0 {
		return 0, 0
	}
	return len(t[0]), len(t)
}

// csv returns `t` in CSV format.
func (t stringTable) csv() string {
	w, h := t.wh()
	b := new(bytes.Buffer)
	csvwriter := csv.NewWriter(b)
	for y, row := range t {
		if len(row) != w {
			err := fmt.Errorf("table = %d x %d row[%d]=%d %q", w, h, y, len(row), row)
			panic(err)
		}
		csvwriter.Write(row)
	}
	csvwriter.Flush()
	return b.String()
}

func (r *docTables) String() string {
	return r.describe(1)
}

// describe returns a string describing the tables in `r`.
//
//	                            (level 0)
//	%d pages %d tables          (level 1)
//	  page %d: %d tables        (level 2)
//	    table %d: %d x %d       (level 3)
//	        contents            (level 4)
func (r *docTables) describe(level int) string {
	if level == 0 || r.numTables() == 0 {
		return "\n"
	}
	var sb strings.Builder
	pageNumbers := r.pageNumbers()
	fmt.Fprintf(&sb, "%d pages %d tables\n", len(pageNumbers), r.numTables())
	if level <= 1 {
		return sb.String()
	}
	for _, pageNum := range r.pageNumbers() {
		tables := r.pageTables[pageNum]
		if len(tables) == 0 {
			continue
		}
		fmt.Fprintf(&sb, "   page %d: %d tables\n", pageNum, len(tables))
		if level <= 2 {
			continue
		}
		for i, table := range tables {
			w, h := table.wh()
			fmt.Fprintf(&sb, "      table %d: %d x %d\n", i+1, w, h)
			if level <= 3 || len(table) == 0 {
				continue
			}
			for _, row := range table {
				cells := make([]string, len(row))
				for i, cell := range row {
					if len(cell) > 0 {
						cells[i] = fmt.Sprintf("%q", cell)
					}
				}
				fmt.Fprintf(&sb, "        [%s]\n", strings.Join(cells, ", "))
			}
		}
	}
	return sb.String()
}

func (r *docTables) pageNumbers() []int {
	pageNums := make([]int, len(r.pageTables))
	i := 0
	for pageNum := range r.pageTables {
		pageNums[i] = pageNum
		i++
	}
	sort.Ints(pageNums)
	return pageNums
}

func (r *docTables) numTables() int {
	n := 0
	for _, tables := range r.pageTables {
		n += len(tables)
	}
	return n
}

// filter returns the tables in `r` that are at least `width` cells wide and `height` cells high.
func (r docTables) filter(width, height int) docTables {
	filtered := docTables{pageTables: make(map[int][]stringTable)}
	for pageNum, tables := range r.pageTables {
		var filteredTables []stringTable
		for _, table := range tables {
			if len(table[0]) >= width && len(table) >= height {
				filteredTables = append(filteredTables, table)
			}
		}
		if len(filteredTables) > 0 {
			filtered.pageTables[pageNum] = filteredTables
		}
	}
	return filtered
}

// asStringTable returns TextTable `table` as a stringTable.
func asStringTable(table extractor.TextTable) stringTable {
	cells := make(stringTable, table.H)
	for y, row := range table.Cells {
		cells[y] = make([]string, table.W)
		for x, cell := range row {
			cells[y][x] = cell.Text
		}
	}
	return normalizeTable(cells)
}

// normalizeTable returns `cells` with each cell normalized.
func normalizeTable(cells stringTable) stringTable {
	for y, row := range cells {
		for x, cell := range row {
			cells[y][x] = normalize(cell)
		}
	}
	return cells
}

// normalize returns a version of `text` that is NFKC normalized and has reduceSpaces() applied.
func normalize(text string) string {
	return reduceSpaces(norm.NFKC.String(text))
}

// reduceSpaces returns `text` with runs of spaces of any kind (spaces, tabs, line breaks, etc)
// reduced to a single space.
func reduceSpaces(text string) string {
	text = reSpace.ReplaceAllString(text, " ")
	return strings.Trim(text, " \t\n\r\v")
}

var reSpace = regexp.MustCompile(`(?m)\s+`)

// patternsToPaths returns the file paths matched by the patterns in `patternList`.
func patternsToPaths(patternList []string) ([]string, error) {
	var pathList []string
	common.Log.Debug("patternList=%d", len(patternList))
	for i, pattern := range patternList {
		pattern = expandUser(pattern)
		files, err := doublestar.Glob(pattern)
		if err != nil {
			common.Log.Error("PatternsToPaths: Glob failed. pattern=%#q err=%v", pattern, err)
			return pathList, err
		}
		common.Log.Debug("patternList[%d]=%q %d matches", i, pattern, len(files))
		for _, filename := range files {
			ok, err := regularFile(filename)
			if err != nil {
				common.Log.Error("PatternsToPaths: regularFile failed. pattern=%#q err=%v", pattern, err)
				return pathList, err
			}
			if !ok {
				continue
			}
			pathList = append(pathList, filename)
		}
	}
	// pathList = StringUniques(pathList)
	sort.Strings(pathList)
	return pathList, nil
}

// homeDir is the current user's home directory.
var homeDir = getHomeDir()

// getHomeDir returns the current user's home directory.
func getHomeDir() string {
	usr, _ := user.Current()
	return usr.HomeDir
}

// expandUser returns `filename` with "~"" replaced with user's home directory.
func expandUser(filename string) string {
	return strings.Replace(filename, "~", homeDir, -1)
}

// regularFile returns true if file `filename` is a regular file.
func regularFile(filename string) (bool, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return false, err
	}
	return fi.Mode().IsRegular(), nil
}

// fileSizeMB returns the size of file `filename` in megabytes.
func fileSizeMB(filename string) float64 {
	fi, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	return float64(fi.Size()) / 1024.0 / 1024.0
}

// makeUsage updates flag.Usage to include usage message `msg`.
func makeUsage(msg string) {
	usage := flag.Usage
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, msg)
		usage()
	}
}

// makeDir creates `outDir`. Name is the name of `outDir` in the calling code.
func makeDir(name, outDir string) {
	if outDir == "." || outDir == ".." {
		panic(fmt.Errorf("%s=%q not allowed", name, outDir))
	}
	if outDir == "" {
		return
	}

	outDir, err := filepath.Abs(outDir)
	if err != nil {
		panic(fmt.Errorf("Abs failed. %s=%q err=%w", name, outDir, err))
	}
	if err := os.MkdirAll(outDir, 0751); err != nil {
		panic(fmt.Errorf("Couldn't create %s=%q err=%w", name, outDir, err))
	}
}

// changeDirExt inserts `qualifier` into `filename` before its extension then changes its
// directory to `dirName` and extension to `extName`,
func changeDirExt(dirName, filename, qualifier, extName string) string {
	if dirName == "" {
		return ""
	}
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	base = base[:len(base)-len(ext)]
	if len(qualifier) > 0 {
		base = fmt.Sprintf("%s.%s", base, qualifier)
	}
	filename = fmt.Sprintf("%s%s", base, extName)
	path := filepath.Join(dirName, filename)
	common.Log.Debug("changeDirExt(%q,%q,%q)->%q", dirName, base, extName, path)
	return path
}

func extractDirectory(filepath string, depth int) (string, error) {
	parts := strings.Split(filepath, "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("cannot get directory")
	}
	if depth == -1 {
		fileName := parts[len(parts)-1]
		dirName := strings.Split(fileName, ".")[0]
		return dirName, nil
	} else {
		return strings.Split(parts[depth], ".")[0], nil
	}
}
