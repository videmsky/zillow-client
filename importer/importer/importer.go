package main

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"bufio"
	"runtime"
	"strings"
	"strconv"
	"flag"
	"sync"
	"database/sql"
	_ "reflect"
	_ "github.com/lib/pq"
	"time"
	"math/rand"
)

// This channel has no buffer, so it only accepts input when something is ready
// to take it out. This keeps the reading from getting ahead of the writers.
var workQueue = make(chan string)
var totalProcessed int
var totalProcessedLock sync.Mutex

func main() {

  runtime.GOMAXPROCS(runtime.NumCPU())

	filename := "../../zone_100_pts_edited.csv"
	logPath := "../../importer.log"

	const (
	  host     = "localhost"
	  port     = 5432
	  user     = "dropbot"
	  dbname   = "realestate"
	)

	// ToDo: pass file paths and db creds via command line
	// var filename string
	// var logPath string
  var workerCount int
	counter := 0
  // flag.StringVar(&filename, "filename", "data.csv", "Enter a filename")
  // flag.StringVar(&logPath, "log_path", "", "Enter a logfile path")
	// flag.StringVar(&pgHost, "pg_host", "localhost", "Enter a postgres hostname")
  // flag.StringVar(&pgPassword, "pg_password", "", "Enter a postgres password")
  // flag.StringVar(&pgUser, "pg_user", "dropcountr", "Enter a postgres user")
  flag.IntVar(&workerCount, "workers", 8, "Enter the number of concurrent workers for processing data")
  flag.Parse()

	threshold := numRand(111, 222)
	fmt.Println("random: ", threshold)

  if len(logPath) > 0 {
    logFile, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
    log.SetOutput(logFile)
  }

	if len(filename) == 0 {
    log.Fatal("filename required")
  }

	// test local postgres connection
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
	  fmt.Println("postgres open error: ", err)
	}

	err = db.Ping()
  if err != nil {
    fmt.Println("postgres ping error: ", err)
  }

	fmt.Println("Successfully connected!")
	// fmt.Println(reflect.TypeOf(db))

  // Now read them all off, concurrently.
  for i := 0; i < workerCount; i++ {
    go worker(workQueue, db)
  }
  readFile(filename, counter, threshold)
	defer db.Close()
}

func numRand(min, max int) int {
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Intn(max-min) + min
}

func cacheWriter(oid int64) {
	fileHandle, err := os.OpenFile("../../cache.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
	  fmt.Println("Cache Writer Error: ", err)
	}
	writer := bufio.NewWriter(fileHandle)
	defer fileHandle.Close()

	fmt.Fprintln(writer, oid)
	writer.Flush()
}

func cacheReader(oid int64) bool{
	var recordExists bool = false
	fileHandle, err := os.Open("../../cache.txt")
	if err != nil {
	  fmt.Println("Cache Reader Error: ", err)
	}
	fileScanner := bufio.NewScanner(fileHandle)

	for fileScanner.Scan() {
		textNumber, _ := strconv.ParseInt(fileScanner.Text(), 10, 64)
		// fmt.Println(reflect.TypeOf(oid))
		if oid == textNumber {
			recordExists = true
			break
			// fmt.Println(textNumber)
		} else {
			recordExists = false
		}
	}
	defer fileHandle.Close()
	return recordExists
}

// Read the lines into the work queue.
func readFile(filename string, counter int, threshold int) {
  cmd := exec.Command("cat", filename)

  stdout, err := cmd.StdoutPipe()
  if err != nil {
    log.Fatalf("Couldn't setup StdoutPipe: %v", err)
  }

  // start the command after having set up the pipe
  if err := cmd.Start(); err != nil {
    log.Fatalf("Couldn't open file for reading: %v", err)
  }

  // var scanner *Scanner
  scanner := bufio.NewScanner(stdout)

  for scanner.Scan() {
		counter++
		// fmt.Println("counter: ", counter)
		if counter == threshold {
			os.Exit(1)
		} else {
			line := scanner.Text()
	    workQueue <- line
		}
  }
  // Close the channel so everyone reading from it knows we're done.
  close(workQueue)
}

func worker(queue <-chan string, db *sql.DB) {
	for line := range queue {
		var cacheResult bool
		// Do the work with the line.
    items := strings.Split(line, ",")

		o_id, _ := strconv.ParseInt(items[0], 10, 64)
		o_street := items[8]
		zipString := items[9]
		o_zip, _ := strconv.ParseInt(items[9], 10, 64)
		o_city := items[10]
		o_lat, _ := strconv.ParseFloat(items[13], 64)
		o_lon, _ := strconv.ParseFloat(items[14], 64)
		o_type := items[15]
		o_zone, _ := strconv.ParseInt(items[17], 10, 64)
		var z_zpid int64
		var zpidString string
		var z_links_home_details string
		var z_links_comparables string
		var z_address_street string
		var z_zipcode int64
		var z_city string
		var z_state string
		var z_latitude float64
		var z_longitude float64
		var z_fips_county string
		var z_use_code string
		var z_tax_assessment_year int
		var z_tax_assessment float64
		var z_year_built int
		var z_lot_size_sqft int
		var z_finished_sqft int
		var z_bathrooms float64
		var z_bedrooms int
		var z_total_rooms int
		var z_last_sold_date string
		var z_last_sold_price_value int
		var z_zestimate_amount_value int
		var z_zestimate_lastupdated string
		var z_zestimate_low_value int
		var z_zestimate_high_value int
		var z_zestimate_percentile int64

		cacheResult = cacheReader(o_id)

		if cacheResult == true {
			log.Printf("Already imported: %v\n", o_id)
			fmt.Println("cached?:", cacheResult, "o_id:", o_id)
		} else {
			cacheWriter(o_id)
			log.Printf("Importing: %v\n", o_id)
			fmt.Println("cached?:", cacheResult, "o_id:", o_id)

			response := queryAPI(o_street, zipString, o_id)
			if len(response) != 0 {
				// fmt.Println("debug: ", response[0])
				var err error
				for i := 0; i < len(response); i++ {
					zpidString = response[0].Result.Zpid
					z_zpid, err = strconv.ParseInt(response[0].Result.Zpid, 10, 64)
					z_links_home_details = response[0].Result.Links.HomeDetails
					z_links_comparables = response[0].Result.Links.Comparables
					z_address_street = response[0].Result.Address.Street
					z_zipcode, err = strconv.ParseInt(response[0].Result.Address.Zipcode, 10, 64)
					z_latitude, err = strconv.ParseFloat(response[0].Result.Address.Latitude, 64)
					z_longitude, err = strconv.ParseFloat(response[0].Result.Address.Longitude, 64)
					z_fips_county = response[0].Result.FIPSCounty
					z_use_code = response[0].Result.UseCode
					z_tax_assessment_year = response[0].Result.TaxAssessmentYear
					z_tax_assessment = response[0].Result.TaxAssessment
					z_year_built = response[0].Result.YearBuilt
					z_lot_size_sqft = response[0].Result.LotSizeSqFt
					z_finished_sqft = response[0].Result.FinishedSqFt
					z_bathrooms = response[0].Result.Bathrooms
					z_bedrooms = response[0].Result.Bedrooms
					z_total_rooms = response[0].Result.TotalRooms
					z_last_sold_date = response[0].Result.LastSoldDate
					z_last_sold_price_value = response[0].Result.LastSoldPrice.Value
					z_zestimate_amount_value = response[0].Result.Zestimate.Amount.Value
					z_zestimate_lastupdated = response[0].Result.Zestimate.LastUpdated
					z_zestimate_low_value = response[0].Result.Zestimate.Low.Value
					z_zestimate_high_value = response[0].Result.Zestimate.High.Value
					z_zestimate_percentile, err = strconv.ParseInt(response[0].Result.Zestimate.Percentile, 10, 64)
				}
				if err != nil {
					log.Printf("Error: %v o_id: %v", err, o_id)
					continue
				}
			}
		}

		z_last_sold_date_obj, _ := time.Parse("1/2/2006 15:04:05", z_last_sold_date)
		z_zestimate_lastupdated_obj, _ := time.Parse("1/2/2006 15:04:05", z_zestimate_lastupdated)

		if len(zpidString) == 0 {
			break
		} else {
			_, err := db.Exec("SELECT * FROM property_insert($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34)", o_id, o_street, o_zip, o_city, o_lat, o_lon, o_type, o_zone, z_zpid, z_links_home_details, z_links_comparables, z_address_street, z_zipcode, z_city, z_state, z_latitude, z_longitude, z_fips_county, z_use_code, z_tax_assessment_year, z_tax_assessment, z_year_built, z_lot_size_sqft, z_finished_sqft, z_bathrooms, z_bedrooms, z_total_rooms, z_last_sold_date_obj, z_last_sold_price_value, z_zestimate_amount_value, z_zestimate_lastupdated_obj, z_zestimate_low_value, z_zestimate_high_value, z_zestimate_percentile)

			if err != nil {
	      log.Printf("Error in property_insert: %v o_id: %v", err, o_id)
	    }
		}

  }
}

func queryAPI(address string, zipcode string, id int64) ([]Results) {
	key := "my_secret_key"
	address = address
	zipcode = zipcode
	id = id
	// QueryEscape escapes the key string so
	// it can be safely placed inside a URL query
	escapedKey := url.QueryEscape(key)
	escapedAddress := url.QueryEscape(address)
	escapedZipcode := url.QueryEscape(zipcode)

	url := fmt.Sprintf("http://www.zillow.com/webservice/GetDeepSearchResults.htm?zws-id=%s&address=%s&citystatezip=%s", escapedKey, escapedAddress, escapedZipcode)

	// Build the request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("NewRequest: ", err)
		// return
	}

	// For control over HTTP client headers,
	// redirect policy, and other settings,
	// create a Client
	// A Client is an HTTP client
	client := &http.Client{}

	// Send the request via a client
	// Do sends an HTTP request and
	// returns an HTTP response
	res, err := client.Do(req)
	if err != nil {
		log.Fatal("Do: ", err)
		// return
	}

	// Callers should close resp.Body
	// when done reading from it
	// Defer the closing of the body
	defer res.Body.Close()

	// fill record with results
	record := SearchResults{}
	var results []Results

	// parse the XML
	if err := xml.NewDecoder(res.Body).Decode(&record); err != nil {
		fmt.Printf("Error: %v %s", err, address)
		log.Printf("Error importing: %v, o_id: (%v), address: %v\n", err, id, address)
	} else if len(record.Results) != 0 {
		results = record.Results
	}
	return results
}

// setup Zillow's Deep Search data model
type Links struct {
  XMLName xml.Name `xml:"links"`

  HomeDetails   string `xml:"homedetails"`
  GraphsAndData string `xml:"graphsanddata"`
  MapThisHome   string `xml:"mapthishome"`
  Comparables   string `xml:"comparables"`
}

type Address struct {
  Street    string `xml:"street"`
  Zipcode   string `xml:"zipcode"`
  City      string `xml:"city"`
  State     string `xml:"state"`
  Latitude  string `xml:"latitude"`
  Longitude string `xml:"longitude"`
}

type Value struct {
  Currency string `xml:"currency,attr"`
  Value    int    `xml:",chardata"`
}

type Zestimate struct {
  Amount      Value  `xml:"amount"`
  LastUpdated string `xml:"last-updated"`
  // TODO(pedge): fix
  //ValueChange ValueChange `xml:"valueChange"`
  Low        Value  `xml:"valuationRange>low"`
  High       Value  `xml:"valuationRange>high"`
  Percentile string `xml:"percentile"`
}

type RealEstateRegion struct {
  XMLName xml.Name `xml:"region"`

  ID                  string  `xml:"id,attr"`
  Type                string  `xml:"type,attr"`
  Name                string  `xml:"name,attr"`
  ZIndex              string  `xml:"zindexValue"`
  ZIndexOneYearChange float64 `xml:"zindexOneYearChange"`
  // Links
  Overview       string `xml:"links>overview"`
  ForSaleByOwner string `xml:"links>forSaleByOwner"`
  ForSale        string `xml:"links>forSale"`
}

type Result struct {
  XMLName xml.Name `xml:"result"`

  Zpid              string             `xml:"zpid"`
  Links             Links              `xml:"links"`
  Address           Address            `xml:"address"`
  FIPSCounty        string             `xml:"FIPScounty"`
  UseCode           string             `xml:"useCode"`
  TaxAssessmentYear int                `xml:"taxAssessmentYear"`
  TaxAssessment     float64            `xml:"taxAssessment"`
  YearBuilt         int                `xml:"yearBuilt"`
  LotSizeSqFt       int                `xml:"lotSizeSqFt"`
  FinishedSqFt      int                `xml:"finishedSqFt"`
  Bathrooms         float64            `xml:"bathrooms"`
  Bedrooms          int                `xml:"bedrooms"`
	TotalRooms        int                `xml:"totalRooms"`
  LastSoldDate      string             `xml:"lastSoldDate"`
  LastSoldPrice     Value              `xml:"lastSoldPrice"`
  Zestimate         Zestimate          `xml:"zestimate"`
  LocalRealEstate   []RealEstateRegion `xml:"localRealEstate>region"`
}

type SearchRequest struct {
  Address       string `xml:"address"`
  CityStateZip  string `xml:"citystatezip"`
  Rentzestimate bool   `xml:"rentzestimate"`
}

type Message struct {
  Text         string `xml:"text"`
  Code         int    `xml:"code"`
  LimitWarning bool   `xml:"limit-warning"`
}

type Results struct {
	Result Result `xml:"result"`
}

type SearchResults struct {
  XMLName xml.Name `xml:"searchresults"`

  Request SearchRequest `xml:"request"`
  Message Message       `xml:"message"`
  Results []Results `xml:"response>results"`
}
