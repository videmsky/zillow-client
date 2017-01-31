package main

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
)

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

func main() {
	key := "my secret key"
	address := "13800 OLD MORRO RD"
	zipcode := "93422"
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
		return
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
		return
	}

	// Callers should close resp.Body
	// when done reading from it
	// Defer the closing of the body
	defer res.Body.Close()

	// fill record with results
	record := SearchResults{}

	// parse the XML
	if err := xml.NewDecoder(res.Body).Decode(&record); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	} else if len(record.Results) != 0 {
		fmt.Println("Results:", record.Results[0])
	}
}
