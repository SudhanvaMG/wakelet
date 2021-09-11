/*
A basic server which as mentioned calls the nasa events api to get
random events on startup
I have set up 2 endpoints -
	1. To get events ordered by title
	2. To get events ordered by datetime
To keep it simple, insted of taking inputs from user or post request to query data
I have set up the above 2 endpoints as the query alternatives.
The server is listening on port 80 and the dynamodb is on port 8000.
No need to run this file, the docker file has been set up to build the server
and start it during the docker build itself.
*/
package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gorilla/mux"
)

var dynamo *dynamodb.DynamoDB

/*
DB schema
I have kept the schema very simple, have created the fields which are used for the queries
alternative schema -
type GeoEvent struct {
	date string
	coordinates [2]int
}
type Event struct {
	id string
	title string
	link string
	geometry []GeoEvent
}
type Data struct {
	title string
	link string
	events []Event
}

I have used date as a string as dynamodb does sort it if stored as a string
I tried with date time object of golang and had a bit of roadblock while
converting the golang datetime object to dynamodb datetime object.
*/
type Event struct {
	id    string // The id here is the title of the returned data
	title string // This is the title of each event
	date  string // Date from each event in geometry
}

const TABLE_NAME = "events"

var result map[string]interface{}

func init() {
	dynamo = connectDynamo()
}

//connecting to local dynamo hosted on docker
func connectDynamo() (db *dynamodb.DynamoDB) {
	return dynamodb.New(session.New(), &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://dynamodb-local:8000")})
}

/*
This function is used to create the table to store events
The dynamodb docs mentioned that each primary key can be split into
a partition key and a sort key. The partition keys are used to store the data
in the same partition to increase read speed.
First i tried with id of each event as the primary key, but the problem
occured is that when calling the query api, to use a sort key we need to
specify a condition for partition key, thus i couldnt find a way to
use sort on all events as the partition key was different. Thus i ended up using
the title of the data as the partition key which allowed me to sort all events
I am sure there is a way in which i can design the table so that sort can be
applied to different events ids together, as this was my first time using dynamodb
i went with this format.

I have set title and date as sort keys as it allows us to order the data
based on these fields. As we can set only one sort key for the main table
i used the local secondary index to set the sort key for the other field.
*/
func CreateTable() error {
	_, err := dynamo.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("title"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("date"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("title"),
				KeyType:       aws.String("RANGE"),
			},
		},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{
			{
				IndexName: aws.String("date"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("id"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("date"),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(TABLE_NAME),
	})
	if err != nil {
		log.Printf(err.Error())
	}
	return err
}

/*
To insert an item into the events table
*/
func PutItem(event Event) error {
	_, err := dynamo.PutItem(&dynamodb.PutItemInput{
		//I wanted to use marshal function here, but
		// The marshal function was converting into empty output
		// Couldnt debug why that was hapenning.
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(event.id),
			},
			"title": {
				S: aws.String(event.title),
			},
			"date": {
				S: aws.String(event.date),
			},
		},
		TableName: aws.String(TABLE_NAME),
	})
	if err != nil {
		log.Println(err.Error())
	}
	return err
}

// api call to sort by title
func sortbytitle(w http.ResponseWriter, r *http.Request) {
	// fetch events ordered by title
	input := &dynamodb.QueryInput{
		TableName: aws.String("events"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":E": {
				S: aws.String("EONET Events"),
			},
		},
		KeyConditionExpression: aws.String("id = :E"),
		ScanIndexForward:       aws.Bool(true),
	}
	result, err := dynamo.Query(input)
	if err != nil {
		log.Println(err.Error())
	}
	/*
		i tried this first
		var m []Event
		dynamodbattribute.UnmarshalListOfMaps(result.Items, &m)

		which should have converted the query output into list of
		Events but the output was coming empty.
	*/
	var m []map[string]string
	dynamodbattribute.UnmarshalListOfMaps(result.Items, &m)

	// I couldnt convert slice of map into json to send as output
	// so just for this task i converted to bytes and wrote to page
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Events ordered in Ascending by title name \n"))
	for _, val := range m {
		strval := "title :- " + val["title"] + " \t date :-" + val["date"] + "\n"
		w.Write([]byte(strval))
	}

}

// Function to sort by date
func sortbydate(w http.ResponseWriter, r *http.Request) {
	// same as previous function, but here i query fro local secondary
	//  index using the index name
	input := &dynamodb.QueryInput{
		TableName: aws.String("events"),
		IndexName: aws.String("date"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":E": {
				S: aws.String("EONET Events"),
			},
		},
		KeyConditionExpression: aws.String("id = :E"),
		ScanIndexForward:       aws.Bool(true),
	}
	result, err := dynamo.Query(input)
	if err != nil {
		log.Println(err.Error())
	}
	// came across same porblem of empty output after unmarshal
	var m []map[string]string
	dynamodbattribute.UnmarshalListOfMaps(result.Items, &m)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Events ordered in Ascending by date time \n"))
	for _, val := range m {
		strval := "title :- " + val["title"] + " \t date :-" + val["date"] + "\n"
		w.Write([]byte(strval))
	}

}

// Main function, on startup calls the api and saves the events into db
func main() {
	//Router init
	CreateTable()
	r := mux.NewRouter()
	url := "https://eonet.sci.gsfc.nasa.gov/api/v3/events?limit=10"
	resp, _ := http.Get(url)
	body, _ := ioutil.ReadAll(resp.Body)

	json.Unmarshal(body, &result)
	// The interface was a new concept, i might have not used it properly here
	events := result["events"].([]interface{})
	// As explained title is used as id for partition key
	id := result["title"].(string)
	for _, value := range events {
		title := value.(map[string]interface{})["title"]
		geo := value.(map[string]interface{})["geometry"].([]interface{})
		for _, key := range geo {
			date := key.(map[string]interface{})["date"]
			var event Event = Event{
				id:    id,
				title: title.(string),
				date:  date.(string),
			}
			PutItem(event)
		}
	}
	// 2 APIs to query order by title or date
	r.HandleFunc("/title", sortbytitle).Methods("GET")
	r.HandleFunc("/date", sortbydate).Methods("GET")
	log.Fatal(http.ListenAndServe(":80", r))
}
