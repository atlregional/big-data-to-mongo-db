# big-data-to-mongo-db

## run queries
- with query-configs/example.json
```
  node QueryBigData.js -useConfig example.json
```
- with command-line arguments
```
  node QueryBigData.js 'databaseKey' 'collection' 'queryField' 'queryValue' 'outputFilename'.json 
```

## set query-config parameters

```
  {
    "databaseKey" : "ZTrans",
    "collection" : "Main",
    "outputFilename" : "FultonTransactions2020"
  }
```

### Two query types:

- using an object

  ``` 
    "queryObj" : {
      // string or array
      "County": "FULTON", 
      // $in query with array of /strings/
      "DocumentDate": { 
        "type" : "in",
        "values" : [
          "2020",
          "2019"
        ]
      }, 
      // $regex query with a single /string/
      "AssessmentLandUseStndCode": { 
        "type" : "regex",
        "values" : "RR"
      }
    }
  }
  ```

- using and array 

  ```
  "queryArray" : [
    {
      "ImportParcelID": 38043716
    },
    {
      "ImportParcelID": 38095632
    }, 
    ...
  ]

  ```
- using post filter on joined data

  ```
    "postFilterCriteria" : {
      "collection": "Building",
      "field" : "PropertyLandUseStndCode",
      "values" :[
        "RR101",  
        "RR999",  
        "RR104",  
        "RR105",  
        "RR106",  
        "RR107",  
        "RR108",  
        "RR109",  
        "RR113",  
        "RR116",  
        "RR119",  
        "RR120"   
      ]
    }
  ```