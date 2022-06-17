module.exports = (county, year) => 
  ({
    "databaseKey" : "ZTrans",
    "collection" : "Main",
    "outputFilename" : `transactions/${year}/${county}-Transactions${year}.json`, 
    "queryObj" : {
      "County" : county, 
      "DocumentDate" : { 
        "type" : "in",
        "values" : [
          year
        ]
      }, 
      "AssessmentLandUseStndCode" : { 
        "type" : "regex",
        "values" : "RR"
      }
    }
  }
)