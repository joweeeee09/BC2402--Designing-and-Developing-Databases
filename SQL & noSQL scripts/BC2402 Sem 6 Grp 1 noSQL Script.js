// Question 1 ---------------------------------------------------------

db.co2_emissions_canada.aggregate([
    {
        $group: {
            _id: "$VehicleClass"
    }},
    {
        $group: {
            _id: null,
            totalClasses: { $sum: 1 }
    }},
    {
        $project: {
            _id: 0,          // Exclude the _id field
            totalClasses: 1  // Include the totalClasses field
    }}
]);

// Question 2 ----------------------------------------------------------

db.co2_emissions_canada.aggregate([
    {$group: {_id:{groupByClass: "$VehicleClass", groupByTransmission: "$Transmission"},
        averageEngineSize: {$avg: {$toDouble: "$EngineSize_L"}},
        averageFuelCity: {$avg: {$toDouble: "$FuelConsumptionCity_L_100km"}},
        averageFuelHighway: {$avg: {$toDouble: "$FuelConsumptionHwy_L_100km"}},
        averageCO2Emission: {$avg: {$toDouble: {$trim: {
                            input: "$CO2Emissions_g_km",
                            chars: "\r"
                        }}}
    }}},
    {$project: {
        averageEngineSize: {$round: ["$averageEngineSize", 2]},
        averageFuelCity: {$round: ["$averageFuelCity", 2]},
        averageFuelHighway: {$round: ["$averageFuelHighway", 2]},
        averageCO2Emission: {$round: ["$averageCO2Emission", 2]}
    }},
    {$sort: {"_id.groupByClass": 1, "_id.groupByTransmission": 1}},
])

// Question 3 ----------------------------------------------------------

db.ev_stations_v1.aggregate([
  {
    $match: {
      DateLastConfirmed: { $ne: "" }
    }},
  {
    $project: {
      "DateLastConfirmed": {
        $convert: {
          input: "$DateLastConfirmed",
          to: "date"
    }},
      "ZIP": {
        $replaceAll: {
          input: "$ZIP",
          find: "\u00A0",  
          replacement: ""
    }},
      "EVNetwork": 1
    }},
  {
    $match: {
      "DateLastConfirmed": {
        $gte: ISODate("2012-01-01"),
        $lt: ISODate("2023-01-01")
    }}},
  {
    $match: {
      "ZIP": {
        $not: {
          $regex: "[A-Za-z]" 
    }}}},
  {
    $group: {
      _id: {
        ZIP: "$ZIP",
        EVNetwork: "$EVNetwork"
      },
      numberOfStations: { $sum: 1 }
    }},
  {
    $project: {
      "ZIP": {
        $convert: {
          input: "$_id.ZIP",
          to: "int",
        }
      },
      "EVNetwork": "$_id.EVNetwork",
      "numberOfStations": 1,
      "_id": 0  
    }},
  {
    $sort: {
      "ZIP": 1,  
      "EVNetwork": 1 
    }}
]);

// Question 4 ----------------------------------------------------------

db.ev_stations_v1.aggregate([
    {
        $addFields: {
            ConvertedLatitude: { $toDecimal: "$Latitude" },
            ConvertedLongitude: { $toDecimal: "$Longitude" }
        }
    },
    {
        $match: {
            ConvertedLatitude: {
                $gte: 33.20,
                $lte: 34.70
            },
            ConvertedLongitude: {
                $gte: -118.40,
                $lte: -117.20
            }
        }
    },
    {
        $group: {
            _id: "$ZIP",
            numberOfStations: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            ZIP: "$_id",
            numberOfStations: 1
        }
    }
]);

// Question 5 ----------------------------------------------------------

db.electric_vehicle_population.aggregate([
    {
        $match: {
            Make: { $regex: "TESLA", $options: "i" } // Case-insensitive search for 'TESLA'
        }
    },
    {
        $group: {
            _id: {
                State: "$State",
                Model: "$Model"
            },
            NumberofTesla: { $sum: 1 }
        }
    },
    {
        $sort: {
            NumberofTesla: -1 // Descending order
        }
    },
    {
        $project: {
            _id: 0,
            State: "$_id.State",
            Model: "$_id.Model",
            NumberofTesla: 1
        }
    }
]);

// Question 6 ----------------------------------------------------------

db.electric_vehicle_population.aggregate([
    {
        $group: {
            _id: {
                ElectricVehicleType: "$ElectricVehicleType",
                CleanAlternativeFuelVehicleEligibility: "$CleanAlternativeFuelVehicleEligibility"
            },
            averageElectricRange: { $avg: { $toInt: "$ElectricRange" } }
        }
    },
    {
        $project: {
            _id: 0,
            ElectricVehicleType: "$_id.ElectricVehicleType",
            CleanAlternativeFuelVehicleEligibility: "$_id.CleanAlternativeFuelVehicleEligibility",
            AverageElectricRange: "$averageElectricRange"
        }
    }
]);

// Question 7 ----------------------------------------------------------

db.co2_emissions_canada.aggregate([
    {
        $group: {
            _id: { Make: "$Make", Model: "$Model" },
            Average_CO2Emissions: { 
                $avg: { 
                    $toDouble: { 
                        $trim: {
                            input: "$CO2Emissions_g_km", 
                            chars: "\r" 
                        } 
                    } 
                } 
            }
        }
    },
    {
        $project: {
            _id: 0,
            Make: "$_id.Make",
            Model: "$_id.Model",
            Average_CO2Emissions: { $round: ["$Average_CO2Emissions", 4] }
        }
    },
    {
        $lookup: {
            from: "electric_vehicle_population",
            localField: "Model",
            foreignField: "Model",
            as: "vehicle_population"
        }
    },
    {
        $unwind: "$vehicle_population"
    },
    {
        $group: {
            _id: {
                State: "$vehicle_population.State",
                Make: "$Make",
                Model: "$Model",
                Average_CO2Emissions: "$Average_CO2Emissions"
            },
            VINs: { $addToSet: "$vehicle_population.VIN" }
        }
    },
    {
        $project: {
            _id: 0,
            State: "$_id.State",
            Make: "$_id.Make",
            Model: "$_id.Model",
            Number_of_Vehicles: { $size: "$VINs" },
            Average_CO2Emissions: "$_id.Average_CO2Emissions"
        }
    },
    {
        $sort: {
            Make: 1,
            Model: 1,
            State: 1,
            Number_of_Vehicles: -1
        }
    }
])

// Question 8 ----------------------------------------------------------

// (i)

db.ev_stations_v1.aggregate([
    {
        $group: {
            _id: "$State",
            Station_Count: {$sum: 1}
    }},
    {
        $lookup: {
            from: "electric_vehicle_population",
            localField: "_id",
            foreignField: "State",
            as: "EVCount"
    }},
    {
        $project: {
            state: "$_id",
            EV_Count: {$cond: {if: {$isArray: "$EVCount"}, then: {$size: "$EVCount"}, else: 0 }},
            Station_Count: 1,
            ratio: {
            $concat: [{$toString: {$round: [{$divide: [{$cond: {if: {$isArray: "$EVCount"}, then: {$size: "$EVCount"}, else: 0 }}, "$Station_Count"]}, 2]}}, 
                " : 1"]}}},
    {$sort: {ratio: -1}},
    {$project: {_id:0}}
    ])

// (ii)

db.evStationByZIP.aggregate([
    {
        $project: {
            modified_zip_numeric: {
                $replaceAll: {
                  input: "$groupByZIP",
                  find: "\u00A0",  
                  replacement: ""
    }},
             stationCount: 1
    }},
    {
        $group: {
            _id: {"ZIP": "$modified_zip_numeric"},
            stationCount: {$first: "$stationCount"}
        }
    },
    {
        $lookup: {
          from: "evByPostalCode",
          localField: "_id.ZIP",
          foreignField: "groupByPostalCode",
          as: "Data"
    }},
    {
        $unwind: {
          path: "$Data",
          preserveNullAndEmptyArrays: true
    }},
    {
        $project: {
            "_id": 0,
            ZIP: {$ifNull: ["$groupByZIP", "$Data.groupByPostalCode"] },
            "Station_Count": "$stationCount",
            "EV_Count": {$ifNull: ["$Data.vehCount", 0]},
            "ratio": {
              $concat: [{$toString: {$round: [{$divide: [{$ifNull: ["$Data.vehCount", 0]}, "$stationCount"]}, 2]}}, 
                " : 1"]
        },
            "sorting": {$round: [{$divide: [{$ifNull: ["$Data.vehCount", 0]}, "$stationCount"]}, 2]}}},
    {$sort: {sorting: -1}},
    {$project: {_id:0, ZIP:1, Station_Count:1, EV_Count:1, ratio:1}}
])

// Question 9 ----------------------------------------------------------

db.nei_2017_full_data.aggregate([
  {
    $match: {
      naicsDescription: { $regex: /auto|motor/i }
    }
  },
  {
    $project: {
      naicsDescription: 1,
      totalEmissions: {
        $convert: {
          input: "$totalEmissions",
          to: "double"
        }
      }
    }
  },
  {
    $group: {
      _id: "$naicsDescription",
      Sum_totalEmissions: { $sum: "$totalEmissions" }
    }
  },
  {
    $project: {
      _id: 1,
      Sum_totalEmissions: { $round: ["$Sum_totalEmissions", 2] }
    }
  }
]);

// Question 10 ----------------------------------------------------------

db.nei_2017_full_data.aggregate([
    {
    $project: {
      companyName: 1,
      state: 1,
      totalEmissions: {
        $cond: {
          if: { $eq: ["$emissionsUom", "TON"] },
          then: { $multiply: [{ $toDouble: "$totalEmissions" }, 2000] },
          else: { $toDouble: "$totalEmissions" }
        }
      }
    }
    },
    {
        $match: {
            $and: [
            {companyName: {
                $in: [
                    "DANA HOLDING CORP",
                    "EMERSON ELECTRIC CO.",
                    "NUCOR CORPORATION",
                    "MICRON TECHNOLOGY INC",
                    "ALLEGHENY TECHNOLOGIES INC",
                    "ALBEMARLE CORPORATION",
                    "SCHNEIDER ELECTRIC"
                ].map(name => new RegExp(`^${name}$`, 'i'))
            }
            }
            ]
        }
    },
    {
        $group: {
            _id: {
                state: "$state",
                companyName: "$companyName"
            },
            emissionsBySupplier: { $sum: { $toDouble: "$totalEmissions" } }
        }
    },
    {
        $project: {
            _id: 0,
            state: "$_id.state",
            companyName: "$_id.companyName",
            emissionsBySupplier: { $round: ["$emissionsBySupplier", 2] }
        }
    },
    {
        $sort: { state: 1 , companyName: 1 } 
    }
]);

// Question 11 ----------------------------------------------------------

// Average EV emissions (1)
db.electric_vehicle_population.aggregate([
    {
        $lookup: { 
            from: "co2_emissions_canada",
            localField: "Make",
            foreignField: "Make",
            as: "co2sub"
    }},
    {$unwind: "$co2sub"},
    {
        $match: { 
            $expr: {
                $and: [
                    {$eq: ["$Model", "$co2sub.Model"]},
                    {$eq: ["$Make", "$co2sub.Make"]}
    ]}}},
    {
        $group: {
            _id: {
                Make: "$co2sub.Make",
                Model: "$co2sub.Model",
            },
            Number_of_Vehicles: {$sum: 1},
            total_emissions: {$sum: { $multiply: [{$toDouble: {$trim: {input: "$co2sub.CO2Emissions_g_km" }}}, 1]}},
            ElectricVehicleType: {$first: "$ElectricVehicleType"}
    }},
    {
        $group: {
            _id: {
                ElectricVehicleType: "$ElectricVehicleType"
            },
            t_cars: { $sum: "$Number_of_Vehicles" },
            t_emissions: {$sum: "$total_emissions"}
    }},
    {
        $group: {
            _id: null,
            avg_EV_emissions: {$avg: {$divide: ["$t_emissions", "$t_cars"]}
                
    }}},
    {$project: {_id:0}}
]);

// Average vehicle emission as a whole (2)
db.co2_emissions_canada.aggregate([
    {
        $project: {
            CO2Emissions_g_km: {
                $toDouble: {
                    $replaceAll: {
                        input: "$CO2Emissions_g_km",
                        find: "\r",
                        replacement: ""
    }}}}},
    {
        $group: {
            _id: null,
            avg_general_emissions: { $avg: "$CO2Emissions_g_km" }
    }},
    {
        $project: {
            _id: 0,
            avg_general_emissions: { $round: ["$avg_general_emissions", 2] }
}}])

// Average emissions by FuelType (3)
db.co2_emissions_canada.aggregate([
    {
        $project: {
            FuelType: 1,
            CO2Emissions_g_km: {
                $toDouble: {
                    $replaceAll: {
                        input: "$CO2Emissions_g_km",
                        find: "\r",
                        replacement: ""
    }}}}},
    {
        $group: {
            _id: {groupbyFuelType: "$FuelType"},
            avg_general_emissions: { $avg: "$CO2Emissions_g_km" }
    }},
    {$sort: {avg_general_emissions: 1}},
    {
        $project: {
            avg_general_emissions: { $round: ["$avg_general_emissions", 2] }
}}])

// Average EV emissions by ElectricVehicleType (4)
db.electric_vehicle_population.aggregate([
    {
        $lookup: {
            from: "co2_emissions_canada",
            let: {make: "$Make", model: "$Model"},
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$Make", "$$make"]},
                                {$eq: ["$Model", "$$model"]}
                ]}}},
                {
                    $group: {
                        _id: {Make: "$Make", Model: "$Model"},
                        Average_CO2Emissions: {$avg: {$toDouble: {$trim: {input: "$CO2Emissions_g_km"}}}},
                        ElectricVehicleType: {$first: "$ElectricVehicleType"}
            }}],
            as: "co2_emissions_subquery"
    }},
    {$unwind: "$co2_emissions_subquery"},
    {
        $group: {
            _id: {
                ElectricVehicleType: "$ElectricVehicleType",
                Make: "$make",
                Model: "$model",
            },
            Number_of_Vehicles: {$sum: 1},
            total_emissions: {$sum: {$multiply: ["$co2_emissions_subquery.Average_CO2Emissions", 1]}}
    }},
    {
        $group: {
            _id: "$_id.ElectricVehicleType",
            avg_EV_emissions: {$avg: {$divide: ["$total_emissions", "$Number_of_Vehicles"]}} 
            
    }},
    {
        $project: {
            ElectricVehicleType: "$_id",
            avg_EV_emissions: {$round: ["$avg_EV_emissions", 2]},
            _id: 0
    }}
])

// Average EV emissions by ElectricVehicleType and VehicleClass (5a)
db.electric_vehicle_population.aggregate([
    {
        $lookup: {
            from: "co2_emissions_canada",
            let: {make: "$Make", model: "$Model"},
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$Make", "$$make"]},
                                {$eq: ["$Model", "$$model"]}
                ]}}},
                {
                    $group: {
                        _id: {Make: "$Make", Model: "$Model", ElectricVehicleType: "$ElectricVehicleType"},
                        Average_CO2Emissions: {$avg: {$toDouble: {$trim: {input: "$CO2Emissions_g_km"}}}},
                        VehicleClass: {$first: "$VehicleClass"} 
            }}],
            as: "co2_emissions_subquery"
    }},
    {$unwind: "$co2_emissions_subquery"},
    {
        $group: {
            _id: {
                ElectricVehicleType: "$ElectricVehicleType",
                VehicleClass: "$co2_emissions_subquery.VehicleClass",
                Make: "$make",
                Model: "$model",
            },
            Number_of_Vehicles: {$sum: 1},
            total_emissions: {$sum: {$multiply: ["$co2_emissions_subquery.Average_CO2Emissions", 1]}}
    }},
    {
        $group: {
            _id:{
                VehicleClass: "$_id.VehicleClass",
                ElectricVehicleType: "$_id.ElectricVehicleType"
            },
            t_cars: { $sum: "$Number_of_Vehicles" },
            t_emissions: {$sum: "$total_emissions"}
    }},
    {
        $group: {
            _id: {
                VehicleClass: "$_id.VehicleClass",
                ElectricVehicleType: "$_id.ElectricVehicleType"
            },
            avg_EV_emissions: {
                $avg: {$divide: ["$t_emissions", "$t_cars"]
    }}}},
    {$sort: {avg_EV_emissions: 1}},
    {$project: {_id:1, avg_EV_emissions:1}}
])

// Average general vehicle emission by VehicleClass (5b)
db.co2_emissions_canada.aggregate([
    {
        $project: {
            VehicleClass: 1,
            CO2Emissions_g_km: {
                $toDouble: {
                    $replaceAll: {
                        input: "$CO2Emissions_g_km",
                        find: "\r",
                        replacement: ""
    }}}}},
    {
        $group: {
            _id: {groupbyVehicleClass: "$VehicleClass"},
            avg_general_emissions: { $avg: "$CO2Emissions_g_km" }
    }},
    {$sort: {avg_general_emissions: 1}},
    {
        $project: {
            avg_general_emissions: { $round: ["$avg_general_emissions", 2] }
}}])

// Average EV emissions by VehicleClass (6a)
db.electric_vehicle_population.aggregate([
    {
        $lookup: {
            from: "co2_emissions_canada",
            let: {make: "$Make", model: "$Model"},
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$Make", "$$make"]},
                                {$eq: ["$Model", "$$model"]}
                ]}}},
                {
                    $group: {
                        _id: {Make: "$Make", Model: "$Model"},
                        Average_CO2Emissions: {$avg: {$toDouble: {$trim: {input: "$CO2Emissions_g_km"}}}},
                        VehicleClass: {$first: "$VehicleClass"}
            }}],
            as: "co2_emissions_subquery"
    }},
    {$unwind: "$co2_emissions_subquery"},
    {
        $group: {
            _id: {
                VehicleClass: "$co2_emissions_subquery.VehicleClass",
                Make: "$make",
                Model: "$model",
            },
            Number_of_Vehicles: {$sum: 1},
            total_emissions: {$sum: {$multiply: ["$co2_emissions_subquery.Average_CO2Emissions", 1]}}
    }},
    {
        $group: {
            _id: "$_id.VehicleClass",
            avg_EV_emissions: {$avg: {$divide: ["$total_emissions", "$Number_of_Vehicles"]}} 
            
    }},
    {
        $project: {
            VehicleClass: "$_id",
            avg_EV_emissions: {$round: ["$avg_EV_emissions", 2]},
            _id: 0
    }}
])

// Average alt fuel vehicle emissions by VehicleClass and FuelType (6b)
db.co2_emissions_canada.aggregate([
    {
        $match: {FuelType: {$in: ["E", "N"]}},
    },
    {
        $project: {
            FuelType: 1,
            VehicleClass: 1,
            CO2Emissions_g_km: {
                $toDouble: {
                    $replaceAll: {
                        input: "$CO2Emissions_g_km",
                        find: "\r",
                        replacement: ""
    }}}}},
    {
        $group: {
            _id: {groupbyVehicleClass: "$VehicleClass", 
                groupbyFuelType: "$FuelType"
            },
            avg_general_emissions: { $avg: "$CO2Emissions_g_km" }
    }},
    {$sort: {avg_general_emissions: 1}},
    {
        $project: {
            avg_general_emissions: { $round: ["$avg_general_emissions", 2] }
}}])

// Question 12 ----------------------------------------------------------

// a
db.electric_vehicle_population.aggregate([
    {
        $group: {
            _id: "$ModelYear",
            number_of_EV_models: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            ModelYear: "$_id",
            number_of_EV_models: 1
        }
    },
    {
        $sort: {
            ModelYear: 1
        }
    }
])

// b
db.co2_emissions_canada.aggregate([
    {
        $match: {
            CO2Emissions_g_km: { $exists: true, $ne: null, $type: "string" },
            $expr: { $gt: [{ $toDouble: { $trim: { input: "$CO2Emissions_g_km", chars: "\r" } } }, 0] }
        }
    },
    {
        $group: {
            _id: "$VehicleClass",
            average_CO2Emissions: { $avg: { $toDouble: { $trim: { input: "$CO2Emissions_g_km", chars: "\r" } } } }
        }
    },
    {
        $project: {
            _id: 0,
            vehicleclass: "$_id",
            average_CO2Emissions: { $round: ["$average_CO2Emissions", 4] }
        }
    },
    {
        $sort: {
            average_CO2Emissions: -1
        }
    }
])

// c
db.electric_vehicle_population.aggregate([
    {
        $lookup: {
            from: "co2_emissions_canada",
            let: {make: "$Make", model: "$Model"},
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$Make", "$$make"]},
                                {$eq: ["$Model", "$$model"]}
                ]}}}],
            as: "co2_emissions_subquery"
    }},
    {$unwind: "$co2_emissions_subquery"},
    {
        $group: {
            _id: {
                VehicleClass: "$co2_emissions_subquery.VehicleClass"
            },
            Number_of_Vehicles: {$sum: 1},
    }}, 
    {$limit: 1000} 
])


// d
db.ev_stations_v1.aggregate([
  {
    $group: {
      _id: "$State",
      Station_Count: {$sum: 1}
    }
  },
  {
    $lookup: {
      from: "electric_vehicle_population",
      localField: "_id",
      foreignField: "State",
      as: "EVCount"
    }},
  {
    $project: {
      state: "$_id",
      EV_Count: {$cond: {if: {$isArray: "$EVCount"}, then: {$size: "$EVCount"}, else: 0 }},
      Station_Count: 1,
      ratio: {
        $concat: [{$toString: {$round: [{$divide: [{$cond: {if: {$isArray: "$EVCount"}, then: {$size: "$EVCount"}, else: 0 }}, "$Station_Count"]}, 2]}}, 
            " : 1"]}}},
  {$sort: {ratio: -1}},
  {$project: {_id:0}}
])

// e
db.electric_vehicle_population.aggregate([
    {
        $group: {
        _id: {ElectricVehicleType: "$ElectricVehicleType", CleanAlternativeFuelVehicleEligibility: "$CleanAlternativeFuelVehicleEligibility"},
        AvgElectricRange: {$avg: {$toDouble: "$ElectricRange"}},
        Count: {$sum: 1}
    }},
    {
        $match: {
            AvgElectricRange: {$ne: 0}
         }
    }
    ])

// Question 13 ----------------------------------------------------------

// Analyze the distribution of charging stations in different types of areas 
// (urban, suburban) in the US, which can inform similar distributions in Singapore.
db.ev_stations_v1.aggregate([
    {
        $group: {
            _id: {
                City: "$City",
                State: "$State"
            },
            NumberOfStations: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            City: "$_id.City",
            State: "$_id.State",
            NumberOfStations: 1
        }
    },
    { $sort: { NumberOfStations: -1 } }
]);

// Based on facility type
db.ev_stations_v1.aggregate([
    {
        $group: {
            _id: "$FacilityType",
            Number_of_Stations: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            FacilityType: "$_id",
            Number_of_Stations: 1
        }
    },
    { $sort: { Number_of_Stations: -1 } }
]);

db.ev_stations_v1.aggregate([
    {
        $group: {
            _id: {
                FacilityType: "$FacilityType",
                City: "$City"
            },
            Number_of_Stations: { $sum: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            FacilityType: "$_id.FacilityType",
            City: "$_id.City",
            Number_of_Stations: 1
        }
    },
    { $sort: { Number_of_Stations: -1 } }
]);

// Question 14 ----------------------------------------------------------

// co2 emissions by sector
db.q14_co2_emissions.aggregate([
  {
    $group: {
      _id: "$sector",
      total_emissions: { $sum: "$value" }
    }
  },
  {
    $lookup: {
      from: "q14_co2_emissions",
      pipeline: [
        {
          $group: {
            _id: null,
            total: { $sum: "$value" }
          }
        }
      ],
      as: "total"
    }
  },
  {
    $unwind: "$total"
  },
  {
    $project: {
      sector: "$_id",
      total_emissions: 1,
      percentage: {
        $multiply: [
          { $divide: ["$total_emissions", "$total.total"] },
          100
        ]
      }
    }
  },
  {
    $sort: { _id: 1 }
  }
])

// renewable energy ratios
db.q14_electricity_production.aggregate([
  { 
    $match: { Entity: 'World' } // Filtering documents for 'World' entity
  },
  {
    $project: {
      _id: 0, // Exclude the default MongoDB _id field from the output
      Year: 1,
      Renewable: {
        $add: [
          "$Other renewables excluding bioenergy (TWh) ",
          "$Electricity from bioenergy (TWh)",
          "$Electricity from solar (TWh)",
          "$Electricity from wind (TWh)",
          "$Electricity from hydro (TWh) "
        ]
      },
      NonRenewable: {
        $add: [
          "$Electricity from nuclear (TWh) ",
          "$Electricity from oil (TWh) ",
          "$Electricity from gas (TWh) ", 
          "$Electricity from coal (TWh) "
        ]
      },
    }
  },
  { 
    $addFields: { // Add a new field 'RenewableRatio'
      RenewableRatio: {
        $divide: [
          "$Renewable",
          { $add: ["$Renewable", "$NonRenewable"] }
        ]
      }
    }
  },
  { 
    $sort: { Year: 1 } // Sorting results by Year in ascending order
  }
])

// Global ratios of high electrical sources of CO2 emissions to low emissions electrical sources  
db.q14_electricity_production.aggregate([
  { 
    $match: { Entity: 'World' } // Filtering documents for 'World' entity
  },
  {
    $project: {
      _id: 0, // Exclude the default MongoDB _id field from the output
      Year: 1,
      lowcarbonemission_sources: {
        $add: [
          "$Other renewables excluding bioenergy (TWh) ",
          "$Electricity from solar (TWh)",
          "$Electricity from wind (TWh)",
          "$Electricity from hydro (TWh) ",
           "$Electricity from nuclear (TWh) ",
        ]
      },
      highcarbonemission_sources: {
        $add: [
          "$Electricity from oil (TWh) ",
          "$Electricity from gas (TWh) ", 
          "$Electricity from coal (TWh) ",
          "$Electricity from bioenergy (TWh)",
        ]
      },
    }
  },
  { 
    $addFields: { // Add a new field 'RenewableRatio'
      low_carbonemission_ratio: {
        $divide: [
          "$lowcarbonemission_sources",
          { $add: ["$lowcarbonemission_sources", "$highcarbonemission_sources"] }
        ]
      }
    }
  },
  { 
    $sort: { Year: 1 } // Sorting results by Year in ascending order
  }
])

// France’s electricity production from renewable energy
db.q14_electricity_production.find(
  {
    "Entity": "France",
    "Year": 2022
  },
  {
    "Entity": 1,
    "Year": 1,
    "Electricity from nuclear (TWh) ": 1,
    "Electricity from hydro (TWh) ": 1
  }
)

//India’s electricity production from renewable energy
db.q14_electricity_production.find(
  {
    "Entity": "India",
    "Year": 2022
  },
  {
    "Entity": 1,
    "Year": 1,
    "Electricity from nuclear (TWh) ": 1,
    "Electricity from hydro (TWh) ": 1
  }
)

// CO2 Emissions in Ground Transport and Power sectors
db.q14_co2_emissions.aggregate([
  {
    $match: {
      sector: { $in: ['Ground Transport', 'Power'] }
    }
  },
  {
    $group: {
      _id: "$country",
      ground_transport_emissions: { 
        $sum: { 
          $cond: [{ $eq: ["$sector", "Ground Transport"] }, "$value", 0] 
        }
      },
      power_sector_emissions: { 
        $sum: { 
          $cond: [{ $eq: ["$sector", "Power"] }, "$value", 0] 
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      country: "$_id",
      ground_transport_emissions: 1,
      power_sector_emissions: 1
    }
  }
]);

// saudi arabia’s fossil fuel percentage
db.q14_electricity_production.find({ "Entity": "Saudi Arabia", "Year": 1995 })

// Brazil’s hydroenergy percentage in 2022
db.q14_electricity_production.find({ "Entity": "Brazil", "Year": 2022 })
