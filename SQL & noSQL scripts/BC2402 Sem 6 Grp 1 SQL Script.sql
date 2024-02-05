# BC2402 S6 GROUP 1 Final Project

-- Q1
SELECT COUNT(DISTINCT VehicleClass) AS Number_of_Vehicle_Classes
FROM co2_emissions_canada;

-- Q2
SELECT 
	VehicleClass, Transmission,
	ROUND(AVG(EngineSize_L), 2) AS "Avg Engine Size (L)", 
    ROUND(AVG(FuelConsumptionCity_L_100km), 2) AS "Fuel Consumption City", 
    ROUND(AVG(FuelConsumptionHwy_L_100km), 2) AS "Fuel Consumption Highway",
    ROUND(AVG(CO2Emissions_g_km), 2) AS "CO2 emissions"
FROM co2_emissions_canada
GROUP BY VehicleClass, Transmission
ORDER BY VehicleClass, Transmission;

-- Q3
SELECT
        REPLACE(zip, UNHEX('C2A0'), '') AS modified_zip_numeric,
        EVNetwork, COUNT(*) as station_count
    FROM
        ev_stations_v1
    WHERE
        YEAR(STR_TO_DATE(DateLastConfirmed, '%m/%d/%Y')) BETWEEN 2010 AND 2022
        AND REPLACE(zip, UNHEX('C2A0'), '') REGEXP '^[0-9]+$'
    GROUP BY
        modified_zip_numeric, EVNetwork
	ORDER BY
		modified_zip_numeric;

-- Q4
SELECT 
	ZIP,
    COUNT(distinct stationname) AS NumberOfStations
FROM 
	ev_stations_v1
WHERE 
	CAST(Latitude AS DECIMAL(9,6)) BETWEEN 33.20 AND 34.70 AND
    CAST(Longitude AS DECIMAL(9,6)) BETWEEN -118.40 AND -117.20 AND
    Length(ZIP) >=5
GROUP BY
	ZIP;
    
-- Q5
SELECT
	State,
    Model,
    COUNT(*) AS NumberofTesla
FROM
	electric_vehicle_population
WHERE
	Make LIKE "%TESLA%"
GROUP BY
	State,
    Model
ORDER BY
	NumberofTesla DESC;
    
-- Q6
SELECT
	ElectricVehicleType,
    CleanAlternativeFuelVehicleEligibility,
    AVG(ElectricRange)
FROM
	electric_vehicle_population
GROUP BY
	ElectricVehicleType,
    CleanAlternativeFuelVehicleEligibility
ORDER BY
	ElectricVehicleType,
    CleanAlternativeFuelVehicleEligibility;

-- Q7
WITH co2_emissions_subquery AS (
    SELECT Make, Model, format(AVG(CO2Emissions_g_km), 4) as Average_CO2Emissions
    FROM co2_emissions_canada
    GROUP BY Make, Model
    ORDER BY Make, Model
)

SELECT State, e.Make, e.Model, COUNT(distinct e.VIN) AS Number_of_Vehicles, Average_CO2Emissions
FROM electric_vehicle_population e
JOIN co2_emissions_subquery c ON e.model = c.Model
GROUP BY e.Make, e.Model, State, Average_CO2Emissions
ORDER BY Make, Model, State, Number_of_Vehicles DESC;

-- Q8
# Q8a
-- For each state, display the number of electric vehicles, the number of EV stations, and the vehicle:station ratio in descending ratio order.
WITH EVcount AS (
	SELECT state, COUNT(*) AS EV_Count FROM electric_vehicle_population
	GROUP BY state),
EVstationCount AS (
	SELECT state, COUNT(*) AS Station_Count FROM ev_stations_v1
	GROUP BY state)
SELECT 
	s.state, COALESCE(c.EV_Count, 0), s.Station_Count,  
    CONCAT(ROUND(COALESCE(c.EV_Count, 0)/Station_Count, 2), " : 1") AS ratio 
FROM EVstationCount s 
LEFT JOIN EVCount c ON s.state = c.state
ORDER BY (EV_Count/Station_Count) DESC;


#8B
-- For each postalcode(zip), display the number of electric vehicles, the number of EV stations, and the vehicle:station ratio in descending ratio order.

WITH EVstationCount_zip AS (
	SELECT REPLACE(ZIP, UNHEX('C2A0'), '') AS "modified_zip_numeric", COUNT(*) AS Station_Count_ZIP FROM ev_stations_v1
	GROUP BY modified_zip_numeric),
EVcount_zip AS (
	SELECT PostalCode, COUNT(*) AS EV_Count_ZIP FROM electric_vehicle_population
	GROUP BY PostalCode)
SELECT 
	s.modified_zip_numeric AS "ZIP", COALESCE(c.EV_Count_ZIP, 0) AS "EVCount_ZIP", s.Station_Count_ZIP, 
    concat(COALESCE(c.EV_Count_ZIP, 0)/s.Station_Count_ZIP, ":1") AS ratio_ZIP
FROM EVstationCount_zip s
LEFT JOIN EVCount_zip c ON s.modified_zip_numeric = c.PostalCode
ORDER BY (EV_Count_ZIP/Station_Count_ZIP) DESC;


-- Q9
select 
	naicsDescription,
    round(SUM(totalEmissions),2) AS totalemissions
FROM 
	nei_2017_full_data
WHERE 
	naicsDescription LIKE "%motor%" OR
    naicsDescription LIKE "%auto%"
GROUP BY
	naicsDescription;

-- Q10
SELECT state, companyName, ROUND(SUM(totalEmissions), 2) AS emissionsBySupplier 
FROM nei_2017_full_data 
WHERE 
    companyName IN (
        "DANA HOLDING CORP",
        "Emerson Electric Co.",
        "NUCOR CORPORATION",
        "Micron Technology Inc",
        "ALLEGHENY TECHNOLOGIES INC",
        "ALBEMARLE CORPORATION",
        "SCHNEIDER ELECTRIC"
    )
GROUP BY state, companyName
ORDER BY state, companyName;

-- Q11
#1)
WITH co2_emissions_subquery AS (
    SELECT Make, Model, AVG(CO2Emissions_g_km) AS Average_CO2Emissions
    FROM co2_emissions_canada
    GROUP BY Make, Model
    ORDER BY Make, Model
),
tb1 AS(
	SELECT e.ElectricVehicleType, COUNT(*) AS Number_of_Vehicles, Average_CO2Emissions, COUNT(*)*Average_CO2Emissions as "total_emissions"
	FROM electric_vehicle_population e
	JOIN co2_emissions_subquery c ON e.make = c.Make AND e.model = c.Model
	GROUP BY Average_CO2Emissions, e.ElectricVehicleType
)
SELECT ROUND((SUM(total_emissions)/SUM(Number_of_Vehicles)),2) as "avg_EV_emissions"
FROM tb1;

#2)
SELECT ROUND(AVG(CO2Emissions_g_km), 2) AS avg_general_emissions FROM co2_emissions_canada;

#3)
SELECT FuelType, ROUND(AVG(CO2Emissions_g_km), 2) AS "CO2_emissions_g/km" FROM co2_emissions_canada
GROUP BY FuelType
ORDER BY `CO2_emissions_g/km`;

#4)
WITH co2_emissions_subquery AS (
    SELECT Make, Model, AVG(CO2Emissions_g_km) AS Average_CO2Emissions
    FROM co2_emissions_canada
    GROUP BY Make, Model, FuelType
    ORDER BY Make, Model
),
tb1 AS(
	SELECT e.ElectricVehicleType, e.Make, e.Model, COUNT(*) AS Number_of_Vehicles, Average_CO2Emissions, COUNT(*)*Average_CO2Emissions AS "total_emissions"
	FROM electric_vehicle_population e
	JOIN co2_emissions_subquery c ON e.make = c.Make AND e.model = c.Model
	GROUP BY e.Make, e.Model, Average_CO2Emissions, e.ElectricVehicleType
	ORDER BY Make, Model
)
SELECT ElectricVehicleType, ROUND((SUM(total_emissions)/SUM(Number_of_Vehicles)),2) AS "avg_EV_emissions"
FROM tb1
GROUP BY ElectricVehicleType;

#5a)
WITH co2_emissions_subquery AS (
    SELECT VehicleClass, Make, Model, FuelType, AVG(CO2Emissions_g_km) as Average_CO2Emissions
    FROM co2_emissions_canada
    GROUP BY Make, Model, FuelType, VehicleClass
    ORDER BY Make, Model
),
tb1 AS(
	SELECT e.ElectricVehicleType, c.VehicleClass, e.Make, e.Model, COUNT(*) AS Number_of_Vehicles, Average_CO2Emissions, COUNT(*)*Average_CO2Emissions as "total_emissions"
	FROM electric_vehicle_population e
	JOIN co2_emissions_subquery c ON e.make = c.Make AND e.model = c.Model
	GROUP BY e.Make, e.Model, Average_CO2Emissions, VehicleClass, e.ElectricVehicleType
)
SELECT tb1.ElectricVehicleType, tb1.vehicleClass, SUM(total_emissions)/SUM(Number_of_Vehicles) AS "avg_EV_emissions"
FROM tb1
GROUP BY vehicleClass, ElectricVehicleType
ORDER BY avg_EV_emissions ASC;

#5b)
SELECT VehicleClass, ROUND(AVG(CO2Emissions_g_km), 2) AS "avg_general_emissions" FROM co2_emissions_canada
GROUP BY VehicleClass
ORDER BY avg_general_emissions ASC;

#6a)
WITH co2_emissions_subquery AS (
    SELECT VehicleClass, Make, Model, AVG(CO2Emissions_g_km) as Average_CO2Emissions
    FROM co2_emissions_canada
    GROUP BY Make, Model, VehicleClass
    ORDER BY Make, Model
),
tb1 AS(
	SELECT c.VehicleClass, e.Make, e.Model, COUNT(*) AS Number_of_Vehicles, Average_CO2Emissions, COUNT(*)*Average_CO2Emissions as "total_emissions"
	FROM electric_vehicle_population e
	JOIN co2_emissions_subquery c ON e.make = c.Make AND e.model = c.Model
	GROUP BY e.Make, e.Model, Average_CO2Emissions, VehicleClass
)
SELECT tb1.vehicleClass, SUM(total_emissions)/SUM(Number_of_Vehicles) as "avg_EV_emissions"
FROM tb1
GROUP BY vehicleClass
ORDER BY avg_EV_emissions ASC;

#6b)
SELECT FuelType, VehicleClass, ROUND(AVG(CO2Emissions_g_km), 2) AS "avg_altfuel_emissions" FROM co2_emissions_canada
WHERE FuelType = "E" or FuelType = "N"
GROUP BY VehicleClass, FuelType
ORDER BY avg_altfuel_emissions ASC;

-- Q12
#a). 
select modelyear, count(*) as number_of_EV_models from electric_vehicle_population 
group by modelyear
order by modelyear asc; 

#b). 
select vehicleclass, avg(co2emissions_g_km) from co2_emissions_canada 
group by vehicleclass
order by avg(co2emissions_g_km) desc; 

#c). 
 WITH co2_emissions_subquery AS (
    SELECT VehicleClass, Make, Model 
    FROM co2_emissions_canada
    GROUP BY Make, Model, VehicleClass
),
tb1 AS (
  SELECT
    c.VehicleClass,
    e.Make,
    e.Model,
    COUNT(*) AS Number_of_Vehicles
  FROM
    electric_vehicle_population e
  JOIN
    co2_emissions_subquery c ON e.make = c.Make AND e.model = c.Model
  GROUP BY
    e.Make,
    e.Model,
    c.VehicleClass
)
SELECT
  tb1.VehicleClass,
  SUM(tb1.Number_of_Vehicles) AS Total_Number_of_Vehicles
FROM
  tb1
GROUP BY
  tb1.VehicleClass
ORDER BY
  tb1.VehicleClass;


#d). 
SELECT
    esbs.state,
    ebs.num_of_EV_vehicles,
    esbs.num_of_EV_stations,
    ebs.num_of_EV_vehicles / esbs.num_of_EV_stations AS ratio
FROM (SELECT state, COUNT(*) AS num_of_EV_stations FROM ev_stations_v1
GROUP BY state
ORDER BY COUNT(*) DESC
    ) AS esbs
JOIN
    (SELECT state, COUNT(DISTINCT DOLVehicleID) AS num_of_EV_vehicles
       FROM electric_vehicle_population
        GROUP BY state
        ORDER BY
            COUNT(DISTINCT DOLVehicleID) DESC
    ) AS ebs ON esbs.state = ebs.state;


#e). 
select electricvehicletype, CleanAlternativeFuelVehicleEligibility, avg(electricrange), count(*) from electric_vehicle_population
where electricrange != 0 
group by  electricvehicletype, CleanAlternativeFuelVehicleEligibility
order by avg(electricrange) desc;


-- Q13

# Analyze the distribution of charging stations in different types of areas 
# (urban, suburban) in the US, which can inform similar distributions in Singapore.
SELECT 
    City, State, COUNT(*) as NumberOfStations
FROM 
    ev_stations_v1
GROUP BY 
    City, State
ORDER BY 
    NumberOfStations DESC;
    
# Based on facility type
SELECT FacilityType, COUNT(*) AS Number_of_Stations
FROM ev_stations_v1
GROUP BY FacilityType
ORDER BY Number_of_Stations DESC;

SELECT FacilityType, City, COUNT(*) AS Number_of_Stations
FROM ev_stations_v1
GROUP BY FacilityType, City
ORDER BY Number_of_Stations DESC;

-- Q14
#co2 emissions by sectors
SELECT 
  sector, 
  SUM(value) AS total_emissions,
  (SUM(value) / (SELECT SUM(value) FROM `table_name`)) * 100 AS percentage
FROM 
  `table_name`
GROUP BY 
  sector;

#renewable energy ratios
SELECT 
	`Year`,
    ROUND(`Other renewables excluding bioenergy (TWh)` +
          `Electricity from nuclear (TWh)` +
          `Electricity from solar (TWh)` +
          `Electricity from wind (TWh)` +
          `Electricity from hydro (TWh)`) AS lowcarbonemission_sources,
    ROUND(`Electricity from bioenergy (TWh)` +
          `Electricity from oil (TWh)` +
          `Electricity from gas (TWh)` +
          `Electricity from coal (TWh)`) AS highcarbonemission_sources,
    ROUND((`Other renewables excluding bioenergy (TWh)` +
           `Electricity from nuclear (TWh)` +
           `Electricity from solar (TWh)` +
           `Electricity from wind (TWh)` +
           `Electricity from hydro (TWh)`) /
          (`Other renewables excluding bioenergy (TWh)` +
           `Electricity from bioenergy (TWh)` +
           `Electricity from solar (TWh)` +
           `Electricity from wind (TWh)` +
           `Electricity from hydro (TWh)` +
           `Electricity from nuclear (TWh)` +
           `Electricity from oil (TWh)` +
           `Electricity from gas (TWh)` +
           `Electricity from coal (TWh)`), 2) AS low_carbonemission_ratio
FROM 
    electricity_production
WHERE 
    Entity = 'World'
ORDER BY 
    year;
    
# Global ratios of high electrical sources of CO2 emissions to low emissions electrical sources  

SELECT 
	`Year`,
    ROUND(`Other renewables excluding bioenergy (TWh)` +
          `Electricity from nuclear (TWh)` +
          `Electricity from solar (TWh)` +
          `Electricity from wind (TWh)` +
          `Electricity from hydro (TWh)`) AS lowcarbonemission_sources,
    ROUND(`Electricity from bioenergy (TWh)` +
          `Electricity from oil (TWh)` +
          `Electricity from gas (TWh)` +
          `Electricity from coal (TWh)`) AS highcarbonemission_sources,
    ROUND((`Other renewables excluding bioenergy (TWh)` +
           `Electricity from nuclear (TWh)` +
           `Electricity from solar (TWh)` +
           `Electricity from wind (TWh)` +
           `Electricity from hydro (TWh)`) /
          (`Other renewables excluding bioenergy (TWh)` +
           `Electricity from bioenergy (TWh)` +
           `Electricity from solar (TWh)` +
           `Electricity from wind (TWh)` +
           `Electricity from hydro (TWh)` +
           `Electricity from nuclear (TWh)` +
           `Electricity from oil (TWh)` +
           `Electricity from gas (TWh)` +
           `Electricity from coal (TWh)`), 2) AS low_carbonemission_ratio
FROM 
    electricity_production
WHERE 
    Entity = 'World'
ORDER BY 
    Year;

# France’s electricity production from renewable energy
select entity, year,  `electricity from nuclear (TWh)`, `Electricity from hydro (TWh)` from electricity_production where entity = 'France' and `year` = 2022;


#India’s electricity production from renewable energy
select entity, year,  `electricity from nuclear (TWh)`, `Electricity from hydro (TWh)` from electricity_production where entity = 'India' and `year` = 2022;

# CO2 Emissions in Ground Transport and Power sectors
SELECT country, 
       SUM(CASE WHEN sector = 'Ground Transport' THEN value ELSE 0 END) AS ground_transport_emissions,
       SUM(CASE WHEN sector = 'Power' THEN value ELSE 0 END) AS power_sector_emissions
FROM table_name
WHERE sector IN ('Ground Transport', 'Power')
GROUP BY country;

# Saudi Arabia’s fossil fuel percentage
select * from electricity_production where entity = 'saudi arabia' and year = 1995;

# Brazil’s hydroenergy percentage in 2022
select * from electricity_production where entity = 'brazil' and year = 1995;
