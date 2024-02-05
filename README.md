# BC2402- Designing and Developing Databases

## Group Project: The Era of Global Boiling has arrived

### Introduction:
#### Background
The following content is extracted from a news article published in The Guardian.
Ajit Niranjan (Thu 27 Jul 2023, 14.31 BST), ‘Era of global boiling has arrived’, says UN chief as July set to be hottest month on record. Retrieved from https://www.theguardian.com/science/2023/jul/27/scientists-july-world-hottest-month-record-climate-temperatures

The era of global warming has ended and “the era of global boiling has arrived”, the UN secretary general, António Guterres, has said after scientists confirmed July was on track to be the world’s hottest month on record.

“Climate change is here. It is terrifying. And it is just the beginning,” Guterres said. “It is still possible to limit global temperature rise to 1.5C [above pre-industrial levels] and avoid the very worst of climate change. But only with dramatic, immediate climate action.”

Guterres’s comments came after scientists confirmed on Thursday that the past three weeks have been the hottest since records began and July is on track to be the hottest month ever recorded.

Global temperatures this month have shattered records, according to the World Meteorological Organization (WMO) and the EU’s Copernicus Earth observation programme, stoked by the burning of fossil fuels and spurring violent weather.

The steady rise in global average temperatures, driven by pollution that traps sunlight and acts like a greenhouse around the Earth, has made weather extremes worse.

“Humanity is in the hot seat,” Guterres told a press conference on Thursday. “For vast parts of North America, Asia, Africa, and Europe, it is a cruel summer. For the entire planet, it is a disaster. And for scientists, it is unequivocal – humans are to blame.

“All this is entirely consistent with predictions and repeated warnings. The only surprise is the speed of the change. Climate change is here, it is terrifying, and it is just the beginning. The era of global warming has ended; the era of global boiling has arrived.”

Guterres urged politicians to take swift action. “The air is unbreathable, the heat is unbearable, and the level of fossil fuel profits and climate inaction is unacceptable. Leaders must lead. No more hesitancy, no more excuses, no more waiting for others to move first. There is simply no more time for that.

### Dataset:
In this project, we utilized two sources of data:
1.	Electric Vehicle Charging Stations
retrieved from https://www.kaggle.com/datasets/prasertk/electric-vehicle-charging-stations-in-usa
2.	Electric Vehicle Population
retrieved from https://www.kaggle.com/datasets/ssarkar445/electric-vehicle-population
3.	CO2 Emission by Vehicles
retrieved from https://www.kaggle.com/datasets/debajyotipodder/co2-emission-by-vehicles?resource=download
4.	US National Emissions Inventory (NEI) 2017
Retrieved from https://www.kaggle.com/datasets/xaviernogueira/us-national-emissions-inventory-nei-2017

### Appendix - 14 queries
1.	How many vehicle classes are in [co2_emissions_canada]?

2.	[co2_emissions_canada] What is the average engine size, fuel consumption in city and highway, and CO2 emission for each vehicle class and transmission?

3.	[ev_stations_v1] For each zip code and each EV network, display the number of stations being last confirmed between 2010 and 2022.

4.	[ev_stations_v1] For each zip code, display the number of stations located between latitudes (33.20 to 34.70) and longitudes (-118.40 and -117.20).

5.	[electric_vehicle_population] Let’s find out where we can find the most Telsa cars. For each state and model, display the number of Telsa cars in descending order.

6.	[electric_vehicle_population] For each electric vehicle type and each clean alternative fuel vehicle eligibility, display the average electric range value.

7.	[co2_emissions_canada] and [electric_vehicle_population] Estimate the CO2 emission of vehicles using alternative fuel in the states. For each make and each model, in each state, display the number of vehicles and the CO2 emissions in grams per km.
Tip: This query involves a large number of records/documents. You may consider limiting the matched VINs (vehicle identification number) to the first 100 before grouping, e.g., {$limit: 100}.

You may need additional $lookup syntax to join two collections with multiple fields. Consider $match and $expr in https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/

8.	[ev_stations_v1] and [electric_vehicle_population] Does the number of EV stations matter? There are two parts to this query:
i)	For each state, display the number of electric vehicles, the number of EV stations, and the vehicle:station ratio in descending ratio order.
Tip: You can consider creating temporary tables/collections.
ii)	For each postalcode(zip), display the number of electric vehicles, the number of EV stations, and the vehicle:station ratio in descending ratio order.
Tip: You can consider creating temporary tables/collections.

9.	[nei_2017_full_data] For each NACIS description that contained “auto” or “motor”, display the sum of total emissions. 
Note: For details on NACIS codes, see https://www.standardtechnology.us/services/nacis-codes
Tip: You may need to use $regex. Do note that string comparison is case sensitive in MongoDB.

10.	[nei_2017_full_data]. Who are the key suppliers of Telsa (a major EV producer)? For each state, display the total emissions for the following suppliers: dana, emerson, nucor, micron, allegheny, albemarle, schneider, and veatch.

 
11.	*Open-ended question using the provided data* Is EV really green compared with various vehicle types? What about comparing EVs to alternative fuel vehicles? 

12.	*Open-ended question using the provided data* Does it make sense for Singapore to fully convert fossil-fuel vehicles to EVs? What can be the important determinants for a successful conversion? 
If you believe a full conversion is not feasible, what else can we do?
Hint: Take a good look at the CO2 emissions of various vehicle types. You may also consider identifying some comparable cities to support your arguments.

13.	*Open-ended question using the provided data* Singapore plans to roll out 60,000 charging points island-wide by 2030 (see https://www.straitstimes.com/singapore/transport/ev-drivers-can-check-availability-of-charging-points-in-real-time-under-mytransportsg-app). Where should these charging stations be located? Substantiate your team’s opinion with the data provided.

14.	*Blue-sky question; you may use any publicly available data* 
“The era of global warming has ended; the era of global boiling has arrived.”
-	Antonio Guterres, Secretary-General of the United Nations. Published at The Strait Times https://str.sg/iieR
“The No.1 weather-related killer is heat.”
-	Joe Biden, President of the United States of America. Published at ABC news https://abcnews.go.com/Politics/weather-related-killer-biden-outlines-new-actions-combat/story?id=101714388
Would a general, robust adoption of EVs be adequate to turn around the climate crisis? Would electricity generation remain to be largely fossil-based, nonetheless? What else can we do in the short term and long term?
Notes: When additional datasets are considered, your team must provide formal references/sources to retrieve the original datasets. 
Evaluation will be performed with attention to the coherence of your team’s narrative. A coherent data narrative can be achieved using a focused dataset. A rich, diversified dataset can muddle the narrative if the data is not meaningfully integrated.


