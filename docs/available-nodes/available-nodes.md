# Available data nodes

In European projects like Blue-Cloud2026 and FAIR-EASE, a set of Beacon nodes were set-up to facilitate the Use Cases in their work. These nodes are not generally available and require an access token to be able to access them. The required steps to obtain your own personal token you can find [here](#obtain-personal-access-token). 

To showcase what can be achieved by Beacon, below we present some of the data nodes that are currently in use. For these nodes, there are notebooks and curl scripts available that give examples on how to access and analyze data from these different nodes. These examples are available in the Beacon Blue-Cloud [GitHub](https://github.com/maris-development/beacon-blue-cloud).

---
### Euro-Argo 
Argo floats are autonomous instruments used to collect data on temperature, salinity, pressure and biogeochemical elements of the ocean. They drift with ocean currents and periodically change their buoyancy to move vertically through the water column, typically profiling from the surface down to 2,000 meters. After completing a profile, the floats surface and transmit their data to satellites, which then relay the information to researchers. The floats are designed to operate for several years, providing continuous data over time. 

The Argo program began in 2000 and has since deployed thousands of floats across the world's oceans. This extensive network of floats provides a detailed and consistent dataset, which is invaluable for understanding ocean circulation, climate variability, and marine environments. The collection contains data from over 20,000 floats deployed worldwide, containing over 3.5 million profiles and billions of observations.

A Beacon node is set-up for a set of the Euro-Argo data, which is retrieved from the [S3 bucket](https://argo-gdac-sandbox.s3.eu-west-3.amazonaws.com/pub/index.html). Below you can find an example subset retrieved via the Beacon node of the salinity data from the Argo float collection. It is made with the example notebook that is available on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

![Argo floats salinity](/argo-psal-example.png)

<!-- ### EMODnet Chemistry
EMODnet Chemistry North East Atlantic Collection (subset of the complete collection), retrieved from EMODnet Chemistry [WebODV](https://emodnet-chemistry.webodv.awi.de/service/emodnet-chem_2023_eutrophication%3EAtlantic%3EEutrophication_Atlantic_profiles_2023_unrestricted). Below you can see an example map plot generated with the demo notebook available on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

![EMODnet Chemistry Temperature](/emodnet-chem-example.png) -->

### World Ocean Database 
The World Ocean Database (WOD) is a comprehensive collection of oceanographic data developed and maintained by the National Oceanographic Data Center (NODC) and the World Data Service for Oceanography. It provides a globally integrated and consistent dataset of oceanographic parameters, including temperature, salinity, oxygen, nutrients, and plankton data, among many others. The data is collected from a variety of sources, such as research vessels, autonomous floats, and fixed ocean stations, covering the world's oceans from the surface to the deep sea.

The Beacon node is built on the [World Ocean Database](https://www.ncei.noaa.gov/products/world-ocean-database) and is available in various formats, including NetCDF, CSV, and ASCII. Below you can see an example map-plot of a subset of the temperature data between 0-10 meters depth from the WOD collection of the first months of 2010 produced with the example Jupyter Notebook available on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

![WOD Temperature](/wod-temp-example2.png)

### CORA Profiles & Time Series
The Copernicus Marine Environment Monitoring Service (CMEMS) CORA (CORA – In situ observations of Temperature and Salinity) collection provides comprehensive datasets of in-situ oceanographic observations. These datasets are crucial for understanding and monitoring the marine environment and are collected from various sources such as Argo floats, CTDs (Conductivity, Temperature, Depth sensors), moorings, gliders, and other oceanographic platforms. The dataset includes long-term time series data essential for marine and climate studies.

There are two Beacon nodes set-up for CORA, one for the profiles data and one for the timeseries data. Below you can see examples of map plots obtained for a subset of temperature measurements obtained on-the-fly in a Notebook. You can find examples on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

CORA Profile data, retrieved from CMEMS [product](https://data.marine.copernicus.eu/product/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/services).
![Cora pr Temperature](/cora-pr-temp-example.png)

CORA Timeseries data, retrieved from CMEMS [product](https://data.marine.copernicus.eu/product/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/services).
![Cora ts Temperature](/cora-ts-temp-example.png)

### Obtain personal access token
1. Register for an account at [Blue-Cloud2026](https://data.blue-cloud.org/). 
2. Login here [Blue-Cloud2026](https://data.blue-cloud.org/).
3. After logging in contact paul@maris.nl, info@maris.nl or tjerk@maris.nl with your account e-mail address and request access to the Beacon Beta.
4. Once we grant you access, you’ll see under your user information a new menu item for Beacon called “BC2026 Beacon Beta Access”. 
5. Upon opening this page, you will find URLs to the different Beacon instances and options to create and remove tokens. 
6. You can then query the different Beacon nodes using your personal Bearer token. 
7. This token can also be used in the Swagger environment via the “Authorize” button. 

<!-- ### CMEMS BGC 
CMEMS BGC data, retrieved from CMEMS [product](https://data.marine.copernicus.eu/product/INSITU_GLO_BGC_DISCRETE_MY_013_046/files).

### SeaDataNet T&S Collection
SeaDataNet CDI TS data, retrieved from [EGI-ACE webODV](https://webodv-egi-ace.cloud.ba.infn.it/login)

### SeaDataNet CDI   -->
