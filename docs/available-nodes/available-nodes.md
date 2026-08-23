# Available data nodes

## Try it now: the World Ocean Database node

<!-- PUBLIC NODE URL: also in .vitepress/theme/components/HeroQuery.vue and docs/2.0.0-rc4/quickstart.md. -->

One node is **open to everyone**. It needs no account and no token:

**<https://beacon-wod.maris.nl>**

It serves the World Ocean Database as one table, `easy-wod`, holding 3.3 billion measurements:

```bash
curl -X POST https://beacon-wod.maris.nl/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT time, latitude, longitude, depth, temperature FROM \"easy-wod\" WHERE temperature > 20 AND depth < 10 LIMIT 5", "output": {"format": "csv"}}'
```

| Column | Type |
|---|---|
| `time` | timestamp |
| `longitude`, `latitude` | float |
| `depth` | float |
| `temperature`, `salinity`, `oxygen` | float |

Reads are anonymous and rate-limited. See the
[Quick Start](/docs/2.0.0-rc4/quickstart#query-a-public-node) for the Python client.

## The Blue-Cloud and FAIR-EASE nodes

The European projects Blue-Cloud2026 and FAIR-EASE run a further set of Beacon nodes. **Those nodes need an access token.** See [Obtain personal access token](#obtain-personal-access-token) for the steps.

This page shows some of the data nodes in use today. Each node has example notebooks and curl scripts. The examples are in the Beacon Blue-Cloud [GitHub repository](https://github.com/maris-development/beacon-blue-cloud).

---
### Euro-Argo 
Argo floats are autonomous instruments. They collect ocean data on temperature, salinity, pressure and biogeochemical elements. The floats drift with ocean currents. They change their buoyancy to move up and down through the water column. A typical profile starts at the surface and ends at 2,000 meters depth. After each profile, the floats surface and send their data to satellites. The satellites relay the data to researchers. Each float operates for several years.

The Argo program started in 2000. It has deployed thousands of floats across the world's oceans. This network gives a detailed and consistent dataset. Researchers use the dataset to study ocean circulation, climate variability and marine environments. The collection holds data from more than 20,000 floats. It contains more than 3.5 million profiles and billions of observations.

A Beacon node serves a part of the Euro-Argo data. The node reads the data from this [S3 bucket](https://argo-gdac-sandbox.s3.eu-west-3.amazonaws.com/pub/index.html). The image below shows a salinity subset from the Argo float collection. The example notebook on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples) produces this image.

![Argo floats salinity](/argo-psal-example.png)

### World Ocean Database 
The World Ocean Database (WOD) is a large collection of oceanographic data. The National Oceanographic Data Center (NODC) and the World Data Service for Oceanography maintain it. The database holds temperature, salinity, oxygen, nutrient and plankton parameters. The data comes from research vessels, autonomous floats and fixed ocean stations. It covers the world's oceans from the surface to the deep sea.

The Beacon node uses the [World Ocean Database](https://www.ncei.noaa.gov/products/world-ocean-database). It returns results in NetCDF, CSV and ASCII. The map below shows WOD temperature data between 0 and 10 meters depth. The data covers the first months of 2010. The example Jupyter notebook on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples) produces this map.

![WOD Temperature](/wod-temp-example2.png)

### CORA Profiles & Time Series
The Copernicus Marine Environment Monitoring Service (CMEMS) runs the CORA collection. CORA means In Situ Observations of Temperature and Salinity. The collection holds in-situ oceanographic observations. The data comes from Argo floats, CTD sensors, moorings, gliders and other oceanographic platforms. The collection also holds long-term time series data for marine and climate studies.

Two Beacon nodes serve CORA. One node serves the profile data. The other node serves the time series data. The maps below show temperature subsets from both nodes. Find more examples on the [Beacon-Blue-Cloud GitHub](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

CORA profile data, from this CMEMS [product](https://data.marine.copernicus.eu/product/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/services).
![Cora pr Temperature](/cora-pr-temp-example.png)

CORA time series data, from this CMEMS [product](https://data.marine.copernicus.eu/product/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/services).
![Cora ts Temperature](/cora-ts-temp-example.png)

### Obtain personal access token

1. Register for an account at [Blue-Cloud2026](https://data.blue-cloud.org/).
2. Log in at [Blue-Cloud2026](https://data.blue-cloud.org/).
3. Contact paul@maris.nl, info@maris.nl or tjerk@maris.nl. Give your account e-mail address. Request access to the Beacon Beta.
4. Wait for approval. Your user information then shows a new menu item, "BC2026 Beacon Beta Access".
5. Open that page. It gives the URLs of the Beacon nodes and the token controls.
6. Create a token. Use the token as a Bearer token to query the Beacon nodes.
7. Use the same token in the Swagger environment. Click the "Authorize" button.
