# Blue Cloud 2026

Within **Blue Cloud 2026**, the main goal is to develop a federated digital research environment in support of the European Digital Twin Ocean. Important components are a central VRE with thematic Virtual Labs that support data science via direct data access to large in-situ data collections, data processing and visualization.



The role of Beacon was to support the direct data access as a data lake: Make various large repositories of in-situ datasets accessible for subsetting on the fly in a unified manner. To achieve this, various Beacon instances were deployed, each with a different data collection, which then instantly made them available for users to subset and download on the fly. These source collections come from major international marine data infrastructures and long-running operational programs. Together, they cover physical and biogeochemical observations across global and European seas, from historical archives to continuously updated streams.



The following in-situ datasets are now available for direct subsetting using Beacon in **Blue Cloud 2026**:

* [Argo Floats](/use-cases/blue-cloud-2026#argo-floats)
* [CMEMS CORA in-situ](/use-cases/blue-cloud-2026#cmems-cora-in-situ)
* [SeaDataNet](/use-cases/blue-cloud-2026#seadatanet)
* [World Ocean Database](/use-cases/blue-cloud-2026#world-ocean-database)
* [CMEMS BGC in-situ](/use-cases/blue-cloud-2026#cmems-bgc-in-situ)

Beacon infers dataset schemas (columns, variables, and attributes) and allows users to freely select and filter on every available column or variable when subsetting these data sources.

On top of these Beacon instances, various notebooks were developed and shared to demonstrate how users can easily subset and download these datasets on the fly using Beacon, and how they can then use the downloaded data for their analyses. These notebooks are publicly available in the [Blue Cloud 2026 GitHub repository](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples/hackathon)

:::info
Using these notebooks requires a token which provides access to the **Blue Cloud 2026** environment. If you would like to gain access to Blue Cloud, please register on the [Blue Cloud website](https://blue-cloud.org/) and apply for access to the **Blue Cloud 2026** environment.
:::

## Argo Floats

**Dataset**: https://registry.opendata.aws/argo-gdac-marinedata/  
**Provider**: https://www.ifremer.fr/en/research-infrastructures/observing-world-s-oceans-real-time

Argo is a global autonomous profiling-float program with around 4,000 active floats that measure temperature and salinity, with BGC extensions adding biogeochemical measurements. Argo GDAC is updated daily and provides open-access NetCDF data and metadata for long-term climate and ocean-state analysis.

For Blue Cloud 2026, Argo NetCDF files are ingested and converted to Beacon Binary Format. In Beacon, these are exposed as multiple binary datasets (core and BGC), enabling subsetting by parameter, depth range, time window, and even platform metadata. Subset from the Core, BGC or even both at the same time for comprehensive Argo analyses.

## CMEMS CORA in-situ

**Dataset**: https://data.marine.copernicus.eu/product/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/

CMEMS CORA is the Copernicus Marine delayed-mode global physical in-situ product, designed for reanalysis use cases. It integrates quality-controlled temperature and salinity observations from major networks (including Argo, GOSUD, OceanSITES, and WOD) into a harmonized product with global spatial coverage and multi-decadal temporal coverage.

In Blue Cloud 2026, CORA NetCDF files are transformed into a single Beacon Binary Format dataset. This setup supports unified subsetting across the full collection, making it easier to retrieve harmonized physical in-situ observations for reproducible analysis workflows.

## SeaDataNet

**Dataset**: https://cdi.seadatanet.org/

SeaDataNet CDI is a pan-European discovery and access infrastructure for marine data, operated by more than 100 data centres across 30+ countries. It provides human and machine interfaces for discovering records and requesting subsets across domains such as physics, chemistry, geology, biology, geophysics, and bathymetry.

For Blue Cloud 2026, SeaDataNet NetCDF files are converted into multiple Beacon Binary Format files and published through Beacon. This allows users to directly query and download subsets from the entire SeaDataNet CDI collection without manual preprocessing.

## World Ocean Database

**Dataset**: https://www.ncei.noaa.gov/products/world-ocean-database

The World Ocean Database (WOD) managed by NOAA NCEI, supported by IODE, is a globally integrated, quality-controlled archive of ocean profile observations and is described as the world’s largest uniformly formatted public collection of this type. It combines historical and modern observations (including the Argo era), with periodic major releases and regular updates.

In Blue Cloud 2026, WOD NetCDF files are converted into multiple Beacon Binary Format datasets. Through Beacon, users can efficiently directly subset large historical in-situ archives by region, depth, variable, period, and other profile attributes.

## CMEMS BGC in-situ

**Dataset**: https://data.marine.copernicus.eu/product/INSITU_GLO_BGC_DISCRETE_MY_013_046/

CMEMS BGC in-situ is the Copernicus Marine delayed-mode global biogeochemical in-situ product. It integrates best-available quality-controlled observations for variables such as dissolved oxygen, chlorophyll/fluorescence, nitrate, phosphate, and silicate, with broad global and long-term temporal coverage.

In Blue Cloud 2026, CMEMS BGC in-situ NetCDF files are converted into a single Beacon Binary Format dataset and served through Beacon. This enables consistent, on-demand subsetting of biogeochemical observations for notebook-based analyses.

## Additional resources

* [Beacon Python SDK Documentation](https://maris-development.github.io/beacon-py/latest/using/datasets/)
* [Beacon Documentation](https://maris-development.github.io/beacon/)
* [Beacon GitHub repository](https://github.com/maris-development/beacon/)
