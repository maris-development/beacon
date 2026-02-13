# EOSC-Future Marine Data Viewer

The EOSC-Future Marine Data Viewer combines in-situ observations, satellite observations, and model products to support interactive exploration of the marine environment.

The map interface is designed for (citizen) scientists and supports exploration of large data collections by:

- Geographic area
- Date and time
- Depth

In-situ observations are provided by SeaDataNet and Euro-Argo. These are co-located with Copernicus Marine product layers derived from modelling and satellite data.

## Project context

This viewer was developed in the EU EOSC-Future project, where ENVRI-FAIR partners contributed in WP6 to the State of the Environment dashboard.

The dashboard is intended to help users assess status and trends across key Earth system domains:

- Atmosphere
- Ocean
- Biodiversity

Within the ocean domain, the Marine Data Viewer serves both interactive exploration and indicator workflows. The same visible data is also used to generate aggregated values for ocean trend indicators.

## Beacon at the core

Beacon is the core data access and subsetting engine behind the Marine Data Viewer.

It provides direct, on-the-fly subsetting over millions of in-situ observations (including SeaDataNet and Argo), enabling efficient retrieval of observational data needed to render WMS layers with detailed data points.

This allows the viewer to deliver responsive, real-time visualisation of large marine observation collections.

## Access

The EOSC-Future Marine Data Viewer is available at [eosc-future.maris.nl](https://eosc-future.maris.nl).

## Further reading

- [EOSC-FUTURE Marine Data Viewer methodology](http://beacon/docs/EOSC_future_Marine_Data_Viewer_Methodology.pdf)
- [Documentation EOSC-FUTURE OCEAN indicator notebook](http://beacon/docs/Documentation%20EOSC-FUTURE%20OCEAN%20indicator%20notebook.pdf)