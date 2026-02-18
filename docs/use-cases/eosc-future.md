# EOSC-Future Marine Data Viewer



EOSC-Future was an EU-funded H2020 project implementing the basis for the European Open Science Cloud (EOSC). EOSC will give European researchers access to a wide web of FAIR data and related services. Beacon supports Open Science through data federation and harmonization support, while keeping the provenance. The EOSC-Future Marine Data Viewer was a prototype to test data access for co-location: It combines in-situ observations, satellite observations, and model products to support interactive exploration of the marine environment.



The map interface is designed for (citizen) scientists and supports exploration of large data collections by subsetting/slicing the collections for:

* Geographic area
* Date and time
* Depth

The marine in-situ observations are provided by SeaDataNet and Euro-Argo. These are co-located with Copernicus Marine product layers derived from modelling and satellite data.



Within the ocean domain, the Marine Data Viewer demonstrator serves both interactive exploration and indicator workflows. The same visible data is also used to generate aggregated values for ocean trend indicators.

## Beacon at the core

Beacon is the core data access and subsetting engine behind the Marine Data Viewer. It provides direct, on-the-fly subsetting over millions of in-situ observations from SeaDataNet and Argo, enabling efficient retrieval of observational data needed to render OGC WMS layers with detailed data points. Since the subsets contain millions of values, displayed on the fly via WMS, a dedicated extension on top of Beacon has been developed to support visualization in high-performance. This setup allows the viewer to deliver responsive, real-time visualisation of large marine observation collections.

## Access

The EOSC-Future Marine Data Viewer is available at [eosc-future.maris.nl](https://eosc-future.maris.nl).

## Further reading

* [EOSC-FUTURE Marine Data Viewer methodology](http://beacon/docs/EOSC_future_Marine_Data_Viewer_Methodology.pdf)
* [Documentation EOSC-FUTURE OCEAN indicator notebook](http://beacon/docs/Documentation%20EOSC-FUTURE%20OCEAN%20indicator%20notebook.pdf)
