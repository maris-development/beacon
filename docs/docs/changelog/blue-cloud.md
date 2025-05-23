# [Blue Cloud] Changelog

## 23-05-2025 (Running Beacon 1.0.1)

### BDI Updates

- Updated SeaDataNet Monolithic to include CDI metadata. Metadata is available through global attributes and includes:
  - `.platform_type`
  - `instrument___gear_type`
  - And more.

- Updated EMODnet Chemistry Monolithic to include featureType and is available through global attributes:
  - `.featureType`

### Merged Mapping updates

- We have a tracking issue on GitHub for others to track progress and to suggest additional mappings. The issue is located at https://github.com/maris-development/beacon/issues/64

- Applied platform mappings for the following BDI's:
  - `SeaDataNet` using `COMMON_PLATFORM_L06`
  - `CORA-PR` using `COMMON_PLATFORM_L06`
  - `CORA-TS` using `COMMON_PLATFORM_L06`
  - `ARGO` using `COMMON_PLATFORM_L06`
  - `WOD` using `COMMON_PLATFORM_C17` for around 25% of the data (We have an open issue on GitHub to track the conversion of `C17` mapping to `L06`.)
  - `EMODnet-chemistry` using `COMMON_PLATFORM_L06`

- Applied {PARAMETER} mappings for the following BDI's:
  - `SeaDataNet` using `COMMON_{PARAMETER}_L05` & `COMMON_{PARAMETER}_L22`
  - `CORA-PR` using `COMMON_{PARAMETER}_L05` & `COMMON_{PARAMETER}_L22` & `COMMON_{PARAMETER}_L33`
  - `CORA-TS` using `COMMON_{PARAMETER}_L05` & `COMMON_{PARAMETER}_L22` & `COMMON_{PARAMETER}_L33`
  - `ARGO` using `COMMON_{PARAMETER}_{PARAMETER}_L05`
  - `WOD` using `COMMON_{PARAMETER}_L05` & `COMMON_{PARAMETER}_L22` & `COMMON_{PARAMETER}_L33`
  - `EMODnet-chemistry` using `COMMON_{PARAMETER}_L05`
  - `CMEMS-BGC` using `COMMON_{PARAMETER}_L05` & `COMMON_{PARAMETER}_L22` & `COMMON_{PARAMETER}_L33`

### Added

- Added support for setting the "unit" field in the ODV output schema for columns. https://github.com/maris-development/beacon/issues/52
- Fixed missing value flag in NetCDF output schema. https://github.com/maris-development/beacon/issues/45