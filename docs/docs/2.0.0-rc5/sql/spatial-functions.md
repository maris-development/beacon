---
description: The 123 spatial functions of Beacon, with PostGIS names. Every function, its arguments, its return type and its limits.
---

# Spatial Functions

Beacon holds 123 spatial functions with PostGIS names: 118 scalar functions, 3 aggregate functions
and 2 window functions. This page lists every one of them.

A name is case insensitive. `ST_Distance` and `st_distance` are the same function.

## Types in the tables below

| Type | Meaning |
| ---- | ------- |
| `GEOMETRY` | A geometry value. A GeoParquet column holds one. `ST_Point` builds one. |
| `BOX` | A bounding box. It holds four `DOUBLE` values. `ST_Envelope` and `ST_Extent` return one. |
| `DOUBLE` | A 64 bit float. |
| `INTEGER` | A 32 bit integer. |
| `BOOLEAN` | True or false. |
| `VARCHAR` | Text. |
| `VARBINARY` | Bytes. |
| `LIST` | A list of `VARBINARY`. `ST_Dump` returns one. |

**constant** after an argument type means the argument must be a literal. A column in that position
gives an error at plan time. Such an argument drives a setup step per batch, so the function cannot
rebuild it per row.

## Build a geometry

A netCDF, Zarr, CSV or Parquet table holds coordinate columns, not geometry. `ST_Point` builds a
geometry from two columns:

```sql
SELECT ST_AsText(ST_Point(longitude, latitude)) AS point
FROM read_parquet(['obs/*.parquet'])
```

A column without geometry metadata also reads as a geometry. A `VARCHAR` column reads as WKT. A
`VARBINARY` column reads as WKB. So a raw CSV column needs no cast.

A [GeoParquet](/docs/2.0.0-rc5/formats/geoparquet) file holds a geometry column, and Beacon decodes
it to native GeoArrow on read. Every function here reads such a column directly:

```sql
SELECT ST_AsText(ST_Extent(geometry)) AS extent
FROM read_geoparquet(['spatial/stations/*.geoparquet'])
```

:::tip
A filter over a GeoParquet geometry column also skips row groups. See
[what the scan skips](/docs/2.0.0-rc5/formats/geoparquet).
:::

## Accessors

An accessor reads one property of a geometry.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_X(geom)` | `geom` GEOMETRY | `DOUBLE` | X ordinate of a point. It copies no data on a point column with separate coordinates. |
| `ST_Y(geom)` | `geom` GEOMETRY | `DOUBLE` | Y ordinate of a point. |
| `ST_Z(geom)` | `geom` GEOMETRY | `DOUBLE` | Z ordinate. A 2D geometry returns `NULL`. |
| `ST_M(geom)` | `geom` GEOMETRY | `DOUBLE` | Measure value. A geometry without a measure returns `NULL`. |
| `ST_SRID(geom)` | `geom` GEOMETRY | `INTEGER` | Coordinate reference system of the column. Every row returns the same value. |
| `ST_GeometryType(geom)` | `geom` GEOMETRY | `VARCHAR` | Type name, such as `ST_Point`. |
| `ST_Dimension(geom)` | `geom` GEOMETRY | `INTEGER` | Topological dimension: 0, 1 or 2. |
| `ST_CoordDim(geom)` | `geom` GEOMETRY | `INTEGER` | Number of ordinates per coordinate: 2, 3 or 4. |
| `ST_NPoints(geom)` | `geom` GEOMETRY | `INTEGER` | Number of coordinates, at any depth. |
| `ST_NumPoints(geom)` | `geom` GEOMETRY | `INTEGER` | Number of points of a line string. Another type returns `NULL`. |
| `ST_NumGeometries(geom)` | `geom` GEOMETRY | `INTEGER` | Number of parts of a collection. |
| `ST_NumInteriorRings(geom)` | `geom` GEOMETRY | `INTEGER` | Number of holes of a polygon. |
| `ST_IsEmpty(geom)` | `geom` GEOMETRY | `BOOLEAN` | True for an empty geometry. |
| `ST_IsClosed(geom)` | `geom` GEOMETRY | `BOOLEAN` | True when the start point equals the end point. |
| `ST_IsRing(geom)` | `geom` GEOMETRY | `BOOLEAN` | True for a line string that is closed and simple. |
| `ST_IsSimple(geom)` | `geom` GEOMETRY | `BOOLEAN` | True when a geometry crosses itself nowhere. An areal geometry is always simple. |

## Components

A component function returns one part of a geometry.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_StartPoint(geom)` | `geom` GEOMETRY | `GEOMETRY` | First point of a line string. |
| `ST_EndPoint(geom)` | `geom` GEOMETRY | `GEOMETRY` | Last point of a line string. |
| `ST_PointN(geom, n)` | `geom` GEOMETRY, `n` INTEGER | `GEOMETRY` | Point `n` of a line string. The index starts at 1. An index outside the line returns `NULL`. |
| `ST_ExteriorRing(geom)` | `geom` GEOMETRY | `GEOMETRY` | Shell of a polygon, as a line string. |
| `ST_InteriorRingN(geom, n)` | `geom` GEOMETRY, `n` INTEGER | `GEOMETRY` | Hole `n` of a polygon. The index starts at 1. |
| `ST_GeometryN(geom, n)` | `geom` GEOMETRY, `n` INTEGER | `GEOMETRY` | Part `n` of a collection. Index 1 returns the input when the input is no collection. |

## Constructors

A constructor builds a geometry from plain columns.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Point(x, y[, z])` | `x` DOUBLE, `y` DOUBLE, `z` DOUBLE | `GEOMETRY` | Point from two or three ordinate columns. It adopts the input buffers and copies nothing. A third argument sets z, not the SRID. |
| `ST_MakePoint(x, y[, z])` | `x` DOUBLE, `y` DOUBLE, `z` DOUBLE | `GEOMETRY` | The same function, under the PostGIS alias. |
| `ST_PointZ(x, y[, z])` | `x` DOUBLE, `y` DOUBLE, `z` DOUBLE | `GEOMETRY` | The same function. Two arguments return a 2D point. PostGIS needs three. |
| `ST_MakeLine(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Two-point line from two point columns. There is no aggregate form. |
| `ST_MakePolygon(ring)` | `ring` GEOMETRY | `GEOMETRY` | Polygon from a closed line string. It builds the shell only, with no holes. |
| `ST_MakeEnvelope(x1, y1, x2, y2)` | four DOUBLE | `GEOMETRY` | Rectangle from four ordinates. It adopts all four input buffers. There is no fifth `srid` argument. |
| `ST_MakeBox2D(x1, y1, x2, y2)` | four DOUBLE | `BOX` | Box from four ordinates. **PostGIS takes two points here.** |

## Input and output

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_AsText(geom)` | `geom` GEOMETRY | `VARCHAR` | WKT text. There is no `maxdecimaldigits` argument. |
| `ST_AsBinary(geom)` | `geom` GEOMETRY | `VARBINARY` | WKB bytes. |
| `ST_AsEWKB(geom)` | `geom` GEOMETRY | `VARBINARY` | Extended WKB bytes. The SRID comes from the column metadata. |
| `ST_AsGeoJSON(geom)` | `geom` GEOMETRY | `VARCHAR` | GeoJSON text. It accepts a geometry only, not a row. |
| `ST_GeomFromText(wkt)` | `wkt` VARCHAR | `GEOMETRY` | Geometry from WKT. There is no `srid` argument. Call `ST_SetSRID` after it. |
| `ST_GeomFromWKB(bytes)` | `bytes` VARBINARY | `GEOMETRY` | Geometry from WKB. |
| `ST_GeomFromEWKB(bytes)` | `bytes` VARBINARY | `GEOMETRY` | Geometry from extended WKB. **The SRID inside the value is lost.** Call `ST_SetSRID` after it. |
| `ST_GeomFromGeoJSON(json)` | `json` VARCHAR | `GEOMETRY` | Geometry from GeoJSON. |
| `ST_GeoHash(geom[, prec])` | `geom` GEOMETRY, `prec` INTEGER | `VARCHAR` | Geohash of a point. Another type returns `NULL`. The default precision is 20. |
| `ST_PointFromGeoHash(hash)` | `hash` VARCHAR | `GEOMETRY` | Centre point of a geohash cell. There is no precision argument. |

## Predicates

A predicate compares two geometries and returns true or false. Each one runs a bounding box test
first. The exact test then runs on the rows that pass.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Intersects(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the two share a point. |
| `ST_Disjoint(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the two share no point. |
| `ST_Contains(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when `a` holds `b`. |
| `ST_ContainsProperly(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when `a` holds `b` and the boundaries meet nowhere. |
| `ST_Within(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when `b` holds `a`. |
| `ST_Covers(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when every point of `b` lies in `a`. |
| `ST_CoveredBy(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | The reverse of `ST_Covers`. |
| `ST_Touches(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when only the boundaries meet. |
| `ST_Crosses(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the interiors cross. |
| `ST_Overlaps(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the two overlap in part. |
| `ST_Equals(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the two cover the same points. |
| `ST_Relate(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `VARCHAR` | The nine character DE-9IM matrix. |
| `ST_Relate(a, b, pattern)` | `a` GEOMETRY, `b` GEOMETRY, `pattern` VARCHAR constant | `BOOLEAN` | True when the matrix matches the pattern. |
| `ST_DWithin(a, b, d)` | `a` GEOMETRY, `b` GEOMETRY, `d` DOUBLE constant | `BOOLEAN` | True when the distance is `d` or less. |
| `ST_DFullyWithin(a, b, d)` | `a` GEOMETRY, `b` GEOMETRY, `d` DOUBLE constant | `BOOLEAN` | True when every point pair lies within `d`. |
| `ST_BBoxIntersects(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `BOOLEAN` | True when the two bounding boxes overlap. **PostGIS writes this as the `&&` operator.** |

## Measurement

Every measurement is planar. Longitude and latitude data therefore returns degrees, not metres.
`ST_DistanceSphere` and `ST_DistanceSpheroid` are the two exceptions. Both return metres.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Area(geom)` | `geom` GEOMETRY | `DOUBLE` | Area. |
| `ST_Length(geom)` | `geom` GEOMETRY | `DOUBLE` | Length of the lineal parts. A polygon returns zero. |
| `ST_Perimeter(geom)` | `geom` GEOMETRY | `DOUBLE` | Perimeter of the areal parts. |
| `ST_Distance(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Shortest distance. |
| `ST_MaxDistance(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Largest distance between two vertices. The cost is the product of the two vertex counts. |
| `ST_HausdorffDistance(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Hausdorff distance. There is no `densifyFrac` argument. |
| `ST_FrechetDistance(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Fréchet distance of two line strings. Another type returns `NULL`. |
| `ST_DistanceSphere(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Distance of two points on a sphere, in metres. **It takes two points only.** |
| `ST_DistanceSpheroid(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Distance of two points on WGS 84, in metres. There is no spheroid argument. |

## Linear reference

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_ClosestPoint(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Point of `a` nearest to `b`. |
| `ST_ShortestLine(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Line between the two nearest points. |
| `ST_LineLocatePoint(line, pt)` | `line` GEOMETRY, `pt` GEOMETRY | `DOUBLE` | Position of a point on a line, from 0 to 1. |
| `ST_LineInterpolatePoint(line, f)` | `line` GEOMETRY, `f` DOUBLE | `GEOMETRY` | Point at fraction `f` of a line. A fraction outside 0 to 1 returns `NULL`. |

## Overlay

Each overlay function takes areal arguments only. Another type returns `NULL`.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Union(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Union of two areal geometries. |
| `ST_Intersection(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Common part of two areal geometries. |
| `ST_Difference(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Part of `a` outside `b`. |
| `ST_SymDifference(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `GEOMETRY` | Part of either one, but not of both. |

`ST_Union` with one argument is an error. PostGIS gives that name to an aggregate, and one name
cannot serve both registries here. The aggregate is [`ST_MemUnion`](#aggregate-functions).

## Processing

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Buffer(geom, d)` | `geom` GEOMETRY, `d` DOUBLE | `GEOMETRY` | Area within distance `d`. It uses round joins and round caps. There is no style argument. |
| `ST_ConvexHull(geom)` | `geom` GEOMETRY | `GEOMETRY` | Smallest convex polygon around a geometry. |
| `ST_ConcaveHull(geom, pct)` | `geom` GEOMETRY, `pct` DOUBLE | `GEOMETRY` | Concave hull. It supports no holes. |
| `ST_OrientedEnvelope(geom)` | `geom` GEOMETRY | `GEOMETRY` | Smallest rectangle of any angle. |
| `ST_Boundary(geom)` | `geom` GEOMETRY | `GEOMETRY` | Boundary of a geometry. A collection returns an empty boundary. |
| `ST_Centroid(geom)` | `geom` GEOMETRY | `GEOMETRY` | Centre of mass. |
| `ST_PointOnSurface(geom)` | `geom` GEOMETRY | `GEOMETRY` | A point that lies on the geometry. |
| `ST_Simplify(geom, tol)` | `geom` GEOMETRY, `tol` DOUBLE | `GEOMETRY` | Ramer-Douglas-Peucker simplification. There is no `preserveCollapsed` flag. |
| `ST_SimplifyVW(geom, tol)` | `geom` GEOMETRY, `tol` DOUBLE | `GEOMETRY` | Visvalingam-Whyatt simplification. |
| `ST_Segmentize(geom, max)` | `geom` GEOMETRY, `max` DOUBLE | `GEOMETRY` | Split every segment longer than `max`. A length of zero or less returns `NULL`. |
| `ST_RemoveRepeatedPoints(geom)` | `geom` GEOMETRY | `GEOMETRY` | Drop repeated coordinates. There is no tolerance argument. |
| `ST_Reverse(geom)` | `geom` GEOMETRY | `GEOMETRY` | Reverse the coordinate order. |
| `ST_ForcePolygonCCW(geom)` | `geom` GEOMETRY | `GEOMETRY` | Counter-clockwise shell. |
| `ST_ForcePolygonCW(geom)` | `geom` GEOMETRY | `GEOMETRY` | Clockwise shell. |
| `ST_Force2D(geom)` | `geom` GEOMETRY | `GEOMETRY` | Drop the Z ordinate. It drops one buffer handle and copies nothing. |
| `ST_Force3D(geom)` | `geom` GEOMETRY | `GEOMETRY` | Add a Z ordinate of zero. |
| `ST_FlipCoordinates(geom)` | `geom` GEOMETRY | `GEOMETRY` | Swap X and Y. It swaps two buffer handles and copies nothing. |
| `ST_SetSRID(geom, srid)` | `geom` GEOMETRY, `srid` INTEGER constant | `GEOMETRY` | Set the coordinate reference system. It reads no row, because it changes the column type. |

`ST_SimplifyPreserveTopology` is absent. The library behind these functions has no topology-safe
simplification. `ST_SimplifyVW` is close, but it gives no guarantee.

## Validity

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_IsValid(geom)` | `geom` GEOMETRY | `BOOLEAN` | True for a valid geometry. |
| `ST_IsValidReason(geom)` | `geom` GEOMETRY | `VARCHAR` | Text that names the fault. A valid geometry returns `Valid Geometry`. |
| `ST_MakeValid(geom)` | `geom` GEOMETRY | `GEOMETRY` | Repair an areal geometry. Another type passes through. |

## Affine

Every argument after the geometry must be a constant.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Translate(geom, dx, dy)` | `geom` GEOMETRY, `dx` DOUBLE constant, `dy` DOUBLE constant | `GEOMETRY` | Move a geometry. There is no 3D form. |
| `ST_Scale(geom, xf, yf)` | `geom` GEOMETRY, `xf` DOUBLE constant, `yf` DOUBLE constant | `GEOMETRY` | Scale about the origin. |
| `ST_Rotate(geom, rad)` | `geom` GEOMETRY, `rad` DOUBLE constant | `GEOMETRY` | Rotate about the origin. The angle is in radians. |
| `ST_Affine(geom, a, b, d, e, xoff, yoff)` | `geom` GEOMETRY, six DOUBLE constant | `GEOMETRY` | 2D affine transform. There is no twelve argument 3D form. |

## Bounding box

A box column holds four `DOUBLE` buffers. An ordinate accessor therefore hands back one buffer and
allocates nothing.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Envelope(geom)` | `geom` GEOMETRY | `BOX` | Bounding box of a geometry. It reads as `ST_Polygon`. |
| `ST_Expand(geom, d)` | `geom` GEOMETRY, `d` DOUBLE | `BOX` | Bounding box grown by `d`. There is no per-axis form. |
| `ST_XMin(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Smallest X. |
| `ST_YMin(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Smallest Y. |
| `ST_XMax(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Largest X. |
| `ST_YMax(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Largest Y. |
| `ST_ZMin(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Always `NULL`. The box pass is two-dimensional. |
| `ST_ZMax(geom)` | `geom` GEOMETRY or BOX | `DOUBLE` | Always `NULL`. |

## Tessellation

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_DelaunayTriangles(geom)` | `geom` GEOMETRY | `GEOMETRY` | Delaunay triangulation. It always returns a collection. |
| `ST_VoronoiPolygons(geom)` | `geom` GEOMETRY | `GEOMETRY` | Voronoi cells as polygons. It clips each cell to the input extent plus 50 percent. |
| `ST_VoronoiLines(geom)` | `geom` GEOMETRY | `GEOMETRY` | Voronoi cell edges as lines. |
| `ST_ChaikinSmoothing(geom, n)` | `geom` GEOMETRY, `n` INTEGER | `GEOMETRY` | Chaikin smoothing. The limit is 8 rounds, because each round doubles the vertex count. |

## Bearings

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Azimuth(a, b)` | `a` GEOMETRY, `b` GEOMETRY | `DOUBLE` | Bearing of two points, in radians clockwise from north, on WGS 84. Two equal points return `NULL`. |
| `ST_Project(pt, dist, azim)` | `pt` GEOMETRY, `dist` DOUBLE, `azim` DOUBLE | `GEOMETRY` | Point at a distance in metres and a bearing in radians, on WGS 84. |

## Edits

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Multi(geom)` | `geom` GEOMETRY | `GEOMETRY` | Wrap a geometry in its multi form. |
| `ST_Points(geom)` | `geom` GEOMETRY | `GEOMETRY` | Every coordinate as one multi point. |
| `ST_SnapToGrid(geom, size)` | `geom` GEOMETRY, `size` DOUBLE | `GEOMETRY` | Round every coordinate onto a grid. One size serves both axes. A size of zero or less returns `NULL`. |
| `ST_AddPoint(line, pt[, pos])` | `line` GEOMETRY, `pt` GEOMETRY, `pos` INTEGER | `GEOMETRY` | Add a point to a line string. The index starts at 0. It appends when you omit the position. |
| `ST_RemovePoint(line, pos)` | `line` GEOMETRY, `pos` INTEGER | `GEOMETRY` | Remove a point from a line string. It returns `NULL` when fewer than two vertices remain. |
| `ST_SetPoint(line, pos, pt)` | `line` GEOMETRY, `pos` INTEGER, `pt` GEOMETRY | `GEOMETRY` | Replace a point of a line string. Note the order: the position comes first. |
| `ST_Dump(geom)` | `geom` GEOMETRY | `LIST` | Parts of a collection, as a list of WKB. |

`ST_Dump` returns a list, because a scalar function returns one value per row. Expand it with
`unnest`:

```sql
SELECT ST_AsText(unnest(ST_Dump(geometry))) AS part
FROM read_geoparquet(['shapes/*.geoparquet'])
```

The parts are WKB, not geometry. `unnest` drops the metadata of the child field, so a list of
geometry arrives as a plain struct. A `VARBINARY` column always reads as WKB, so the parts stay
usable with no cast.

## Aggregate functions

An aggregate function reads every row of a group and returns one value.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Extent(geom)` | `geom` GEOMETRY | `BOX` | Bounding box of every row. The state is four `DOUBLE` values, so it builds no geometry. |
| `ST_Collect(geom)` | `geom` GEOMETRY | `GEOMETRY` | Every row as one geometry collection. PostGIS returns a multi type for one input type. |
| `ST_MemUnion(geom)` | `geom` GEOMETRY | `GEOMETRY` | Union of every row. **PostGIS also calls this `ST_Union`.** |

Read the box of `ST_Extent` with the four ordinate accessors:

```sql
SELECT ST_XMin(ST_Extent(ST_Point(longitude, latitude))) AS west,
       ST_XMax(ST_Extent(ST_Point(longitude, latitude))) AS east,
       ST_YMin(ST_Extent(ST_Point(longitude, latitude))) AS south,
       ST_YMax(ST_Extent(ST_Point(longitude, latitude))) AS north
FROM read_parquet(['obs/*.parquet'])
```

## Window functions

Both cluster functions read every row of a partition at once. PostGIS defines them as window
functions, and so does Beacon. Each one needs `OVER ()`.

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_ClusterKMeans(geom, k)` | `geom` GEOMETRY, `k` INTEGER | `INTEGER` | Group the rows into `k` clusters. A fixed seed makes the query repeatable. There is no `max_radius` argument. |
| `ST_ClusterDBSCAN(geom, eps, min)` | `geom` GEOMETRY, `eps` DOUBLE, `min` INTEGER | `INTEGER` | Group the rows by density. Noise returns `NULL`. |

Both functions cluster the centroids. PostGIS uses the whole geometry, so a large geometry can give
another answer.

```sql
SELECT platform,
       ST_ClusterKMeans(ST_Point(longitude, latitude), 5) OVER () AS cluster
FROM read_parquet(['obs/*.parquet'])
```

## Reprojection

| Function | Arguments | Returns | Description |
| -------- | --------- | ------- | ----------- |
| `ST_Transform(geom, srid)` | `geom` GEOMETRY, `srid` INTEGER constant | `GEOMETRY` | Reproject a geometry to the target `srid`. |

The source system comes from the column metadata. Set it first when the file states none:

```sql
SELECT ST_AsText(ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3035)) AS point
FROM read_parquet(['obs/*.parquet'])
```

The function takes an EPSG code, not a PROJ string.

`ST_Transform` links [PROJ](https://proj.org), a C++ library, and a standard Beacon build ships it.
A build from source therefore needs PROJ 9.6.2 or later, beside the netCDF and HDF5 it already
needs:

```bash
sudo apt-get install -y libproj-dev pkg-config   # Debian and Ubuntu
brew install proj pkg-config                     # macOS
```

Two build options cover a machine without PROJ. `--features spatial-proj-bundled` builds PROJ from
source. `--no-default-features` drops `ST_Transform`, and the other 122 functions stay.

## Differences from PostGIS

Four rules explain most of the differences. Each table above marks the rest per function.

- **The coordinate reference system belongs to the column.** GeoArrow holds it once, in the column
  metadata. `ST_SRID` therefore returns one value for every row, and `ST_SetSRID` needs a constant.
  One PostGIS column can hold rows in different systems. A Beacon column cannot.
- **A constant argument stays constant.** A radius, a pattern, an SRID or a matrix drives a setup
  step per batch. A column in that position gives an error at plan time.
- **A row that does not fit returns `NULL`.** PostGIS raises an error for a wrong geometry type. One
  bad row does not stop a query here. A wrong static type is still an error at plan time.
- **A plain column reads by its type.** A `VARCHAR` column reads as WKT. A `VARBINARY` column reads
  as WKB. One surprise follows: `ST_AsText` on any text column returns that text. Pass such a column
  through `ST_GeomFromText` to check the parse step.

## Functions that are absent

| Area | PostGIS functions | Reason |
| ---- | ----------------- | ------ |
| Geography type | every `geography` overload | It needs a second type and a spherical algorithm set. |
| Edits | `ST_Snap`, `ST_Node`, `ST_Split`, `ST_LineMerge`, `ST_LineSubstring`, `ST_Subdivide` | The library behind these functions has no equivalent. |
| 3D | `ST_3DDistance`, `ST_3DIntersects`, `ST_3DLength` and the rest | That library is two-dimensional. |
| Other output | `ST_AsGML`, `ST_AsKML`, `ST_AsSVG`, `ST_AsMVT` | Beacon needs a writer per format. |
| Simplification | `ST_SimplifyPreserveTopology` | No topology-safe simplification exists there. |
| Set output | `ST_DumpPoints`, `ST_DumpRings` | These have the shape of `ST_Dump`. `ST_Points` covers the common use. |

:::note
`SHOW FUNCTIONS` lists 10 of these functions. It reads `information_schema.parameters`, and a
function that accepts any argument type states no argument types. Such a function gets no row
there. Every function on this page runs, listed or not. Use this page as the reference. See
[datafusion-spatial#1](https://github.com/robinskil/datafusion-spatial/issues/1).
:::
