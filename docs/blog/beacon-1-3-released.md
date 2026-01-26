
# Beacon 1.3 Released: cloud storage, SQL, and a generic Python client

Beacon 1.3 is a milestone release built to remove friction. It adds native support for multiple cloud storage backends, introduces SQL querying alongside the JSON query API, and ships a generic Python client you can use with any Beacon deployment. If you want faster onboarding, easier integrations, and more flexibility, this is the upgrade that delivers.

Below is a quick overview of what’s new and why it matters.

## Native support for multiple cloud stores

Beacon 1.3 now integrates with the Rust `object_store` crate, giving Beacon a unified way to access different storage backends. In practice, this means you can point Beacon at multiple cloud stores using a single configuration model, without rewriting datasets or building custom adapters. Less setup, more data online—faster.

Why this matters:

- **One abstraction, many backends**: S3-compatible storage, local file systems, and other object stores can be handled the same way.
- **Simpler deployments**: move the same Beacon setup between environments with minimal changes.
- **Future-proofing**: new object store backends become easier to support.

## SQL queries, in addition to JSON

Beacon has long supported a JSON query API. With 1.3, you can also use SQL to query datasets. This is especially useful for data teams who already think in SQL and want a familiar way to explore or integrate Beacon into existing analytics workflows. It’s the quickest path to adoption for analysts and BI users.

You still keep the JSON API for programmatic workflows, but now you can choose whichever interface is most convenient for the task.

## A generic Python client for any Beacon instance

Beacon 1.3 includes a new Python library, designed to work with any Beacon deployment. It provides a lightweight, consistent way to submit queries and handle results without needing to write direct HTTP calls. If you live in notebooks or pipelines, this is the fastest way to get results.

Documentation and examples are available here:

https://maris-development.github.io/beacon-py/latest/

## What this unlocks

Taken together, these three updates make Beacon easier to adopt in real-world environments:

- Cloud storage support removes barriers for hosted datasets.
- SQL lowers the entry point for users and analysts.
- The Python client makes integration straightforward in notebooks and pipelines.

## Get started

If you are already running Beacon, the upgrade to 1.3 is the easiest way to unlock these capabilities. If you’re new to Beacon, this release is a great starting point for building a flexible, cloud‑friendly data service.

As always, reach out if you want help with configuration or migration. We’re happy to help you get it live quickly.
