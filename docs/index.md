---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Beacon"
  text: "A High-Performance ARCO Data Lake Query Engine"
  tagline: Making large climate datasets instantly queryable by users using SQL and JSON.
  actions:
    - theme: brand
      text: Install Beacon Node
      link: /docs/1.5.0-install
    - theme: brand
      text: Query Beacon Nodes
      link: /docs/1.5.0/query-docs/
    - theme: alt
      text: Existing Nodes
      link: /available-nodes/available-nodes

features:
  - title: Performance 
    details: Beacon is written in Rust 🦀 and in combination with Apache Arrow and Apache Datafusion provides unmatched performance. Experience lightning-fast ⚡ data retrieval, enabling you to explore and query millions of datasets on the fly with ease.
    icon: 🚀
  - title: Open Source (AGPL V3)
    details: Beacon is fully open source available under the AGPL V3 license 👐. This means you have full control over your data and can contribute to its development.
    icon: 🤝
  - title: Efficiency
    details: Say goodbye to storage woes! 📊 Store and manage millions of datasets efficiently, in the cloud using S3 or locally. Beacon ensures you will have high-performance access to your datasets.
    icon: ⚙️
  - title: It just works
    details: Beacon has been designed for simplicity! 🌟 Experience seamless integration with existing file formats such as NetCDF, Zarr, Parquet and many others. 🛠️ You'll have a powerful data lake query engine up in minutes.
    icon: 🙂

---
