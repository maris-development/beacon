# Hardware

Beacon is a lightweight tool that can run on most modern hardware. The following are the minimum and recommended hardware requirements for running Beacon.

# Server Hardware Specifications for Software Tool

## Minimum Hardware Requirements

To ensure the software tool runs smoothly on a server, your system must meet the following minimum hardware requirements:

- **CPU**: Processor with SSE4.2 or newer (e.g., Intel Xeon E3 or newer, AMD EPYC 7000 series)
- **RAM**: At least 4 GB (may vary depending on the number of datasets to import)
- **Storage**: 
  - HDD with at least 50 GB free space

## Recommended Hardware Requirements

For optimal performance, especially when handling large datasets or performing complex operations, the following server hardware is recommended:

- **CPU**: 
  - Intel Xeon Scalable (Silver 4208 or newer) or AMD EPYC 7002 series (EPYC 7402 or newer). More cores and higher clock speeds are beneficial as Beacon is highly paralyzed.
- **RAM**: 
  - 64 GB or more
- **Storage**:
  - SATA SSD with at least 1 TB free space (for faster read/write speeds)
- **Network**: 
  - 10 Gbps Ethernet for high-speed data transfer

## Detailed Specifications

### CPU
- **Minimum**: Processor with SSE4.2 instruction set
  - Examples: Intel Xeon E3-1220 v5 or AMD EPYC 7251
- **Recommended**: 
  - Intel Xeon Silver 4208 or AMD EPYC 7402 for better performance and scalability.
- **Optimal**
  - Intel Xeon Gold 6248 or AMD EPYC 7502 for high-performance computing.

### RAM
- **Minimum**: 16 GB (increases based on the size and number of datasets)
- **Recommended**: 
  - 64 GB to handle multiple datasets and complex calculations efficiently.
  - ECC (Error-Correcting Code) RAM is recommended for improved reliability.

### Storage
- **Minimum**: HDD with 50 GB free space
- **Recommended**: 
  - SATA SSD for faster read/write speeds and better overall performance.
- **Optimal**: 
  - RAID configuration for redundancy and improved data protection.
  - Example: Samsung PM1735 NVMe SSD with RAID 5 on a 4 x 1 TB NVMe SSDs for high-speed data access and fault tolerance.

### Network
- **Minimum**: 1 Gbps Ethernet
- **Recommended**: 
  - 10 Gbps Ethernet for high-speed data transfer and reduced latency.

### Additional Recommendations
- **Redundancy**: 
  - RAID configuration for storage redundancy and improved data protection.
- **Backup**: 
  - Regular backup solutions, such as a secondary server or cloud backup services.
- **Operating System**: 
  - Compatible with server editions of major operating systems: Ubuntu Server 20.04 LTS, Red Hat Enterprise Linux 8.

## Conclusion

By meeting the minimum requirements, servers can run the software tool with basic functionality. However, for a smooth and efficient experience, especially when dealing with larger datasets or more intensive operations, it is highly recommended to follow the suggested hardware specifications.

For any further queries or support, please refer to our [support page](#).