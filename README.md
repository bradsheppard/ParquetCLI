# Parquet CLI

<img src="docs/parquet_logo.svg" alt="Parquet Logo" width="400">

## Description
Command line tool for reading Apache Parquet files. Supports:
- Reading the rows of the file.
- Reading metadata:
  - Total number of rows
  - Encryption algorithm
  - Author
  - Column formats

## Usage
<table>
  <tr>
    <td><code>parquetcli meta</code></td><td>Prints metadata about the parquet file</td>
  </tr>
  <tr>
    <td><code>parquetcli rows</code></td><td>Prints row information about the parquet file</td>
  </tr>
</table>
