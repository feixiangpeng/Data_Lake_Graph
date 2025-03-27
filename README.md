# Graph to PFB Converter Demo

A proof-of-concept for converting graph-structured healthcare data to Portable Format for Biomedical data (PFB).

## Overview

This demo implements a conversion from NetworkX graph models to Avro-based PFB format, focusing on maintaining relationships between healthcare entities during serialization.

- Data retrieval and display

## Running the Demo

```bash
# Install dependencies
pip install networkx avro-python3

# Run the demo
python graph_pfb_demo.py
```

## Sample Output

The demo creates:
- A clinical graph with patients and diagnoses
- Avro schemas for the entities
- PFB files with the serialized data
- A verification display of the converted data
