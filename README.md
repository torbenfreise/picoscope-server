# Picoscope 5444DMSO Service

Implementation of a gRPC service for a model 5444DMSO Picoscope intended for use with h2pcontrol.

This service should be run on a device connected to picoscope, and enables
remote programming via gRPC.

Configure the address where the service listens in the [config.toml](config.toml) file.

This project has been adapted from the [h2pcontrol-server-template](https://github.com/torbenfreise/h2pcontrol-server-template)

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

## Quick Start

```bash
uv run src/main.py
```

## Usage

The protobuf contract implemented by this service can be found [here](https://buf.build/beyer-labs/h2pcontrol/docs/main%3Ah2pcontrol.picoscope.v1)
