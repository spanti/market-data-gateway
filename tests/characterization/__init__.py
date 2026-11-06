"""Pre-migration characterization fixtures and tests.

Everything in this package freezes the observable behaviour of the current
PyArrow/Parquet implementation through existing public seams (Store, REST via
`GatewayRuntime`, Replay Service and the collector pipeline). Nothing here is a
production test hook: the fixtures build real Parquet Data Files and Manifests.
"""
