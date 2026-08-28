# Configuration

This package owns the bootstrap configuration schema, strict YAML decoding,
validation, and safe diagnostic rendering used before broker services start.
Input is rejected above 64 KiB before YAML parsing, listener addresses are
numeric at this bootstrap layer, and rendered values/errors are escaped and
deterministically truncated.
