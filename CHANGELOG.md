# Changelog

## 1.1.0
  * Fixed `SchemaMismatch` on `workspaces` stream — allow null array items in `workspaces`, `docs`, `folders`, `platform_api`, `reply`, and `teams` schemas. [#23](https://github.com/singer-io/tap-monday/pull/23)
  * Fixed set operator precedence bug in `_process_properties` and `KeyError` risk in `add_object_to_id`.
  * Added discovery unit tests.
  * Bumped requests module version.

## 1.0.0
  * Updates "board_columns", "board_groups", "board_views", and "column_values" to be INCREMENTAL. [#21](https://github.com/singer-io/tap-monday/pull/21)

## 0.1.0
  * Updated python version. [#14](https://github.com/singer-io/tap-monday/pull/14)
  * Upgraded singer library version to latest.
  * Restart queries if the cursor expires. [#15](https://github.com/singer-io/tap-monday/pull/15)

## 0.0.1
  * Initial Commit
