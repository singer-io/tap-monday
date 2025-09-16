# tap-monday

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)

This tap:

- Pulls raw data from the [Monday API].
- Extracts the following resources:
    - [Account](https://developer.monday.com/api-reference/reference/account#queries)

    - [Assets](https://developer.monday.com/api-reference/reference/assets-1#queries)

    - [AuditEventCatalogue](https://developer.monday.com/api-reference/reference/audit-event-catalogue#queries)

    - [Boards](https://developer.monday.com/api-reference/reference/boards#queries)

    - [BoardActivityLogs](https://developer.monday.com/api-reference/reference/activity-logs#queries)

    - [BoardColumns](https://developer.monday.com/api-reference/reference/columns#queries)

    - [BoardGroups](https://developer.monday.com/api-reference/reference/groups#queries)

    - [BoardItems](https://developer.monday.com/api-reference/reference/items#queries)

    - [BoardViews](https://developer.monday.com/api-reference/reference/board-views#queries)

    - [ColumnValues](https://developer.monday.com/api-reference/reference/column-values-v2#queries)

    - [Docs](https://developer.monday.com/api-reference/reference/docs#queries)

    - [Folders](https://developer.monday.com/api-reference/reference/folders#queries)

    - [PlatformApi](https://developer.monday.com/api-reference/reference/platform-api#queries)

    - [Reply](https://developer.monday.com/api-reference/reference/other-types#reply)

    - [Tags](https://developer.monday.com/api-reference/reference/tags-1#queries)

    - [Teams](https://developer.monday.com/api-reference/reference/teams#queries)

    - [Updates](https://developer.monday.com/api-reference/reference/updates#queries)

    - [Users](https://developer.monday.com/api-reference/reference/users#queries)

    - [Workspaces](https://developer.monday.com/api-reference/reference/workspaces#queries)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[account](https://developer.monday.com/api-reference/reference/account#queries)**
- Data Key = data.account
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[assets](https://developer.monday.com/api-reference/reference/assets-1#queries)**
- Data Key = assets
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[audit_event_catalogue](https://developer.monday.com/api-reference/reference/audit-event-catalogue#queries)**
- Data Key = data.audit_event_catalogue
- Primary keys: ['name']
- Replication strategy: FULL_TABLE

**[boards](https://developer.monday.com/api-reference/reference/boards#queries)**
- Data Key = data.boards
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[board_activity_logs](https://developer.monday.com/api-reference/reference/activity-logs#queries)**
- Data Key = activity_logs
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[board_columns](https://developer.monday.com/api-reference/reference/columns#queries)**
- Data Key = columns
- Primary keys: ['id', 'board_id']
- Replication strategy: FULL_TABLE

**[board_groups](https://developer.monday.com/api-reference/reference/groups#queries)**
- Data Key = groups
- Primary keys: ['id', 'board_id']
- Replication strategy: FULL_TABLE

**[board_items](https://developer.monday.com/api-reference/reference/items#queries)**
- Data Key = data.boards.items_page.items
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[board_views](https://developer.monday.com/api-reference/reference/board-views#queries)**
- Data Key = data.boards.views
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[column_values](https://developer.monday.com/api-reference/reference/column-values-v2#queries)**
- Data Key = column_values
- Primary keys: ['id', 'item_id']
- Replication strategy: FULL_TABLE

**[docs](https://developer.monday.com/api-reference/reference/docs#queries)**
- Data Key = data.docs
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[folders](https://developer.monday.com/api-reference/reference/folders#queries)**
- Data Key = data.folders
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[platform_api](https://developer.monday.com/api-reference/reference/platform-api#queries)**
- Data Key = data.platform_api
- Primary keys: ['daily_analytics.last_update']
- Replication strategy: INCREMENTAL

**[reply](https://developer.monday.com/api-reference/reference/other-types#reply)**
- Data Key = reply
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[tags](https://developer.monday.com/api-reference/reference/tags-1#queries)**
- Data Key = data.tags
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[teams](https://developer.monday.com/api-reference/reference/teams#queries)**
- Data Key = data.teams
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[updates](https://developer.monday.com/api-reference/reference/updates#queries)**
- Data Key = data.updates
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[users](https://developer.monday.com/api-reference/reference/users#queries)**
- Data Key = data.users
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[workspaces](https://developer.monday.com/api-reference/reference/workspaces#queries)**
- Data Key = data.workspaces
- Primary keys: ['id']
- Replication strategy: FULL_TABLE



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-monday
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json

    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
    -`api_token` - the authorization token to access the monday apis.
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-monday <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.

    ```json
    {
        "api_token": "api_token",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-monday <api_user_email@your_company.com>",
        "request_timeout": 300,
        "api_version": "api_version"
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-monday --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md)

    For Sync mode:
    ```bash
    > tap-monday --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-monday --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-monday --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap

    While developing the monday tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md)
    ```bash
    > pylint tap_monday -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools)
    ```bash
    > tap_monday --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2019 Stitch

