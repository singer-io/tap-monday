from tap_monday.streams.account import Account
from tap_monday.streams.assets import Assets
from tap_monday.streams.audit_event_catalogue import AuditEventCatalogue
from tap_monday.streams.boards import Boards
from tap_monday.streams.board_activity_logs import BoardActivityLogs
from tap_monday.streams.board_columns import BoardColumns
from tap_monday.streams.board_groups import BoardGroups
from tap_monday.streams.board_items import BoardItems
from tap_monday.streams.board_views import BoardViews
from tap_monday.streams.column_values import ColumnValues
from tap_monday.streams.docs import Docs
from tap_monday.streams.folders import Folders
from tap_monday.streams.platform_api import PlatformApi
from tap_monday.streams.reply import Reply
from tap_monday.streams.tags import Tags
from tap_monday.streams.teams import Teams
from tap_monday.streams.updates import Updates
from tap_monday.streams.users import Users
from tap_monday.streams.workspaces import Workspaces

STREAMS = {
    "account": Account,
    "assets": Assets,
    "audit_event_catalogue": AuditEventCatalogue,
    "boards": Boards,
    "board_activity_logs": BoardActivityLogs,
    "board_columns": BoardColumns,
    "board_groups": BoardGroups,
    "board_items": BoardItems,
    "board_views": BoardViews,
    "column_values": ColumnValues,
    "docs": Docs,
    "folders": Folders,
    "platform_api": PlatformApi,
    "reply": Reply,
    "tags": Tags,
    "teams": Teams,
    "updates": Updates,
    "users": Users,
    "workspaces": Workspaces,
}

