"""Admin Export Jobs: persistent CSV jobs with quota, expiry and Range.

Public entry point for the app composition: ``create_export_router(context)``
registers the ``/api/v1/admin/export-jobs`` routes, the ``export_executor``
adapter and the ``export_job`` capability. The ``ExportService`` hangs off
``AdminContext.exports`` and is closed before the Bars Store.
"""

from mdgw.admin.export.routes import create_export_router
from mdgw.admin.export.service import ExportService

__all__ = ["ExportService", "create_export_router"]
