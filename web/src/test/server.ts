import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

/**
 * Default handler models the current repository state: the Admin API is
 * absent, so capability discovery returns the public `not_found` envelope.
 * Individual tests override this handler to model the Admin API contract.
 */
export const server = setupServer(
  http.get("*/api/v1/admin/capabilities", () => {
    return HttpResponse.json(
      {
        error: {
          code: "not_found",
          message: "not found",
          details: {},
          request_id: "default-current-contract",
        },
      },
      {
        status: 404,
        headers: { "X-Request-ID": "default-current-contract" },
      },
    );
  }),
);
