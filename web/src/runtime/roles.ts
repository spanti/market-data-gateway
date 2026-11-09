export const ACCESS_ROLES = ["SuperAdmin", "Operator", "Auditor"] as const;

export type AccessRole = (typeof ACCESS_ROLES)[number];

export function isAccessRole(value: unknown): value is AccessRole {
  return (
    typeof value === "string" &&
    (ACCESS_ROLES as readonly string[]).includes(value)
  );
}
