resource "snowflake_warehouse" "app" {
  name                = "PE_ORG_AIR_WH_${upper(var.environment)}"
  warehouse_size      = "X-SMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  comment = "Compute warehouse for PE Org-AI-R Platform"
}

resource "snowflake_database" "app" {
  name    = "PE_ORG_AIR_${upper(var.environment)}"
  comment = "Database for PE Org-AI-R Platform"
}

resource "snowflake_schema" "app" {
  database = snowflake_database.app.name
  name     = "PUBLIC"

  depends_on = [snowflake_database.app]
}

resource "snowflake_account_role" "app" {
  name    = "PE_ORG_AIR_APP_${upper(var.environment)}"
  comment = "Application role with least-privilege access"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_account_role.app.name
  privileges        = ["USAGE", "OPERATE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.app.name
  }

  depends_on = [
    snowflake_account_role.app,
    snowflake_warehouse.app
  ]
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  account_role_name = snowflake_account_role.app.name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.app.name
  }

  depends_on = [
    snowflake_account_role.app,
    snowflake_database.app
  ]
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  account_role_name = snowflake_account_role.app.name
  privileges        = ["USAGE", "CREATE TABLE"]

  on_schema {
    schema_name = "\"${snowflake_database.app.name}\".\"PUBLIC\""
  }

  depends_on = [
    snowflake_account_role.app,
    snowflake_schema.app
  ]
}

resource "snowflake_grant_privileges_to_account_role" "future_tables" {
  account_role_name = snowflake_account_role.app.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.app.name}\".\"PUBLIC\""
    }
  }

  depends_on = [
    snowflake_account_role.app,
    snowflake_schema.app
  ]
}

resource "snowflake_grant_privileges_to_account_role" "all_tables" {
  account_role_name = snowflake_account_role.app.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.app.name}\".\"PUBLIC\""
    }
  }

  depends_on = [
    snowflake_account_role.app,
    snowflake_schema.app
  ]
}

resource "snowflake_grant_account_role" "app_user" {
  role_name = snowflake_account_role.app.name
  user_name = snowflake_user.app.name

  depends_on = [
    snowflake_user.app,
    snowflake_account_role.app,
    snowflake_grant_privileges_to_account_role.warehouse_usage,
    snowflake_grant_privileges_to_account_role.database_usage,
    snowflake_grant_privileges_to_account_role.schema_usage,
    snowflake_grant_privileges_to_account_role.future_tables,
    snowflake_grant_privileges_to_account_role.all_tables
  ]
}

resource "snowflake_user" "app" {
  name                 = "pe_org_air_user_${var.environment}"
  password             = var.app_user_password
  default_role         = snowflake_account_role.app.name
  default_warehouse    = snowflake_warehouse.app.name
  default_namespace    = "${snowflake_database.app.name}.PUBLIC"
  must_change_password = false

  comment = "Application service account"

  depends_on = [
    snowflake_account_role.app,
    snowflake_warehouse.app,
    snowflake_database.app,
    snowflake_schema.app
  ]
}