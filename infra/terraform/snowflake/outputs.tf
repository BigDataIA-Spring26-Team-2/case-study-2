output "warehouse_name" {
  description = "Warehouse name"
  value       = snowflake_warehouse.app.name
}

output "database_name" {
  description = "Database name"
  value       = snowflake_database.app.name
}

output "schema_name" {
  description = "Schema name"
  value       = snowflake_schema.app.name
}

output "role_name" {
  description = "Application role name"
  value       = snowflake_account_role.app.name
}

output "app_user" {
  description = "Application username"
  value       = snowflake_user.app.name
}

output "app_user_password" {
  description = "Application user password"
  value       = snowflake_user.app.password
  sensitive   = true
}

output "connection_string" {
  description = "Snowflake connection details for application"
  value = {
    account   = var.snowflake_account
    user      = snowflake_user.app.name
    warehouse = snowflake_warehouse.app.name
    database  = snowflake_database.app.name
    schema    = snowflake_schema.app.name
    role      = snowflake_account_role.app.name
  }
  sensitive = true
}